use anyhow::Result;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::config::{ExecutorConfig, TradingMode};

#[derive(Debug, Clone, Default)]
pub struct DaemonOptions {
    pub max_runtime: Option<Duration>,
}

/// Cross-task "please run a REST reconcile on the next tick" signal. The WS loops
/// set it after a successful (re)connect (spec: reconcile after WS reconnect); the
/// daemon main loop takes it on each tick. Arc-shared so the WS tasks and the main
/// loop can reach it without channel plumbing. Pure accessors so the set/take
/// semantics are unit-testable without a network round-trip.
#[derive(Debug, Clone, Default)]
pub struct ReconcileSignal {
    pending: Arc<AtomicBool>,
}

pub async fn run_live_dry_validate(cfg: ExecutorConfig) -> Result<()> {
    cfg.validate_for_dry_validate()?;
    Ok(())
}

impl ReconcileSignal {
    pub fn new() -> Self {
        Self::default()
    }
    /// WS loop calls this after a successful (re)connect.
    pub fn request(&self) {
        self.pending.store(true, Ordering::SeqCst);
    }
    /// Main loop calls this each tick; returns true iff a reconcile was requested,
    /// and clears the flag (so one reconnect triggers exactly one reconcile).
    pub fn take(&self) -> bool {
        self.pending.swap(false, Ordering::SeqCst)
    }
}

/// Cross-task "private account state is ready" flag. The private WS loop sets it
/// true after a successful (re)connect (login + subscribe acked without error) and
/// false on disconnect; the daemon's intent loop reads it when risk-checking a new
/// OPEN (spec: refuse new opening exposure when private state is not ready). Close /
/// reduce / cancel bypass it (they're risk-reducing and use REST). Arc-shared so the
/// WS task and the main loop can reach it without channel plumbing. Pure accessors so
/// the set/get semantics are unit-testable without a network round-trip.
///
/// ponytail: "ready" means the private WS is connected, authenticated, and subscribed
/// (login + subscribe acks succeeded) — NOT "we have received the first data push". An
/// idle demo account may never push an orders/positions message, so gating on first
/// data would deadlock all opens on an idle account. Connection-level readiness is the
/// correct, non-deadlocking signal.
#[derive(Debug, Clone, Default)]
pub struct PrivateStateReady {
    ready: Arc<AtomicBool>,
}

impl PrivateStateReady {
    pub fn new() -> Self {
        Self::default()
    }
    /// Private WS loop: set true after a successful (re)connect, false on disconnect.
    pub fn set(&self, value: bool) {
        self.ready.store(value, Ordering::SeqCst);
    }
    /// Intent loop: is private state ready (private WS connected + subscribed)?
    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::SeqCst)
    }
}

pub async fn run_daemon(cfg: ExecutorConfig, options: DaemonOptions) -> Result<()> {
    cfg.validate_demo_only()?;
    let conn = rusqlite::Connection::open(&cfg.db_path)?;
    // ponytail: WAL persists in the DB file header (idempotent; matches src/prodigy/db.py).
    // M6 has Python writing intents, the daemon R/W, the private WS writing, and Telegram
    // operator polling — all concurrent. WAL lets those readers/writers proceed in parallel instead of
    // serializing on a single rollback journal; busy_timeout makes them wait out SQLITE_BUSY.
    if let Err(err) = conn.pragma_update(None, "journal_mode", "wal") {
        // WAL is the assumed journal mode for concurrent Python/daemon/WS/Telegram
        // access. A failure isn't fatal (busy_timeout still serializes writers), but
        // surface it as a warning event so the operator knows the DB isn't WAL — M4
        // claims WAL-compatible behavior and shouldn't swallow this silently.
        crate::db::write_event(
            &conn,
            "warning",
            "daemon",
            &format!("WAL journal_mode setup failed: {err}"),
            "{}",
        )?;
    }
    conn.busy_timeout(Duration::from_secs(5))?;
    let instance_id = new_instance_id(&conn)?;
    crate::db::acquire_active_executor_lock(
        &conn,
        cfg.mode.as_str(),
        &instance_id,
        now_ms_i64(),
        30_000,
    )?;
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let mut active_lock_heartbeat_task =
        spawn_active_lock_heartbeat(&cfg, &instance_id, shutdown_rx.clone());
    if cfg.mode == TradingMode::Live {
        crate::db::live_startup_clean_state(&conn, cfg.mode.as_str(), &instance_id)?;
    }
    let rest = crate::bitget::BitgetRestClient::new(cfg.clone())?;

    if cfg.test_reset_demo_state {
        crate::db::write_event(
            &conn,
            "warning",
            "daemon",
            "test reset requested in daemon mode",
            "{}",
        )?;
    }

    rest.set_leverage(cfg.leverage).await.map_err(|e| {
        anyhow::anyhow!(
            "set-leverage failed (configured {}x): {e} — refusing to trade at unknown leverage",
            cfg.leverage
        )
    })?;
    // Startup reconcile BEFORE processing intents: repair any local/exchange
    // divergence left over from a prior run so the first tick starts from
    // exchange-truth. (Daemon mode does NOT call reset_demo_symbol_state here —
    // reset is the one-shot's job; daemon only logs the warning above.)
    crate::reconcile::reconcile_once(
        &conn,
        &rest,
        "daemon-startup",
        !cfg.test_reset_demo_state,
        cfg.telegram_bot_token.as_deref(),
        cfg.telegram_chat_id.as_deref(),
    )
    .await?;
    crate::db::write_event(&conn, "info", "daemon", "daemon started", "{}")?;

    let market_cache = Arc::new(tokio::sync::Mutex::new(
        crate::executor::MarketCache::default(),
    ));

    // Cross-task signal: a WS (re)connect sets it, the main loop consumes it on
    // each tick to run a REST reconcile immediately (spec: reconcile after WS
    // reconnect) instead of waiting for the periodic interval.
    let reconcile_signal = ReconcileSignal::new();
    // Cross-task signal: the private WS loop sets this true after a successful
    // (re)connect (login + subscribe sent) and false on disconnect; the intent
    // loop reads it to gate NEW opening exposure (spec: refuse new opening
    // exposure when private state is not ready). Only the PRIVATE WS owns it —
    // public WS and telegram do not.
    let private_ready = PrivateStateReady::new();

    let mut public_task = tokio::spawn(run_public_ws_loop(
        cfg.clone(),
        market_cache.clone(),
        shutdown_rx.clone(),
        reconcile_signal.clone(),
    ));
    let mut private_task = tokio::spawn(run_private_ws_loop(
        cfg.clone(),
        shutdown_rx.clone(),
        reconcile_signal.clone(),
        private_ready.clone(),
    ));
    let mut telegram_task = tokio::spawn(run_telegram_query_loop(cfg.clone(), shutdown_rx.clone()));

    // ponytail: monotonic Instant for the bounded-runtime check — immune to
    // wall-clock skew that SystemTime would inject mid-loop.
    let started = tokio::time::Instant::now();
    let mut poll = tokio::time::interval(Duration::from_millis(250));
    let mut last_reconcile_ms = now_ms_i64();

    loop {
        tokio::select! {
            _ = shutdown_signal() => {
                log_shutdown_requested(&conn)?;
                break;
            }
            _ = poll.tick() => {
                let now_ms = now_ms_i64();
                if !crate::db::heartbeat_active_executor_lock(
                    &conn,
                    cfg.mode.as_str(),
                    &instance_id,
                    now_ms,
                )? {
                    anyhow::bail!("active executor lock lost for {}", instance_id);
                }
                if options.max_runtime.is_some_and(|max| started.elapsed() >= max) {
                    crate::db::write_event(
                        &conn,
                        "info",
                        "daemon",
                        "bounded daemon runtime elapsed",
                        "{}",
                    )?;
                    break;
                }
                // Reconnect-triggered reconcile takes priority over the periodic
                // interval: a WS (re)connect set the signal, so run the SAME
                // error-isolated reconcile immediately and reset the interval
                // clock (so the next periodic pass is a full interval away).
                if reconcile_signal.take() {
                    if let Err(err) = crate::reconcile::reconcile_once(
                        &conn,
                        &rest,
                        "daemon-ws-reconnect",
                        !cfg.test_reset_demo_state,
                        cfg.telegram_bot_token.as_deref(),
                        cfg.telegram_chat_id.as_deref(),
                    )
                    .await
                    {
                        crate::db::write_event(
                            &conn,
                            "warning",
                            "reconcile",
                            &format!("reconcile failed: {err}"),
                            "{}",
                        )?;
                    }
                    last_reconcile_ms = now_ms;
                }
                if should_run_reconcile(now_ms, last_reconcile_ms, cfg.reconcile_interval_secs) {
                    // Periodic reconcile errors are LOGGED, not propagated: a single
                    // flaky REST pass must not bring the daemon down (the next tick
                    // retries). Same isolation as the intent-loop below.
                    if let Err(err) = crate::reconcile::reconcile_once(
                        &conn,
                        &rest,
                        "daemon-periodic",
                        !cfg.test_reset_demo_state,
                        cfg.telegram_bot_token.as_deref(),
                        cfg.telegram_chat_id.as_deref(),
                    )
                    .await
                    {
                        crate::db::write_event(
                            &conn,
                            "warning",
                            "reconcile",
                            &format!("reconcile failed: {err}"),
                            "{}",
                        )?;
                    }
                    last_reconcile_ms = now_ms;
                }

                let local_cache = {
                    let cache = market_cache.lock().await;
                    cache.clone()
                };
                process_daemon_queues_once(
                    &conn,
                    &cfg,
                    &instance_id,
                    &rest,
                    &local_cache,
                    private_ready.is_ready(),
                )
                .await?;
            }
        }
    }

    // Shutdown ordering: signal WS loops via the watch channel, then give them
    // a short grace window to observe it and return cooperatively (flush/close).
    // abort() is the hard fallback so the process still exits within the
    // bounded test runtime if a task is stuck mid-await on a socket read.
    let _ = shutdown_tx.send(true);
    if tokio::time::timeout(Duration::from_millis(200), &mut active_lock_heartbeat_task)
        .await
        .is_err()
    {
        active_lock_heartbeat_task.abort();
        let _ =
            tokio::time::timeout(Duration::from_millis(50), &mut active_lock_heartbeat_task).await;
    }
    let _ = tokio::time::timeout(
        Duration::from_millis(200),
        futures_util::future::join3(&mut public_task, &mut private_task, &mut telegram_task),
    )
    .await;
    public_task.abort();
    private_task.abort();
    telegram_task.abort();
    crate::db::write_event(&conn, "info", "daemon", "daemon stopped", "{}")?;
    release_lock_on_shutdown(&conn, &cfg, &instance_id)?;
    Ok(())
}

fn new_instance_id(conn: &rusqlite::Connection) -> Result<String> {
    Ok(conn.query_row("select lower(hex(randomblob(16)))", [], |row| row.get(0))?)
}

fn now_ms_i64() -> i64 {
    crate::bitget::now_ms().parse().unwrap_or(0)
}

fn release_lock_on_shutdown(
    conn: &rusqlite::Connection,
    cfg: &ExecutorConfig,
    instance_id: &str,
) -> Result<()> {
    crate::db::release_active_executor_lock(conn, cfg.mode.as_str(), instance_id)?;
    Ok(())
}

fn active_lock_heartbeat_interval() -> Duration {
    Duration::from_secs(5)
}

fn spawn_active_lock_heartbeat(
    cfg: &ExecutorConfig,
    instance_id: &str,
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    let db_path = cfg.db_path.clone();
    let mode = cfg.mode.as_str().to_string();
    let instance_id = instance_id.to_string();
    tokio::spawn(async move {
        if let Err(err) = active_lock_heartbeat_loop(
            db_path,
            mode,
            instance_id,
            shutdown,
            active_lock_heartbeat_interval(),
        )
        .await
        {
            eprintln!("active lock heartbeat stopped: {err}");
        }
    })
}

async fn active_lock_heartbeat_loop(
    db_path: std::path::PathBuf,
    mode: String,
    instance_id: String,
    shutdown: tokio::sync::watch::Receiver<bool>,
    interval: Duration,
) -> Result<()> {
    let conn = rusqlite::Connection::open(db_path)?;
    conn.busy_timeout(Duration::from_secs(5))?;
    let mut shutdown = shutdown;
    loop {
        if *shutdown.borrow() {
            return Ok(());
        }
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return Ok(());
                }
            }
            _ = tokio::time::sleep(interval) => {
                match crate::db::heartbeat_active_executor_lock(
                    &conn,
                    &mode,
                    &instance_id,
                    now_ms_i64(),
                ) {
                    Ok(true) => {}
                    Ok(false) => anyhow::bail!("active executor lock lost for {instance_id}"),
                    Err(err) => eprintln!("active lock heartbeat failed: {err}"),
                }
            }
        }
    }
}

async fn process_daemon_queues_once(
    conn: &rusqlite::Connection,
    cfg: &ExecutorConfig,
    instance_id: &str,
    rest: &crate::bitget::BitgetRestClient,
    market_cache: &crate::executor::MarketCache,
    private_state_ready: bool,
) -> Result<()> {
    let mut control_cache = market_cache.clone();
    if let Err(err) = crate::control::process_pending_control_commands_once(
        conn,
        cfg,
        instance_id,
        rest,
        &mut control_cache,
    )
    .await
    {
        let _ = crate::db::write_event(
            conn,
            "error",
            "control_loop",
            &format!("control loop failed: {err}"),
            "{}",
        );
        return Ok(());
    }

    let mut intent_cache = market_cache.clone();
    // Error isolation: a stale-market or REST failure here (common in the first
    // few hundred ms before the public WS delivers, or on a transient network
    // blip) is logged as an event and the loop continues — the daemon must not
    // crash on a loop-iteration error. The next tick retries once the WS cache is
    // fresh.
    if let Err(err) = crate::executor::process_pending_intents_once(
        conn,
        cfg,
        rest,
        &mut intent_cache,
        private_state_ready,
    )
    .await
    {
        crate::db::write_event(
            conn,
            "error",
            "intent_loop",
            &format!("intent loop failed: {err}"),
            "{}",
        )?;
    }
    Ok(())
}

/// Pure gate for the periodic reconcile cadence: true once `interval_secs`
/// have elapsed since the last reconcile. Saturating subtraction keeps it
/// safe against clock-skew-driven `now < last` orderings.
pub fn should_run_reconcile(now_ms: i64, last_reconcile_ms: i64, interval_secs: u64) -> bool {
    now_ms.saturating_sub(last_reconcile_ms) >= (interval_secs as i64) * 1000
}

/// Shared shutdown-requested event write for both the ctrl_c (SIGINT) and
/// SIGTERM arms of `run_daemon`'s main select. Same body either way so a
/// production signal (SIGTERM from `kill`/systemd/container stop) gets the
/// identical graceful-shutdown audit trail as an interactive Ctrl+C.
fn log_shutdown_requested(conn: &rusqlite::Connection) -> Result<()> {
    crate::db::write_event(conn, "info", "daemon", "shutdown requested", "{}")
}

/// Wait for SIGTERM. Production daemons receive SIGTERM from `kill`, systemd
/// and container stop; without this handler the default disposition kills the
/// process hard — no "shutdown requested" event, no task abort, no
/// "daemon stopped". Unix-only (Windows has no SIGTERM equivalent here).
#[cfg(unix)]
async fn sigterm() {
    use tokio::signal::unix::{signal, SignalKind};
    let mut s = signal(SignalKind::terminate()).expect("install SIGTERM handler");
    s.recv().await;
}

/// Neutral shutdown-signal future for the main select: resolves on either
/// ctrl_c (SIGINT) or, on Unix, SIGTERM. Wrapping both in one future lets
/// `tokio::select!` take a single branch (the macro rejects `#[cfg]` on its
/// own arms). Same shutdown path either way — SIGTERM is the signal
/// production daemons actually receive.
#[cfg(unix)]
async fn shutdown_signal() {
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = sigterm() => {}
    }
}

#[cfg(not(unix))]
async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

/// Pure glue: stamp a parsed public-WS books5 update into the shared market cache
/// with the local-received time. Wraps `MarketCache::update_at` so the WS loop and
/// its test share one call site, and the freshness window stays LOCAL-received.
pub fn apply_public_market_update(
    cache: &mut crate::executor::MarketCache,
    update: crate::types::MarketUpdate,
    local_received_at_ms: i64,
) {
    cache.update_at(update, local_received_at_ms);
}

/// Pure glue: write a parsed private-WS update (orders/fills/positions) to SQLite
/// via the existing db upsert/insert helpers. Re-applying the same update is safe
/// (upserts by PK, fills insert-or-ignore). Wraps the three writes so the WS loop
/// and its test share one call site. Apply errors are surfaced (the loop logs them
/// and never crashes the daemon).
pub fn apply_private_ws_update(
    conn: &rusqlite::Connection,
    update: crate::types::PrivateWsUpdate,
) -> Result<()> {
    for order in update.orders {
        // ponytail: the private WS is a fast cache, not the source of truth for
        // order identity. Only refresh orders we already placed locally — never
        // insert a new row (that would steal identity from the REST execution
        // path: intent_id stays NULL and system_net_base ignores a real system
        // position) and never adopt a manual/imported order before REST reconcile
        // detects it (local_oids would then contain it and reconcile would skip
        // imported/manual detection). REST reconcile remains the authority for
        // discovering new orders; the executor owns intent_id.
        if crate::db::order_exists(conn, &order.client_oid)? {
            crate::db::refresh_order_from_ws(conn, &order)?;
        }
    }
    for fill in update.fills {
        crate::db::insert_fill(conn, &fill)?;
    }
    for position in update.positions {
        // ponytail: WS positions refresh market fields only — REST reconcile owns
        // ownership classification (refresh_position_from_ws preserves
        // ownership/adopted_at/source_intent_id on conflict; upsert_position would
        // clobber them). Spec: if WS and REST disagree, REST wins.
        crate::db::refresh_position_from_ws(conn, &position)?;
    }
    // The private-WS `account` event is parsed (PrivateWsUpdate.account) but NOT
    // persisted: equity_snapshots is REST reconcile's authoritative table
    // (telegram /pnl and /risk read it). Writing WS-derived equity there would
    // let the display disagree with the last REST reconcile. REST wins.
    Ok(())
}

/// Long-running public-WS loop: connect, subscribe books5 for the configured
/// symbol, parse every incoming books5 message and refresh the shared market
/// cache with a LOCAL timestamp. Disconnects (or shutdown) reset the loop.
/// Spawned by `run_daemon`; demo-only invariant enforced at entry.
pub async fn run_public_ws_loop(
    cfg: ExecutorConfig,
    market_cache: Arc<tokio::sync::Mutex<crate::executor::MarketCache>>,
    shutdown: tokio::sync::watch::Receiver<bool>,
    reconcile_signal: ReconcileSignal,
) -> Result<()> {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;

    cfg.validate_demo_only()?;
    let mut shutdown = shutdown;
    loop {
        if *shutdown.borrow() {
            return Ok(());
        }
        {
            let mut cache = market_cache.lock().await;
            cache.invalidate();
        }
        match tokio_tungstenite::connect_async(&cfg.public_ws_url).await {
            Ok((mut socket, _)) => {
                // ponytail: a failed subscribe send is recoverable — log and
                // break to the outer reconnect loop (1s backoff) instead of
                // killing the WS task with `?`. A dead WS loop is undetectable
                // (the main loop never monitors the JoinHandle), so we keep it
                // alive via reconnect until shutdown.
                if let Err(err) = socket
                    .send(Message::Text(
                        crate::bitget::public_books5_subscribe_message(&cfg).to_string(),
                    ))
                    .await
                {
                    eprintln!("public ws subscribe send failed: {err}; reconnecting");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
                let subscribe_deadline = std::time::Instant::now() + Duration::from_secs(10);
                let mut confirmed = false;
                while std::time::Instant::now() < subscribe_deadline {
                    let msg = tokio::select! {
                        _ = shutdown.changed() => {
                            if *shutdown.borrow() {
                                return Ok(());
                            }
                            continue;
                        }
                        msg = socket.next() => match msg {
                            Some(Ok(m)) => m,
                            Some(Err(_)) | None => break,
                        },
                    };
                    let Ok(text) = msg.into_text() else {
                        continue;
                    };
                    match crate::bitget::public_ws_message_confirms_subscription(&text, &cfg) {
                        Ok(true) => {
                            if let Ok(Some(update)) = crate::bitget::parse_public_ws_message(&text)
                            {
                                let now_ms = crate::bitget::now_ms().parse::<i64>().unwrap_or(0);
                                let mut cache = market_cache.lock().await;
                                apply_public_market_update(&mut cache, update, now_ms);
                            }
                            confirmed = true;
                            break;
                        }
                        Ok(false) => {}
                        Err(err) => {
                            eprintln!("public ws subscribe failed: {err}; reconnecting");
                            break;
                        }
                    }
                }
                if !confirmed {
                    let mut cache = market_cache.lock().await;
                    cache.invalidate();
                    eprintln!("public ws subscribe ack timed out; reconnecting");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
                // (Re)connect succeeded and subscription was ACKED (or first
                // valid books update arrived): request a REST reconcile on the
                // next main-loop tick (spec: reconcile after WS reconnect).
                reconcile_signal.request();
                'inner: loop {
                    tokio::select! {
                        _ = shutdown.changed() => {
                            if *shutdown.borrow() {
                                return Ok(());
                            }
                        }
                        msg = socket.next() => {
                            let Some(msg) = msg else { break 'inner; };
                            let Ok(msg) = msg else { break 'inner; };
                            let Ok(text) = msg.into_text() else { continue; };
                            match crate::bitget::parse_public_ws_message(&text) {
                                Ok(Some(update)) => {
                                    let now_ms = crate::bitget::now_ms().parse::<i64>().unwrap_or(0);
                                    let mut cache = market_cache.lock().await;
                                    apply_public_market_update(&mut cache, update, now_ms);
                                }
                                Ok(None) => {}
                                Err(err) => {
                                    eprintln!("public ws parse error: {err}");
                                }
                            }
                        }
                    }
                }
                {
                    let mut cache = market_cache.lock().await;
                    cache.invalidate();
                }
                eprintln!("public ws socket closed; reconnecting");
            }
            Err(err) => {
                let mut cache = market_cache.lock().await;
                cache.invalidate();
                eprintln!("public ws disconnected: {err}");
            }
        }
        // ponytail: fixed 1s reconnect backoff on EVERY reconnect path
        // (connect failure AND mid-stream socket close/error); exponential
        // backoff if disconnects become frequent (a flapping link would hammer
        // the endpoint and risk a temporary IP block). Shutdown exits return
        // above before reaching here, so they aren't delayed.
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

#[derive(Debug, Clone)]
struct TelegramUpdateParts {
    update_id: i64,
    from_user_id: String,
    reply_chat_id: String,
    text: Option<String>,
    callback_query_id: Option<String>,
    callback_data: Option<String>,
}

fn telegram_update_parts(update: &serde_json::Value) -> Option<TelegramUpdateParts> {
    let update_id = update.get("update_id")?.as_i64()?;
    if let Some(message) = update.get("message") {
        return Some(TelegramUpdateParts {
            update_id,
            from_user_id: message.get("from")?.get("id")?.as_i64()?.to_string(),
            reply_chat_id: message.get("chat")?.get("id")?.as_i64()?.to_string(),
            text: Some(message.get("text")?.as_str()?.to_string()),
            callback_query_id: None,
            callback_data: None,
        });
    }
    let callback = update.get("callback_query")?;
    Some(TelegramUpdateParts {
        update_id,
        from_user_id: callback.get("from")?.get("id")?.as_i64()?.to_string(),
        reply_chat_id: callback
            .get("message")?
            .get("chat")?
            .get("id")?
            .as_i64()?
            .to_string(),
        text: None,
        callback_query_id: Some(callback.get("id")?.as_str()?.to_string()),
        callback_data: Some(callback.get("data")?.as_str()?.to_string()),
    })
}

fn telegram_http_client(timeout: Duration) -> Result<reqwest::Client> {
    Ok(reqwest::Client::builder().timeout(timeout).build()?)
}

fn telegram_send_message_form(
    chat_id: &str,
    reply: &crate::telegram_query::TelegramReply,
) -> Vec<(String, String)> {
    let mut form = vec![
        ("chat_id".to_string(), chat_id.to_string()),
        ("text".to_string(), reply.text.clone()),
    ];
    if let Some(parse_mode) = reply.parse_mode {
        form.push(("parse_mode".to_string(), parse_mode.to_string()));
    }
    if let Some(markup) = &reply.reply_markup {
        form.push(("reply_markup".to_string(), markup.to_string()));
    }
    form
}

fn telegram_operator_http_timeout() -> Duration {
    Duration::from_secs(15)
}

fn telegram_command_registration_timeout() -> Duration {
    Duration::from_secs(2)
}

/// Optional Telegram operator polling loop (M6). Runs ONLY when
/// `telegram_bot_token` is configured and `telegram_allowed_user_ids` is
/// non-empty — otherwise it returns immediately, since Telegram is not an
/// execution dependency. It long-polls `getUpdates`, authorizes by
/// `message.from.id`, replies to `message.chat.id`, and handles recognized
/// commands via `telegram_query::operator_response`.
///
/// Error isolation: EVERY network/parse/SQLite error here is logged and the
/// loop continues — a flaky getUpdates or a transient DB lock must NEVER crash
/// the daemon. Uses the same hoisted-shutdown `select!` pattern as the WS
/// loops so the 1s throttle never blocks a shutdown. Open its own SQLite
/// connection per update batch (rusqlite Connection is not Sync).
pub async fn run_telegram_query_loop(
    cfg: ExecutorConfig,
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> Result<()> {
    let Some(token) = cfg.telegram_bot_token.clone() else {
        return Ok(());
    };
    if cfg.telegram_allowed_user_ids.is_empty() {
        return Ok(());
    }
    let client = telegram_http_client(telegram_operator_http_timeout())?;
    let set_commands_url = format!("https://api.telegram.org/bot{token}/setMyCommands");
    let _ = client
        .post(set_commands_url)
        .timeout(telegram_command_registration_timeout())
        .json(&crate::telegram_query::bot_commands_payload())
        .send()
        .await;
    let mut offset: i64 = 0;
    let mut shutdown = shutdown;
    loop {
        if *shutdown.borrow() {
            return Ok(());
        }
        let get_url = format!("https://api.telegram.org/bot{token}/getUpdates");
        // ponytail: a failed long-poll (network blip, 5xx) is logged and we
        // back off via the shutdown-aware sleep below — never propagated, the
        // daemon must not die on a Telegram outage.
        let response = client
            .get(&get_url)
            .query(&[
                ("timeout", "10".to_string()),
                ("offset", offset.to_string()),
            ])
            .send()
            .await;
        if let Ok(resp) = response {
            if let Ok(value) = resp.json::<serde_json::Value>().await {
                if let Some(updates) = value.get("result").and_then(serde_json::Value::as_array) {
                    for update in updates {
                        if let Some(id) =
                            update.get("update_id").and_then(serde_json::Value::as_i64)
                        {
                            offset = id + 1;
                        }
                        let Some(parts) = telegram_update_parts(update) else {
                            continue;
                        };
                        offset = parts.update_id + 1;
                        if let Some(callback_query_id) = &parts.callback_query_id {
                            let answer_url =
                                format!("https://api.telegram.org/bot{token}/answerCallbackQuery");
                            let _ = client
                                .post(answer_url)
                                .form(&[("callback_query_id", callback_query_id.as_str())])
                                .send()
                                .await;
                        }
                        match rusqlite::Connection::open(&cfg.db_path) {
                            Ok(conn) => {
                                if let Err(err) = conn
                                    .busy_timeout(std::time::Duration::from_secs(5))
                                    .map_err(anyhow::Error::from)
                                {
                                    eprintln!("telegram sqlite busy_timeout error: {err}");
                                    continue;
                                }
                                let now_ms = crate::bitget::now_ms().parse::<i64>().unwrap_or(0);
                                let reply = match if let Some(callback_data) =
                                    parts.callback_data.as_deref()
                                {
                                    crate::telegram_query::operator_callback_reply(
                                        &conn,
                                        callback_data,
                                        &parts.from_user_id,
                                        &cfg.telegram_allowed_user_ids,
                                        now_ms,
                                    )
                                } else if let Some(text) = parts.text.as_deref() {
                                    crate::telegram_query::operator_reply(
                                        &conn,
                                        text,
                                        &parts.from_user_id,
                                        &cfg.telegram_allowed_user_ids,
                                        now_ms,
                                    )
                                } else {
                                    Ok(None)
                                } {
                                    Ok(reply) => reply,
                                    Err(err) => {
                                        eprintln!("telegram query error: {err}");
                                        continue;
                                    }
                                };
                                if let Some(reply) = reply {
                                    let send_url =
                                        format!("https://api.telegram.org/bot{token}/sendMessage");
                                    let form =
                                        telegram_send_message_form(&parts.reply_chat_id, &reply);
                                    // ponytail: best-effort send — a failed
                                    // sendMessage is dropped on the floor; the
                                    // operator can re-issue the command.
                                    let _ = client.post(send_url).form(&form).send().await;
                                }
                            }
                            Err(err) => eprintln!("telegram sqlite open error: {err}"),
                        }
                    }
                }
            }
        }
        // ponytail: hoisted shutdown-aware throttle — same pattern as the WS
        // loops, so a shutdown observed mid-throttle returns promptly instead
        // of sleeping the full 1s (the Task 4 backoff-bug fix applied here too).
        tokio::select! {
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    return Ok(());
                }
            }
            _ = tokio::time::sleep(Duration::from_secs(1)) => {}
        }
    }
}

/// Record a `websocket_auth_failed` event in SQLite and fire the demo Telegram
/// notification for it (`notify::should_send_telegram` gates demo delivery).
/// Best-effort throughout: a sqlite/telegram failure only logs to stderr — the
/// private-WS loop must not die on a logging path (a dead WS loop is
/// undetectable). Shared by the PRE-ready login-failure path and the POST-ready
/// mid-session auth-error path so the operator sees the failure either way.
/// ponytail: extracted so both auth-failure paths emit the identical event +
/// notification instead of drifting apart.
async fn emit_websocket_auth_failed(cfg: &ExecutorConfig, detail: &str) {
    match rusqlite::Connection::open(&cfg.db_path) {
        Ok(conn) => {
            let _ = conn.busy_timeout(Duration::from_secs(5));
            if let Err(err) = crate::db::write_event(
                &conn,
                "warning",
                "private_ws",
                &format!("websocket auth failed: {detail}"),
                "{}",
            ) {
                eprintln!("websocket_auth_failed event write error: {err}");
            }
        }
        Err(err) => eprintln!("websocket_auth_failed event sqlite open error: {err}"),
    }
    crate::notify::send_telegram(
        cfg.telegram_bot_token.as_deref(),
        cfg.telegram_chat_id.as_deref(),
        "websocket_auth_failed",
        &format!("private websocket login failed: {detail}"),
    )
    .await
    .ok();
}

/// Long-running private-WS loop: connect, send the per-connection login (signed
/// `GET /user/verify`), then parse every incoming message and apply orders/fills/
/// positions to SQLite via `apply_private_ws_update`. A parse error or a SQLite
/// apply error is logged and the loop continues — the daemon must not crash on a
/// bad update. Disconnects (or shutdown) reset the loop. Spawned by `run_daemon`
/// (Task 7 wires it); demo-only invariant enforced at entry.
pub async fn run_private_ws_loop(
    cfg: ExecutorConfig,
    shutdown: tokio::sync::watch::Receiver<bool>,
    reconcile_signal: ReconcileSignal,
    private_ready: PrivateStateReady,
) -> Result<()> {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;

    cfg.validate_demo_only()?;
    let mut shutdown = shutdown;
    loop {
        if *shutdown.borrow() {
            return Ok(());
        }
        // ponytail: flip not-ready at the top of every outer iteration — a
        // disconnect that broke the inner read-loop lands here on its next
        // iteration, so readiness reflects the disconnect BEFORE we attempt to
        // reconnect. set(true) below only fires after the subscribe send
        // succeeds, so a connect/login/subscribe failure stays not-ready.
        private_ready.set(false);
        match tokio_tungstenite::connect_async(&cfg.private_ws_url).await {
            Ok((mut socket, _)) => {
                let timestamp = crate::bitget::now_seconds();
                // ponytail: failed login/subscribe sends are recoverable — log
                // and break to the outer reconnect loop (1s backoff) instead of
                // killing the WS task with `?`. A dead WS loop is undetectable,
                // so we keep it alive via reconnect until shutdown.
                if let Err(err) = socket
                    .send(Message::Text(
                        crate::bitget::private_login_message(&cfg, &timestamp).to_string(),
                    ))
                    .await
                {
                    eprintln!("private ws login send failed: {err}; reconnecting");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
                // Wait for the login ack BEFORE subscribing or marking ready. A
                // failed login (bad signature / expired timestamp / banned key)
                // arrives as {"event":"error",...} or a non-zero login code; the
                // parser surfaces it as auth_error. Only {"event":"login","code":"0"}
                // makes private state ready. Spec: emit websocket_auth_failed on
                // auth failure and keep new opens gated out until a successful
                // reconnect re-acks. Bounded by a 10s ack deadline so a dead
                // socket can't hang the loop.
                let ack_deadline = std::time::Instant::now() + Duration::from_secs(10);
                let mut acked = false;
                let mut auth_failure: Option<String> = None;
                while std::time::Instant::now() < ack_deadline {
                    let msg = tokio::select! {
                        _ = shutdown.changed() => {
                            if *shutdown.borrow() {
                                return Ok(());
                            }
                            continue;
                        }
                        msg = socket.next() => match msg {
                            Some(Ok(m)) => m,
                            Some(Err(_)) | None => break,
                        },
                    };
                    let Ok(text) = msg.into_text() else {
                        continue;
                    };
                    match crate::bitget::parse_private_ws_message(&text, &cfg) {
                        Ok(u) if u.auth_error.is_some() => {
                            auth_failure = u.auth_error;
                            break;
                        }
                        Ok(u) if u.login_ack => {
                            acked = true;
                            break;
                        }
                        Ok(_) => continue,
                        Err(err) => {
                            eprintln!("private ws parse error pre-ack: {err}");
                            continue;
                        }
                    }
                }
                if let Some(detail) = auth_failure {
                    // Auth failed: surface it (event + demo Telegram) and stay
                    // not-ready. A corrected key/timestamp re-acks on the next
                    // connect. ponytail: sleep BEFORE continue so a bad key /
                    // consistently-rejected login can't tight-loop reconnect and
                    // flood the events table / Telegram — same 1s backoff every
                    // other reconnect path takes below (the bare `continue` here
                    // previously skipped the outer-loop sleep at the bottom).
                    emit_websocket_auth_failed(&cfg, &detail).await;
                    eprintln!("private ws login rejected: {detail}; reconnecting");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
                if !acked {
                    eprintln!("private ws login ack timed out; reconnecting");
                    // ponytail: same 1s backoff — an ack that never comes (dead
                    // socket, network blackhole) must not tight-loop the 10s ack
                    // wait + immediate reconnect.
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
                // Subscribe to orders/positions/account now that login is acked.
                if let Err(err) = socket
                    .send(Message::Text(
                        crate::bitget::private_subscribe_message(&cfg).to_string(),
                    ))
                    .await
                {
                    eprintln!("private ws subscribe send failed: {err}; reconnecting");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
                let subscribe_deadline = std::time::Instant::now() + Duration::from_secs(10);
                let mut subscribe_acks = crate::bitget::PrivateSubscribeAcks::default();
                let mut subscribe_failure: Option<String> = None;
                while std::time::Instant::now() < subscribe_deadline && !subscribe_acks.ready() {
                    let msg = tokio::select! {
                        _ = shutdown.changed() => {
                            if *shutdown.borrow() {
                                return Ok(());
                            }
                            continue;
                        }
                        msg = socket.next() => match msg {
                            Some(Ok(m)) => m,
                            Some(Err(_)) | None => break,
                        },
                    };
                    let Ok(text) = msg.into_text() else {
                        continue;
                    };
                    match crate::bitget::parse_private_ws_message(&text, &cfg) {
                        Ok(update) if update.auth_error.is_some() => {
                            subscribe_failure = update.auth_error;
                            break;
                        }
                        Ok(update) => subscribe_acks.record(&update),
                        Err(err) => {
                            eprintln!("private ws parse error pre-subscribe-ack: {err}");
                        }
                    }
                }
                if let Some(detail) = subscribe_failure {
                    emit_websocket_auth_failed(&cfg, &detail).await;
                    eprintln!("private ws subscribe rejected: {detail}; reconnecting");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
                if !subscribe_acks.ready() {
                    eprintln!("private ws subscribe ack timed out; reconnecting");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
                // (Re)connect succeeded, login was ACKED, and subscribe was ACKED:
                // request a REST reconcile on the next main-loop tick (spec:
                // reconcile after WS reconnect) so any orders/fills/positions gap
                // is repaired, and mark private state READY so new opens are no
                // longer gated out (spec: refuse new opening exposure when
                // private state is not ready). Readiness is connection-level: we
                // wait for subscribe ack but NOT for a first data push — an idle
                // demo account may never push one, and gating on it would
                // deadlock all opens on an idle account.
                private_ready.set(true);
                reconcile_signal.request();
                loop {
                    tokio::select! {
                        _ = shutdown.changed() => {
                            if *shutdown.borrow() {
                                return Ok(());
                            }
                        }
                        msg = socket.next() => {
                            let Some(msg) = msg else { break; };
                            let Ok(msg) = msg else { break; };
                            let Ok(text) = msg.into_text() else { continue; };
                            let update = match crate::bitget::parse_private_ws_message(&text, &cfg) {
                                Ok(update) => update,
                                Err(err) => {
                                    eprintln!("private ws parse error: {err}");
                                    continue;
                                }
                            };
                            // A post-ready {"event":"error",...} (subscribe args
                            // rejected, key revoked mid-session, etc.) sets
                            // auth_error but carries no orders/fills/positions/
                            // account. Without this check the empty-skip below
                            // would drop it and private state would stay READY
                            // while the private channel is actually broken — new
                            // opens would proceed on a dead feed. Surface it,
                            // gate opens back out, and break to the outer
                            // reconnect loop (1s backoff + fresh login).
                            if let Some(detail) = &update.auth_error {
                                private_ready.set(false);
                                emit_websocket_auth_failed(&cfg, detail).await;
                                eprintln!(
                                    "private ws session error after ready: {detail}; reconnecting"
                                );
                                break;
                            }
                            if update.orders.is_empty()
                                && update.fills.is_empty()
                                && update.positions.is_empty()
                                && update.account.is_none()
                            {
                                continue;
                            }
                            match rusqlite::Connection::open(&cfg.db_path) {
                                Ok(conn) => {
                                    // ponytail: busy_timeout failure is benign
                                    // (sqlite3_busy_timeout essentially never errors);
                                    // skip this batch instead of killing the WS task.
                                    if let Err(err) = conn.busy_timeout(std::time::Duration::from_secs(5)) {
                                        eprintln!("private ws sqlite busy_timeout error: {err}");
                                        continue;
                                    }
                                    if let Err(err) = apply_private_ws_update(&conn, update) {
                                        eprintln!("private ws sqlite apply error: {err}");
                                    }
                                }
                                Err(err) => eprintln!("private ws sqlite open error: {err}"),
                            }
                        }
                    }
                }
                // The socket died (close/err broke the read loop) — flip readiness
                // OFF immediately so a 250ms intent-loop tick in the ~1s reconnect
                // window can't slip a new OPEN through the gate on stale readiness.
                // (set(false) at the top of the next outer iteration would be ~1s late.)
                private_ready.set(false);
                eprintln!("private ws socket closed; reconnecting");
            }
            Err(err) => {
                // Connect itself failed: readiness is already false (set at the top
                // of this outer iteration), but set again for clarity/symmetry.
                private_ready.set(false);
                eprintln!("private ws disconnected: {err}");
            }
        }
        // ponytail: fixed 1s reconnect backoff on EVERY reconnect path — hoisted
        // to the outer loop so BOTH connect failure and mid-stream close back off
        // (the Task 4 bug only slept the connect-err arm, letting a flapping
        // mid-stream disconnect hammer the endpoint). Add exponential backoff if
        // disconnects become frequent. Shutdown exits return above before here.
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ExecutorConfig, TradingMode};

    fn test_conn() -> rusqlite::Connection {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute_batch(include_str!("../../../schema/001_initial.sql"))
            .unwrap();
        conn.execute_batch(include_str!("../../../schema/002_execution.sql"))
            .unwrap();
        conn
    }

    fn temp_db_path(name: &str) -> std::path::PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "prodigy-test-{name}-{}-{nanos}.sqlite",
            std::process::id()
        ))
    }

    fn insert_pending_open_intent(conn: &rusqlite::Connection, intent_id: &str) {
        conn.execute(
            "insert into trade_intents (
               intent_id, created_at, symbol, side, action, target_notional,
               max_order_notional, status, source
             ) values (?1, '2026-07-01T00:00:00Z', 'ETH/USDT:USDT',
                       'long', 'open', 100, 100, 'pending', 'test')",
            rusqlite::params![intent_id],
        )
        .unwrap();
    }

    fn intent_status_and_error(
        conn: &rusqlite::Connection,
        intent_id: &str,
    ) -> (String, Option<String>) {
        conn.query_row(
            "select status, error from trade_intents where intent_id = ?1",
            rusqlite::params![intent_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap()
    }

    #[test]
    fn daemon_options_default_runs_forever() {
        let options = DaemonOptions::default();

        assert!(options.max_runtime.is_none());
    }

    #[test]
    fn live_clean_state_gate_appears_before_private_exchange_calls() {
        let source = include_str!("daemon.rs");
        let clean_state = source
            .find("live_startup_clean_state")
            .expect("clean-state gate exists");
        let rest_new = source
            .find("BitgetRestClient::new")
            .expect("REST client exists");
        let set_leverage = source.find("set_leverage").expect("set leverage exists");

        assert!(
            clean_state < rest_new,
            "clean-state gate must precede REST client creation"
        );
        assert!(
            clean_state < set_leverage,
            "clean-state gate must precede set-leverage"
        );
    }

    #[test]
    fn daemon_release_helper_clears_only_matching_active_lock() {
        let conn = test_conn();
        let cfg = ExecutorConfig::demo_for_tests();
        crate::db::acquire_active_executor_lock(&conn, "demo", "inst-demo", 1_000, 30_000).unwrap();

        release_lock_on_shutdown(&conn, &cfg, "other").unwrap();
        assert_eq!(
            crate::db::get_executor_state(&conn, crate::db::ACTIVE_INSTANCE_ID_KEY)
                .unwrap()
                .as_deref(),
            Some("inst-demo")
        );

        release_lock_on_shutdown(&conn, &cfg, "inst-demo").unwrap();
        assert_eq!(
            crate::db::get_executor_state(&conn, crate::db::ACTIVE_INSTANCE_ID_KEY).unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn active_lock_background_heartbeat_refreshes_during_long_tick() {
        let db_path = temp_db_path("active-heartbeat");
        {
            let conn = rusqlite::Connection::open(&db_path).unwrap();
            conn.execute_batch(include_str!("../../../schema/001_initial.sql"))
                .unwrap();
            conn.execute_batch(include_str!("../../../schema/002_execution.sql"))
                .unwrap();
            crate::db::acquire_active_executor_lock(&conn, "demo", "inst-demo", 1_000, 30_000)
                .unwrap();
        }

        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let mut task = tokio::spawn(active_lock_heartbeat_loop(
            db_path.clone(),
            "demo".to_string(),
            "inst-demo".to_string(),
            shutdown_rx,
            Duration::from_millis(10),
        ));

        tokio::time::sleep(Duration::from_millis(50)).await;
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        assert_ne!(
            crate::db::get_executor_state(&conn, crate::db::ACTIVE_HEARTBEAT_AT_KEY)
                .unwrap()
                .as_deref(),
            Some("1000")
        );

        let _ = shutdown_tx.send(true);
        tokio::time::timeout(Duration::from_millis(100), &mut task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let _ = std::fs::remove_file(&db_path);
    }

    #[test]
    fn active_lock_heartbeat_starts_before_rest_client() {
        let source = include_str!("daemon.rs");
        let acquire = source
            .find("acquire_active_executor_lock")
            .expect("active lock acquisition exists");
        let heartbeat_spawn = source
            .find("spawn_active_lock_heartbeat(&cfg, &instance_id")
            .expect("active lock heartbeat task is spawned");
        let rest_new = source
            .find("BitgetRestClient::new")
            .expect("REST client exists");

        assert!(acquire < heartbeat_spawn);
        assert!(heartbeat_spawn < rest_new);
    }

    #[test]
    fn should_run_reconcile_when_interval_elapsed() {
        assert!(should_run_reconcile(10_000, 0, 10));
        assert!(!should_run_reconcile(9_999, 0, 10));
    }

    #[test]
    fn telegram_update_parts_use_from_id_for_auth_and_chat_id_for_reply() {
        let update = serde_json::json!({
            "update_id": 42,
            "message": {
                "from": { "id": 123 },
                "chat": { "id": 999 },
                "text": "/status"
            }
        });

        let parts = telegram_update_parts(&update).unwrap();

        assert_eq!(parts.update_id, 42);
        assert_eq!(parts.from_user_id, "123");
        assert_eq!(parts.reply_chat_id, "999");
        assert_eq!(parts.text.as_deref(), Some("/status"));
        assert!(parts.callback_query_id.is_none());
        assert!(parts.callback_data.is_none());
    }

    #[test]
    fn telegram_update_parts_parse_callback_query_for_auth_reply_and_answer() {
        let update = serde_json::json!({
            "update_id": 43,
            "callback_query": {
                "id": "callback-1",
                "from": { "id": 123 },
                "data": "tgux:status",
                "message": {
                    "chat": { "id": 456 },
                    "message_id": 99
                }
            }
        });

        let parts = telegram_update_parts(&update).unwrap();

        assert_eq!(parts.update_id, 43);
        assert_eq!(parts.from_user_id, "123");
        assert_eq!(parts.reply_chat_id, "456");
        assert_eq!(parts.callback_query_id.as_deref(), Some("callback-1"));
        assert_eq!(parts.callback_data.as_deref(), Some("tgux:status"));
        assert!(parts.text.is_none());
    }

    #[test]
    fn telegram_send_message_form_includes_html_and_reply_markup() {
        let reply = crate::telegram_query::TelegramReply {
            text: "<b>Status</b>".to_string(),
            parse_mode: Some("HTML"),
            reply_markup: Some(serde_json::json!({"inline_keyboard": []})),
        };

        let form = telegram_send_message_form("456", &reply);

        assert!(form.contains(&("chat_id".to_string(), "456".to_string())));
        assert!(form.contains(&("text".to_string(), "<b>Status</b>".to_string())));
        assert!(form.contains(&("parse_mode".to_string(), "HTML".to_string())));
        assert!(form
            .iter()
            .any(|(k, v)| k == "reply_markup" && v.contains("inline_keyboard")));
    }

    #[tokio::test]
    async fn telegram_operator_http_client_times_out_hung_requests() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            if let Ok((_stream, _)) = listener.accept() {
                std::thread::sleep(std::time::Duration::from_secs(2));
            }
        });

        let client = telegram_http_client(std::time::Duration::from_millis(50)).unwrap();
        let err = client
            .get(format!("http://{addr}/hang"))
            .send()
            .await
            .unwrap_err();

        assert!(err.is_timeout());
    }

    #[test]
    fn telegram_operator_http_timeout_covers_long_poll() {
        assert_eq!(
            telegram_operator_http_timeout(),
            std::time::Duration::from_secs(15)
        );
        assert!(telegram_operator_http_timeout() > std::time::Duration::from_secs(10));
    }

    #[test]
    fn telegram_command_registration_timeout_is_shorter_than_long_poll() {
        assert!(telegram_command_registration_timeout() < telegram_operator_http_timeout());
        assert!(telegram_command_registration_timeout() <= std::time::Duration::from_secs(2));
    }

    #[test]
    fn reconcile_signal_request_then_take() {
        let sig = ReconcileSignal::new();
        assert!(!sig.take(), "fresh signal should not request reconcile");
        sig.request();
        assert!(sig.take(), "after request, take is true");
        assert!(
            !sig.take(),
            "take clears the flag (one reconnect → one reconcile)"
        );
    }

    #[test]
    fn reconcile_signal_shared_between_clones() {
        let sig = ReconcileSignal::new();
        let clone = sig.clone();
        clone.request(); // WS task sets via its clone
        assert!(
            sig.take(),
            "main loop sees the request through its own handle"
        );
    }

    #[test]
    fn private_state_ready_default_false_then_set() {
        let r = PrivateStateReady::new();
        assert!(!r.is_ready(), "fresh signal should be not-ready");
        r.set(true);
        assert!(r.is_ready());
        r.set(false);
        assert!(!r.is_ready());
    }

    #[test]
    fn private_state_ready_shared_between_clones() {
        let r = PrivateStateReady::new();
        let clone = r.clone();
        clone.set(true); // private WS task sets via its clone
        assert!(
            r.is_ready(),
            "main loop sees the readiness through its own handle"
        );
    }

    #[test]
    fn private_ready_set_only_after_subscribe_ack() {
        let source = include_str!("daemon.rs");
        let subscribe_wait = source
            .find("PrivateSubscribeAcks")
            .expect("private WS loop should track subscribe acks");
        let ready_set = source
            .find("private_ready.set(true)")
            .expect("private WS loop sets readiness");

        assert!(
            subscribe_wait < ready_set,
            "private readiness must be set after subscribe ack handling, not after send"
        );
    }

    #[test]
    fn daemon_allows_bounded_runtime_for_tests() {
        let options = DaemonOptions {
            max_runtime: Some(std::time::Duration::from_millis(5)),
        };

        assert_eq!(
            options.max_runtime.unwrap(),
            std::time::Duration::from_millis(5)
        );
    }

    #[tokio::test]
    async fn emit_websocket_auth_failed_writes_event_to_db() {
        // Regression for Fix 1/2: the shared helper both auth-failure paths use
        // must persist a websocket_auth_failed event so the operator sees a
        // broken private channel. It opens its own connection from cfg.db_path,
        // so point it at a temp file initialized with the schema.
        let dir = std::env::temp_dir();
        let db_path = dir.join(format!(
            "prodigy-test-emit-auth-{}.sqlite",
            std::process::id()
        ));
        {
            let conn = rusqlite::Connection::open(&db_path).unwrap();
            conn.execute_batch(include_str!("../../../schema/001_initial.sql"))
                .unwrap();
            conn.execute_batch(include_str!("../../../schema/002_execution.sql"))
                .unwrap();
        }
        let cfg = ExecutorConfig {
            db_path: db_path.clone(),
            // telegram None → send_telegram is a no-op (helper stays best-effort).
            telegram_bot_token: None,
            telegram_chat_id: None,
            ..ExecutorConfig::demo_for_tests()
        };

        emit_websocket_auth_failed(&cfg, "login code 30001: sign invalid").await;

        let conn = rusqlite::Connection::open(&db_path).unwrap();
        let row: (String, String, String) = conn
            .query_row(
                "select severity, component, message from events \
                 where message like '%websocket auth failed%' order by created_at desc limit 1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(row.0, "warning");
        assert_eq!(row.1, "private_ws");
        assert!(row.2.contains("sign invalid"));

        let _ = std::fs::remove_file(&db_path);
    }

    #[tokio::test]
    async fn daemon_rejects_non_demo_mode_before_opening_db() {
        let cfg = ExecutorConfig {
            mode: TradingMode::Live,
            ..ExecutorConfig::demo_for_tests()
        };

        let err = run_daemon(
            cfg,
            DaemonOptions {
                max_runtime: Some(std::time::Duration::from_millis(1)),
            },
        )
        .await
        .unwrap_err();

        assert!(err.to_string().contains("demo"));
    }

    #[tokio::test]
    async fn daemon_tick_processes_stop_before_pending_open() {
        let conn = test_conn();
        conn.execute(
            "insert into control_commands (
              command_id, created_at, command, status, requested_by, mode, instance_id
            ) values ('cmd-stop', '2026-07-01T00:00:00Z', 'stop', 'pending', '123', 'demo', 'inst-demo')",
            [],
        )
        .unwrap();
        insert_pending_open_intent(&conn, "i-open");
        let cfg = ExecutorConfig {
            rest_base_url: "http://127.0.0.1:9".to_string(),
            ..ExecutorConfig::demo_for_tests()
        };
        let rest = crate::bitget::BitgetRestClient::new(cfg.clone()).unwrap();

        process_daemon_queues_once(
            &conn,
            &cfg,
            "inst-demo",
            &rest,
            &crate::executor::MarketCache::default(),
            false,
        )
        .await
        .unwrap();

        assert_eq!(
            crate::db::get_executor_state(&conn, crate::control::OPERATOR_STOP_KEY)
                .unwrap()
                .as_deref(),
            Some("active")
        );
        let command_status: String = conn
            .query_row(
                "select status from control_commands where command_id = 'cmd-stop'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(command_status, "executed");
        assert_eq!(
            intent_status_and_error(&conn, "i-open"),
            (
                "failed".to_string(),
                Some("operator stop active".to_string())
            )
        );
        let deferred_events: i64 = conn
            .query_row(
                "select count(*) from events where message like 'deferred open%'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(deferred_events, 0);
    }

    #[tokio::test]
    async fn daemon_tick_logs_control_error_and_skips_intents() {
        let conn = test_conn();
        insert_pending_open_intent(&conn, "i-open");
        let demo_cfg = ExecutorConfig {
            rest_base_url: "http://127.0.0.1:9".to_string(),
            ..ExecutorConfig::demo_for_tests()
        };
        let live_cfg = ExecutorConfig {
            mode: TradingMode::Live,
            ..demo_cfg.clone()
        };
        let rest = crate::bitget::BitgetRestClient::new(demo_cfg).unwrap();

        process_daemon_queues_once(
            &conn,
            &live_cfg,
            "inst-demo",
            &rest,
            &crate::executor::MarketCache::default(),
            false,
        )
        .await
        .unwrap();

        let control_errors: i64 = conn
            .query_row(
                "select count(*) from events
                 where component = 'control_loop'
                   and severity = 'error'
                   and message like 'control loop failed:%live profile must use Bitget live websocket URLs%'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(control_errors, 1);
        let intent_loop_events: i64 = conn
            .query_row(
                "select count(*) from events
                 where component = 'intent_loop'
                   and message like 'deferred open i-open:%'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(intent_loop_events, 0);
        assert_eq!(intent_status_and_error(&conn, "i-open").0, "pending");
    }

    #[test]
    fn public_ws_update_refreshes_market_cache() {
        let mut cache = crate::executor::MarketCache::default();

        apply_public_market_update(
            &mut cache,
            crate::types::MarketUpdate {
                symbol: "ETHUSDT".to_string(),
                best_bid: 100.0,
                best_ask: 101.0,
                exchange_ts_ms: 10,
            },
            1_000,
        );

        assert!(cache.latest_fresh(1_500, 3).is_some());
    }

    #[test]
    fn private_ws_update_upserts_orders_and_positions() {
        let conn = test_conn();
        // Fix-A: the private WS only refreshes orders we already placed locally —
        // it no longer INSERTS orders. So pre-insert the order (as the executor
        // would) with an intent_id, then assert the WS push refreshes it (status
        // flips) and keeps the count at 1. The position assertion (count 1) still
        // holds: refresh_position_from_ws inserts on first write.
        // FK: orders.intent_id references trade_intents — seed the parent intent.
        conn.execute(
            "insert into trade_intents (intent_id, created_at, symbol, side, action,
               target_notional, max_order_notional, status, source)
             values ('intent-1','2026-07-01T00:00:00Z','ETHUSDT','long','open',100,100,'executed','t')",
            [],
        )
        .unwrap();
        crate::db::upsert_order(
            &conn,
            &crate::types::OrderRecord {
                order_id: "local-order-1".to_string(),
                exchange_order_id: None,
                client_oid: "client-1".to_string(),
                intent_id: Some("intent-1".to_string()),
                symbol: "ETHUSDT".to_string(),
                side: "buy".to_string(),
                action: "open".to_string(),
                order_type: "market".to_string(),
                status: "submitted".to_string(),
                price: Some(100.0),
                size: 0.1,
                filled_size: 0.0,
                attempt: 1,
                raw_json: "{}".to_string(),
                last_error: None,
            },
        )
        .unwrap();

        let update = crate::types::PrivateWsUpdate {
            login_ack: false,
            auth_error: None,
            orders: vec![crate::types::OrderRecord {
                exchange_order_id: Some("ex-1".to_string()),
                status: "filled".to_string(),
                filled_size: 0.1,
                intent_id: None,
                ..crate::types::OrderRecord {
                    order_id: "local-order-1".to_string(),
                    exchange_order_id: None,
                    client_oid: "client-1".to_string(),
                    intent_id: None,
                    symbol: "ETHUSDT".to_string(),
                    side: "buy".to_string(),
                    action: "open".to_string(),
                    order_type: "market".to_string(),
                    status: "filled".to_string(),
                    price: Some(100.0),
                    size: 0.1,
                    filled_size: 0.1,
                    attempt: 1,
                    raw_json: "{}".to_string(),
                    last_error: None,
                }
            }],
            positions: vec![crate::types::PositionRecord {
                symbol: "ETH/USDT:USDT".to_string(),
                side: "long".to_string(),
                notional: 10.0,
                entry_price: 100.0,
                unrealized_pnl: 1.0,
                ownership: "system".to_string(),
                opened_at: Some("now".to_string()),
                adopted_at: None,
                source_intent_id: None,
                raw_json: "{}".to_string(),
            }],
            fills: vec![],
            account: None,
            subscribe_ack_channel: None,
        };

        apply_private_ws_update(&conn, update).unwrap();

        let order_count: i64 = conn
            .query_row("select count(*) from orders", [], |r| r.get(0))
            .unwrap();
        let position_count: i64 = conn
            .query_row("select count(*) from positions", [], |r| r.get(0))
            .unwrap();
        assert_eq!(order_count, 1);
        assert_eq!(position_count, 1);
        // The known order was REFRESHED: status flipped submitted -> filled, and
        // the executor's intent_id was preserved (not clobbered to NULL by WS).
        let (status, intent_id): (String, Option<String>) = conn
            .query_row(
                "select status, intent_id from orders where client_oid = 'client-1'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(status, "filled");
        assert_eq!(intent_id.as_deref(), Some("intent-1"));
    }

    #[test]
    fn apply_private_ws_update_does_not_persist_account_snapshot() {
        // Finding 2 regression: a private-WS `account` event must NOT be written
        // into the REST-authoritative equity_snapshots table (reconcile owns it;
        // telegram /pnl and /risk read it). WS is only a fast cache. The account
        // event is still PARSED (PrivateWsUpdate.account is populated here), but
        // not persisted. Spec: REST wins.
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute_batch(include_str!("../../../schema/001_initial.sql"))
            .unwrap();
        conn.execute_batch(include_str!("../../../schema/002_execution.sql"))
            .unwrap();

        let update = crate::types::PrivateWsUpdate {
            account: Some(crate::types::AccountSnapshotUpdate {
                equity: 999.0,
                available_margin: 500.0,
                unrealized_pnl: -2.0,
            }),
            ..Default::default()
        };

        apply_private_ws_update(&conn, update).unwrap();

        let count: i64 = conn
            .query_row("select count(*) from equity_snapshots", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 0, "WS account events must not be persisted");
    }

    #[test]
    fn apply_private_ws_update_skips_unknown_orders() {
        // Fix-A regression: the private WS is a fast cache, NOT the source of truth
        // for order identity. apply_private_ws_update must only refresh orders we
        // already placed locally — it must neither steal identity from the REST
        // execution path (inserting a system order with intent_id NULL so
        // system_net_base drops it) nor adopt a manual/imported order before REST
        // reconcile detects it (local_oids would then contain it and reconcile
        // would skip imported/manual detection).
        let conn = test_conn();
        // FK: orders.intent_id references trade_intents — seed the parent intent.
        conn.execute(
            "insert into trade_intents (intent_id, created_at, symbol, side, action,
               target_notional, max_order_notional, status, source)
             values ('intent-a','2026-07-01T00:00:00Z','ETH/USDT:USDT','long','open',100,100,'executed','t')",
            [],
        )
        .unwrap();
        // Pre-existing LOCAL system order (order-A) — known to us, with intent_id.
        crate::db::upsert_order(
            &conn,
            &crate::types::OrderRecord {
                order_id: "order-a".to_string(),
                exchange_order_id: None,
                client_oid: "client-a".to_string(),
                intent_id: Some("intent-a".to_string()),
                symbol: "ETH/USDT:USDT".to_string(),
                side: "buy".to_string(),
                action: "open".to_string(),
                order_type: "limit".to_string(),
                status: "submitted".to_string(),
                price: Some(3000.0),
                size: 0.05,
                filled_size: 0.0,
                attempt: 1,
                raw_json: "{}".to_string(),
                last_error: None,
            },
        )
        .unwrap();

        // WS update: order-A refresh (intent_id None, now filled) + order-B that has
        // NO local row (a manual/unknown order the WS would otherwise adopt).
        let update = crate::types::PrivateWsUpdate {
            orders: vec![
                crate::types::OrderRecord {
                    exchange_order_id: Some("ex-a".to_string()),
                    status: "filled".to_string(),
                    filled_size: 0.05,
                    intent_id: None,
                    ..crate::types::OrderRecord {
                        order_id: "order-a".to_string(),
                        exchange_order_id: None,
                        client_oid: "client-a".to_string(),
                        intent_id: None,
                        symbol: "ETH/USDT:USDT".to_string(),
                        side: "buy".to_string(),
                        action: "open".to_string(),
                        order_type: "limit".to_string(),
                        status: "filled".to_string(),
                        price: Some(3000.0),
                        size: 0.05,
                        filled_size: 0.05,
                        attempt: 1,
                        raw_json: "{}".to_string(),
                        last_error: None,
                    }
                },
                crate::types::OrderRecord {
                    order_id: "order-b".to_string(),
                    exchange_order_id: Some("ex-b".to_string()),
                    client_oid: "client-b".to_string(),
                    intent_id: None,
                    symbol: "ETH/USDT:USDT".to_string(),
                    side: "buy".to_string(),
                    action: "open".to_string(),
                    order_type: "market".to_string(),
                    status: "filled".to_string(),
                    price: Some(3000.0),
                    size: 0.02,
                    filled_size: 0.02,
                    attempt: 1,
                    raw_json: "{}".to_string(),
                    last_error: None,
                },
            ],
            ..Default::default()
        };

        apply_private_ws_update(&conn, update).unwrap();

        // order-A was refreshed: status flipped to filled, intent_id preserved.
        let a: (String, f64, Option<String>) = conn
            .query_row(
                "select status, filled_size, intent_id from orders where client_oid = 'client-a'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(a.0, "filled");
        assert!((a.1 - 0.05).abs() < 1e-9);
        assert_eq!(
            a.2.as_deref(),
            Some("intent-a"),
            "known order's intent_id must be preserved"
        );
        // order-B (unknown/manual) was NOT adopted: no row inserted.
        let b: i64 = conn
            .query_row(
                "select count(*) from orders where client_oid = 'client-b'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(b, 0, "WS must not adopt an unknown/manual order");
    }
}
