use anyhow::Result;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::config::ExecutorConfig;

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

pub async fn run_daemon(cfg: ExecutorConfig, options: DaemonOptions) -> Result<()> {
    cfg.validate_demo_only()?;
    let conn = rusqlite::Connection::open(&cfg.db_path)?;
    // ponytail: WAL persists in the DB file header (idempotent; matches src/prodigy/db.py).
    // M4 has Python writing intents, the daemon R/W, the private WS writing, and Telegram
    // reading — all concurrent. WAL lets those readers/writers proceed in parallel instead of
    // serializing on a single rollback journal; busy_timeout makes them wait out SQLITE_BUSY.
    let _ = conn.pragma_update(None, "journal_mode", "wal");
    conn.busy_timeout(Duration::from_secs(5))?;
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
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    // Cross-task signal: a WS (re)connect sets it, the main loop consumes it on
    // each tick to run a REST reconcile immediately (spec: reconcile after WS
    // reconnect) instead of waiting for the periodic interval.
    let reconcile_signal = ReconcileSignal::new();

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
    ));
    let mut telegram_task = tokio::spawn(run_telegram_query_loop(cfg.clone(), shutdown_rx.clone()));

    // ponytail: monotonic Instant for the bounded-runtime check — immune to
    // wall-clock skew that SystemTime would inject mid-loop.
    let started = tokio::time::Instant::now();
    let mut poll = tokio::time::interval(Duration::from_millis(250));
    let mut last_reconcile_ms = crate::bitget::now_ms().parse::<i64>().unwrap_or(0);

    loop {
        tokio::select! {
            _ = shutdown_signal() => {
                log_shutdown_requested(&conn)?;
                break;
            }
            _ = poll.tick() => {
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
                let now_ms = crate::bitget::now_ms().parse::<i64>().unwrap_or(0);
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

                let mut local_cache = {
                    let cache = market_cache.lock().await;
                    cache.clone()
                };
                // Error isolation: a stale-market or REST failure here (common in
                // the first few hundred ms before the public WS delivers, or on a
                // transient network blip) is logged as an event and the loop
                // continues — the daemon must not crash on a loop-iteration error.
                // The next tick retries once the WS cache is fresh.
                if let Err(err) = crate::executor::process_pending_intents_once(
                    &conn,
                    &cfg,
                    &rest,
                    &mut local_cache,
                )
                .await
                {
                    crate::db::write_event(
                        &conn,
                        "error",
                        "intent_loop",
                        &format!("intent loop failed: {err}"),
                        "{}",
                    )?;
                }
            }
        }
    }

    // Shutdown ordering: signal WS loops via the watch channel, then give them
    // a short grace window to observe it and return cooperatively (flush/close).
    // abort() is the hard fallback so the process still exits within the
    // bounded test runtime if a task is stuck mid-await on a socket read.
    let _ = shutdown_tx.send(true);
    let _ = tokio::time::timeout(
        Duration::from_millis(200),
        futures_util::future::join3(&mut public_task, &mut private_task, &mut telegram_task),
    )
    .await;
    public_task.abort();
    private_task.abort();
    telegram_task.abort();
    crate::db::write_event(&conn, "info", "daemon", "daemon stopped", "{}")?;
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
                // (Re)connect succeeded and we're subscribed: request a REST
                // reconcile on the next main-loop tick (spec: reconcile after
                // WS reconnect) so any books5 / cache gap is repaired promptly.
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
                eprintln!("public ws socket closed; reconnecting");
            }
            Err(err) => {
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

/// Optional read-only Telegram polling loop (M4). Runs ONLY when both
/// `telegram_bot_token` and `telegram_chat_id` are configured — otherwise it
/// returns immediately, since Telegram is not an execution dependency. It
/// long-polls `getUpdates`, filters to the operator's chat_id only (other
/// chats get no reply), and answers recognized `/status /positions /orders
/// /pnl /risk` commands via `telegram_query::query_response`. `/stop /resume
/// /close_all` are refused by the query layer (M4 forbids remote trading
/// control).
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
    let (Some(token), Some(chat_id)) =
        (cfg.telegram_bot_token.clone(), cfg.telegram_chat_id.clone())
    else {
        return Ok(());
    };
    let client = reqwest::Client::new();
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
                        let message = update.get("message").unwrap_or(&serde_json::Value::Null);
                        let chat = message
                            .get("chat")
                            .and_then(|c| c.get("id"))
                            .and_then(serde_json::Value::as_i64)
                            .map(|v| v.to_string());
                        // Security gate: only the operator's configured chat
                        // gets any reply; messages from any other chat are
                        // ignored (offset still advances so they aren't redelivered).
                        if chat.as_deref() != Some(chat_id.as_str()) {
                            continue;
                        }
                        let Some(text) = message.get("text").and_then(serde_json::Value::as_str)
                        else {
                            continue;
                        };
                        match rusqlite::Connection::open(&cfg.db_path) {
                            Ok(conn) => {
                                if let Err(err) = conn
                                    .busy_timeout(std::time::Duration::from_secs(5))
                                    .map_err(anyhow::Error::from)
                                {
                                    eprintln!("telegram sqlite busy_timeout error: {err}");
                                    continue;
                                }
                                let reply = match crate::telegram_query::query_response(&conn, text)
                                {
                                    Ok(reply) => reply,
                                    Err(err) => {
                                        eprintln!("telegram query error: {err}");
                                        continue;
                                    }
                                };
                                if let Some(reply) = reply {
                                    let send_url =
                                        format!("https://api.telegram.org/bot{token}/sendMessage");
                                    // ponytail: best-effort send — a failed
                                    // sendMessage is dropped on the floor; the
                                    // operator can re-issue the command.
                                    let _ = client
                                        .post(send_url)
                                        .form(&[
                                            ("chat_id", chat_id.as_str()),
                                            ("text", reply.as_str()),
                                        ])
                                        .send()
                                        .await;
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
) -> Result<()> {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;

    cfg.validate_demo_only()?;
    let mut shutdown = shutdown;
    loop {
        if *shutdown.borrow() {
            return Ok(());
        }
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
                // Subscribe to orders/positions/account after login. A production
                // client waits for the login ack; for this demo daemon sending both
                // immediately is acceptable and matches the existing verify style.
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
                // (Re)connect succeeded and we're subscribed: request a REST
                // reconcile on the next main-loop tick (spec: reconcile after
                // WS reconnect) so any orders/fills/positions gap is repaired.
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
                eprintln!("private ws socket closed; reconnecting");
            }
            Err(err) => {
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

    #[test]
    fn daemon_options_default_runs_forever() {
        let options = DaemonOptions::default();

        assert!(options.max_runtime.is_none());
    }

    #[test]
    fn should_run_reconcile_when_interval_elapsed() {
        assert!(should_run_reconcile(10_000, 0, 10));
        assert!(!should_run_reconcile(9_999, 0, 10));
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
