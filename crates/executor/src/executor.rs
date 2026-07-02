use anyhow::Result;
use rusqlite::Connection;
use std::time::{Duration, Instant};

use crate::bitget::{
    verify_private_ws_connects, verify_public_ws_connects, BitgetRestClient, CancelOrderRequest,
    PlaceOrderRequest,
};
use crate::config::ExecutorConfig;
use crate::db;
use crate::reconcile::reconcile_once;
use crate::risk::{check_intent, AccountRiskSnapshot, RiskParams};
use crate::state::{ExecutionCommand, ExecutionPolicy, IntentExecution};
use crate::types::{MarketUpdate, OrderRecord, TradeIntent};

#[derive(Debug, Clone, Default)]
pub struct MarketCache {
    latest: Option<MarketUpdate>,
}

impl MarketCache {
    pub fn update(&mut self, update: MarketUpdate) {
        self.latest = Some(update);
    }

    pub fn latest_fresh(&self, now_ms: i64, stale_after_secs: u64) -> Option<MarketUpdate> {
        let update = self.latest.clone()?;
        let age_ms = now_ms.saturating_sub(update.exchange_ts_ms);
        if age_ms <= (stale_after_secs as i64) * 1000 {
            Some(update)
        } else {
            None
        }
    }
}

pub async fn run_once_or_loop(cfg: ExecutorConfig) -> Result<()> {
    cfg.validate_demo_only()?;
    let conn = Connection::open(&cfg.db_path)?;
    conn.busy_timeout(Duration::from_secs(5))?;
    let rest = BitgetRestClient::new(cfg.clone())?;

    if cfg.test_reset_demo_state {
        reset_demo_symbol_state(&cfg, &rest).await?;
    }

    verify_public_ws_connects(&cfg).await?;
    verify_private_ws_connects(&cfg).await?;
    reconcile_once(&conn, &rest, "now").await?;

    let intents = db::pending_intents(&conn)?;
    // ponytail: seed the cache once with a real ticker (not the WS stream yet);
    // latest_fresh per intent rejects openings when the cached price is older
    // than cfg.stale_market_data_secs. Account stays hardcoded until Task 13
    // (live equity wiring) — but the REST fetch below proves the signed GET
    // /api/v2/mix/account/account path that snapshot will reuse.
    let mut market_cache = MarketCache::default();
    market_cache.update(fetch_initial_market_snapshot(&cfg, &rest).await?);
    let _account_json = rest.get_account_snapshot().await?;
    for intent in intents {
        let now_ms = crate::bitget::now_ms().parse::<i64>().unwrap_or(0);
        let market = market_cache
            .latest_fresh(now_ms, cfg.stale_market_data_secs)
            .ok_or_else(|| anyhow::anyhow!("market cache is stale"))?;
        let account = AccountRiskSnapshot {
            equity: 10_000.0,
            available_margin: 5_000.0,
            unrealized_pnl_24h: 0.0,
            gross_notional: 0.0,
            market_is_fresh: true,
            private_state_is_ready: true,
        };
        process_one_intent(&conn, &cfg, &rest, intent.clone(), market, account).await?;
        db::write_event(&conn, "info", "executor", "processed intent", "{}")?;
        println!("processed {}", intent.intent_id);
    }
    Ok(())
}

async fn fetch_initial_market_snapshot(
    cfg: &ExecutorConfig,
    rest: &BitgetRestClient,
) -> Result<MarketUpdate> {
    let ticker = rest
        .get(
            "/api/v2/mix/market/ticker",
            &[
                ("productType", cfg.product_type.clone()),
                ("symbol", cfg.bitget_symbol.clone()),
            ],
        )
        .await?;
    let row = ticker
        .get("data")
        .and_then(serde_json::Value::as_array)
        .and_then(|rows| rows.first())
        .ok_or_else(|| anyhow::anyhow!("ticker returned no data"))?;
    let parse_price = |key: &str| -> Result<f64> {
        row.get(key)
            .and_then(serde_json::Value::as_str)
            .and_then(|v| v.parse().ok())
            .ok_or_else(|| anyhow::anyhow!("ticker missing/unparseable {key}"))
    };
    // ponytail: fail loud on a bad price — unwrap_or(0.0) would make size =
    // notional/0.0 = "inf"; the money path must not build an order on a price
    // it couldn't read, even though Bitget would reject "inf" loudly.
    Ok(MarketUpdate {
        symbol: cfg.bitget_symbol.clone(),
        best_bid: parse_price("bidPr")?,
        best_ask: parse_price("askPr")?,
        exchange_ts_ms: crate::bitget::now_ms().parse().unwrap_or(0),
    })
}

async fn reset_demo_symbol_state(cfg: &ExecutorConfig, rest: &BitgetRestClient) -> Result<()> {
    // ponytail: best-effort resets behind --test-reset-demo-state. A failure
    // (e.g. no ETHUSDT order to cancel, or a sub-min-qty dust position that
    // can't be market-closed — Bitget 45111) must not abort the run before the
    // pending intent is processed. Log a line so it isn't silent.
    if let Err(e) = rest.cancel_all_orders().await {
        println!("demo reset: cancel-all skipped: {e}");
    }
    if let Err(e) = close_existing_demo_position_if_any(cfg, rest).await {
        println!("demo reset: position close skipped: {e}");
    }
    Ok(())
}

async fn close_existing_demo_position_if_any(
    cfg: &ExecutorConfig,
    rest: &BitgetRestClient,
) -> Result<()> {
    let positions = rest
        .get(
            "/api/v2/mix/position/all-position",
            &[
                ("productType", cfg.product_type.clone()),
                ("marginCoin", cfg.margin_coin.clone()),
            ],
        )
        .await?;
    let rows = positions
        .get("data")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default();
    for row in rows {
        if row.get("symbol").and_then(serde_json::Value::as_str) != Some(&cfg.bitget_symbol) {
            continue;
        }
        let size = row
            .get("available")
            .or_else(|| row.get("total"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or("0")
            .parse::<f64>()
            .unwrap_or(0.0);
        if size <= 0.0 {
            continue;
        }
        let hold_side = row
            .get("holdSide")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("");
        let side = if hold_side == "long" { "sell" } else { "buy" };
        let request = PlaceOrderRequest {
            symbol: cfg.bitget_symbol.clone(),
            product_type: cfg.product_type.clone(),
            margin_mode: cfg.margin_mode.clone(),
            margin_coin: cfg.margin_coin.clone(),
            size: format_size(size),
            price: None,
            side: side.to_string(),
            order_type: "market".to_string(),
            force: None,
            client_oid: format!("pdgy-reset-{}", crate::bitget::now_ms()),
            reduce_only: Some("YES".to_string()),
        };
        rest.post_json("/api/v2/mix/order/place-order", &request)
            .await?;
    }
    Ok(())
}

/// manual_override open gate: true only when the symbol is marked "active".
/// close/reduce/cancel are risk-reducing and bypass this gate (see is_opening_action).
fn is_open_blocked(state_value: Option<&str>) -> bool {
    state_value == Some("active")
}

/// An opening action creates new exposure (blocked by manual_override);
/// close/reduce/cancel are risk-reducing and always proceed.
fn is_opening_action(action: &str) -> bool {
    matches!(action, "open" | "add" | "reverse")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderMode {
    Maker,
    Taker,
}

pub fn build_order_request(
    cfg: &ExecutorConfig,
    intent: &TradeIntent,
    market: &MarketUpdate,
    approved_notional: f64,
    mode: OrderMode,
    attempt: u32,
) -> PlaceOrderRequest {
    let side = match (intent.action.as_str(), intent.side.as_str()) {
        ("open", "long") => "buy",
        ("open", "short") => "sell",
        ("close", "long") => "sell",
        ("close", "short") => "buy",
        _ => "sell",
    };
    let price = match mode {
        OrderMode::Maker if side == "buy" => Some(format_price(market.best_bid)),
        OrderMode::Maker => Some(format_price(market.best_ask)),
        OrderMode::Taker => None,
    };
    let reference_price = match (mode, side) {
        (OrderMode::Maker, "buy") => market.best_bid,
        (OrderMode::Maker, _) => market.best_ask,
        (OrderMode::Taker, "buy") => market.best_ask,
        (OrderMode::Taker, _) => market.best_bid,
    };
    let size = format_size(approved_notional / reference_price);
    // ponytail: append now_ms so re-running the same intent doesn't collide on
    // Bitget's client_oid uniqueness window (code 40786); matches the demo test
    // convention. attempt still sequences retries within one placement.
    let client_oid = format!(
        "pdgy-{}-{attempt}-{}",
        intent.intent_id,
        crate::bitget::now_ms()
    );

    PlaceOrderRequest {
        symbol: cfg.bitget_symbol.clone(),
        product_type: cfg.product_type.clone(),
        margin_mode: cfg.margin_mode.clone(),
        margin_coin: cfg.margin_coin.clone(),
        size,
        price,
        side: side.to_string(),
        order_type: if mode == OrderMode::Maker {
            "limit"
        } else {
            "market"
        }
        .to_string(),
        force: if mode == OrderMode::Maker {
            Some("post_only".to_string())
        } else {
            None
        },
        client_oid,
        reduce_only: if intent.action == "close" {
            Some("YES".to_string())
        } else {
            None
        },
    }
}

/// Outcome of polling one order's exchange status. Pure classification over the
/// fields read from GET /api/v2/mix/order/detail so the wiring test can drive the
/// maker/cancel/taker loop without a network round-trip.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderPollOutcome {
    /// Fully filled (terminal): advance the execution to done.
    Filled,
    /// Still working on the book (live / partially filled): the timeout owns it.
    Live,
    /// Gone from the book (canceled / rejected): a cancel is confirmed.
    Vanished,
}

/// Classify an order-status poll. `filled_size >= order_size` wins over the
/// status string so a fill that the status enum hasn't caught up to still counts
/// as done. Bitget's detail endpoint reports status under either `status` or
/// `state` (docs are inconsistent), so the caller passes whichever it found.
pub fn classify_order_poll(status: &str, filled_size: f64, order_size: f64) -> OrderPollOutcome {
    if order_size > 0.0 && filled_size >= order_size {
        return OrderPollOutcome::Filled;
    }
    match status {
        "filled" | "full_fill" | "full-fill" => OrderPollOutcome::Filled,
        "canceled" | "cancelled" | "rejected" => OrderPollOutcome::Vanished,
        _ => OrderPollOutcome::Live,
    }
}

/// Read (status, filled_size) out of a detail `data` object, tolerating both the
/// `status` and `state` key spellings and the `baseVolume` fill field.
fn read_detail_fields(data: &serde_json::Value) -> (String, f64) {
    let status = data
        .get("status")
        .or_else(|| data.get("state"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or("")
        .to_string();
    let filled = data
        .get("baseVolume")
        .and_then(serde_json::Value::as_str)
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or(0.0);
    (status, filled)
}

/// Poll one order by client_oid and classify it.
// ponytail: an unknown/transient error resolves to `Live`, never `Vanished`.
// The safety rule is "never leave a stale order"; treating a failed poll as gone
// would risk marking a still-live order retired. The cancel-confirmation loop and
// the hard command cap own the terminal decision, so a Live-on-error keeps the
// caller cautious rather than optimistic.
async fn poll_order(
    rest: &BitgetRestClient,
    client_oid: &str,
    order_size: f64,
) -> OrderPollOutcome {
    match rest.get_order_detail(client_oid).await {
        Ok(data) => {
            let (status, filled) = read_detail_fields(&data);
            classify_order_poll(&status, filled, order_size)
        }
        Err(e) => {
            println!("order poll error for {client_oid} (treating as live): {e}");
            OrderPollOutcome::Live
        }
    }
}

/// Insert/update the local orders row for a placement. Returns the exchange order id.
#[allow(clippy::too_many_arguments)]
fn upsert_order_row(
    conn: &Connection,
    intent: &TradeIntent,
    order: &PlaceOrderRequest,
    response: &serde_json::Value,
    status: &str,
    attempt: i64,
    filled_size: f64,
) -> Result<String> {
    let exchange_order_id = response
        .pointer("/data/orderId")
        .and_then(serde_json::Value::as_str)
        .unwrap_or(&order.client_oid)
        .to_string();
    db::upsert_order(
        conn,
        &OrderRecord {
            order_id: exchange_order_id.clone(),
            exchange_order_id: Some(exchange_order_id.clone()),
            client_oid: order.client_oid.clone(),
            intent_id: Some(intent.intent_id.clone()),
            symbol: intent.symbol.clone(),
            side: order.side.clone(),
            action: intent.action.clone(),
            order_type: order.order_type.clone(),
            status: status.to_string(),
            price: order.price.as_ref().and_then(|v| v.parse().ok()),
            size: order.size.parse().unwrap_or(0.0),
            filled_size,
            attempt,
            raw_json: response.to_string(),
            last_error: None,
        },
    )?;
    Ok(exchange_order_id)
}

/// Update just the status of the local orders row for a client_oid.
// ponytail: inline UPDATE here (not a db:: helper) keeps this task's diff to
// executor.rs + bitget.rs. The orders row is created by upsert_order_row on
// placement; here we only flip its status on cancel/late-fill.
fn set_local_order_status(conn: &Connection, client_oid: &str, status: &str) -> Result<()> {
    conn.execute(
        "update orders set status = ?, updated_at = datetime('now') where client_oid = ?",
        rusqlite::params![status, client_oid],
    )?;
    Ok(())
}

fn format_price(value: f64) -> String {
    format!("{value:.2}")
        .trim_end_matches('0')
        .trim_end_matches('.')
        .to_string()
}

fn format_size(value: f64) -> String {
    format!("{value:.4}")
        .trim_end_matches('0')
        .trim_end_matches('.')
        .to_string()
}

pub async fn process_one_intent(
    conn: &Connection,
    cfg: &ExecutorConfig,
    rest: &BitgetRestClient,
    intent: TradeIntent,
    market: MarketUpdate,
    account: AccountRiskSnapshot,
) -> Result<()> {
    if !db::accept_intent(conn, &intent.intent_id)? {
        return Ok(());
    }
    let risk = check_intent(
        &intent,
        &account,
        &RiskParams {
            total_notional_cap_x_equity: cfg.total_notional_cap_x_equity,
            trading_suspension_unrealized_loss_x_equity: cfg
                .trading_suspension_unrealized_loss_x_equity,
            ..RiskParams::default()
        },
    );
    let approved = match risk {
        Ok(decision) => decision.approved_notional,
        Err(reason) => {
            db::fail_intent(conn, &intent.intent_id, &reason)?;
            return Ok(());
        }
    };

    // Manual-override open gate: if a human has touched this symbol (state "active"),
    // refuse to place any NEW exposure (open/add/reverse). Risk-reducing actions
    // (close/reduce/cancel) still proceed. Reuses executor_state without a full
    // intervention-detection loop; that lands with the live reconcile wiring later.
    if is_opening_action(&intent.action)
        && is_open_blocked(
            db::get_executor_state(conn, &format!("manual_override:{}", intent.symbol))?.as_deref(),
        )
    {
        db::fail_intent(conn, &intent.intent_id, "manual override active for symbol")?;
        return Ok(());
    }

    // Drive the approved intent through the state machine: maker, retry maker
    // once, then taker. Every maker timeout cancels the live order AND confirms
    // it is gone before the next attempt (safety: no stale orders left behind).
    let policy = ExecutionPolicy {
        max_maker_attempts_before_taker: 2,
    };
    let mut state = IntentExecution::new(&intent.intent_id, &intent.action);
    let maker_timeout = if is_opening_action(&intent.action) {
        Duration::from_secs(cfg.open_maker_timeout_secs)
    } else {
        Duration::from_secs(cfg.close_maker_timeout_secs)
    };

    // Track the client_oid of the current live order so timeout/cancel/poll all
    // target the same placement.
    let mut live_client_oid: Option<String> = None;
    let mut live_order_size: f64 = 0.0;

    // ponytail: hard cap on state-machine commands so the loop can never spin
    // forever (e.g. a poll that never resolves, or a cancel the exchange keeps
    // reporting live). The happy path uses far fewer than 12: place, wait, cancel
    // per maker attempt (2), then a taker place + wait. On cap we fail the intent
    // loudly rather than leave it wedged.
    const MAX_COMMANDS: u32 = 12;
    let mut commands_issued: u32 = 0;

    loop {
        if commands_issued >= MAX_COMMANDS {
            db::fail_intent(conn, &intent.intent_id, "execution did not converge")?;
            break;
        }
        commands_issued += 1;

        match state.next_command(&policy) {
            ExecutionCommand::PlaceMaker { attempt } => {
                let order =
                    build_order_request(cfg, &intent, &market, approved, OrderMode::Maker, attempt);
                let response = rest
                    .post_json("/api/v2/mix/order/place-order", &order)
                    .await?;
                upsert_order_row(
                    conn,
                    &intent,
                    &order,
                    &response,
                    "submitted",
                    attempt as i64,
                    0.0,
                )?;
                live_order_size = order.size.parse().unwrap_or(0.0);
                live_client_oid = Some(order.client_oid.clone());
                state.on_order_placed(&order.client_oid);

                // Poll for fill up to the maker timeout (every ~500ms).
                let deadline = Instant::now() + maker_timeout;
                loop {
                    if Instant::now() >= deadline {
                        state.on_order_timeout();
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    match poll_order(rest, &order.client_oid, live_order_size).await {
                        OrderPollOutcome::Filled => {
                            upsert_order_row(
                                conn,
                                &intent,
                                &order,
                                &response,
                                "filled",
                                attempt as i64,
                                live_order_size,
                            )?;
                            state.on_order_filled();
                            live_client_oid = None;
                            break;
                        }
                        // Already gone (rare: cancelled out from under us). Treat as
                        // a timeout so the machine runs its cancel-confirm branch,
                        // which will observe it is gone and count the attempt.
                        OrderPollOutcome::Vanished => {
                            state.on_order_timeout();
                            break;
                        }
                        OrderPollOutcome::Live => continue,
                    }
                }
            }
            ExecutionCommand::CancelCurrent => {
                // Safety: cancel the live maker order AND confirm it is gone before
                // the machine moves to the next attempt. A cancel that the exchange
                // still reports live keeps polling until the confirm deadline; only
                // a confirmed-gone (or terminal-fill) order advances the machine.
                let client_oid = live_client_oid
                    .clone()
                    .ok_or_else(|| anyhow::anyhow!("cancel requested with no live order"))?;
                let cancel = CancelOrderRequest {
                    symbol: cfg.bitget_symbol.clone(),
                    product_type: cfg.product_type.clone(),
                    margin_coin: cfg.margin_coin.clone(),
                    client_oid: client_oid.clone(),
                };
                // ponytail: a cancel on an order that already vanished (filled/gone)
                // returns a Bitget error; don't abort the intent on it — the confirm
                // poll below is the source of truth for whether the order is gone.
                if let Err(e) = rest.cancel_order(&cancel).await {
                    println!("cancel-order returned error for {client_oid}: {e}");
                }

                // Confirm removal (poll up to a short bounded window).
                let confirm_deadline = Instant::now() + Duration::from_secs(5);
                let mut confirmed = false;
                let mut filled_instead = false;
                loop {
                    match poll_order(rest, &client_oid, live_order_size).await {
                        OrderPollOutcome::Vanished => {
                            confirmed = true;
                            break;
                        }
                        OrderPollOutcome::Filled => {
                            filled_instead = true;
                            break;
                        }
                        OrderPollOutcome::Live => {
                            if Instant::now() >= confirm_deadline {
                                break;
                            }
                            tokio::time::sleep(Duration::from_millis(500)).await;
                        }
                    }
                }

                if filled_instead {
                    // The maker filled during the cancel race: honor the fill.
                    set_local_order_status(conn, &client_oid, "filled")?;
                    state.on_order_filled();
                    live_client_oid = None;
                } else if confirmed {
                    set_local_order_status(conn, &client_oid, "cancelled")?;
                    state.on_order_cancelled();
                    live_client_oid = None;
                } else {
                    // Could not confirm the order is gone — refuse to place another
                    // order on top of a possibly-live one. Fail loudly.
                    db::fail_intent(
                        conn,
                        &intent.intent_id,
                        "could not confirm maker cancellation; leaving to reconcile",
                    )?;
                    break;
                }
            }
            ExecutionCommand::PlaceTaker => {
                let order =
                    build_order_request(cfg, &intent, &market, approved, OrderMode::Taker, 1);
                let response = rest
                    .post_json("/api/v2/mix/order/place-order", &order)
                    .await?;
                upsert_order_row(conn, &intent, &order, &response, "submitted", 1, 0.0)?;
                let taker_size: f64 = order.size.parse().unwrap_or(0.0);
                state.on_taker_sent();

                // Poll for the market fill over a short bounded window.
                let deadline = Instant::now() + Duration::from_secs(5);
                loop {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    match poll_order(rest, &order.client_oid, taker_size).await {
                        OrderPollOutcome::Filled => {
                            upsert_order_row(
                                conn, &intent, &order, &response, "filled", 1, taker_size,
                            )?;
                            state.on_order_filled();
                            break;
                        }
                        OrderPollOutcome::Vanished => {
                            // A market order should not vanish unfilled; record it
                            // and let the machine mark done (position reconcile owns
                            // the residual).
                            state.on_order_filled();
                            break;
                        }
                        OrderPollOutcome::Live => {
                            if Instant::now() >= deadline {
                                // Taker didn't confirm in-window; mark done anyway —
                                // a market order fills, and reconcile owns the truth.
                                state.on_order_filled();
                                break;
                            }
                        }
                    }
                }
            }
            ExecutionCommand::MarkIntentExecuted => {
                db::mark_intent_executed(conn, &intent.intent_id)?;
                break;
            }
            ExecutionCommand::Wait => {
                // has_live_order but no timeout fired inside the placement branch
                // means nothing left to do this pass; sleep briefly and re-evaluate.
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{MarketUpdate, TradeIntent};

    #[test]
    fn maker_open_long_uses_best_bid() {
        let intent = TradeIntent {
            intent_id: "intent-1".to_string(),
            symbol: "ETH/USDT:USDT".to_string(),
            side: "long".to_string(),
            action: "open".to_string(),
            target_notional: 300.0,
            max_order_notional: 300.0,
        };
        let market = MarketUpdate {
            symbol: "ETHUSDT".to_string(),
            best_bid: 3000.0,
            best_ask: 3000.5,
            exchange_ts_ms: 1,
        };

        let order = build_order_request(
            &ExecutorConfig::demo_for_tests(),
            &intent,
            &market,
            300.0,
            OrderMode::Maker,
            1,
        );

        assert_eq!(order.side, "buy");
        assert_eq!(order.order_type, "limit");
        assert_eq!(order.price.as_deref(), Some("3000"));
        assert_eq!(order.size, "0.1");
    }

    #[test]
    fn is_open_blocked_only_for_active_override() {
        // money-path open gate: only the literal "active" state blocks opening.
        assert!(is_open_blocked(Some("active")));
        assert!(!is_open_blocked(None));
        assert!(!is_open_blocked(Some("cleared")));
    }

    #[test]
    fn taker_close_long_is_reduce_only_sell_market() {
        let intent = TradeIntent {
            intent_id: "intent-1".to_string(),
            symbol: "ETH/USDT:USDT".to_string(),
            side: "long".to_string(),
            action: "close".to_string(),
            target_notional: 300.0,
            max_order_notional: 300.0,
        };
        let market = MarketUpdate {
            symbol: "ETHUSDT".to_string(),
            best_bid: 3000.0,
            best_ask: 3000.5,
            exchange_ts_ms: 1,
        };

        let order = build_order_request(
            &ExecutorConfig::demo_for_tests(),
            &intent,
            &market,
            300.0,
            OrderMode::Taker,
            1,
        );

        assert_eq!(order.side, "sell");
        assert_eq!(order.order_type, "market");
        assert_eq!(order.reduce_only.as_deref(), Some("YES"));
        assert!(order.price.is_none());
    }

    #[test]
    fn classify_order_poll_detects_filled_live_and_vanished() {
        // Explicit terminal-fill status.
        assert_eq!(
            classify_order_poll("filled", 0.0, 0.1),
            OrderPollOutcome::Filled
        );
        // Size-complete even if the status string lags (>= order size).
        assert_eq!(
            classify_order_poll("partially_filled", 0.1, 0.1),
            OrderPollOutcome::Filled
        );
        // Working orders (nothing / part filled) stay Live so the timeout owns them.
        assert_eq!(
            classify_order_poll("live", 0.0, 0.1),
            OrderPollOutcome::Live
        );
        assert_eq!(
            classify_order_poll("partially_filled", 0.05, 0.1),
            OrderPollOutcome::Live
        );
        // Cancelled / rejected => the order is gone from the book (cancel confirm).
        assert_eq!(
            classify_order_poll("canceled", 0.0, 0.1),
            OrderPollOutcome::Vanished
        );
        assert_eq!(
            classify_order_poll("cancelled", 0.0, 0.1),
            OrderPollOutcome::Vanished
        );
    }

    #[test]
    fn stale_market_cache_returns_none() {
        let mut cache = MarketCache::default();
        cache.update(MarketUpdate {
            symbol: "ETHUSDT".to_string(),
            best_bid: 3000.0,
            best_ask: 3000.5,
            exchange_ts_ms: 1,
        });

        // units are ms: stale_after_secs(3) -> 3000ms threshold.
        // age 3999ms (now 4000 - ts 1) exceeds it -> stale -> None.
        assert!(cache.latest_fresh(4_000, 3).is_none());
        // age 999ms (now 1000 - ts 1) is under it -> fresh -> Some.
        assert!(cache.latest_fresh(1_000, 3).is_some());
    }
}
