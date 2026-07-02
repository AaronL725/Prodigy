use anyhow::Result;
use rusqlite::Connection;
use std::time::Duration;

use crate::bitget::{
    verify_private_ws_connects, verify_public_ws_connects, BitgetRestClient, PlaceOrderRequest,
};
use crate::config::ExecutorConfig;
use crate::db;
use crate::reconcile::reconcile_once;
use crate::risk::{check_intent, AccountRiskSnapshot, RiskParams};
use crate::types::{MarketUpdate, OrderRecord, TradeIntent};

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
    // ponytail: TEMPORARY snapshot. Account stays hardcoded (Task 12 wires live
    // equity), but the market price MUST be real: a maker limit at a stale
    // hardcoded price is rejected by Bitget's price band (code 22047), so the
    // money path would never place. One ticker fetch gives a band-valid price;
    // Task 12 replaces this with the streaming WS book cache.
    let market = fetch_market_snapshot(&cfg, &rest).await?;
    for intent in intents {
        let account = AccountRiskSnapshot {
            equity: 10_000.0,
            available_margin: 5_000.0,
            unrealized_pnl_24h: 0.0,
            gross_notional: 0.0,
            market_is_fresh: true,
            private_state_is_ready: true,
        };
        process_one_intent(&conn, &cfg, &rest, intent.clone(), market.clone(), account).await?;
        db::write_event(&conn, "info", "executor", "processed intent", "{}")?;
        println!("processed {}", intent.intent_id);
    }
    Ok(())
}

async fn fetch_market_snapshot(
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
        exchange_ts_ms: 0,
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

    let order = build_order_request(cfg, &intent, &market, approved, OrderMode::Maker, 1);
    let response = rest
        .post_json("/api/v2/mix/order/place-order", &order)
        .await?;
    let exchange_order_id = response
        .pointer("/data/orderId")
        .and_then(serde_json::Value::as_str)
        .unwrap_or(&order.client_oid)
        .to_string();
    db::upsert_order(
        conn,
        &OrderRecord {
            order_id: exchange_order_id.clone(),
            exchange_order_id: Some(exchange_order_id),
            client_oid: order.client_oid.clone(),
            intent_id: Some(intent.intent_id.clone()),
            symbol: intent.symbol.clone(),
            side: order.side.clone(),
            action: intent.action.clone(),
            order_type: order.order_type.clone(),
            status: "submitted".to_string(),
            price: order.price.as_ref().and_then(|v| v.parse().ok()),
            size: order.size.parse().unwrap_or(0.0),
            filled_size: 0.0,
            attempt: 1,
            raw_json: response.to_string(),
            last_error: None,
        },
    )?;
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
}
