use anyhow::Result;
use rusqlite::Connection;
use std::collections::HashSet;

use crate::bitget::BitgetRestClient;
use crate::db;
use crate::manual_override::{apply_exchange_intervention, ExchangeIntervention, InterventionKind};
use crate::notify;
use crate::types::{OrderRecord, PositionRecord};

pub fn classify_position(
    mut exchange_position: PositionRecord,
    local_order_intents: &HashSet<String>,
    now: &str,
) -> PositionRecord {
    if exchange_position
        .source_intent_id
        .as_ref()
        .map(|id| local_order_intents.contains(id))
        .unwrap_or(false)
    {
        exchange_position.ownership = "system".to_string();
    } else {
        exchange_position.ownership = "imported".to_string();
        exchange_position.adopted_at = Some(now.to_string());
        exchange_position.opened_at = Some(now.to_string());
    }
    exchange_position
}

pub async fn reconcile_once(conn: &Connection, rest: &BitgetRestClient, now: &str) -> Result<()> {
    // ponytail: WS is the fast path; this REST pass repairs anything it missed.
    // Exchange state wins on conflict (spec). We INSERT missing orders/positions
    // and refresh position fields from exchange truth; we do not delete local rows
    // the exchange no longer lists (a filled/cancelled order's terminal state is
    // already persisted by the execution loop).
    let local_oids = db::local_order_client_oids(conn)?;
    let system_intents = db::local_system_intent_ids(conn)?;
    let mut repaired_orders = 0u32;
    let mut repaired_positions = 0u32;
    let symbol = rest.display_symbol().to_string();
    let override_key = format!("manual_override:{symbol}");
    let mut override_active = matches!(
        db::get_executor_state(conn, &override_key)?.as_deref(),
        Some("active")
    );
    let mut exchange_open_count: usize = 0;

    // Open orders: insert any exchange order we don't already have locally.
    let open_orders = rest
        .get(
            "/api/v2/mix/order/orders-pending",
            &[
                ("productType", rest.product_type().to_string()),
                ("marginCoin", rest.margin_coin().to_string()),
            ],
        )
        .await?;
    for row in open_orders
        .get("data")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default()
    {
        let client_oid = str_field(&row, "clientOid");
        if row.get("symbol").and_then(serde_json::Value::as_str) != Some(rest.bitget_symbol()) {
            continue;
        }
        exchange_open_count += 1;
        if client_oid.is_empty() || local_oids.contains(&client_oid) {
            continue;
        }
        // An exchange order we did NOT place (no local client_oid) = manual
        // intervention. Enter per-symbol override (persisted) so auto-open pauses.
        if !override_active {
            let kind = intervention_kind_for_side(&str_field(&row, "side"));
            let mut state = override_state_from(override_active);
            if let crate::manual_override::ManualOverrideDecision::Entered(sym) =
                apply_exchange_intervention(
                    &mut state,
                    ExchangeIntervention {
                        symbol: symbol.clone(),
                        matched_local_client_oid: false,
                        kind,
                    },
                )
            {
                db::set_executor_state(conn, &override_key, "active")?;
                override_active = true;
                db::write_event(
                    conn,
                    "warning",
                    "executor",
                    "manual override entered",
                    &format!("{{\"symbol\":\"{sym}\"}}"),
                )?;
                notify::send_telegram(
                    None,
                    None,
                    "manual_override_entered",
                    &format!("manual override entered for {sym}"),
                )
                .await
                .ok();
            }
        }
        let order_id = str_field(&row, "orderId");
        let order = OrderRecord {
            order_id: order_id.clone(),
            exchange_order_id: Some(order_id),
            client_oid: client_oid.clone(),
            intent_id: None,
            symbol: rest.display_symbol().to_string(),
            side: str_field(&row, "side"),
            action: str_field(&row, "tradeSide"),
            order_type: str_field(&row, "orderType"),
            status: str_field(&row, "status"),
            price: row
                .get("price")
                .and_then(serde_json::Value::as_str)
                .and_then(|v| v.parse().ok()),
            size: row
                .get("size")
                .and_then(serde_json::Value::as_str)
                .and_then(|v| v.parse().ok())
                .unwrap_or(0.0),
            filled_size: row
                .get("baseVolume")
                .or_else(|| row.get("accBaseVolume"))
                .and_then(serde_json::Value::as_str)
                .and_then(|v| v.parse().ok())
                .unwrap_or(0.0),
            attempt: 1,
            raw_json: row.to_string(),
            last_error: None,
        };
        db::upsert_order(conn, &order)?;
        repaired_orders += 1;
    }

    // Positions: classify (system vs imported) and upsert exchange truth.
    let positions = rest
        .get(
            "/api/v2/mix/position/all-position",
            &[
                ("productType", rest.product_type().to_string()),
                ("marginCoin", rest.margin_coin().to_string()),
            ],
        )
        .await?;
    for row in positions
        .get("data")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default()
    {
        let symbol = rest.display_symbol().to_string();
        let size = row
            .get("total")
            .or_else(|| row.get("available"))
            .and_then(serde_json::Value::as_str)
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(0.0);
        let entry = row
            .get("averageOpenPrice")
            .or_else(|| row.get("openPriceAvg"))
            .and_then(serde_json::Value::as_str)
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(0.0);
        let upnl = row
            .get("unrealizedPL")
            .or_else(|| row.get("unrealizedPnl"))
            .and_then(serde_json::Value::as_str)
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(0.0);
        let mut record = PositionRecord {
            symbol,
            side: str_field(&row, "holdSide"),
            notional: size.abs() * entry,
            entry_price: entry,
            unrealized_pnl: upnl,
            ownership: "system".to_string(),
            opened_at: None,
            adopted_at: None,
            source_intent_id: None,
            raw_json: row.to_string(),
        };
        record = classify_position(record, &system_intents, now);
        db::upsert_position(conn, &record)?;
        repaired_positions += 1;
    }

    // Auto-clear the per-symbol override once the exchange has no position and no
    // open orders for it (spec: resume auto-open when pos+orders reach zero).
    if override_active {
        let exchange_position_notional: f64 = {
            let pos = rest
                .get(
                    "/api/v2/mix/position/all-position",
                    &[
                        ("productType", rest.product_type().to_string()),
                        ("marginCoin", rest.margin_coin().to_string()),
                    ],
                )
                .await?;
            pos.get("data")
                .and_then(serde_json::Value::as_array)
                .map(|rows| {
                    rows.iter()
                        .filter(|r| {
                            r.get("symbol").and_then(serde_json::Value::as_str)
                                == Some(rest.bitget_symbol())
                        })
                        .filter_map(|r| {
                            r.get("total")
                                .or_else(|| r.get("available"))
                                .and_then(serde_json::Value::as_str)
                                .and_then(|v| v.parse::<f64>().ok())
                        })
                        .map(|v| v.abs())
                        .sum::<f64>()
                })
                .unwrap_or(0.0)
        };
        let mut state = override_state_from(true);
        if let crate::manual_override::ManualOverrideDecision::Cleared(sym) =
            crate::manual_override::maybe_clear_manual_override(
                &mut state,
                &symbol,
                exchange_position_notional,
                exchange_open_count,
            )
        {
            db::set_executor_state(conn, &override_key, "cleared")?;
            db::write_event(
                conn,
                "info",
                "executor",
                "manual override cleared",
                &format!("{{\"symbol\":\"{sym}\"}}"),
            )?;
            notify::send_telegram(
                None,
                None,
                "manual_override_cleared",
                &format!("manual override cleared for {sym}"),
            )
            .await
            .ok();
        }
    }

    let summary = format!(
        "{{\"repaired_orders\":{repaired_orders},\"repaired_positions\":{repaired_positions}}}"
    );
    db::write_event(
        conn,
        "info",
        "executor",
        "reconciliation completed",
        &summary,
    )?;
    Ok(())
}

fn str_field(value: &serde_json::Value, key: &str) -> String {
    value
        .get(key)
        .and_then(serde_json::Value::as_str)
        .unwrap_or("")
        .to_string()
}

/// Map an exchange order side to a manual-intervention kind for the override gate.
fn intervention_kind_for_side(side: &str) -> InterventionKind {
    match side {
        "buy" => InterventionKind::Open,
        "sell" => InterventionKind::Close,
        _ => InterventionKind::Open,
    }
}

/// Build the in-memory override state from the persisted flag. The detection
/// functions need a ManualOverrideState; we seed it from executor_state so the
/// "already blocked → NoChange" path holds across restarts.
fn override_state_from(active: bool) -> crate::manual_override::ManualOverrideState {
    let mut s = crate::manual_override::ManualOverrideState::default();
    if active {
        s.enter("ETH/USDT:USDT");
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::PositionRecord;

    #[test]
    fn exchange_position_without_local_order_is_imported() {
        let local_order_intents = std::collections::HashSet::new();
        let exchange = PositionRecord {
            symbol: "ETH/USDT:USDT".to_string(),
            side: "long".to_string(),
            notional: 1000.0,
            entry_price: 3000.0,
            unrealized_pnl: 0.0,
            ownership: "system".to_string(),
            opened_at: None,
            adopted_at: None,
            source_intent_id: None,
            raw_json: "{}".to_string(),
        };

        let adopted = classify_position(exchange, &local_order_intents, "2026-07-01T00:00:00Z");

        assert_eq!(adopted.ownership, "imported");
        assert_eq!(adopted.adopted_at.as_deref(), Some("2026-07-01T00:00:00Z"));
    }

    #[test]
    fn exchange_position_matching_local_intent_is_system() {
        // Positive branch: an exchange position whose source_intent_id is in the
        // local set is system-owned (NOT imported). Catches a regression toward
        // unwrap_or(true)/is_some() that would silently reclassify everything.
        let mut local_order_intents = std::collections::HashSet::new();
        local_order_intents.insert("intent-7".to_string());
        let exchange = PositionRecord {
            symbol: "ETH/USDT:USDT".to_string(),
            side: "long".to_string(),
            notional: 1000.0,
            entry_price: 3000.0,
            unrealized_pnl: 0.0,
            ownership: "system".to_string(),
            opened_at: Some("2026-06-01T00:00:00Z".to_string()),
            adopted_at: None,
            source_intent_id: Some("intent-7".to_string()),
            raw_json: "{}".to_string(),
        };

        let adopted = classify_position(exchange, &local_order_intents, "2026-07-01T00:00:00Z");

        assert_eq!(adopted.ownership, "system");
        // system-owned keeps its prior opened_at; adoption timestamp not overwritten.
        assert_eq!(adopted.adopted_at, None);
    }
}
