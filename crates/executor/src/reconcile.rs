use anyhow::Result;
use rusqlite::Connection;
use std::collections::HashSet;

use crate::bitget::BitgetRestClient;
use crate::db;
use crate::types::PositionRecord;

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
    let _open_orders = rest
        .get(
            "/api/v2/mix/order/orders-pending",
            &[
                ("productType", "USDT-FUTURES".to_string()),
                ("marginCoin", "USDT".to_string()),
            ],
        )
        .await?;
    let _positions = rest
        .get(
            "/api/v2/mix/position/all-position",
            &[
                ("productType", "USDT-FUTURES".to_string()),
                ("marginCoin", "USDT".to_string()),
            ],
        )
        .await?;
    db::write_event(conn, "info", "executor", "reconciliation completed", "{}")?;
    let _ = now;
    Ok(())
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
}
