use anyhow::Result;
use rusqlite::{params, Connection};

use crate::types::TradeIntent;

pub fn pending_intents(conn: &Connection) -> Result<Vec<TradeIntent>> {
    let mut stmt = conn.prepare(
        "select intent_id, symbol, side, action, target_notional, max_order_notional
         from trade_intents
         where status = 'pending'
         order by created_at asc"
    )?;
    let rows = stmt.query_map([], |row| {
        Ok(TradeIntent {
            intent_id: row.get(0)?,
            symbol: row.get(1)?,
            side: row.get(2)?,
            action: row.get(3)?,
            target_notional: row.get(4)?,
            max_order_notional: row.get(5)?,
        })
    })?;

    let mut intents = Vec::new();
    for row in rows {
        intents.push(row?);
    }
    Ok(intents)
}

pub fn reject_intent(conn: &Connection, intent_id: &str, reason: &str) -> Result<()> {
    conn.execute(
        "update trade_intents
         set status = 'rejected',
             processed_at = datetime('now'),
             error = ?
         where intent_id = ? and status = 'pending'",
        params![reason, intent_id],
    )?;
    Ok(())
}
