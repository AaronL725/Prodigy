use anyhow::Result;
use rusqlite::{params, Connection};

use crate::types::{FillRecord, OrderRecord, PositionRecord, TradeIntent};

pub fn pending_intents(conn: &Connection) -> Result<Vec<TradeIntent>> {
    let mut stmt = conn.prepare(
        "select intent_id, symbol, side, action, target_notional, max_order_notional
         from trade_intents
         where status = 'pending'
         order by created_at asc",
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

pub fn accept_intent(conn: &Connection, intent_id: &str) -> Result<bool> {
    let rows = conn.execute(
        "update trade_intents
         set status = 'accepted', processed_at = datetime('now'), error = null
         where intent_id = ? and status = 'pending'",
        params![intent_id],
    )?;
    Ok(rows == 1)
}

pub fn mark_intent_executed(conn: &Connection, intent_id: &str) -> Result<()> {
    conn.execute(
        "update trade_intents
         set status = 'executed', processed_at = datetime('now'), error = null
         where intent_id = ?",
        params![intent_id],
    )?;
    Ok(())
}

pub fn fail_intent(conn: &Connection, intent_id: &str, reason: &str) -> Result<()> {
    conn.execute(
        "update trade_intents
         set status = 'failed', processed_at = datetime('now'), error = ?
         where intent_id = ?",
        params![reason, intent_id],
    )?;
    Ok(())
}

pub fn upsert_order(conn: &Connection, order: &OrderRecord) -> Result<()> {
    conn.execute(
        "insert into orders (
          order_id, exchange_order_id, client_oid, intent_id, symbol, side,
          action, order_type, status, price, size, filled_size, created_at,
          updated_at, attempt, raw_json, last_error
        ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'),
          datetime('now'), ?, ?, ?)
        on conflict(client_oid) do update set
          exchange_order_id = excluded.exchange_order_id,
          status = excluded.status,
          price = excluded.price,
          size = excluded.size,
          filled_size = excluded.filled_size,
          updated_at = datetime('now'),
          attempt = excluded.attempt,
          raw_json = excluded.raw_json,
          last_error = excluded.last_error",
        params![
            order.order_id,
            order.exchange_order_id,
            order.client_oid,
            order.intent_id,
            order.symbol,
            order.side,
            order.action,
            order.order_type,
            order.status,
            order.price,
            order.size,
            order.filled_size,
            order.attempt,
            order.raw_json,
            order.last_error,
        ],
    )?;
    Ok(())
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

/// Upsert a position row by symbol (PK). Reconciliation writes exchange-truth
/// positions here; system-owned keeps its source_intent_id, imported gets the
/// adoption timestamp set by classify_position before this call.
pub fn upsert_position(conn: &Connection, position: &PositionRecord) -> Result<()> {
    conn.execute(
        "insert into positions (
           symbol, side, notional, entry_price, unrealized_pnl, updated_at,
           ownership, opened_at, adopted_at, source_intent_id, raw_json
         ) values (?, ?, ?, ?, ?, datetime('now'), ?, ?, ?, ?, ?)
         on conflict(symbol) do update set
           side = excluded.side,
           notional = excluded.notional,
           entry_price = excluded.entry_price,
           unrealized_pnl = excluded.unrealized_pnl,
           updated_at = datetime('now'),
           ownership = excluded.ownership,
           opened_at = excluded.opened_at,
           adopted_at = excluded.adopted_at,
           source_intent_id = excluded.source_intent_id,
           raw_json = excluded.raw_json",
        params![
            position.symbol,
            position.side,
            position.notional,
            position.entry_price,
            position.unrealized_pnl,
            position.ownership,
            position.opened_at,
            position.adopted_at,
            position.source_intent_id,
            position.raw_json,
        ],
    )?;
    Ok(())
}

/// client_oids of orders we already have locally (used to detect exchange orders
/// we're missing → repair). Reconciliation inserts the missing ones.
pub fn local_order_client_oids(conn: &Connection) -> Result<std::collections::HashSet<String>> {
    let mut stmt = conn.prepare("select client_oid from orders")?;
    let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
    let mut set = std::collections::HashSet::new();
    for row in rows {
        set.insert(row?);
    }
    Ok(set)
}

/// intent_ids of orders the executor has placed (filled or submitted). Used by
/// reconcile to tell a system position (we have a local order for this symbol)
/// from an imported/manual one. This is more reliable than source_intent_id on
/// the position row because exchange all-position doesn't carry our intent_id.
pub fn local_order_intent_ids(conn: &Connection) -> Result<std::collections::HashSet<String>> {
    let mut stmt =
        conn.prepare("select distinct intent_id from orders where intent_id is not null")?;
    let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
    let mut set = std::collections::HashSet::new();
    for row in rows {
        set.insert(row?);
    }
    Ok(set)
}

/// Signed net base the system expects to hold for a symbol, summed across ALL
/// filled orders by direction (buy = +, sell = −): a filled open-long adds, a
/// filled close-long (a sell) subtracts. Reconcile compares this against the
/// exchange position size to detect a client manually adding, reducing, or
/// closing a system-owned position. Returns (signed_base, side): e.g.
/// +0.10/"long", -0.10/"short", or 0.0/"" when the system holds nothing.
pub fn system_net_base_for_symbol(
    conn: &Connection,
    symbol: &str,
) -> Result<(f64, &'static str)> {
    let mut stmt = conn.prepare(
        "select side, filled_size from orders
         where symbol = ? and status = 'filled' and filled_size > 0",
    )?;
    let rows = stmt.query_map(params![symbol], |row| {
        let side: String = row.get(0)?;
        let filled: f64 = row.get(1)?;
        Ok((side, filled))
    })?;
    let mut net = 0.0;
    for row in rows {
        let (side, filled) = row?;
        net += if side == "sell" { -filled } else { filled };
    }
    let side = if net > 0.0 {
        "long"
    } else if net < 0.0 {
        "short"
    } else {
        ""
    };
    Ok((net, side))
}

/// Insert a fill record. Idempotent by fill_id (PK) via insert-or-ignore.
pub fn insert_fill(conn: &Connection, fill: &FillRecord) -> Result<()> {
    conn.execute(
        "insert or ignore into fills (
           fill_id, order_id, symbol, side, price, size, fee, created_at,
           trade_id, client_oid, raw_json
         ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        params![
            fill.fill_id,
            fill.order_id,
            fill.symbol,
            fill.side,
            fill.price,
            fill.size,
            fill.fee,
            fill.created_at,
            fill.trade_id,
            fill.client_oid,
            fill.raw_json,
        ],
    )?;
    Ok(())
}

/// Insert an equity snapshot row for the audit trail.
pub fn insert_equity_snapshot(
    conn: &Connection,
    equity: f64,
    available_margin: f64,
    unrealized_pnl: f64,
    realized_pnl_24h: f64,
) -> Result<()> {
    conn.execute(
        "insert into equity_snapshots (
           snapshot_id, created_at, equity, available_margin,
           unrealized_pnl, realized_pnl_24h
         ) values (lower(hex(randomblob(16))), datetime('now'), ?, ?, ?, ?)",
        params![equity, available_margin, unrealized_pnl, realized_pnl_24h],
    )?;
    Ok(())
}

pub fn write_event(
    conn: &Connection,
    severity: &str,
    component: &str,
    message: &str,
    payload_json: &str,
) -> Result<()> {
    conn.execute(
        "insert into events (
          event_id, created_at, severity, component, message, payload_json
        ) values (lower(hex(randomblob(16))), datetime('now'), ?, ?, ?, ?)",
        params![severity, component, message, payload_json],
    )?;
    Ok(())
}

pub fn set_executor_state(conn: &Connection, key: &str, value: &str) -> Result<()> {
    conn.execute(
        "insert into executor_state (key, value, updated_at)
         values (?, ?, datetime('now'))
         on conflict(key) do update set
           value = excluded.value,
           updated_at = datetime('now')",
        params![key, value],
    )?;
    Ok(())
}

pub fn get_executor_state(conn: &Connection, key: &str) -> Result<Option<String>> {
    let mut stmt = conn.prepare("select value from executor_state where key = ?")?;
    let mut rows = stmt.query(params![key])?;
    Ok(match rows.next()? {
        Some(row) => Some(row.get(0)?),
        None => None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    fn memory_db() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(include_str!("../../../schema/001_initial.sql"))
            .unwrap();
        conn.execute_batch(include_str!("../../../schema/002_execution.sql"))
            .unwrap();
        conn
    }

    #[test]
    fn accept_intent_is_idempotent() {
        let conn = memory_db();
        conn.execute(
            "insert into trade_intents (
              intent_id, created_at, symbol, side, action, target_notional,
              max_order_notional, status, source
            ) values ('i1', '2026-07-01T00:00:00Z', 'ETH/USDT:USDT',
              'long', 'open', 100, 100, 'pending', 'test')",
            [],
        )
        .unwrap();

        assert!(accept_intent(&conn, "i1").unwrap());
        assert!(!accept_intent(&conn, "i1").unwrap());
    }

    #[test]
    fn upsert_order_keeps_client_oid_unique() {
        let conn = memory_db();
        let order = OrderRecord {
            order_id: "exchange-1".to_string(),
            exchange_order_id: Some("exchange-1".to_string()),
            client_oid: "client-1".to_string(),
            // ponytail: intent_id None so the FK to trade_intents isn't tripped;
            // this test is about client_oid upsert uniqueness, not the intent relation.
            intent_id: None,
            symbol: "ETH/USDT:USDT".to_string(),
            side: "buy".to_string(),
            action: "open".to_string(),
            order_type: "limit".to_string(),
            status: "live".to_string(),
            price: Some(3000.0),
            size: 0.01,
            filled_size: 0.0,
            attempt: 1,
            raw_json: "{}".to_string(),
            last_error: None,
        };

        upsert_order(&conn, &order).unwrap();
        upsert_order(
            &conn,
            &OrderRecord {
                status: "filled".to_string(),
                ..order
            },
        )
        .unwrap();

        let status: String = conn
            .query_row(
                "select status from orders where client_oid = 'client-1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(status, "filled");
    }

    #[test]
    fn executor_state_upserts_and_reads_back() {
        let conn = memory_db();
        assert_eq!(
            get_executor_state(&conn, "manual_override:ETH/USDT:USDT").unwrap(),
            None
        );
        set_executor_state(&conn, "manual_override:ETH/USDT:USDT", "active").unwrap();
        assert_eq!(
            get_executor_state(&conn, "manual_override:ETH/USDT:USDT").unwrap(),
            Some("active".to_string())
        );
        // upsert overwrites the same key (no duplicate PK error)
        set_executor_state(&conn, "manual_override:ETH/USDT:USDT", "cleared").unwrap();
        assert_eq!(
            get_executor_state(&conn, "manual_override:ETH/USDT:USDT").unwrap(),
            Some("cleared".to_string())
        );
    }

    #[test]
    fn upsert_position_reconciles_exchange_truth() {
        // ponytail: reconciliation repairs a missing local position by upserting
        // exchange truth (ownership imported, adoption timestamp set by classifier).
        use crate::types::PositionRecord;
        let conn = memory_db();
        let imported = PositionRecord {
            symbol: "ETH/USDT:USDT".to_string(),
            side: "long".to_string(),
            notional: 1000.0,
            entry_price: 3000.0,
            unrealized_pnl: 12.0,
            ownership: "imported".to_string(),
            opened_at: Some("2026-07-01T00:00:00Z".to_string()),
            adopted_at: Some("2026-07-01T00:00:00Z".to_string()),
            source_intent_id: None,
            raw_json: "{}".to_string(),
        };
        upsert_position(&conn, &imported).unwrap();
        // re-reconcile with updated exchange fields → upsert overwrites mutable cols
        let updated = PositionRecord {
            unrealized_pnl: 50.0,
            ..imported
        };
        upsert_position(&conn, &updated).unwrap();
        let row = conn
            .query_row(
                "select unrealized_pnl, ownership from positions where symbol='ETH/USDT:USDT'",
                [],
                |r| Ok((r.get::<_, f64>(0)?, r.get::<_, String>(1)?)),
            )
            .unwrap();
        assert_eq!(row.0, 50.0);
        assert_eq!(row.1, "imported");
    }

    #[test]
    fn insert_fill_and_equity_snapshot_persist() {
        use crate::types::{FillRecord, OrderRecord};
        let conn = memory_db();
        // FK: fills.order_id references orders.order_id — insert a parent order first.
        let order = OrderRecord {
            order_id: "order-1".to_string(),
            exchange_order_id: Some("order-1".to_string()),
            client_oid: "client-1".to_string(),
            intent_id: None,
            symbol: "ETH/USDT:USDT".to_string(),
            side: "buy".to_string(),
            action: "open".to_string(),
            order_type: "limit".to_string(),
            status: "filled".to_string(),
            price: Some(3000.0),
            size: 0.01,
            filled_size: 0.01,
            attempt: 1,
            raw_json: "{}".to_string(),
            last_error: None,
        };
        upsert_order(&conn, &order).unwrap();
        let fill = FillRecord {
            fill_id: "fill-1".to_string(),
            order_id: "order-1".to_string(),
            trade_id: Some("trade-1".to_string()),
            client_oid: Some("client-1".to_string()),
            symbol: "ETH/USDT:USDT".to_string(),
            side: "buy".to_string(),
            price: 3000.0,
            size: 0.01,
            fee: 0.006,
            created_at: "2026-07-01T00:00:00Z".to_string(),
            raw_json: "{}".to_string(),
        };
        insert_fill(&conn, &fill).unwrap();
        let count: i64 = conn
            .query_row(
                "select count(*) from fills where fill_id = 'fill-1'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);

        insert_equity_snapshot(&conn, 5000.0, 4500.0, -50.0, 0.0).unwrap();
        let eq: f64 = conn
            .query_row(
                "select equity from equity_snapshots order by created_at desc limit 1",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(eq, 5000.0);
    }

    #[test]
    fn system_net_base_sums_filled_opens_minus_closes_and_signs_by_side() {
        // Reconcile uses this to detect a client manually changing a system
        // position. The net must subtract closes from opens and sign long+/short-.
        let conn = memory_db();
        conn.execute(
            "insert into trade_intents (intent_id, created_at, symbol, side, action,
               target_notional, max_order_notional, status, source)
             values ('i1','2026-07-01T00:00:00Z','ETH/USDT:USDT','long','open',100,100,'executed','t')",
            [],
        )
        .unwrap();
        // Opened 0.05 long (filled), then opened another 0.03 long (filled) → net +0.08.
        for (oid, sz) in [("o1", 0.05), ("o2", 0.03)] {
            conn.execute(
                "insert into orders (order_id, client_oid, intent_id, symbol, side, action,
                   order_type, status, price, size, filled_size, created_at, updated_at)
                 values (?1, ?2, 'i1', 'ETH/USDT:USDT', 'buy', 'open', 'limit', 'filled',
                   3000, ?3, ?3, '2026-07-01T00:00:00Z', '2026-07-01T00:00:00Z')",
                params![oid, oid, sz],
            )
            .unwrap();
        }
        let (net, side) = system_net_base_for_symbol(&conn, "ETH/USDT:USDT").unwrap();
        assert!((net - 0.08).abs() < 1e-9 && side == "long");

        // A filled close of 0.02 reduces the net to +0.06 long.
        conn.execute(
            "insert into trade_intents (intent_id, created_at, symbol, side, action,
               target_notional, max_order_notional, status, source)
             values ('i2','2026-07-01T00:00:00Z','ETH/USDT:USDT','long','close',100,100,'executed','t')",
            [],
        )
        .unwrap();
        conn.execute(
            "insert into orders (order_id, client_oid, intent_id, symbol, side, action,
               order_type, status, price, size, filled_size, created_at, updated_at)
             values ('o3','o3','i2','ETH/USDT:USDT','sell','close','market','filled',
               3000, 0.02, 0.02, '2026-07-01T00:00:00Z', '2026-07-01T00:00:00Z')",
            [],
        )
        .unwrap();
        let (net2, side2) = system_net_base_for_symbol(&conn, "ETH/USDT:USDT").unwrap();
        assert!((net2 - 0.06).abs() < 1e-9 && side2 == "long");

        // Unfilled/submitted orders don't count (not yet part of the position).
        conn.execute(
            "insert into orders (order_id, client_oid, symbol, side, action, order_type,
               status, price, size, filled_size, created_at, updated_at)
             values ('o4','o4','ETH/USDT:USDT','buy','open','limit','submitted',
               3000, 0.10, 0.0, '2026-07-01T00:00:00Z', '2026-07-01T00:00:00Z')",
            [],
        )
        .unwrap();
        let (net3, _) = system_net_base_for_symbol(&conn, "ETH/USDT:USDT").unwrap();
        assert!((net3 - 0.06).abs() < 1e-9);
    }
}
