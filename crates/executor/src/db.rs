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

/// Remove the local positions row for a symbol. Called by reconcile when the
/// exchange no longer lists a position for it (manual full-close): exchange state
/// wins, so the local row must not keep reporting a position Bitget no longer
/// holds. Idempotent (no-op when no row exists).
pub fn clear_local_position(conn: &Connection, symbol: &str) -> Result<()> {
    conn.execute("delete from positions where symbol = ?", params![symbol])?;
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

/// Signed net base the system expects to hold for a symbol, summed across all
/// SYSTEM orders with any filled base, by direction (buy = +, sell = −): a filled
/// open-long adds, a filled close-long (a sell) subtracts. Reconcile compares this
/// against the exchange position size to detect a client manually adding,
/// reducing, or closing a system-owned position. Returns (signed_base, side):
/// e.g. +0.10/"long", -0.10/"short", or 0.0/"" when the system holds nothing.
///
/// Counts by filled_size, not terminal fill status: a PARTIAL fill keeps the order 'submitted'
/// (set_order_fill_state / set_order_filled_from_detail only flip to 'filled' at
/// the full ordered size) but its filled base is real position the system holds.
/// Keying on status='filled' would zero-out partials and mis-classify the
/// position as imported. Only counts orders WE placed (intent_id is not null): a
/// manual/imported order repaired into the orders table has no intent_id and must
/// not pollute the system's expected position. Orders marked externally_closed are
/// historical fills for a position the client already closed outside the executor,
/// so they no longer contribute to current system exposure.
pub fn system_net_base_for_symbol(conn: &Connection, symbol: &str) -> Result<(f64, &'static str)> {
    let mut stmt = conn.prepare(
        "select side, filled_size from orders
         where symbol = ? and filled_size > 0 and intent_id is not null
           and status != 'externally_closed'",
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

/// trade_ids of fills we already recorded (reconcile dedupes missing-fill repair
/// against this so a fill isn't inserted twice across runs).
pub fn local_fill_trade_ids(conn: &Connection) -> Result<std::collections::HashSet<String>> {
    let mut stmt = conn.prepare("select trade_id from fills where trade_id is not null")?;
    let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
    let mut set = std::collections::HashSet::new();
    for row in rows {
        set.insert(row?);
    }
    Ok(set)
}

/// Map of exchange order_id → our client_oid for orders we placed. The exchange
/// fillList carries orderId but not clientOid, so reconcile joins a fill to our
/// order via this map to populate the fills.client_oid FK.
pub fn local_order_id_to_client_oid(
    conn: &Connection,
) -> Result<std::collections::HashMap<String, String>> {
    let mut stmt =
        conn.prepare("select order_id, client_oid from orders where order_id is not null")?;
    let rows = stmt.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;
    let mut map = std::collections::HashMap::new();
    for row in rows {
        let (oid, cid) = row?;
        map.insert(oid, cid);
    }
    Ok(map)
}

/// Set an order's filled_size/status directly from the exchange order-detail's
/// CUMULATIVE baseVolume (as observed by the caller), WITHOUT writing a fills row.
/// Used by reconcile's missing-pending-order second-confirm: order detail's
/// baseVolume is cumulative (not a single trade), so it must NOT be inserted into
/// the per-trade fills ledger (that would double-count once the real fillList
/// arrives and mask partials). The fills table stays the per-trade record from
/// fillList repair; only orders.filled_size/status are synced here so
/// system_net_base sees the position. Flips to 'filled' once filled reaches the
/// ordered size (within dust); a partial keeps the order working (not 'filled').
pub fn set_order_filled_from_detail(
    conn: &Connection,
    order_id: &str,
    filled_size: f64,
) -> Result<()> {
    const DUST_BASE: f64 = 1e-6;
    if filled_size <= 0.0 {
        return Ok(());
    }
    let ordered: f64 = conn
        .query_row(
            "select coalesce(size, 0) from orders where order_id = ?",
            params![order_id],
            |row| row.get(0),
        )
        .unwrap_or(0.0);
    if ordered > 0.0 && filled_size + DUST_BASE >= ordered {
        conn.execute(
            "update orders set status = 'filled', filled_size = ?, updated_at = datetime('now')
             where order_id = ?",
            params![filled_size, order_id],
        )?;
    } else {
        conn.execute(
            "update orders set filled_size = ?, updated_at = datetime('now')
             where order_id = ?",
            params![filled_size, order_id],
        )?;
    }
    Ok(())
}

/// Sync an order's status/filled_size from the fills recorded against it. Called
/// after reconcile inserts a missing fill so the order row reflects exchange
/// truth: filled_size = max(existing, sum of its fills' base size); status flips
/// to 'filled' once that sum reaches the ordered size (minus a dust epsilon). A
/// partial sum updates filled_size but leaves the status untouched (still
/// working). Without this, a crash-then-repair leaves the order
/// 'submitted'/filled_size=0, so the system net base stays 0 and reconcile
/// mis-fires manual-override drift.
///
/// Takes max(existing, sum(fills)) — never reduces filled_size. The execution
/// path / set_order_filled_from_detail may have already set filled_size from
/// order detail (the full cumulative fill). If the exchange fillList temporarily
/// returns only a subset of trades, sum(fills) would be smaller and blindly
/// overwriting would UNDERCOUNT real exposure (breaks system_net_base / override
/// detection). A lagging fillList can only ever raise filled_size as it catches
/// up, never lower it.
pub fn sync_order_fill_state(conn: &Connection, order_id: &str) -> Result<()> {
    const DUST_BASE: f64 = 1e-6;
    let sum_fills: f64 = conn
        .query_row(
            "select coalesce(sum(size), 0) from fills where order_id = ?",
            params![order_id],
            |row| row.get(0),
        )
        .unwrap_or(0.0);
    if sum_fills <= 0.0 {
        return Ok(());
    }
    let (existing_filled, ordered): (f64, f64) = conn
        .query_row(
            "select coalesce(filled_size, 0), coalesce(size, 0) from orders where order_id = ?",
            params![order_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap_or((0.0, 0.0));
    // Never let a lagging fillList reduce filled_size below what execution/order
    // detail already established.
    let filled = existing_filled.max(sum_fills);
    // Reached the ordered size (within dust) → terminal fill; else partial.
    if ordered > 0.0 && filled + DUST_BASE >= ordered {
        conn.execute(
            "update orders set status = 'filled', filled_size = ?, updated_at = datetime('now')
             where order_id = ?",
            params![filled, order_id],
        )?;
    } else {
        conn.execute(
            "update orders set filled_size = ?, updated_at = datetime('now')
             where order_id = ?",
            params![filled, order_id],
        )?;
    }
    Ok(())
}

/// Retire the symbol's filled system orders (intent_id set, filled_size > 0) to
/// 'externally_closed' after a client manually closed the whole position in the
/// Bitget UI. The historical filled_size stays intact for audit; current exposure
/// is zero because system_net_base_for_symbol excludes externally_closed rows.
/// Returns the number of rows changed.
pub fn mark_system_orders_externally_closed(conn: &Connection, symbol: &str) -> Result<usize> {
    let changed = conn.execute(
        "update orders set status = 'externally_closed', updated_at = datetime('now')
         where symbol = ? and intent_id is not null and filled_size > 0",
        params![symbol],
    )?;
    Ok(changed)
}

/// System orders (intent_id set) still in a working ('submitted') state for a
/// symbol. Reconcile compares these against the exchange pending list: a working
/// system order the exchange no longer lists was either cancelled outside the
/// executor (manual cancel) OR filled but briefly absent from pending. Reconcile
/// second-confirms via order detail, using the returned ordered size to tell full
/// vs partial fill. Returns (client_oid, order_id, ordered_size) triples.
pub fn local_working_system_orders(
    conn: &Connection,
    symbol: &str,
) -> Result<Vec<(String, String, f64)>> {
    let mut stmt = conn.prepare(
        "select client_oid, order_id, coalesce(size, 0) from orders
         where symbol = ? and intent_id is not null and status = 'submitted'",
    )?;
    let rows = stmt.query_map(params![symbol], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, f64>(2)?,
        ))
    })?;
    let mut out = Vec::new();
    for row in rows {
        out.push(row?);
    }
    Ok(out)
}

/// Mark a system order externally_cancelled after the client cancelled it in the
/// Bitget UI (it vanished from the exchange pending list without filling). The
/// spec requires system orders manually cancelled to be recorded as such.
pub fn mark_order_externally_cancelled(conn: &Connection, client_oid: &str) -> Result<()> {
    conn.execute(
        "update orders set status = 'externally_cancelled', updated_at = datetime('now')
         where client_oid = ?",
        params![client_oid],
    )?;
    Ok(())
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

    #[test]
    fn system_net_base_ignores_orders_without_intent_id() {
        // "System-expected base" must only count orders WE placed (intent_id set).
        // An imported/manual order (no intent_id) that reconcile inserted must not
        // pollute the system net — else drift detection compares against a base
        // that includes manual size and mis-classifies.
        let conn = memory_db();
        // A filled order with NO intent_id (e.g. a manual/imported order row).
        conn.execute(
            "insert into orders (order_id, client_oid, symbol, side, action, order_type,
               status, price, size, filled_size, created_at, updated_at)
             values ('m1','m1','ETH/USDT:USDT','buy','open','market','filled',
               3000, 0.20, 0.20, '2026-07-01T00:00:00Z', '2026-07-01T00:00:00Z')",
            [],
        )
        .unwrap();
        let (net, side) = system_net_base_for_symbol(&conn, "ETH/USDT:USDT").unwrap();
        assert!(
            net.abs() < 1e-9 && side.is_empty(),
            "manual order (no intent_id) must not count toward system net, got {net}/{side}"
        );
    }

    #[test]
    fn system_net_base_counts_partial_fills_still_in_submitted() {
        // F1: a partial fill keeps the order 'submitted' (set_order_fill_state /
        // set_order_filled_from_detail only flip to 'filled' at the full ordered
        // size). That partial base is REAL position the system holds, so net base
        // must count it — otherwise reconcile mis-classifies the position as
        // imported and mis-fires manual override, or skips local cleanup on a
        // manual full-close (sys_base wrongly reads 0).
        let conn = memory_db();
        conn.execute(
            "insert into trade_intents (intent_id, created_at, symbol, side, action,
               target_notional, max_order_notional, status, source)
             values ('i1','2026-07-01T00:00:00Z','ETH/USDT:USDT','long','open',100,100,'executed','t')",
            [],
        )
        .unwrap();
        // Ordered 0.05 but only 0.02 filled → stays 'submitted' with filled_size 0.02.
        conn.execute(
            "insert into orders (order_id, client_oid, intent_id, symbol, side, action,
               order_type, status, price, size, filled_size, created_at, updated_at)
             values ('o1','o1','i1','ETH/USDT:USDT','buy','open','limit','submitted',
               3000, 0.05, 0.02, '2026-07-01T00:00:00Z', '2026-07-01T00:00:00Z')",
            [],
        )
        .unwrap();
        let (net, side) = system_net_base_for_symbol(&conn, "ETH/USDT:USDT").unwrap();
        assert!(
            (net - 0.02).abs() < 1e-9 && side == "long",
            "partial fill in 'submitted' must count toward system net, got {net}/{side}"
        );

        // A full fill ('filled', filled_size = ordered) still counts as before.
        conn.execute(
            "insert into orders (order_id, client_oid, intent_id, symbol, side, action,
               order_type, status, price, size, filled_size, created_at, updated_at)
             values ('o2','o2','i1','ETH/USDT:USDT','buy','open','limit','filled',
               3000, 0.03, 0.03, '2026-07-01T00:00:00Z', '2026-07-01T00:00:00Z')",
            [],
        )
        .unwrap();
        let (net2, side2) = system_net_base_for_symbol(&conn, "ETH/USDT:USDT").unwrap();
        assert!((net2 - 0.05).abs() < 1e-9 && side2 == "long");

        // A 'submitted' order with NO fill (filled_size 0) still contributes nothing.
        conn.execute(
            "insert into orders (order_id, client_oid, intent_id, symbol, side, action,
               order_type, status, price, size, filled_size, created_at, updated_at)
             values ('o3','o3','i1','ETH/USDT:USDT','buy','open','limit','submitted',
               3000, 0.10, 0.0, '2026-07-01T00:00:00Z', '2026-07-01T00:00:00Z')",
            [],
        )
        .unwrap();
        let (net3, _) = system_net_base_for_symbol(&conn, "ETH/USDT:USDT").unwrap();
        assert!(
            (net3 - 0.05).abs() < 1e-9,
            "unfilled submitted order must not count"
        );
    }

    #[test]
    fn sync_order_fill_state_marks_filled_and_sums_base() {
        // After reconcile inserts a missing fill, the parent order must be updated
        // so system_net_base sees it: a 'submitted' order whose fills now sum to
        // its ordered size flips to 'filled' with filled_size = summed base. Before
        // this sync, a crash-then-repair leaves the order 'submitted'/filled_size=0,
        // so the system net stays 0 and reconcile mis-fires manual-override drift.
        let conn = memory_db();
        conn.execute(
            "insert into trade_intents (intent_id, created_at, symbol, side, action,
               target_notional, max_order_notional, status, source)
             values ('i1','2026-07-01T00:00:00Z','ETH/USDT:USDT','long','open',100,100,'accepted','t')",
            [],
        )
        .unwrap();
        // Order was placed (submitted) but its fill was never recorded locally.
        conn.execute(
            "insert into orders (order_id, client_oid, intent_id, symbol, side, action,
               order_type, status, price, size, filled_size, created_at, updated_at)
             values ('o1','o1','i1','ETH/USDT:USDT','buy','open','limit','submitted',
               3000, 0.05, 0.0, '2026-07-01T00:00:00Z', '2026-07-01T00:00:00Z')",
            [],
        )
        .unwrap();
        // system net is 0 (order still submitted) — the bug reconcile must fix.
        let (net_before, _) = system_net_base_for_symbol(&conn, "ETH/USDT:USDT").unwrap();
        assert!(net_before.abs() < 1e-9);

        // Reconcile inserts the missing fill (0.05), then syncs the order.
        let fill = FillRecord {
            fill_id: "f1".to_string(),
            order_id: "o1".to_string(),
            trade_id: Some("t1".to_string()),
            client_oid: Some("o1".to_string()),
            symbol: "ETH/USDT:USDT".to_string(),
            side: "buy".to_string(),
            price: 3000.0,
            size: 0.05,
            fee: 0.03,
            created_at: "2026-07-01T00:00:00Z".to_string(),
            raw_json: "{}".to_string(),
        };
        insert_fill(&conn, &fill).unwrap();
        sync_order_fill_state(&conn, "o1").unwrap();

        let (status, filled): (String, f64) = conn
            .query_row(
                "select status, filled_size from orders where order_id = 'o1'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(status, "filled");
        assert!((filled - 0.05).abs() < 1e-9);
        // system net now reflects the repaired fill.
        let (net_after, side) = system_net_base_for_symbol(&conn, "ETH/USDT:USDT").unwrap();
        assert!((net_after - 0.05).abs() < 1e-9 && side == "long");
    }

    #[test]
    fn sync_order_fill_state_leaves_partial_as_submitted() {
        // A partial repair (fills sum below ordered size) must update filled_size
        // but NOT prematurely mark the order 'filled'.
        let conn = memory_db();
        conn.execute(
            "insert into trade_intents (intent_id, created_at, symbol, side, action,
               target_notional, max_order_notional, status, source)
             values ('i1','2026-07-01T00:00:00Z','ETH/USDT:USDT','long','open',100,100,'accepted','t')",
            [],
        )
        .unwrap();
        conn.execute(
            "insert into orders (order_id, client_oid, intent_id, symbol, side, action,
               order_type, status, price, size, filled_size, created_at, updated_at)
             values ('o1','o1','i1','ETH/USDT:USDT','buy','open','limit','submitted',
               3000, 0.05, 0.0, '2026-07-01T00:00:00Z', '2026-07-01T00:00:00Z')",
            [],
        )
        .unwrap();
        let fill = FillRecord {
            fill_id: "f1".to_string(),
            order_id: "o1".to_string(),
            trade_id: Some("t1".to_string()),
            client_oid: Some("o1".to_string()),
            symbol: "ETH/USDT:USDT".to_string(),
            side: "buy".to_string(),
            price: 3000.0,
            size: 0.02,
            fee: 0.0,
            created_at: "2026-07-01T00:00:00Z".to_string(),
            raw_json: "{}".to_string(),
        };
        insert_fill(&conn, &fill).unwrap();
        sync_order_fill_state(&conn, "o1").unwrap();

        let (status, filled): (String, f64) = conn
            .query_row(
                "select status, filled_size from orders where order_id = 'o1'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(status, "submitted");
        assert!((filled - 0.02).abs() < 1e-9);
    }

    #[test]
    fn sync_order_fill_state_never_reduces_filled_size() {
        // Regression: the execution path / set_order_filled_from_detail may have
        // already set orders.filled_size = 0.05 from order detail. If Bitget
        // fillList temporarily returns only ONE trade (0.02) — a lagging/partial
        // fillList — reconcile inserts it and calls sync_order_fill_state, which
        // used to overwrite filled_size 0.05 → 0.02 (SUM(fills)). That undercounted
        // real exposure and broke system_net_base / override detection. sync must
        // take max(existing, sum(fills)) so it never goes backwards.
        let conn = memory_db();
        conn.execute(
            "insert into trade_intents (intent_id, created_at, symbol, side, action,
               target_notional, max_order_notional, status, source)
             values ('i1','2026-07-01T00:00:00Z','ETH/USDT:USDT','long','open',100,100,'executed','t')",
            [],
        )
        .unwrap();
        // Order already reflects the full 0.05 fill from order detail.
        conn.execute(
            "insert into orders (order_id, client_oid, intent_id, symbol, side, action,
               order_type, status, price, size, filled_size, created_at, updated_at)
             values ('o1','o1','i1','ETH/USDT:USDT','buy','open','limit','filled',
               3000, 0.05, 0.05, '2026-07-01T00:00:00Z', '2026-07-01T00:00:00Z')",
            [],
        )
        .unwrap();
        // fillList so far only carries one trade (0.02); reconcile inserts it.
        let partial = FillRecord {
            fill_id: "f1".to_string(),
            order_id: "o1".to_string(),
            trade_id: Some("t1".to_string()),
            client_oid: Some("o1".to_string()),
            symbol: "ETH/USDT:USDT".to_string(),
            side: "buy".to_string(),
            price: 3000.0,
            size: 0.02,
            fee: 0.0,
            created_at: "2026-07-01T00:00:00Z".to_string(),
            raw_json: "{}".to_string(),
        };
        insert_fill(&conn, &partial).unwrap();
        sync_order_fill_state(&conn, "o1").unwrap();

        let filled_size: f64 = conn
            .query_row(
                "select filled_size from orders where order_id = 'o1'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(
            (filled_size - 0.05).abs() < 1e-9,
            "sync must not reduce filled_size below the existing 0.05; got {filled_size}"
        );

        // When the rest of fillList arrives (0.03), the sum now matches 0.05 and
        // filled_size stays 0.05 (the full truth), not regressed.
        let rest = FillRecord {
            fill_id: "f2".to_string(),
            size: 0.03,
            trade_id: Some("t2".to_string()),
            ..partial
        };
        insert_fill(&conn, &rest).unwrap();
        sync_order_fill_state(&conn, "o1").unwrap();
        let filled_size_after: f64 = conn
            .query_row(
                "select filled_size from orders where order_id = 'o1'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!((filled_size_after - 0.05).abs() < 1e-9);
    }

    #[test]
    fn mark_system_orders_externally_closed_zeroes_system_net() {
        // When a client manually closes a system position, the exchange stops
        // returning a position row and our local filled opens still imply a
        // nonzero net. Reconcile marks the contributing system orders
        // 'externally_closed' so the net base returns to 0 and the manual-close
        // drift doesn't re-fire on every subsequent pass (flapping).
        let conn = memory_db();
        conn.execute(
            "insert into trade_intents (intent_id, created_at, symbol, side, action,
               target_notional, max_order_notional, status, source)
             values ('i1','2026-07-01T00:00:00Z','ETH/USDT:USDT','long','open',100,100,'executed','t')",
            [],
        )
        .unwrap();
        conn.execute(
            "insert into orders (order_id, client_oid, intent_id, symbol, side, action,
               order_type, status, price, size, filled_size, created_at, updated_at)
             values ('o1','o1','i1','ETH/USDT:USDT','buy','open','limit','filled',
               3000, 0.05, 0.05, '2026-07-01T00:00:00Z', '2026-07-01T00:00:00Z')",
            [],
        )
        .unwrap();
        let (net, side) = system_net_base_for_symbol(&conn, "ETH/USDT:USDT").unwrap();
        assert!((net - 0.05).abs() < 1e-9 && side == "long");

        let changed = mark_system_orders_externally_closed(&conn, "ETH/USDT:USDT").unwrap();
        assert!(changed >= 1, "expected at least one system order retired");

        // Net base is now 0 because externally_closed no longer counts as current
        // exposure, but historical filled_size remains intact for audit.
        let (net_after, side_after) = system_net_base_for_symbol(&conn, "ETH/USDT:USDT").unwrap();
        assert!(
            net_after.abs() < 1e-9 && side_after.is_empty(),
            "system net must be flat after external close, got {net_after}/{side_after}"
        );
        let (status, filled_size): (String, f64) = conn
            .query_row(
                "select status, filled_size from orders where order_id = 'o1'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(status, "externally_closed");
        assert!((filled_size - 0.05).abs() < 1e-9);
    }

    #[test]
    fn working_system_orders_lists_only_submitted_system_orders() {
        // C2: a system pending order the client cancels in the Bitget UI vanishes
        // from the exchange pending list. Reconcile finds our still-'submitted'
        // system orders and, if they're no longer pending on the exchange, marks
        // them externally_cancelled. This lister returns the candidates: system
        // (intent_id set) orders still in a working ('submitted') state.
        let conn = memory_db();
        conn.execute(
            "insert into trade_intents (intent_id, created_at, symbol, side, action,
               target_notional, max_order_notional, status, source)
             values ('i1','2026-07-01T00:00:00Z','ETH/USDT:USDT','long','open',100,100,'accepted','t')",
            [],
        )
        .unwrap();
        // A working system order (submitted) → candidate.
        conn.execute(
            "insert into orders (order_id, client_oid, intent_id, symbol, side, action,
               order_type, status, price, size, filled_size, created_at, updated_at)
             values ('o1','c1','i1','ETH/USDT:USDT','buy','open','limit','submitted',
               3000, 0.05, 0.0, '2026-07-01T00:00:00Z', '2026-07-01T00:00:00Z')",
            [],
        )
        .unwrap();
        // A filled system order → NOT a candidate (terminal).
        conn.execute(
            "insert into orders (order_id, client_oid, intent_id, symbol, side, action,
               order_type, status, price, size, filled_size, created_at, updated_at)
             values ('o2','c2','i1','ETH/USDT:USDT','buy','open','limit','filled',
               3000, 0.05, 0.05, '2026-07-01T00:00:00Z', '2026-07-01T00:00:00Z')",
            [],
        )
        .unwrap();
        // A working NON-system order (no intent_id) → NOT a candidate.
        conn.execute(
            "insert into orders (order_id, client_oid, symbol, side, action,
               order_type, status, price, size, filled_size, created_at, updated_at)
             values ('o3','c3','ETH/USDT:USDT','buy','open','limit','submitted',
               3000, 0.05, 0.0, '2026-07-01T00:00:00Z', '2026-07-01T00:00:00Z')",
            [],
        )
        .unwrap();

        let working = local_working_system_orders(&conn, "ETH/USDT:USDT").unwrap();
        assert_eq!(working.len(), 1);
        assert_eq!(working[0].0, "c1"); // client_oid
        assert_eq!(working[0].1, "o1"); // order_id
        assert!((working[0].2 - 0.05).abs() < 1e-9); // ordered size
    }

    #[test]
    fn mark_order_externally_cancelled_sets_status() {
        let conn = memory_db();
        conn.execute(
            "insert into trade_intents (intent_id, created_at, symbol, side, action,
               target_notional, max_order_notional, status, source)
             values ('i1','2026-07-01T00:00:00Z','ETH/USDT:USDT','long','open',100,100,'accepted','t')",
            [],
        )
        .unwrap();
        conn.execute(
            "insert into orders (order_id, client_oid, intent_id, symbol, side, action,
               order_type, status, price, size, filled_size, created_at, updated_at)
             values ('o1','c1','i1','ETH/USDT:USDT','buy','open','limit','submitted',
               3000, 0.05, 0.0, '2026-07-01T00:00:00Z', '2026-07-01T00:00:00Z')",
            [],
        )
        .unwrap();

        mark_order_externally_cancelled(&conn, "c1").unwrap();

        let status: String = conn
            .query_row(
                "select status from orders where client_oid = 'c1'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(status, "externally_cancelled");
    }

    #[test]
    fn clear_local_position_removes_the_row() {
        // D1: when the exchange fully closes a system position, the exchange no
        // longer lists a position for the symbol. Exchange state wins, so the
        // local positions row must be removed — otherwise local /positions and
        // PnL queries keep reporting a position Bitget no longer holds.
        let conn = memory_db();
        let pos = PositionRecord {
            symbol: "ETH/USDT:USDT".to_string(),
            side: "long".to_string(),
            notional: 150.0,
            entry_price: 3000.0,
            unrealized_pnl: 5.0,
            ownership: "system".to_string(),
            opened_at: Some("2026-07-01T00:00:00Z".to_string()),
            adopted_at: None,
            source_intent_id: Some("i1".to_string()),
            raw_json: "{}".to_string(),
        };
        upsert_position(&conn, &pos).unwrap();
        let before: i64 = conn
            .query_row(
                "select count(*) from positions where symbol='ETH/USDT:USDT'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(before, 1);

        clear_local_position(&conn, "ETH/USDT:USDT").unwrap();

        let after: i64 = conn
            .query_row(
                "select count(*) from positions where symbol='ETH/USDT:USDT'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(after, 0, "local position row must be removed on full-close");
    }

    #[test]
    fn set_order_filled_from_detail_updates_order_without_writing_a_fill() {
        // E3: when reconcile second-confirms a missing-pending order via order
        // detail, the detail's baseVolume is CUMULATIVE (not a single trade).
        // Writing it as one fills row would pollute the ledger — sync_order_fill_state
        // sums fills, so a later real partial+full would double-count. Instead set
        // orders.filled_size/status directly from the cumulative base and leave the
        // fills table untouched (it stays the per-trade record from fillList repair).
        let conn = memory_db();
        conn.execute(
            "insert into trade_intents (intent_id, created_at, symbol, side, action,
               target_notional, max_order_notional, status, source)
             values ('i1','2026-07-01T00:00:00Z','ETH/USDT:USDT','long','open',100,100,'accepted','t')",
            [],
        )
        .unwrap();
        // A submitted system order, ordered 0.05, exchange detail says 0.05 filled.
        conn.execute(
            "insert into orders (order_id, client_oid, intent_id, symbol, side, action,
               order_type, status, price, size, filled_size, created_at, updated_at)
             values ('o1','c1','i1','ETH/USDT:USDT','buy','open','limit','submitted',
               3000, 0.05, 0.0, '2026-07-01T00:00:00Z', '2026-07-01T00:00:00Z')",
            [],
        )
        .unwrap();

        set_order_filled_from_detail(&conn, "o1", 0.05).unwrap();

        let (status, filled): (String, f64) = conn
            .query_row(
                "select status, filled_size from orders where order_id = 'o1'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(status, "filled");
        assert!((filled - 0.05).abs() < 1e-9);
        // CRITICAL: no fills row was fabricated from the cumulative detail.
        let fills: i64 = conn
            .query_row(
                "select count(*) from fills where order_id = 'o1'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            fills, 0,
            "must not write a synthetic fill from cumulative detail"
        );

        // system_net_base still sees the filled order (it reads orders.filled_size).
        let (net, side) = system_net_base_for_symbol(&conn, "ETH/USDT:USDT").unwrap();
        assert!((net - 0.05).abs() < 1e-9 && side == "long");
    }

    #[test]
    fn set_order_filled_from_detail_partial_keeps_submitted() {
        // A partial cumulative fill (< ordered size) updates filled_size but does
        // NOT prematurely mark the order 'filled'.
        let conn = memory_db();
        conn.execute(
            "insert into trade_intents (intent_id, created_at, symbol, side, action,
               target_notional, max_order_notional, status, source)
             values ('i1','2026-07-01T00:00:00Z','ETH/USDT:USDT','long','open',100,100,'accepted','t')",
            [],
        )
        .unwrap();
        conn.execute(
            "insert into orders (order_id, client_oid, intent_id, symbol, side, action,
               order_type, status, price, size, filled_size, created_at, updated_at)
             values ('o1','c1','i1','ETH/USDT:USDT','buy','open','limit','submitted',
               3000, 0.05, 0.0, '2026-07-01T00:00:00Z', '2026-07-01T00:00:00Z')",
            [],
        )
        .unwrap();

        set_order_filled_from_detail(&conn, "o1", 0.02).unwrap();

        let (status, filled): (String, f64) = conn
            .query_row(
                "select status, filled_size from orders where order_id = 'o1'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(status, "submitted");
        assert!((filled - 0.02).abs() < 1e-9);
    }
}
