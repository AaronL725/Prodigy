use anyhow::Result;
use rusqlite::{params, Connection, OptionalExtension};

use crate::types::{ControlCommand, FillRecord, OrderRecord, PositionRecord, TradeIntent};

pub fn pending_intents(conn: &Connection) -> Result<Vec<TradeIntent>> {
    let mut stmt = conn.prepare(
        "select intent_id, symbol, side, action, target_notional, max_order_notional
         from trade_intents
         where status in ('pending', 'accepted')
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

    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

pub fn accept_intent(conn: &Connection, intent_id: &str) -> Result<bool> {
    let rows = conn.execute(
        "update trade_intents
         set status = 'accepted', processed_at = datetime('now'), error = null
         where intent_id = ? and status in ('pending', 'accepted')",
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

pub fn pending_control_commands(
    conn: &Connection,
    mode: &str,
    instance_id: &str,
) -> Result<Vec<ControlCommand>> {
    let mut stmt = conn.prepare(
        "select command_id, command, requested_by, mode, instance_id
         from control_commands
         where status = 'pending' and mode = ? and coalesce(instance_id, '') = ?
         order by created_at asc",
    )?;
    let rows = stmt.query_map(params![mode, instance_id], |row| {
        Ok(ControlCommand {
            command_id: row.get(0)?,
            command: row.get(1)?,
            requested_by: row.get(2)?,
            mode: row.get(3)?,
            instance_id: row.get(4)?,
        })
    })?;

    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

pub fn accept_control_command(conn: &Connection, command_id: &str) -> Result<bool> {
    let rows = conn.execute(
        "update control_commands
         set status = 'accepted', processed_at = datetime('now'), error = null
         where command_id = ? and status = 'pending'",
        params![command_id],
    )?;
    Ok(rows == 1)
}

pub fn mark_control_command_executed(conn: &Connection, command_id: &str) -> Result<()> {
    conn.execute(
        "update control_commands
         set status = 'executed', processed_at = datetime('now'), error = null
         where command_id = ?",
        params![command_id],
    )?;
    Ok(())
}

pub fn fail_control_command(conn: &Connection, command_id: &str, reason: &str) -> Result<()> {
    conn.execute(
        "update control_commands
         set status = 'failed', processed_at = datetime('now'), error = ?
         where command_id = ?",
        params![reason, command_id],
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
          order_id = case
            when excluded.exchange_order_id is not null then excluded.order_id
            else orders.order_id
          end,
          exchange_order_id = coalesce(excluded.exchange_order_id, orders.exchange_order_id),
          intent_id = excluded.intent_id,
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

/// True iff we already have a local order row for this client_oid. The private-WS
/// loop uses this to refresh ONLY orders we placed locally — it never inserts a
/// row the executor didn't write (REST reconcile owns order discovery).
pub fn order_exists(conn: &Connection, client_oid: &str) -> Result<bool> {
    conn.query_row(
        "select exists(select 1 from orders where client_oid = ?)",
        params![client_oid],
        |r| r.get(0),
    )
    .map_err(Into::into)
}

/// Refresh an ALREADY-LOCAL order's live fields from a private-WS update WITHOUT
/// touching identity columns (order_id, client_oid, intent_id stay as the
/// executor wrote them). REST reconcile owns order discovery; the executor owns
/// intent_id. The WS only refreshes status/filled_size/price/raw_json (the fast
/// cache). Bare UPDATE by positional params (excluded.* is UPSERT-only) and it
/// matches zero rows when no local row exists — callers gate on order_exists.
pub fn refresh_order_from_ws(conn: &Connection, order: &OrderRecord) -> Result<()> {
    conn.execute(
        "update orders set
           exchange_order_id = coalesce(?1, exchange_order_id),
           status = ?2,
           price = ?3,
           size = ?4,
           filled_size = ?5,
           updated_at = datetime('now'),
           attempt = ?6,
           raw_json = ?7
         where client_oid = ?8",
        params![
            order.exchange_order_id,
            order.status,
            order.price,
            order.size,
            order.filled_size,
            order.attempt,
            order.raw_json,
            order.client_oid,
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
          opened_at = case
            when positions.ownership = 'imported'
             and excluded.ownership = 'imported'
             and positions.opened_at is not null
            then positions.opened_at
            else excluded.opened_at
          end,
          adopted_at = case
            when positions.ownership = 'imported'
             and excluded.ownership = 'imported'
             and positions.adopted_at is not null
            then positions.adopted_at
            else excluded.adopted_at
          end,
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

pub fn equity_loss_24h_from(conn: &Connection, current_equity: f64, now: &str) -> Result<f64> {
    let baseline: Option<f64> = conn
        .query_row(
            "select equity from equity_snapshots
             where julianday(created_at) >= julianday(?1, '-24 hours')
               and julianday(created_at) <= julianday(?1)
             order by julianday(created_at) asc, created_at asc limit 1",
            params![now],
            |row| row.get(0),
        )
        .optional()?;
    Ok(baseline
        .map(|equity| (equity - current_equity).max(0.0))
        .unwrap_or(0.0))
}

/// Refresh a position's live market fields from a private-WS update WITHOUT
/// overwriting reconcile's authoritative ownership classification. REST
/// reconcile owns ownership/adopted_at/source_intent_id; the WS feed only
/// refreshes side/notional/entry_price/unrealized_pnl/raw_json (the fast cache).
/// If no local row exists yet, insert with WS-supplied ownership ("system")
/// pending the next reconcile reclassification. (Spec: if WS and REST disagree,
/// REST wins — so we never let a WS push downgrade an existing classification.)
pub fn refresh_position_from_ws(conn: &Connection, position: &PositionRecord) -> Result<()> {
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

pub fn system_positions(conn: &Connection) -> Result<Vec<PositionRecord>> {
    let mut stmt = conn.prepare(
        "select symbol, side, notional, entry_price, unrealized_pnl,
           ownership, opened_at, adopted_at, source_intent_id, raw_json
         from positions
         where ownership = 'system'
         order by symbol",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok(PositionRecord {
            symbol: row.get(0)?,
            side: row.get(1)?,
            notional: row.get(2)?,
            entry_price: row.get(3)?,
            unrealized_pnl: row.get(4)?,
            ownership: row.get(5)?,
            opened_at: row.get(6)?,
            adopted_at: row.get(7)?,
            source_intent_id: row.get(8)?,
            raw_json: row.get(9)?,
        })
    })?;

    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

/// client_oids of orders we already have locally (used to detect exchange orders
/// we're missing → repair). Reconciliation inserts the missing ones.
pub fn local_order_client_oids(conn: &Connection) -> Result<std::collections::HashSet<String>> {
    let mut stmt = conn.prepare("select client_oid from orders")?;
    let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
    Ok(rows.collect::<rusqlite::Result<std::collections::HashSet<_>>>()?)
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
    let net: f64 = conn.query_row(
        "select coalesce(sum(case when side = 'sell' then -filled_size else filled_size end), 0.0)
         from orders
         where symbol = ? and filled_size > 0 and intent_id is not null
           and status != 'externally_closed'",
        params![symbol],
        |r| r.get(0),
    )?;
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
    Ok(rows.collect::<rusqlite::Result<std::collections::HashSet<_>>>()?)
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
    Ok(rows.collect::<rusqlite::Result<std::collections::HashMap<_, _>>>()?)
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

/// System orders (intent_id set) still in a local working state for a symbol.
/// Reconcile compares these against the exchange pending list: a working system
/// order the exchange no longer lists was either cancelled outside the executor
/// (manual cancel) OR filled but briefly absent from pending. Reconcile
/// second-confirms via order detail, using the returned ordered size to tell full
/// vs partial fill. Returns (client_oid, order_id, ordered_size) triples.
pub fn local_working_system_orders(
    conn: &Connection,
    symbol: &str,
) -> Result<Vec<(String, String, f64)>> {
    let mut stmt = conn.prepare(
        "select client_oid, order_id, coalesce(size, 0) from orders
         where symbol = ? and intent_id is not null and status in ('submitted', 'live')
         order by order_id",
    )?;
    let rows = stmt.query_map(params![symbol], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, f64>(2)?,
        ))
    })?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
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

pub fn mark_system_order_cancelled_by_command(conn: &Connection, client_oid: &str) -> Result<()> {
    conn.execute(
        "update orders set status = 'cancelled', updated_at = datetime('now')
         where client_oid = ? and intent_id is not null",
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

pub const ACTIVE_MODE_KEY: &str = "active_mode";
pub const ACTIVE_INSTANCE_ID_KEY: &str = "active_instance_id";
pub const ACTIVE_STARTED_AT_KEY: &str = "active_started_at";
pub const ACTIVE_HEARTBEAT_AT_KEY: &str = "active_heartbeat_at";

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
    conn.query_row(
        "select value from executor_state where key = ?",
        params![key],
        |r| r.get(0),
    )
    .optional()
    .map_err(Into::into)
}

pub fn acquire_active_executor_lock(
    conn: &Connection,
    mode: &str,
    instance_id: &str,
    now_ms: i64,
    stale_timeout_ms: i64,
) -> Result<()> {
    // Top-level BEGIN IMMEDIATE serializes active-lock read/takeover/write.
    conn.execute_batch("begin immediate")?;
    let result = (|| -> Result<()> {
        let active_mode = get_executor_state(conn, ACTIVE_MODE_KEY)?;
        let active_instance = get_executor_state(conn, ACTIVE_INSTANCE_ID_KEY)?;
        let started_at = get_executor_state(conn, ACTIVE_STARTED_AT_KEY)?;
        let heartbeat = get_executor_state(conn, ACTIVE_HEARTBEAT_AT_KEY)?;
        match (active_mode, active_instance, started_at, heartbeat) {
            (None, None, None, None) => {}
            (Some(old_mode), Some(old_instance), Some(_), Some(old_heartbeat)) => {
                let Ok(old_heartbeat_ms) = old_heartbeat.parse::<i64>() else {
                    anyhow::bail!("active executor lock corrupt heartbeat");
                };
                let age_ms = now_ms.saturating_sub(old_heartbeat_ms);
                if age_ms <= stale_timeout_ms {
                    anyhow::bail!("active executor lock held by {old_mode}/{old_instance}");
                }
                write_event(
                    conn,
                    "warning",
                    "daemon",
                    "active executor lock takeover",
                    &serde_json::json!({
                        "old_mode": old_mode,
                        "old_instance_id": old_instance,
                        "new_mode": mode,
                        "new_instance_id": instance_id,
                    })
                    .to_string(),
                )?;
            }
            _ => anyhow::bail!("active executor lock state incomplete"),
        }
        set_executor_state(conn, ACTIVE_MODE_KEY, mode)?;
        set_executor_state(conn, ACTIVE_INSTANCE_ID_KEY, instance_id)?;
        set_executor_state(conn, ACTIVE_STARTED_AT_KEY, &now_ms.to_string())?;
        set_executor_state(conn, ACTIVE_HEARTBEAT_AT_KEY, &now_ms.to_string())?;
        Ok(())
    })();
    if let Err(err) = result {
        let _ = conn.execute_batch("rollback");
        return Err(err);
    }
    if let Err(err) = conn.execute_batch("commit") {
        let _ = conn.execute_batch("rollback");
        return Err(err.into());
    }
    Ok(())
}

pub fn heartbeat_active_executor_lock(
    conn: &Connection,
    mode: &str,
    instance_id: &str,
    now_ms: i64,
) -> Result<bool> {
    let rows = conn.execute(
        "update executor_state
         set value = ?, updated_at = datetime('now')
         where key = ?
           and exists (
             select 1 from executor_state where key = ? and value = ?
           )
           and exists (
             select 1 from executor_state where key = ? and value = ?
           )",
        params![
            now_ms.to_string(),
            ACTIVE_HEARTBEAT_AT_KEY,
            ACTIVE_MODE_KEY,
            mode,
            ACTIVE_INSTANCE_ID_KEY,
            instance_id,
        ],
    )?;
    Ok(rows == 1)
}

pub fn release_active_executor_lock(
    conn: &Connection,
    mode: &str,
    instance_id: &str,
) -> Result<bool> {
    let rows = conn.execute(
        "delete from executor_state
         where key in (?, ?, ?, ?)
           and exists (
             select 1 from executor_state where key = ? and value = ?
           )
           and exists (
             select 1 from executor_state where key = ? and value = ?
           )",
        params![
            ACTIVE_MODE_KEY,
            ACTIVE_INSTANCE_ID_KEY,
            ACTIVE_STARTED_AT_KEY,
            ACTIVE_HEARTBEAT_AT_KEY,
            ACTIVE_MODE_KEY,
            mode,
            ACTIVE_INSTANCE_ID_KEY,
            instance_id,
        ],
    )?;
    Ok(rows > 0)
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
    fn accept_intent_claims_pending_but_not_terminal() {
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
        mark_intent_executed(&conn, "i1").unwrap();
        assert!(!accept_intent(&conn, "i1").unwrap());
    }

    #[test]
    fn pending_intents_include_accepted_for_restart_recovery() {
        let conn = memory_db();
        for (intent_id, status) in [("i-pending", "pending"), ("i-accepted", "accepted")] {
            conn.execute(
                "insert into trade_intents (
                  intent_id, created_at, symbol, side, action, target_notional,
                  max_order_notional, status, source
                ) values (?1, '2026-07-01T00:00:00Z', 'ETH/USDT:USDT',
                  'long', 'open', 100, 100, ?2, 'test')",
                params![intent_id, status],
            )
            .unwrap();
        }

        let mut ids: Vec<String> = pending_intents(&conn)
            .unwrap()
            .into_iter()
            .map(|i| i.intent_id)
            .collect();
        ids.sort();

        assert_eq!(ids, vec!["i-accepted", "i-pending"]);
    }

    #[test]
    fn accept_intent_reclaims_accepted_intent_after_restart() {
        let conn = memory_db();
        conn.execute(
            "insert into trade_intents (
              intent_id, created_at, symbol, side, action, target_notional,
              max_order_notional, status, source
            ) values ('i-accepted', '2026-07-01T00:00:00Z', 'ETH/USDT:USDT',
              'long', 'open', 100, 100, 'accepted', 'test')",
            [],
        )
        .unwrap();

        assert!(
            accept_intent(&conn, "i-accepted").unwrap(),
            "accepted intents must be claimable after a restart"
        );
    }

    #[test]
    fn pending_control_commands_are_accepted_idempotently() {
        let conn = memory_db();
        conn.execute(
            "insert into control_commands (
              command_id, created_at, command, status, requested_by
            ) values ('cmd-1', '2026-07-06T00:00:00Z', 'stop', 'pending', '123')",
            [],
        )
        .unwrap();

        let pending = pending_control_commands(&conn, "demo", "").unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].command, "stop");
        assert!(accept_control_command(&conn, "cmd-1").unwrap());
        assert!(!accept_control_command(&conn, "cmd-1").unwrap());
    }

    #[test]
    fn pending_control_commands_filter_by_mode_and_instance() {
        let conn = memory_db();
        conn.execute_batch(
            "
            insert into control_commands (
              command_id, created_at, command, status, requested_by, mode, instance_id
            ) values
              ('cmd-demo-a', '2026-07-01T00:00:00Z', 'stop', 'pending', '123', 'demo', 'inst-a'),
              ('cmd-demo-b', '2026-07-01T00:00:01Z', 'resume', 'pending', '123', 'demo', 'inst-b'),
              ('cmd-live-a', '2026-07-01T00:00:02Z', 'stop', 'pending', '123', 'live', 'inst-a');
            ",
        )
        .unwrap();

        let pending = pending_control_commands(&conn, "demo", "inst-a").unwrap();

        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].command_id, "cmd-demo-a");
        assert_eq!(pending[0].mode, "demo");
        assert_eq!(pending[0].instance_id.as_deref(), Some("inst-a"));
    }

    #[test]
    fn system_positions_lists_only_system_owned_positions() {
        let conn = memory_db();
        conn.execute(
            "insert into positions (
              symbol, side, notional, entry_price, unrealized_pnl, updated_at,
              ownership, opened_at, adopted_at, source_intent_id, raw_json
            ) values
            ('ETHUSDT', 'long', 100, 2000, 1, 'now', 'system', 'now', null, 'i1', '{}'),
            ('ADAUSDT', 'long', 100, 2000, 1, 'now', 'system', 'now', null, 'i2', '{}'),
            ('BTCUSDT', 'long', 100, 2000, 1, 'now', 'imported', 'now', 'now', null, '{}')",
            [],
        )
        .unwrap();

        let positions = system_positions(&conn).unwrap();

        assert_eq!(positions.len(), 2);
        assert_eq!(positions[0].symbol, "ADAUSDT");
        assert_eq!(positions[1].symbol, "ETHUSDT");
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
    fn active_executor_lock_blocks_non_stale_second_instance() {
        let conn = memory_db();

        acquire_active_executor_lock(&conn, "demo", "inst-a", 1_000, 60_000).unwrap();

        let err =
            acquire_active_executor_lock(&conn, "live", "inst-b", 31_000, 60_000).unwrap_err();

        assert!(err.to_string().contains("active executor"));
    }

    #[test]
    fn active_executor_lock_rejects_incomplete_state() {
        let conn = memory_db();
        set_executor_state(&conn, ACTIVE_MODE_KEY, "demo").unwrap();
        set_executor_state(&conn, ACTIVE_INSTANCE_ID_KEY, "inst-a").unwrap();

        let err =
            acquire_active_executor_lock(&conn, "live", "inst-b", 122_000, 60_000).unwrap_err();

        assert!(err.to_string().contains("active executor"));
        assert_eq!(
            get_executor_state(&conn, ACTIVE_INSTANCE_ID_KEY)
                .unwrap()
                .as_deref(),
            Some("inst-a")
        );
    }

    #[test]
    fn active_executor_lock_rejects_malformed_heartbeat() {
        let conn = memory_db();
        set_executor_state(&conn, ACTIVE_MODE_KEY, "demo").unwrap();
        set_executor_state(&conn, ACTIVE_INSTANCE_ID_KEY, "inst-a").unwrap();
        set_executor_state(&conn, ACTIVE_STARTED_AT_KEY, "1000").unwrap();
        set_executor_state(&conn, ACTIVE_HEARTBEAT_AT_KEY, "not-ms").unwrap();

        let err =
            acquire_active_executor_lock(&conn, "live", "inst-b", 122_000, 60_000).unwrap_err();

        assert!(err.to_string().contains("active executor"));
        assert_eq!(
            get_executor_state(&conn, ACTIVE_INSTANCE_ID_KEY)
                .unwrap()
                .as_deref(),
            Some("inst-a")
        );
    }

    #[test]
    fn stale_active_executor_lock_can_be_taken_over_and_audited() {
        let conn = memory_db();
        acquire_active_executor_lock(&conn, "demo", "inst-a", 1_000, 60_000).unwrap();

        acquire_active_executor_lock(&conn, "live", "inst-b", 122_000, 60_000).unwrap();

        assert_eq!(
            get_executor_state(&conn, "active_mode").unwrap().as_deref(),
            Some("live")
        );
        let takeover_events: i64 = conn
            .query_row(
                "select count(*) from events where component = 'daemon' and message = 'active executor lock takeover'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(takeover_events, 1);
    }

    #[test]
    fn heartbeat_active_executor_lock_reports_ownership() {
        let conn = memory_db();
        acquire_active_executor_lock(&conn, "demo", "inst-a", 1_000, 60_000).unwrap();

        assert!(!heartbeat_active_executor_lock(&conn, "demo", "other", 2_000).unwrap());
        assert!(heartbeat_active_executor_lock(&conn, "demo", "inst-a", 3_000).unwrap());
        assert_eq!(
            get_executor_state(&conn, ACTIVE_HEARTBEAT_AT_KEY)
                .unwrap()
                .as_deref(),
            Some("3000")
        );
    }

    #[test]
    fn release_active_executor_lock_reports_ownership() {
        let conn = memory_db();
        acquire_active_executor_lock(&conn, "demo", "inst-a", 1_000, 60_000).unwrap();

        assert!(!release_active_executor_lock(&conn, "demo", "other").unwrap());
        assert!(release_active_executor_lock(&conn, "demo", "inst-a").unwrap());
        assert_eq!(get_executor_state(&conn, ACTIVE_MODE_KEY).unwrap(), None);
    }

    #[test]
    fn active_executor_lock_acquire_rolls_back_partial_state_on_write_error() {
        let conn = memory_db();
        conn.execute_batch(
            "
            create trigger fail_active_instance_lock_insert
            before insert on executor_state
            when new.key = 'active_instance_id'
            begin
              select raise(abort, 'fail active instance');
            end;
            ",
        )
        .unwrap();

        let err = acquire_active_executor_lock(&conn, "demo", "inst-a", 1_000, 60_000)
            .expect_err("trigger should fail the second lock state write");

        assert!(err.to_string().contains("fail active instance"));
        assert_eq!(get_executor_state(&conn, ACTIVE_MODE_KEY).unwrap(), None);
        assert_eq!(
            get_executor_state(&conn, ACTIVE_INSTANCE_ID_KEY).unwrap(),
            None
        );
    }

    #[test]
    fn heartbeat_active_executor_lock_ignores_mismatched_instance() {
        let conn = memory_db();
        acquire_active_executor_lock(&conn, "demo", "inst-a", 1_000, 60_000).unwrap();

        heartbeat_active_executor_lock(&conn, "demo", "other", 2_000).unwrap();

        assert_eq!(
            get_executor_state(&conn, ACTIVE_HEARTBEAT_AT_KEY)
                .unwrap()
                .as_deref(),
            Some("1000")
        );
    }

    #[test]
    fn stale_active_executor_lock_owner_cannot_touch_lock_after_takeover() {
        let conn = memory_db();
        acquire_active_executor_lock(&conn, "demo", "inst-a", 1_000, 60_000).unwrap();
        acquire_active_executor_lock(&conn, "live", "inst-b", 122_000, 60_000).unwrap();

        heartbeat_active_executor_lock(&conn, "demo", "inst-a", 130_000).unwrap();
        release_active_executor_lock(&conn, "demo", "inst-a").unwrap();

        assert_eq!(
            get_executor_state(&conn, ACTIVE_INSTANCE_ID_KEY)
                .unwrap()
                .as_deref(),
            Some("inst-b")
        );
        assert_eq!(
            get_executor_state(&conn, ACTIVE_HEARTBEAT_AT_KEY)
                .unwrap()
                .as_deref(),
            Some("122000")
        );
    }

    #[test]
    fn release_active_executor_lock_only_releases_matching_instance() {
        let conn = memory_db();
        acquire_active_executor_lock(&conn, "demo", "inst-a", 1_000, 60_000).unwrap();

        release_active_executor_lock(&conn, "demo", "other").unwrap();
        assert_eq!(
            get_executor_state(&conn, "active_instance_id")
                .unwrap()
                .as_deref(),
            Some("inst-a")
        );

        release_active_executor_lock(&conn, "demo", "inst-a").unwrap();
        for key in [
            ACTIVE_MODE_KEY,
            ACTIVE_INSTANCE_ID_KEY,
            ACTIVE_STARTED_AT_KEY,
            ACTIVE_HEARTBEAT_AT_KEY,
        ] {
            assert_eq!(get_executor_state(&conn, key).unwrap(), None);
        }
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
    fn upsert_position_preserves_first_imported_adoption_time() {
        use crate::types::PositionRecord;
        let conn = memory_db();
        let first = PositionRecord {
            symbol: "ETHUSDT".to_string(),
            side: "long".to_string(),
            notional: 1000.0,
            entry_price: 3000.0,
            unrealized_pnl: 0.0,
            ownership: "imported".to_string(),
            opened_at: Some("2026-07-01T00:00:00Z".to_string()),
            adopted_at: Some("2026-07-01T00:00:00Z".to_string()),
            source_intent_id: None,
            raw_json: "{}".to_string(),
        };
        upsert_position(&conn, &first).unwrap();

        upsert_position(
            &conn,
            &PositionRecord {
                unrealized_pnl: 25.0,
                opened_at: Some("2026-07-02T00:00:00Z".to_string()),
                adopted_at: Some("2026-07-02T00:00:00Z".to_string()),
                ..first
            },
        )
        .unwrap();

        let row: (Option<String>, Option<String>, f64) = conn
            .query_row(
                "select adopted_at, opened_at, unrealized_pnl from positions where symbol='ETHUSDT'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(row.0.as_deref(), Some("2026-07-01T00:00:00Z"));
        assert_eq!(row.1.as_deref(), Some("2026-07-01T00:00:00Z"));
        assert_eq!(row.2, 25.0);
    }

    #[test]
    fn equity_loss_24h_uses_window_baseline_not_current_unrealized() {
        let conn = memory_db();
        conn.execute(
            "insert into equity_snapshots (
              snapshot_id, created_at, equity, available_margin, unrealized_pnl, realized_pnl_24h
            ) values
              ('old', '2026-07-04 00:00:00', 2000, 2000, 0, 0),
              ('base', '2026-07-05 12:00:00', 1000, 1000, 0, 0),
              ('later', '2026-07-06 11:00:00', 970, 970, 0, 0)",
            [],
        )
        .unwrap();

        let loss = equity_loss_24h_from(&conn, 900.0, "2026-07-06 12:00:00").unwrap();

        assert_eq!(loss, 100.0);
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
    fn working_system_orders_lists_local_working_system_orders() {
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
        // A private-WS-refreshed system order can carry Bitget's raw "live"
        // status and is still working.
        conn.execute(
            "insert into orders (order_id, client_oid, intent_id, symbol, side, action,
               order_type, status, price, size, filled_size, created_at, updated_at)
             values ('o4','c4','i1','ETH/USDT:USDT','buy','open','limit','live',
               3000, 0.07, 0.0, '2026-07-01T00:00:00Z', '2026-07-01T00:00:00Z')",
            [],
        )
        .unwrap();

        let working = local_working_system_orders(&conn, "ETH/USDT:USDT").unwrap();
        assert_eq!(working.len(), 2);
        assert_eq!(working[0].0, "c1"); // client_oid
        assert_eq!(working[0].1, "o1"); // order_id
        assert!((working[0].2 - 0.05).abs() < 1e-9); // ordered size
        assert_eq!(working[1].0, "c4");
        assert_eq!(working[1].1, "o4");
        assert!((working[1].2 - 0.07).abs() < 1e-9);
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
    fn refresh_position_from_ws_preserves_reconcile_ownership() {
        // Finding 1 regression: a private-WS positions push must NOT clobber the
        // ownership classification (imported/system), adopted_at, or source_intent_id
        // that REST reconcile authoritatively wrote. WS only refreshes market-movement
        // fields (side/notional/entry_price/unrealized_pnl/raw_json); reconcile's
        // ownership stays put until the next reconcile reclassifies. Spec: REST wins.
        use crate::types::PositionRecord;
        let conn = memory_db();
        // Reconcile's authoritative write: position classified imported + adopted.
        let reconciled = PositionRecord {
            symbol: "ETH/USDT:USDT".to_string(),
            side: "long".to_string(),
            notional: 1000.0,
            entry_price: 3000.0,
            unrealized_pnl: 12.0,
            ownership: "imported".to_string(),
            opened_at: Some("2026-07-01T00:00:00Z".to_string()),
            adopted_at: Some("2026-07-01T00:00:00Z".to_string()),
            source_intent_id: Some("intent-9".to_string()),
            raw_json: "{\"src\":\"reconcile\"}".to_string(),
        };
        upsert_position(&conn, &reconciled).unwrap();

        // Private-WS push for the same symbol: WS parser hardcodes ownership "system"
        // and different market fields.
        let ws = PositionRecord {
            notional: 1100.0,
            unrealized_pnl: 50.0,
            ownership: "system".to_string(),
            adopted_at: None,
            source_intent_id: None,
            raw_json: "{\"src\":\"ws\"}".to_string(),
            ..reconciled.clone()
        };
        refresh_position_from_ws(&conn, &ws).unwrap();

        let row = conn
            .query_row(
                "select notional, unrealized_pnl, ownership, adopted_at, source_intent_id, raw_json
                 from positions where symbol='ETH/USDT:USDT'",
                [],
                |r| {
                    Ok((
                        r.get::<_, f64>(0)?,
                        r.get::<_, f64>(1)?,
                        r.get::<_, String>(2)?,
                        r.get::<_, Option<String>>(3)?,
                        r.get::<_, Option<String>>(4)?,
                        r.get::<_, String>(5)?,
                    ))
                },
            )
            .unwrap();
        // Market fields refreshed from WS.
        assert!((row.0 - 1100.0).abs() < 1e-9, "notional should refresh");
        assert!((row.1 - 50.0).abs() < 1e-9, "unrealized_pnl should refresh");
        // Ownership authority preserved.
        assert_eq!(
            row.2, "imported",
            "ownership must stay reconcile-authoritative"
        );
        assert_eq!(
            row.3.as_deref(),
            Some("2026-07-01T00:00:00Z"),
            "adopted_at must be preserved"
        );
        assert_eq!(
            row.4.as_deref(),
            Some("intent-9"),
            "source_intent_id must be preserved"
        );
        assert_eq!(row.5, "{\"src\":\"ws\"}", "raw_json should refresh");
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

    #[test]
    fn refresh_order_from_ws_preserves_intent_id_and_does_not_insert() {
        // Fix-A regression: a private-WS order refresh must update the live fields
        // (status/filled_size/exchange_order_id/price/...) but MUST preserve the
        // identity columns the executor wrote (intent_id). The WS order parser sets
        // intent_id None; if a refresh clobbered intent_id, system_net_base_for_symbol
        // (filters intent_id is not null) would drop a real system position and
        // reconcile would mis-classify it. Also it must never INSERT a row.
        let conn = memory_db();
        conn.execute(
            "insert into trade_intents (intent_id, created_at, symbol, side, action,
               target_notional, max_order_notional, status, source)
             values ('intent-7','2026-07-01T00:00:00Z','ETH/USDT:USDT','long','open',100,100,'executed','t')",
            [],
        )
        .unwrap();
        // Executor's authoritative write: system order, submitted, no fill yet.
        let system = OrderRecord {
            order_id: "order-1".to_string(),
            exchange_order_id: None,
            client_oid: "client-1".to_string(),
            intent_id: Some("intent-7".to_string()),
            symbol: "ETH/USDT:USDT".to_string(),
            side: "buy".to_string(),
            action: "open".to_string(),
            order_type: "limit".to_string(),
            status: "submitted".to_string(),
            price: Some(3000.0),
            size: 0.05,
            filled_size: 0.0,
            attempt: 1,
            raw_json: "{\"src\":\"rest\"}".to_string(),
            last_error: None,
        };
        upsert_order(&conn, &system).unwrap();

        // Private-WS push for the SAME client_oid with intent_id None (WS parser),
        // status filled, fill size 0.05, exchange order id now known.
        let ws = OrderRecord {
            exchange_order_id: Some("ex-1".to_string()),
            status: "filled".to_string(),
            filled_size: 0.05,
            intent_id: None,
            raw_json: "{\"src\":\"ws\"}".to_string(),
            ..system.clone()
        };
        refresh_order_from_ws(&conn, &ws).unwrap();

        let row = conn
            .query_row(
                "select status, filled_size, exchange_order_id, intent_id, raw_json
                 from orders where client_oid = 'client-1'",
                [],
                |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, f64>(1)?,
                        r.get::<_, Option<String>>(2)?,
                        r.get::<_, Option<String>>(3)?,
                        r.get::<_, String>(4)?,
                    ))
                },
            )
            .unwrap();
        // Live fields refreshed from WS.
        assert_eq!(row.0, "filled");
        assert!((row.1 - 0.05).abs() < 1e-9);
        assert_eq!(row.2.as_deref(), Some("ex-1"));
        assert_eq!(row.4, "{\"src\":\"ws\"}");
        // Identity preserved: the executor's intent_id is untouched.
        assert_eq!(
            row.3.as_deref(),
            Some("intent-7"),
            "intent_id must be preserved on WS refresh"
        );

        // CRITICAL: a refresh on a client_oid with NO local row must not INSERT.
        let fresh = OrderRecord {
            client_oid: "client-unknown".to_string(),
            order_id: "order-2".to_string(),
            ..ws.clone()
        };
        refresh_order_from_ws(&conn, &fresh).unwrap();
        let unknown: i64 = conn
            .query_row(
                "select count(*) from orders where client_oid = 'client-unknown'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(unknown, 0, "refresh must not insert an unknown order");
    }
}
