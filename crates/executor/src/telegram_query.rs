//! Read-only Telegram query formatting (M4).
//!
//! Maps the operator's `/status /positions /orders /pnl /risk` commands to
//! short SQLite-backed replies. These are STRICTLY read-only: no rows are
//! written, no side effects on the trading system. `/stop /resume /close_all`
//! are explicit non-grata in M4 — remote trading control is forbidden, so
//! they get a fixed "not supported in M4" refusal rather than acting.
//!
//! `query_response` returns `Ok(None)` for anything that isn't a recognized
//! command, so the polling loop simply doesn't reply to noise.

use anyhow::Result;
use rusqlite::Connection;

/// Map a single command line to its read-only reply, or `None` if it isn't a
/// recognized command (no reply). Remote trading controls are refused.
pub fn query_response(conn: &Connection, text: &str) -> Result<Option<String>> {
    let command = text.split_whitespace().next().unwrap_or("");
    match command {
        "/status" => Ok(Some(status_response(conn)?)),
        "/positions" => Ok(Some(positions_response(conn)?)),
        "/orders" => Ok(Some(orders_response(conn)?)),
        "/pnl" => Ok(Some(pnl_response(conn)?)),
        "/risk" => Ok(Some(risk_response(conn)?)),
        "/stop" | "/resume" | "/close_all" => Ok(Some(
            "remote trading controls are not supported in M4".to_string(),
        )),
        _ => Ok(None),
    }
}

fn status_response(conn: &Connection) -> Result<String> {
    let events: i64 = conn.query_row("select count(*) from events", [], |r| r.get(0))?;
    let pending: i64 = conn.query_row(
        "select count(*) from trade_intents where status = 'pending'",
        [],
        |r| r.get(0),
    )?;
    Ok(format!(
        "status: daemon\npending_intents: {pending}\nevents: {events}"
    ))
}

fn positions_response(conn: &Connection) -> Result<String> {
    let mut stmt = conn.prepare(
        "select symbol, side, notional, entry_price, unrealized_pnl, ownership
         from positions order by symbol",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok(format!(
            "{} {} notional={} entry={} upnl={} ownership={}",
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, f64>(2)?,
            row.get::<_, f64>(3)?,
            row.get::<_, f64>(4)?,
            row.get::<_, String>(5)?,
        ))
    })?;
    let lines = rows.collect::<Result<Vec<_>, _>>()?;
    Ok(if lines.is_empty() {
        "positions: none".to_string()
    } else {
        lines.join("\n")
    })
}

fn orders_response(conn: &Connection) -> Result<String> {
    let mut stmt = conn.prepare(
        "select client_oid, symbol, side, action, status, size, filled_size
         from orders order by updated_at desc limit 10",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok(format!(
            "{} {} {} {} status={} size={} filled={}",
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, String>(4)?,
            row.get::<_, f64>(5)?,
            row.get::<_, f64>(6)?,
        ))
    })?;
    let lines = rows.collect::<Result<Vec<_>, _>>()?;
    Ok(if lines.is_empty() {
        "orders: none".to_string()
    } else {
        lines.join("\n")
    })
}

fn pnl_response(conn: &Connection) -> Result<String> {
    let unrealized: f64 = conn.query_row(
        "select coalesce(sum(unrealized_pnl), 0) from positions",
        [],
        |r| r.get(0),
    )?;
    let equity: Option<f64> = conn
        .query_row(
            "select equity from equity_snapshots order by created_at desc limit 1",
            [],
            |r| r.get(0),
        )
        .ok();
    // ponytail: M4 /pnl is unrealized-only — it does NOT report realized PnL. Realized
    // PnL per closed trade is a future-milestone concern (it needs the per-trade fills
    // ledger aggregated by close intent); M4 surfaces only live unrealized PnL + the
    // last REST equity snapshot. Saying so explicitly so an operator isn't misled into
    // treating this as total account PnL.
    Ok(format!(
        "pnl (unrealized-only, M4):\nunrealized={unrealized}\nequity={}\nrealized=n/a (not tracked in M4)",
        equity.unwrap_or(0.0)
    ))
}

fn risk_response(conn: &Connection) -> Result<String> {
    let manual_overrides: i64 = conn.query_row(
        "select count(*) from executor_state
         where key like 'manual_override:%' and value = 'active'",
        [],
        |r| r.get(0),
    )?;
    let available_margin: Option<f64> = conn
        .query_row(
            "select available_margin from equity_snapshots order by created_at desc limit 1",
            [],
            |r| r.get(0),
        )
        .ok();
    Ok(format!(
        "risk:\nmanual_overrides={manual_overrides}\navailable_margin={}",
        available_margin.unwrap_or(0.0)
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_conn() -> rusqlite::Connection {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute_batch(include_str!("../../../schema/001_initial.sql"))
            .unwrap();
        conn.execute_batch(include_str!("../../../schema/002_execution.sql"))
            .unwrap();
        conn
    }

    #[test]
    fn status_query_reads_sqlite_without_side_effects() {
        let conn = test_conn();
        crate::db::write_event(&conn, "info", "daemon", "daemon started", "{}").unwrap();

        let response = query_response(&conn, "/status").unwrap().unwrap();

        assert!(response.contains("status"));
        assert!(response.contains("daemon"));
    }

    #[test]
    fn positions_query_lists_current_positions() {
        let conn = test_conn();
        conn.execute(
            "insert into positions (
              symbol, side, notional, entry_price, unrealized_pnl, updated_at,
              ownership, raw_json
            ) values ('ETH/USDT:USDT', 'long', 100.0, 2000.0, 3.5, 'now', 'system', '{}')",
            [],
        )
        .unwrap();

        let response = query_response(&conn, "/positions").unwrap().unwrap();

        assert!(response.contains("ETH/USDT:USDT"));
        assert!(response.contains("long"));
        assert!(response.contains("3.5"));
    }

    #[test]
    fn orders_query_lists_recent_orders() {
        let conn = test_conn();
        conn.execute(
            "insert into orders (
               order_id, client_oid, intent_id, symbol, side, action, order_type,
               status, price, size, filled_size, created_at, updated_at
             ) values (
               'order-1', 'client-xyz', null, 'ETH/USDT:USDT', 'buy', 'open',
               'limit', 'filled', 2000.0, 0.5, 0.5, 'now', 'now'
             )",
            [],
        )
        .unwrap();

        let response = query_response(&conn, "/orders").unwrap().unwrap();

        assert!(response.contains("client-xyz"));
        assert!(response.contains("ETH/USDT:USDT"));
        assert!(response.contains("status=filled"));
    }

    #[test]
    fn remote_control_commands_are_not_supported_in_m4() {
        for command in ["/stop", "/resume", "/close_all"] {
            let conn = test_conn();
            let response = query_response(&conn, command).unwrap().unwrap();
            assert!(response.contains("not supported in M4"));
        }
    }

    #[test]
    fn unrecognized_command_returns_none_so_no_reply_is_sent() {
        let conn = test_conn();
        // ponytail: None = "don't reply" — the polling loop only sends on Some,
        // so noise from other chats or stray text never spams the operator.
        assert!(query_response(&conn, "hello world").unwrap().is_none());
    }

    #[test]
    fn pnl_query_reports_unrealized_and_equity() {
        let conn = test_conn();
        crate::db::insert_equity_snapshot(&conn, 1000.0, 500.0, -2.0, 0.0).unwrap();

        let response = query_response(&conn, "/pnl").unwrap().unwrap();

        assert!(response.contains("unrealized=0"));
        assert!(response.contains("equity=1000"));
    }

    #[test]
    fn risk_query_counts_active_manual_overrides() {
        let conn = test_conn();
        crate::db::insert_equity_snapshot(&conn, 1000.0, 500.0, 0.0, 0.0).unwrap();
        crate::db::set_executor_state(&conn, "manual_override:ETH/USDT:USDT", "active").unwrap();

        let response = query_response(&conn, "/risk").unwrap().unwrap();

        assert!(response.contains("manual_overrides=1"));
        assert!(response.contains("available_margin=500"));
    }
}
