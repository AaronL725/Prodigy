//! SQLite-backed Telegram query formatting.
//!
//! Maps the operator's `/status /positions /orders /pnl /risk` commands to
//! short SQLite-backed replies. `query_response` keeps the M4 read-only
//! behavior; `operator_response` adds M6 authorization and operator commands.
//!
//! `query_response` returns `Ok(None)` for anything that isn't a recognized
//! command, so the polling loop simply doesn't reply to noise.

use anyhow::Result;
use rusqlite::Connection;
use sha2::{Digest, Sha256};

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

pub fn operator_response(
    conn: &Connection,
    text: &str,
    from_user_id: &str,
    allowed_user_ids: &[String],
    now_ms: i64,
) -> Result<Option<String>> {
    let command = text.split_whitespace().next().unwrap_or("");
    if command.is_empty() {
        return Ok(None);
    }
    if !allowed_user_ids.iter().any(|id| id == from_user_id) {
        crate::db::write_event(
            conn,
            "warning",
            "telegram",
            "unauthorized telegram command",
            &serde_json::json!({
                "from_user_id": from_user_id,
                "command": command,
            })
            .to_string(),
        )
        .ok();
        return Ok(Some("unauthorized".to_string()));
    }
    match command {
        "/help" => Ok(Some(help_response())),
        "/status" => Ok(Some(status_response(conn)?)),
        "/positions" => Ok(Some(positions_response(conn)?)),
        "/orders" => Ok(Some(orders_response(conn)?)),
        "/trades" => Ok(Some(trades_response(conn)?)),
        "/pnl" => Ok(Some(pnl_response(conn)?)),
        "/risk" => Ok(Some(risk_response(conn)?)),
        "/events" => Ok(Some(events_response(conn)?)),
        "/smoke_status" => Ok(Some(smoke_status_response(conn)?)),
        "/stop" | "/resume" | "/cancel_all" | "/close_all" | "/confirm" => {
            control_response(conn, text, from_user_id, now_ms)
        }
        _ => Ok(None),
    }
}

fn help_response() -> String {
    "/help /status /positions /orders /trades /pnl /risk /events /smoke_status\ncontrols: /stop /resume /cancel_all /close_all /confirm <code>".to_string()
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

fn trades_response(conn: &Connection) -> Result<String> {
    let mut stmt = conn.prepare(
        "select symbol, side, price, size, fee, created_at
         from fills order by created_at desc limit 10",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok(format!(
            "{} {} price={} size={} fee={} at={}",
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
        "trades: none".to_string()
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
    // ponytail: no realized-PnL ledger yet; report unknown instead of implying total PnL.
    Ok(format!(
        "pnl:\nunrealized={unrealized}\nequity={}\nrealized=n/a\ntotal=n/a",
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

fn events_response(conn: &Connection) -> Result<String> {
    let mut stmt = conn.prepare(
        "select created_at, severity, component, message
         from events
         where severity in ('warning', 'error', 'critical')
         order by created_at desc limit 10",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok(format!(
            "{} {} {}: {}",
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
        ))
    })?;
    let lines = rows.collect::<Result<Vec<_>, _>>()?;
    Ok(if lines.is_empty() {
        "events: none".to_string()
    } else {
        lines.join("\n")
    })
}

fn smoke_status_response(conn: &Connection) -> Result<String> {
    Ok(format!(
        "smoke_status: {}",
        crate::db::get_executor_state(conn, "smoke:status")?.unwrap_or_else(|| "n/a".to_string())
    ))
}

fn audit(conn: &Connection, message: &str, payload_json: &str) -> Result<()> {
    crate::db::write_event(conn, "info", "telegram", message, payload_json)
}

fn with_savepoint<T>(
    conn: &Connection,
    name: &str,
    f: impl FnOnce(&Connection) -> Result<T>,
) -> Result<T> {
    conn.execute_batch(&format!("savepoint {name}"))?;
    match f(conn) {
        Ok(value) => {
            conn.execute_batch(&format!("release {name}"))?;
            Ok(value)
        }
        Err(err) => {
            conn.execute_batch(&format!("rollback to {name}"))?;
            conn.execute_batch(&format!("release {name}"))?;
            Err(err)
        }
    }
}

fn queue_control_command(conn: &Connection, command: &str, requested_by: &str) -> Result<String> {
    with_savepoint(conn, "telegram_queue_control", |conn| {
        let command_id: String =
            conn.query_row("select lower(hex(randomblob(16)))", [], |r| r.get(0))?;
        conn.execute(
            "insert into control_commands (
               command_id, created_at, command, status, requested_by
             ) values (?, datetime('now'), ?, 'pending', ?)",
            rusqlite::params![command_id, command, requested_by],
        )?;
        audit(
            conn,
            "telegram control command queued",
            &serde_json::json!({
                "command_id": command_id,
                "command": command,
                "requested_by": requested_by,
            })
            .to_string(),
        )?;
        Ok(command_id)
    })
}

fn sha256_hex(value: &str) -> String {
    let digest = Sha256::digest(value.as_bytes());
    digest.iter().map(|b| format!("{b:02x}")).collect()
}

fn start_close_all_confirmation(
    conn: &Connection,
    requested_by: &str,
    now_ms: i64,
) -> Result<String> {
    let code: String = conn.query_row("select lower(hex(randomblob(3)))", [], |r| r.get(0))?;
    let value = serde_json::json!({
        "status": "pending",
        "requested_by": requested_by,
        "code_hash": sha256_hex(&code),
        "expires_ms": now_ms + 60_000,
    })
    .to_string();
    crate::db::set_executor_state(conn, &format!("close_all_confirm:{requested_by}"), &value)?;
    audit(
        conn,
        "telegram close_all confirmation generated",
        &serde_json::json!({
            "requested_by": requested_by,
            "expires_ms": now_ms + 60_000,
        })
        .to_string(),
    )?;
    Ok(format!("confirm close_all with /confirm {code}"))
}

fn confirm_close_all(
    conn: &Connection,
    text: &str,
    requested_by: &str,
    now_ms: i64,
) -> Result<String> {
    let code = text.split_whitespace().nth(1).unwrap_or("");
    let key = format!("close_all_confirm:{requested_by}");
    let Some(raw) = crate::db::get_executor_state(conn, &key)? else {
        audit(
            conn,
            "telegram close_all confirmation rejected",
            &serde_json::json!({
                "reason": "missing",
                "requested_by": requested_by,
            })
            .to_string(),
        )?;
        return Ok("confirmation rejected".to_string());
    };
    let value: serde_json::Value = match serde_json::from_str(&raw) {
        Ok(value) => value,
        Err(_) => {
            audit(
                conn,
                "telegram close_all confirmation rejected",
                &serde_json::json!({
                    "reason": "invalid_state",
                    "requested_by": requested_by,
                })
                .to_string(),
            )?;
            return Ok("confirmation rejected".to_string());
        }
    };
    if value.get("status").and_then(serde_json::Value::as_str) == Some("used") {
        audit(
            conn,
            "telegram close_all confirmation rejected",
            &serde_json::json!({
                "reason": "used",
                "requested_by": requested_by,
            })
            .to_string(),
        )?;
        return Ok("confirmation rejected".to_string());
    }
    let expires_ms = value
        .get("expires_ms")
        .and_then(serde_json::Value::as_i64)
        .unwrap_or(0);
    let expected = value
        .get("code_hash")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("");
    if now_ms >= expires_ms {
        audit(
            conn,
            "telegram close_all confirmation expired",
            &serde_json::json!({
                "reason": "expired",
                "requested_by": requested_by,
            })
            .to_string(),
        )?;
        return Ok("confirmation expired".to_string());
    }
    if expected != sha256_hex(code) {
        audit(
            conn,
            "telegram close_all confirmation rejected",
            &serde_json::json!({
                "reason": "bad_code",
                "requested_by": requested_by,
            })
            .to_string(),
        )?;
        return Ok("confirmation rejected".to_string());
    }
    let command_id = with_savepoint(conn, "telegram_confirm_close_all", |conn| {
        let command_id = queue_control_command(conn, "close_all", requested_by)?;
        audit(
            conn,
            "telegram close_all confirmation accepted",
            &serde_json::json!({
                "command_id": command_id,
                "requested_by": requested_by,
            })
            .to_string(),
        )?;
        crate::db::set_executor_state(
            conn,
            &key,
            &serde_json::json!({
                "status": "used",
                "requested_by": requested_by,
                "command_id": command_id,
                "used_ms": now_ms,
            })
            .to_string(),
        )?;
        Ok(command_id)
    })?;
    Ok(format!("close_all queued command_id={command_id}"))
}

fn control_response(
    conn: &Connection,
    text: &str,
    from_user_id: &str,
    now_ms: i64,
) -> Result<Option<String>> {
    let command = text.split_whitespace().next().unwrap_or("");
    match command {
        "/stop" => Ok(Some(format!(
            "stop queued command_id={}",
            queue_control_command(conn, "stop", from_user_id)?
        ))),
        "/resume" => Ok(Some(format!(
            "resume queued command_id={}",
            queue_control_command(conn, "resume", from_user_id)?
        ))),
        "/cancel_all" => Ok(Some(format!(
            "cancel_all queued command_id={}",
            queue_control_command(conn, "cancel_all", from_user_id)?
        ))),
        "/close_all" => Ok(Some(start_close_all_confirmation(
            conn,
            from_user_id,
            now_ms,
        )?)),
        "/confirm" => Ok(Some(confirm_close_all(conn, text, from_user_id, now_ms)?)),
        _ => Ok(None),
    }
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
    fn unauthorized_user_gets_no_sqlite_state_and_no_control() {
        let conn = test_conn();
        let response = operator_response(&conn, "/bad\"cmd", "999", &["123".to_string()], 1_000)
            .unwrap()
            .unwrap();

        let command_count: i64 = conn
            .query_row("select count(*) from control_commands", [], |r| r.get(0))
            .unwrap();
        let payload: String = conn
            .query_row(
                "select payload_json from events where component = 'telegram'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&payload).unwrap();

        assert!(response.contains("unauthorized"));
        assert_eq!(command_count, 0);
        assert_eq!(parsed["from_user_id"], "999");
        assert_eq!(parsed["command"], "/bad\"cmd");
    }

    #[test]
    fn help_lists_m6_commands_for_allowed_user() {
        let conn = test_conn();
        let response = operator_response(&conn, "/help", "123", &["123".to_string()], 1_000)
            .unwrap()
            .unwrap();

        assert!(response.contains("/status"));
        assert!(response.contains("/trades"));
        assert!(response.contains("/close_all"));
    }

    #[test]
    fn smoke_report_is_not_a_telegram_command() {
        let conn = test_conn();
        crate::db::set_executor_state(&conn, "smoke:last_report", "reports/local.md").unwrap();

        let help = operator_response(&conn, "/help", "123", &["123".to_string()], 1_000)
            .unwrap()
            .unwrap();
        let response = operator_response(
            &conn,
            "/smoke_report",
            "123",
            &["123".to_string()],
            1_000,
        )
        .unwrap();

        assert!(!help.contains("/smoke_report"));
        assert!(response.is_none());
    }

    #[test]
    fn stop_resume_and_cancel_all_queue_commands_and_events() {
        for (text, command) in [
            ("/stop", "stop"),
            ("/resume", "resume"),
            ("/cancel_all", "cancel_all"),
        ] {
            let conn = test_conn();
            let response = operator_response(&conn, text, "123", &["123".to_string()], 1_000)
                .unwrap()
                .unwrap();
            let row = conn
                .query_row(
                    "select command, status, requested_by from control_commands",
                    [],
                    |r| {
                        Ok((
                            r.get::<_, String>(0)?,
                            r.get::<_, String>(1)?,
                            r.get::<_, String>(2)?,
                        ))
                    },
                )
                .unwrap();
            let event_count: i64 = conn
                .query_row(
                    "select count(*) from events where component = 'telegram'",
                    [],
                    |r| r.get(0),
                )
                .unwrap();

            assert!(response.contains("queued"));
            assert_eq!(
                row,
                (
                    command.to_string(),
                    "pending".to_string(),
                    "123".to_string()
                )
            );
            assert!(event_count >= 1);
        }
    }

    #[test]
    fn close_all_requires_same_user_confirmation_before_queueing() {
        let conn = test_conn();
        let first = operator_response(&conn, "/close_all", "123", &["123".to_string()], 10_000)
            .unwrap()
            .unwrap();
        assert!(first.contains("/confirm"));
        assert_eq!(
            conn.query_row("select count(*) from control_commands", [], |r| {
                r.get::<_, i64>(0)
            })
            .unwrap(),
            0
        );
        let raw_state = crate::db::get_executor_state(&conn, "close_all_confirm:123")
            .unwrap()
            .unwrap();
        let state: serde_json::Value = serde_json::from_str(&raw_state).unwrap();
        assert_eq!(state["requested_by"], "123");
        assert_eq!(state["expires_ms"], 70_000);
        assert!(state["code_hash"].as_str().unwrap().len() >= 64);

        let code = first.split_whitespace().last().unwrap().to_string();
        let second = operator_response(
            &conn,
            &format!("/confirm {code}"),
            "123",
            &["123".to_string()],
            20_000,
        )
        .unwrap()
        .unwrap();

        let command = conn
            .query_row("select command from control_commands", [], |r| {
                r.get::<_, String>(0)
            })
            .unwrap();
        let accepted_events: i64 = conn
            .query_row(
                "select count(*) from events
                 where component = 'telegram'
                   and message = 'telegram close_all confirmation accepted'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(second.contains("queued"));
        assert_eq!(command, "close_all");
        assert_eq!(accepted_events, 1);
    }

    #[test]
    fn close_all_confirmation_rejects_wrong_user_wrong_code_and_expiry() {
        let conn = test_conn();
        let first = operator_response(&conn, "/close_all", "123", &["123".to_string()], 10_000)
            .unwrap()
            .unwrap();
        let code = first.split_whitespace().last().unwrap().to_string();

        let wrong_user = operator_response(
            &conn,
            &format!("/confirm {code}"),
            "456",
            &["123".to_string(), "456".to_string()],
            20_000,
        )
        .unwrap()
        .unwrap();
        let wrong_code = operator_response(
            &conn,
            "/confirm badbad",
            "123",
            &["123".to_string()],
            20_000,
        )
        .unwrap()
        .unwrap();
        let expired = operator_response(
            &conn,
            &format!("/confirm {code}"),
            "123",
            &["123".to_string()],
            80_001,
        )
        .unwrap()
        .unwrap();

        let command_count: i64 = conn
            .query_row("select count(*) from control_commands", [], |r| r.get(0))
            .unwrap();
        assert!(wrong_user.contains("rejected"));
        assert!(wrong_code.contains("rejected"));
        assert!(expired.contains("expired"));
        assert_eq!(command_count, 0);
    }

    #[test]
    fn close_all_confirmation_rejects_replay_and_expiry_boundary() {
        let conn = test_conn();
        let first = operator_response(&conn, "/close_all", "123", &["123".to_string()], 10_000)
            .unwrap()
            .unwrap();
        let code = first.split_whitespace().last().unwrap().to_string();

        let at_expiry = operator_response(
            &conn,
            &format!("/confirm {code}"),
            "123",
            &["123".to_string()],
            70_000,
        )
        .unwrap()
        .unwrap();
        assert!(at_expiry.contains("expired"));

        let first = operator_response(&conn, "/close_all", "123", &["123".to_string()], 100_000)
            .unwrap()
            .unwrap();
        let code = first.split_whitespace().last().unwrap().to_string();
        operator_response(
            &conn,
            &format!("/confirm {code}"),
            "123",
            &["123".to_string()],
            110_000,
        )
        .unwrap()
        .unwrap();
        let replay = operator_response(
            &conn,
            &format!("/confirm {code}"),
            "123",
            &["123".to_string()],
            120_000,
        )
        .unwrap()
        .unwrap();
        let command_count: i64 = conn
            .query_row("select count(*) from control_commands", [], |r| r.get(0))
            .unwrap();
        let rejected_events: i64 = conn
            .query_row(
                "select count(*) from events
                 where component = 'telegram'
                   and message = 'telegram close_all confirmation rejected'",
                [],
                |r| r.get(0),
            )
            .unwrap();

        assert!(replay.contains("rejected"));
        assert_eq!(command_count, 1);
        assert!(rejected_events >= 1);
    }

    #[test]
    fn trades_query_reads_recent_fills() {
        let conn = test_conn();
        conn.execute(
            "insert into orders (
               order_id, client_oid, intent_id, symbol, side, action, order_type,
               status, price, size, filled_size, created_at, updated_at
             ) values (
               'order-1', 'client-1', null, 'ETHUSDT', 'buy', 'open',
               'market', 'filled', 2000.0, 0.1, 0.1, '2026-07-06T00:00:00Z', '2026-07-06T00:00:00Z'
             )",
            [],
        )
        .unwrap();
        conn.execute(
            "insert into fills (
               fill_id, order_id, symbol, side, price, size, fee, created_at,
               trade_id, client_oid, raw_json
             ) values (
               'fill-1', 'order-1', 'ETHUSDT', 'buy', 2000.0, 0.1, 0.2,
               '2026-07-06T00:00:01Z', 'trade-1', 'client-1', '{}'
             )",
            [],
        )
        .unwrap();

        let response = operator_response(&conn, "/trades", "123", &["123".to_string()], 1_000)
            .unwrap()
            .unwrap();

        assert!(response.contains("ETHUSDT"));
        assert!(response.contains("buy"));
        assert!(response.contains("fee=0.2"));
    }

    #[test]
    fn pnl_query_is_conservative_about_realized_and_total() {
        let conn = test_conn();
        crate::db::insert_equity_snapshot(&conn, 1000.0, 500.0, 0.0, 0.0).unwrap();

        let response = operator_response(&conn, "/pnl", "123", &["123".to_string()], 1_000)
            .unwrap()
            .unwrap();

        assert!(response.contains("realized=n/a"));
        assert!(response.contains("total=n/a"));
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
