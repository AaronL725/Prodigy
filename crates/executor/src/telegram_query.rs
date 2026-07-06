//! SQLite-backed Telegram query formatting.
//!
//! Maps the operator's `/status /positions /orders /pnl /risk` commands to
//! short SQLite-backed replies. `query_response` keeps the M4 read-only
//! behavior; `operator_response` adds M6 authorization and operator commands.
//!
//! `query_response` returns `Ok(None)` for anything that isn't a recognized
//! command, so the polling loop simply doesn't reply to noise.

use anyhow::Result;
use rusqlite::{Connection, OptionalExtension};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TelegramReply {
    pub text: String,
    pub parse_mode: Option<&'static str>,
    pub reply_markup: Option<serde_json::Value>,
}

impl TelegramReply {
    pub fn html(text: String, reply_markup: Option<serde_json::Value>) -> Self {
        Self {
            text,
            parse_mode: Some("HTML"),
            reply_markup,
        }
    }

    pub fn plain(text: String) -> Self {
        Self {
            text,
            parse_mode: None,
            reply_markup: None,
        }
    }
}

pub fn html_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn button(text: &str, callback_data: &str) -> serde_json::Value {
    serde_json::json!({
        "text": text,
        "callback_data": callback_data,
    })
}

fn inline_keyboard(rows: Vec<Vec<serde_json::Value>>) -> serde_json::Value {
    serde_json::json!({ "inline_keyboard": rows })
}

pub fn navigation_keyboard() -> serde_json::Value {
    inline_keyboard(vec![
        vec![
            button("Status", "tgux:status"),
            button("PnL", "tgux:pnl"),
            button("Risk", "tgux:risk"),
        ],
        vec![
            button("Positions", "tgux:positions"),
            button("Orders", "tgux:orders"),
            button("Trades", "tgux:trades"),
        ],
        vec![
            button("Events", "tgux:events"),
            button("Smoke", "tgux:smoke"),
            button("Help", "tgux:help"),
        ],
        vec![button("Control", "tgux:control")],
    ])
}

pub fn control_keyboard() -> serde_json::Value {
    inline_keyboard(vec![
        vec![button("Stop", "tgux:stop"), button("Resume", "tgux:resume")],
        vec![
            button("Cancel All", "tgux:cancel_all"),
            button("Close All", "tgux:close_all"),
        ],
        vec![button("Back", "tgux:status")],
    ])
}

pub fn close_all_confirm_keyboard() -> serde_json::Value {
    inline_keyboard(vec![
        vec![button("Confirm Close All", "tgux:confirm_close_all")],
        vec![button("Cancel", "tgux:cancel_close_all")],
    ])
}

/// Map a single command line to its read-only reply, or `None` if it isn't a
/// recognized command (no reply). Remote trading controls are refused.
pub fn query_response(conn: &Connection, text: &str) -> Result<Option<String>> {
    Ok(query_reply(conn, text)?.map(|reply| reply.text))
}

pub fn query_reply(conn: &Connection, text: &str) -> Result<Option<TelegramReply>> {
    let command = text.split_whitespace().next().unwrap_or("");
    match command {
        "/status" => Ok(Some(status_reply(conn)?)),
        "/positions" => Ok(Some(positions_reply(conn)?)),
        "/orders" => Ok(Some(orders_reply(conn)?)),
        "/pnl" => Ok(Some(pnl_reply(conn)?)),
        "/risk" => Ok(Some(risk_reply(conn)?)),
        "/trades" => Ok(Some(trades_reply(conn)?)),
        "/events" => Ok(Some(events_reply(conn)?)),
        "/smoke_status" => Ok(Some(smoke_status_reply(conn)?)),
        "/help" => Ok(Some(help_reply())),
        "/stop" | "/resume" | "/close_all" => Ok(Some(TelegramReply::plain(
            "remote trading controls are not supported in M4".to_string(),
        ))),
        _ => Ok(None),
    }
}

fn row(label: &str, value: impl ToString) -> String {
    format!(
        "<b>{}</b> — <b>{}</b>",
        html_escape(label),
        html_escape(&value.to_string())
    )
}

fn muted(label: &str, value: impl ToString) -> String {
    format!(
        "{} — {}",
        html_escape(label),
        html_escape(&value.to_string())
    )
}

fn html_card(title: &str, rows: Vec<String>, footer: Option<String>) -> TelegramReply {
    let mut text = format!("◆ {}\n\n{}", html_escape(title), rows.join("\n"));
    if let Some(footer) = footer {
        text.push_str("\n\n— ");
        text.push_str(&html_escape(&footer));
    }
    TelegramReply::html(text, Some(navigation_keyboard()))
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
        "/help" => Ok(Some(help_reply().text)),
        "/status" => Ok(Some(status_reply(conn)?.text)),
        "/positions" => Ok(Some(positions_reply(conn)?.text)),
        "/orders" => Ok(Some(orders_reply(conn)?.text)),
        "/trades" => Ok(Some(trades_reply(conn)?.text)),
        "/pnl" => Ok(Some(pnl_reply(conn)?.text)),
        "/risk" => Ok(Some(risk_reply(conn)?.text)),
        "/events" => Ok(Some(events_reply(conn)?.text)),
        "/smoke_status" => Ok(Some(smoke_status_reply(conn)?.text)),
        "/stop" | "/resume" | "/cancel_all" | "/close_all" | "/confirm" => {
            match control_response(conn, text, from_user_id, now_ms) {
                Ok(reply) => Ok(reply),
                Err(err) => Ok(Some(control_failure_response(err))),
            }
        }
        _ => Ok(None),
    }
}

fn help_reply() -> TelegramReply {
    html_card(
        "HELP",
        vec![
            muted(
                "READ",
                "/help /status /positions /orders /trades /pnl /risk /events /smoke_status",
            ),
            muted(
                "CONTROL",
                "/stop /resume /cancel_all /close_all /confirm <code>",
            ),
        ],
        None,
    )
}

fn status_reply(conn: &Connection) -> Result<TelegramReply> {
    let pending: i64 = conn.query_row(
        "select count(*) from trade_intents where status = 'pending'",
        [],
        |r| r.get(0),
    )?;
    let pending_controls: i64 = conn.query_row(
        "select count(*) from control_commands where status = 'pending'",
        [],
        |r| r.get(0),
    )?;
    let manual_overrides = active_manual_override_count(conn)?;
    Ok(html_card(
        "PRODIGY OPERATOR",
        vec![
            row("MODE", "DEMO"),
            row("DAEMON", latest_daemon_status(conn)?),
            row("SIGNAL", latest_signal_status(conn)?),
            row("RECONCILE", latest_reconcile_status(conn)?),
            row("OPERATOR STOP", operator_stop_state(conn)?),
            row("MANUAL OVERRIDES", manual_overrides),
            row("PENDING INTENTS", pending),
            row("PENDING CONTROLS", pending_controls),
            row("LATEST ERROR", latest_critical_error(conn)?),
        ],
        None,
    ))
}

fn operator_stop_state(conn: &Connection) -> Result<String> {
    Ok(
        match crate::db::get_executor_state(conn, crate::control::OPERATOR_STOP_KEY)?.as_deref() {
            Some("active") => "active",
            _ => "cleared",
        }
        .to_string(),
    )
}

fn active_manual_override_count(conn: &Connection) -> Result<i64> {
    conn.query_row(
        "select count(*) from executor_state
         where key like 'manual_override:%' and value = 'active'",
        [],
        |r| r.get(0),
    )
    .map_err(Into::into)
}

fn latest_daemon_status(conn: &Connection) -> Result<String> {
    let row: Option<(String, String)> = conn
        .query_row(
            "select message, created_at
             from events
             where component = 'daemon'
             order by created_at desc, event_id desc
             limit 1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?;
    Ok(row
        .map(|(message, created_at)| format!("{message} at {created_at}"))
        .unwrap_or_else(|| "n/a".to_string()))
}

fn latest_signal_status(conn: &Connection) -> Result<String> {
    let row: Option<(String, String)> = conn
        .query_row(
            "select value, updated_at
             from executor_state
             where key like 'signal_processed:%'
                or key in ('signal:status', 'signal:last_run', 'smoke:status')
             order by updated_at desc, key desc
             limit 1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?;
    if let Some((value, updated_at)) = row {
        return Ok(format!("{value} at {updated_at}"));
    }
    let row: Option<(String, String)> = conn
        .query_row(
            "select message, created_at
             from events
             where component = 'signal'
             order by created_at desc, event_id desc
             limit 1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?;
    Ok(row
        .map(|(message, created_at)| format!("{message} at {created_at}"))
        .unwrap_or_else(|| "n/a".to_string()))
}

fn latest_reconcile_status(conn: &Connection) -> Result<String> {
    conn.query_row(
        "select created_at
         from events
         where message = 'reconciliation completed'
         order by created_at desc, event_id desc
         limit 1",
        [],
        |r| r.get(0),
    )
    .optional()?
    .map_or_else(|| Ok("n/a".to_string()), Ok)
}

fn latest_critical_error(conn: &Connection) -> Result<String> {
    let row: Option<(String, String, String, String)> = conn
        .query_row(
            "select severity, component, message, created_at
             from events
             where severity in ('critical', 'error')
             order by created_at desc, event_id desc
             limit 1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .optional()?;
    Ok(row
        .map(|(severity, component, message, created_at)| {
            format!("{severity} {component}: {message} at {created_at}")
        })
        .unwrap_or_else(|| "n/a".to_string()))
}

fn positions_reply(conn: &Connection) -> Result<TelegramReply> {
    let mut stmt = conn.prepare(
        "select symbol, side, notional, entry_price, unrealized_pnl, ownership
         from positions order by symbol",
    )?;
    let rows = stmt.query_map([], |r| {
        Ok(row(
            "POSITION",
            format!(
                "{} {} notional={} entry={} upnl={} ownership={}",
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, f64>(2)?,
                r.get::<_, f64>(3)?,
                r.get::<_, f64>(4)?,
                r.get::<_, String>(5)?,
            ),
        ))
    })?;
    let mut lines = rows.collect::<Result<Vec<_>, _>>()?;
    if lines.is_empty() {
        lines.push(row("POSITIONS", "NONE"));
    }
    Ok(html_card("POSITIONS", lines, None))
}

fn orders_reply(conn: &Connection) -> Result<TelegramReply> {
    let mut stmt = conn.prepare(
        "with working as (
           select client_oid, symbol, side, action, status, size, filled_size, updated_at, 0 as bucket
           from orders
           where intent_id is not null and status in ('submitted', 'live')
         ),
         recent as (
           select client_oid, symbol, side, action, status, size, filled_size, updated_at, 1 as bucket
           from orders
           where client_oid not in (select client_oid from working)
           order by updated_at desc
           limit 10
         )
         select client_oid, symbol, side, action, status, size, filled_size
         from (
           select * from working
           union all
           select * from recent
         )
         order by bucket asc, updated_at desc",
    )?;
    let rows = stmt.query_map([], |r| {
        Ok(row(
            "ORDER",
            format!(
                "{} {} {} {} status={} size={} filled={}",
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, String>(4)?,
                r.get::<_, f64>(5)?,
                r.get::<_, f64>(6)?,
            ),
        ))
    })?;
    let mut lines = rows.collect::<Result<Vec<_>, _>>()?;
    if lines.is_empty() {
        lines.push(row("ORDERS", "NONE"));
    }
    Ok(html_card("ORDERS", lines, None))
}

fn trades_reply(conn: &Connection) -> Result<TelegramReply> {
    let mut stmt = conn.prepare(
        "select symbol, side, price, size, fee, created_at
         from fills order by created_at desc limit 10",
    )?;
    let rows = stmt.query_map([], |r| {
        Ok(row(
            "TRADE",
            format!(
                "{} {} price={} size={} fee={} at={}",
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, f64>(2)?,
                r.get::<_, f64>(3)?,
                r.get::<_, f64>(4)?,
                r.get::<_, String>(5)?,
            ),
        ))
    })?;
    let mut lines = rows.collect::<Result<Vec<_>, _>>()?;
    if lines.is_empty() {
        lines.push(row("TRADES", "NONE"));
    }
    Ok(html_card("TRADES", lines, None))
}

fn pnl_reply(conn: &Connection) -> Result<TelegramReply> {
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
    Ok(html_card(
        "PNL",
        vec![
            row("UNREALIZED", unrealized),
            row("EQUITY", equity.unwrap_or(0.0)),
            row("REALIZED", "n/a"),
            row("TOTAL", "n/a"),
        ],
        None,
    ))
}

fn risk_reply(conn: &Connection) -> Result<TelegramReply> {
    let manual_overrides = active_manual_override_count(conn)?;
    let operator_stop = operator_stop_state(conn)?;
    let equity: Option<(f64, f64, f64)> = conn
        .query_row(
            "select equity, available_margin, unrealized_pnl
             from equity_snapshots
             order by created_at desc
             limit 1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .optional()?;
    let (equity_text, available_text, margin_state, trading_suspension) = match equity {
        Some((equity, available_margin, unrealized_pnl)) => {
            let params = crate::risk::RiskParams::default();
            let margin_state = if equity <= 0.0
                || available_margin < equity * params.min_available_margin_fraction
            {
                "low"
            } else {
                "ok"
            };
            let trading_suspension = if equity > 0.0
                && unrealized_pnl <= -equity * params.trading_suspension_unrealized_loss_x_equity
            {
                "active"
            } else {
                "inactive"
            };
            (
                equity.to_string(),
                available_margin.to_string(),
                margin_state.to_string(),
                trading_suspension.to_string(),
            )
        }
        None => (
            "n/a".to_string(),
            "n/a".to_string(),
            "n/a".to_string(),
            "n/a".to_string(),
        ),
    };
    let risk_state = if operator_stop == "active"
        || manual_overrides > 0
        || margin_state == "low"
        || trading_suspension == "active"
    {
        "blocked"
    } else if margin_state == "n/a" {
        "unknown"
    } else {
        "ok"
    };
    Ok(html_card(
        "RISK",
        vec![
            row("RISK STATE", risk_state),
            row("EQUITY", equity_text),
            row("AVAILABLE MARGIN", available_text),
            row("MARGIN STATE", margin_state),
            row("MANUAL OVERRIDES", manual_overrides),
            row("OPERATOR STOP", operator_stop),
            row("TRADING SUSPENSION", trading_suspension),
        ],
        None,
    ))
}

fn events_reply(conn: &Connection) -> Result<TelegramReply> {
    let mut stmt = conn.prepare(
        "select created_at, severity, component, message
         from events
         where severity in ('warning', 'error', 'critical')
         order by created_at desc limit 10",
    )?;
    let rows = stmt.query_map([], |r| {
        Ok(row(
            "EVENT",
            format!(
                "{} {} {}: {}",
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?,
            ),
        ))
    })?;
    let mut lines = rows.collect::<Result<Vec<_>, _>>()?;
    if lines.is_empty() {
        lines.push(row("EVENTS", "NONE"));
    }
    Ok(html_card("EVENTS", lines, None))
}

fn smoke_status_reply(conn: &Connection) -> Result<TelegramReply> {
    Ok(html_card(
        "SMOKE STATUS",
        vec![row(
            "STATUS",
            crate::db::get_executor_state(conn, "smoke:status")?
                .unwrap_or_else(|| "n/a".to_string()),
        )],
        None,
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
    with_savepoint(conn, "telegram_close_all_confirm", |conn| {
        crate::db::set_executor_state(conn, &format!("close_all_confirm:{requested_by}"), &value)?;
        audit(
            conn,
            "telegram close_all confirmation generated",
            &serde_json::json!({
                "requested_by": requested_by,
                "expires_ms": now_ms + 60_000,
            })
            .to_string(),
        )
    })?;
    Ok(format!("confirm close_all with /confirm {code}"))
}

fn control_failure_response(err: anyhow::Error) -> String {
    format!("telegram command failed: {err}")
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
    fn html_escape_escapes_dynamic_telegram_values() {
        assert_eq!(html_escape("ETH<&>USDT"), "ETH&lt;&amp;&gt;USDT");
    }

    #[test]
    fn navigation_keyboard_contains_query_and_control_buttons() {
        let keyboard = navigation_keyboard();
        let text = serde_json::to_string(&keyboard).unwrap();

        for callback in [
            "tgux:status",
            "tgux:pnl",
            "tgux:risk",
            "tgux:positions",
            "tgux:orders",
            "tgux:trades",
            "tgux:events",
            "tgux:smoke",
            "tgux:help",
            "tgux:control",
        ] {
            assert!(text.contains(callback), "missing {callback}");
        }
    }

    #[test]
    fn control_keyboard_contains_only_existing_control_commands() {
        let keyboard = control_keyboard();
        let text = serde_json::to_string(&keyboard).unwrap();

        for callback in [
            "tgux:stop",
            "tgux:resume",
            "tgux:cancel_all",
            "tgux:close_all",
        ] {
            assert!(text.contains(callback), "missing {callback}");
        }
        assert!(text.contains("tgux:status"), "missing Back callback");
        assert!(!text.contains("open"));
        assert!(!text.contains("set_param"));
        assert!(!text.contains("live"));
    }

    #[test]
    fn status_reply_uses_editorial_html_layout_and_navigation_keyboard() {
        let conn = test_conn();
        crate::db::write_event(&conn, "info", "daemon", "daemon started", "{}").unwrap();

        let reply = query_reply(&conn, "/status").unwrap().unwrap();

        assert_eq!(reply.parse_mode, Some("HTML"));
        assert!(reply.text.contains("◆ PRODIGY OPERATOR"));
        assert!(reply.text.contains("<b>MODE</b>"));
        assert!(reply.text.contains("<b>DEMO</b>"));
        assert!(reply.text.contains("<b>DAEMON</b>"));
        assert!(reply.reply_markup.is_some());
    }

    #[test]
    fn pnl_reply_keeps_realized_and_total_conservative() {
        let conn = test_conn();
        conn.execute(
            "insert into equity_snapshots (
               snapshot_id, created_at, equity, available_margin, unrealized_pnl, realized_pnl_24h
             ) values ('s1', '2026-07-06T00:00:00Z', 1000, 900, 2.5, 0)",
            [],
        )
        .unwrap();

        let reply = query_reply(&conn, "/pnl").unwrap().unwrap();

        assert_eq!(reply.parse_mode, Some("HTML"));
        assert!(reply.text.contains("◆ PNL"));
        assert!(reply.text.contains("<b>REALIZED</b>"));
        assert!(reply.text.contains("<b>n/a</b>"));
        assert!(reply.text.contains("<b>TOTAL</b>"));
    }

    #[test]
    fn dynamic_values_are_escaped_in_position_reply() {
        let conn = test_conn();
        conn.execute(
            "insert into positions (
              symbol, side, notional, entry_price, unrealized_pnl, updated_at,
              ownership, raw_json
            ) values ('BAD<&>SYM', 'long', 100.0, 2000.0, 3.5, 'now', 'system', '{}')",
            [],
        )
        .unwrap();

        let reply = query_reply(&conn, "/positions").unwrap().unwrap();

        assert!(reply.text.contains("BAD&lt;&amp;&gt;SYM"));
        assert!(!reply.text.contains("BAD<&>SYM"));
    }

    #[test]
    fn status_query_reads_sqlite_without_side_effects() {
        let conn = test_conn();
        crate::db::write_event(&conn, "info", "daemon", "daemon started", "{}").unwrap();

        let response = query_response(&conn, "/status").unwrap().unwrap();

        assert!(response.contains("◆ PRODIGY OPERATOR"));
        assert!(response.contains("<b>DAEMON</b>"));
        assert!(response.contains("daemon started"));
    }

    #[test]
    fn status_query_reports_operator_stop_pending_controls_latest_error_and_freshness() {
        let conn = test_conn();
        conn.execute(
            "insert into events (
              event_id, created_at, severity, component, message, payload_json
            ) values
            ('e-daemon', '2026-07-06T00:00:00Z', 'info', 'daemon', 'daemon started', '{}'),
            ('e-reconcile', '2026-07-06T00:01:00Z', 'info', 'executor', 'reconciliation completed', '{}'),
            ('e-error', '2026-07-06T00:03:00Z', 'critical', 'executor', 'risk gate failed', '{}')",
            [],
        )
        .unwrap();
        conn.execute(
            "insert into executor_state (key, value, updated_at)
             values
             ('operator_stop:global', 'active', '2026-07-06T00:00:30Z'),
             ('manual_override:ETHUSDT', 'active', '2026-07-06T00:00:40Z'),
             ('signal_processed:example-factors:ETHUSDT:15m:2026-07-06T00:00:00Z', 'no_signal', '2026-07-06T00:02:00Z')",
            [],
        )
        .unwrap();
        conn.execute(
            "insert into trade_intents (
              intent_id, created_at, symbol, side, action, target_notional,
              max_order_notional, status, source
            ) values ('i-pending', '2026-07-06T00:02:30Z', 'ETHUSDT', 'long', 'open', 100, 100, 'pending', 'test')",
            [],
        )
        .unwrap();
        conn.execute(
            "insert into control_commands (
              command_id, created_at, command, status, requested_by
            ) values
            ('cmd-stop', '2026-07-06T00:02:31Z', 'stop', 'pending', '123'),
            ('cmd-cancel', '2026-07-06T00:02:32Z', 'cancel_all', 'pending', '123')",
            [],
        )
        .unwrap();

        let response = query_response(&conn, "/status").unwrap().unwrap();

        assert!(response.contains("<b>DAEMON</b> — <b>daemon started at 2026-07-06T00:00:00Z</b>"));
        assert!(response.contains("<b>SIGNAL</b> — <b>no_signal at 2026-07-06T00:02:00Z</b>"));
        assert!(response.contains("<b>RECONCILE</b> — <b>2026-07-06T00:01:00Z</b>"));
        assert!(response.contains("<b>OPERATOR STOP</b> — <b>active</b>"));
        assert!(response.contains("<b>MANUAL OVERRIDES</b> — <b>1</b>"));
        assert!(response.contains("<b>PENDING INTENTS</b> — <b>1</b>"));
        assert!(response.contains("<b>PENDING CONTROLS</b> — <b>2</b>"));
        assert!(response
            .contains("<b>LATEST ERROR</b> — <b>critical executor: risk gate failed at 2026-07-06T00:03:00Z</b>"));
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
    fn orders_query_prioritizes_old_working_system_orders_before_recent_orders() {
        let conn = test_conn();
        conn.execute(
            "insert into trade_intents (
              intent_id, created_at, symbol, side, action, target_notional,
              max_order_notional, status, source
            ) values ('intent-working', '2026-07-06T00:00:00Z', 'ETHUSDT', 'long', 'open', 100, 100, 'executed', 'test')",
            [],
        )
        .unwrap();
        conn.execute(
            "insert into orders (
               order_id, client_oid, intent_id, symbol, side, action, order_type,
               status, price, size, filled_size, created_at, updated_at
             ) values (
               'old-working', 'old-working', 'intent-working', 'ETHUSDT', 'buy', 'open',
               'limit', 'submitted', 2000.0, 0.5, 0.0, '2026-07-06T00:00:00Z', '2026-07-06T00:00:00Z'
             )",
            [],
        )
        .unwrap();
        for n in 0..11 {
            conn.execute(
                "insert into orders (
                   order_id, client_oid, intent_id, symbol, side, action, order_type,
                   status, price, size, filled_size, created_at, updated_at
                 ) values (?, ?, null, 'ETHUSDT', 'buy', 'open',
                   'market', 'filled', 2000.0, 0.1, 0.1, ?, ?)",
                rusqlite::params![
                    format!("recent-{n}"),
                    format!("recent-{n}"),
                    format!("2026-07-06T00:{:02}:00Z", n + 1),
                    format!("2026-07-06T00:{:02}:00Z", n + 1),
                ],
            )
            .unwrap();
        }

        let response = query_response(&conn, "/orders").unwrap().unwrap();

        let lines: Vec<_> = response.lines().collect();
        let old_working = lines
            .iter()
            .position(|line| line.contains("old-working"))
            .unwrap();
        let newest_recent = lines
            .iter()
            .position(|line| line.contains("recent-10"))
            .unwrap();
        assert!(old_working < newest_recent);
        assert_eq!(response.matches("old-working").count(), 1);
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
        let response =
            operator_response(&conn, "/smoke_report", "123", &["123".to_string()], 1_000).unwrap();

        assert!(!help.contains("/smoke_report"));
        assert!(response.is_none());
    }

    #[test]
    fn remote_open_param_model_shell_and_live_commands_are_not_operator_commands() {
        let conn = test_conn();
        for text in [
            "/open long",
            "/buy ETHUSDT",
            "/set_param leverage 1",
            "/model_debug",
            "/shell ls",
            "/live on",
        ] {
            let response =
                operator_response(&conn, text, "123", &["123".to_string()], 1_000).unwrap();
            assert!(
                response.is_none(),
                "{text} should not be a Telegram operator command"
            );
        }

        let command_count: i64 = conn
            .query_row("select count(*) from control_commands", [], |r| r.get(0))
            .unwrap();
        assert_eq!(command_count, 0);
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
    fn control_command_write_failure_returns_failure_reply_without_queued_semantics() {
        let conn = test_conn();
        conn.execute("drop table control_commands", []).unwrap();

        let response = operator_response(&conn, "/stop", "123", &["123".to_string()], 1_000)
            .unwrap()
            .unwrap();
        let event_count: i64 = conn
            .query_row(
                "select count(*) from events where component = 'telegram'",
                [],
                |r| r.get(0),
            )
            .unwrap();

        assert!(response.contains("failed"));
        assert!(!response.contains("queued"));
        assert_eq!(event_count, 0);
    }

    #[test]
    fn audit_write_failure_returns_failure_reply_and_rolls_back_control_command() {
        let conn = test_conn();
        conn.execute("drop table events", []).unwrap();

        let response = operator_response(&conn, "/stop", "123", &["123".to_string()], 1_000)
            .unwrap()
            .unwrap();
        let command_count: i64 = conn
            .query_row("select count(*) from control_commands", [], |r| r.get(0))
            .unwrap();

        assert!(response.contains("failed"));
        assert!(!response.contains("queued"));
        assert_eq!(command_count, 0);
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
    fn close_all_pending_confirmation_write_failure_returns_failure_without_pending_state() {
        let conn = test_conn();
        conn.execute("drop table events", []).unwrap();

        let response = operator_response(&conn, "/close_all", "123", &["123".to_string()], 1_000)
            .unwrap()
            .unwrap();
        let state = crate::db::get_executor_state(&conn, "close_all_confirm:123").unwrap();

        assert!(response.contains("failed"));
        assert!(!response.contains("/confirm"));
        assert!(state.is_none());
    }

    #[test]
    fn confirm_write_failure_returns_failure_reply_without_queueing_close_all() {
        let conn = test_conn();
        crate::db::set_executor_state(
            &conn,
            "close_all_confirm:123",
            &serde_json::json!({
                "status": "pending",
                "requested_by": "123",
                "code_hash": sha256_hex("abc123"),
                "expires_ms": 70_000,
            })
            .to_string(),
        )
        .unwrap();
        conn.execute("drop table events", []).unwrap();

        let response = operator_response(
            &conn,
            "/confirm abc123",
            "123",
            &["123".to_string()],
            20_000,
        )
        .unwrap()
        .unwrap();
        let command_count: i64 = conn
            .query_row("select count(*) from control_commands", [], |r| r.get(0))
            .unwrap();

        assert!(response.contains("failed"));
        assert!(!response.contains("queued"));
        assert_eq!(command_count, 0);
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

        assert!(response.contains("<b>REALIZED</b> — <b>n/a</b>"));
        assert!(response.contains("<b>TOTAL</b> — <b>n/a</b>"));
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

        assert!(response.contains("<b>UNREALIZED</b> — <b>0</b>"));
        assert!(response.contains("<b>EQUITY</b> — <b>1000</b>"));
    }

    #[test]
    fn risk_query_counts_active_manual_overrides() {
        let conn = test_conn();
        crate::db::insert_equity_snapshot(&conn, 1000.0, 500.0, 0.0, 0.0).unwrap();
        crate::db::set_executor_state(&conn, "manual_override:ETH/USDT:USDT", "active").unwrap();

        let response = query_response(&conn, "/risk").unwrap().unwrap();

        assert!(response.contains("<b>MANUAL OVERRIDES</b> — <b>1</b>"));
        assert!(response.contains("<b>AVAILABLE MARGIN</b> — <b>500</b>"));
    }

    #[test]
    fn risk_query_reports_margin_manual_override_and_suspension() {
        let conn = test_conn();
        conn.execute(
            "insert into equity_snapshots (
              snapshot_id, created_at, equity, available_margin, unrealized_pnl,
              realized_pnl_24h
            ) values ('snap-1', '2026-07-06T00:00:00Z', 1000, 25, -125, 0)",
            [],
        )
        .unwrap();
        conn.execute(
            "insert into executor_state (key, value, updated_at)
             values
             ('operator_stop:global', 'active', '2026-07-06T00:00:01Z'),
             ('manual_override:ETHUSDT', 'active', '2026-07-06T00:00:02Z'),
             ('manual_override:BTCUSDT', 'active', '2026-07-06T00:00:03Z')",
            [],
        )
        .unwrap();

        let response = query_response(&conn, "/risk").unwrap().unwrap();

        assert!(response.contains("<b>RISK STATE</b> — <b>blocked</b>"));
        assert!(response.contains("<b>EQUITY</b> — <b>1000</b>"));
        assert!(response.contains("<b>AVAILABLE MARGIN</b> — <b>25</b>"));
        assert!(response.contains("<b>MARGIN STATE</b> — <b>low</b>"));
        assert!(response.contains("<b>MANUAL OVERRIDES</b> — <b>2</b>"));
        assert!(response.contains("<b>OPERATOR STOP</b> — <b>active</b>"));
        assert!(response.contains("<b>TRADING SUSPENSION</b> — <b>active</b>"));
    }
}
