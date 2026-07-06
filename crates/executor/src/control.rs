use anyhow::Result;
use rusqlite::params;

use crate::bitget::{BitgetRestClient, CancelOrderRequest};
use crate::config::ExecutorConfig;
use crate::executor::MarketCache;
use crate::types::ControlCommand;

pub const OPERATOR_STOP_KEY: &str = "operator_stop:global";

pub fn apply_stop(conn: &rusqlite::Connection, command_id: &str) -> Result<()> {
    crate::db::set_executor_state(conn, OPERATOR_STOP_KEY, "active")?;
    crate::db::write_event(
        conn,
        "info",
        "control",
        "operator stop applied",
        &serde_json::json!({"command_id": command_id}).to_string(),
    )?;
    Ok(())
}

pub fn apply_resume(conn: &rusqlite::Connection, command_id: &str) -> Result<()> {
    crate::db::set_executor_state(conn, OPERATOR_STOP_KEY, "cleared")?;
    crate::db::write_event(
        conn,
        "info",
        "control",
        "operator stop cleared",
        &serde_json::json!({"command_id": command_id}).to_string(),
    )?;
    Ok(())
}

pub fn enqueue_close_all_intents(
    conn: &rusqlite::Connection,
    command_id: &str,
    requested_by: &str,
    symbol: &str,
) -> Result<usize> {
    audit_skipped_non_system_positions(conn, command_id, requested_by)?;
    audit_skipped_unsupported_system_positions(conn, command_id, requested_by, symbol)?;
    let mut queued = 0usize;
    for position in crate::db::system_positions(conn)?
        .into_iter()
        .filter(|position| position.symbol == symbol)
    {
        queued += conn.execute(
            "insert or ignore into trade_intents (
               intent_id, created_at, symbol, side, action, target_notional,
               max_order_notional, status, source, reason
             ) values (?, datetime('now'), ?, ?, 'close', 0, 0, 'pending',
               'telegram-control', ?)",
            params![
                format!("control-close:{command_id}:{}", position.symbol),
                position.symbol,
                position.side,
                format!("requested by {requested_by}"),
            ],
        )?;
    }
    Ok(queued)
}

fn audit_skipped_unsupported_system_positions(
    conn: &rusqlite::Connection,
    command_id: &str,
    requested_by: &str,
    supported_symbol: &str,
) -> Result<usize> {
    let mut stmt = conn.prepare(
        "select symbol, side
         from positions
         where ownership = 'system' and symbol <> ?
         order by symbol",
    )?;
    let rows = stmt.query_map([supported_symbol], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;
    let mut skipped = 0usize;
    for row in rows {
        let (symbol, side) = row?;
        crate::db::write_event(
            conn,
            "warning",
            "control",
            "close_all skipped unsupported system position",
            &serde_json::json!({
                "command_id": command_id,
                "requested_by": requested_by,
                "symbol": symbol,
                "side": side,
                "supported_symbol": supported_symbol
            })
            .to_string(),
        )?;
        skipped += 1;
    }
    Ok(skipped)
}

fn audit_skipped_non_system_positions(
    conn: &rusqlite::Connection,
    command_id: &str,
    requested_by: &str,
) -> Result<usize> {
    let mut stmt = conn.prepare(
        "select symbol, side, ownership
         from positions
         where ownership <> 'system'
         order by symbol",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
        ))
    })?;
    let mut skipped = 0usize;
    for row in rows {
        let (symbol, side, ownership) = row?;
        crate::db::write_event(
            conn,
            "warning",
            "control",
            "close_all skipped non-system position",
            &serde_json::json!({
                "command_id": command_id,
                "requested_by": requested_by,
                "symbol": symbol,
                "side": side,
                "ownership": ownership
            })
            .to_string(),
        )?;
        skipped += 1;
    }
    Ok(skipped)
}

pub async fn process_pending_control_commands_once(
    conn: &rusqlite::Connection,
    cfg: &ExecutorConfig,
    rest: &BitgetRestClient,
    market_cache: &mut MarketCache,
) -> Result<()> {
    cfg.validate_demo_only()?;
    let commands = crate::db::pending_control_commands(conn)?;
    for command in commands {
        if !crate::db::accept_control_command(conn, &command.command_id)? {
            continue;
        }
        crate::db::write_event(
            conn,
            "info",
            "control",
            "control command accepted",
            &serde_json::json!({
                "command_id": command.command_id,
                "command": command.command,
                "requested_by": command.requested_by
            })
            .to_string(),
        )?;
        let result = match command.command.as_str() {
            "stop" => apply_stop(conn, &command.command_id),
            "resume" => apply_resume(conn, &command.command_id),
            "cancel_all" => apply_cancel_all(conn, cfg, rest, &command.command_id).await,
            "close_all" => apply_close_all(conn, cfg, rest, market_cache, &command).await,
            other => Err(anyhow::anyhow!("unsupported control command: {other}")),
        };
        match result {
            Ok(()) => {
                crate::db::mark_control_command_executed(conn, &command.command_id)?;
                crate::db::write_event(
                    conn,
                    "info",
                    "control",
                    "control command executed",
                    &serde_json::json!({
                        "command_id": command.command_id,
                        "command": command.command,
                        "requested_by": command.requested_by
                    })
                    .to_string(),
                )?;
            }
            Err(err) => {
                crate::db::fail_control_command(conn, &command.command_id, &err.to_string())?;
                crate::db::write_event(
                    conn,
                    "error",
                    "control",
                    &format!("control command failed: {err}"),
                    &serde_json::json!({
                        "command_id": command.command_id,
                        "command": command.command,
                        "requested_by": command.requested_by
                    })
                    .to_string(),
                )?;
            }
        }
    }
    Ok(())
}

async fn apply_cancel_all(
    conn: &rusqlite::Connection,
    cfg: &ExecutorConfig,
    rest: &BitgetRestClient,
    command_id: &str,
) -> Result<()> {
    let orders = crate::db::local_working_system_orders(conn, &cfg.bitget_symbol)?;
    let mut cancelled = 0usize;
    let mut failed = 0usize;
    let mut left_live = false;
    let mut requested = Vec::new();
    for (client_oid, order_id, ordered_size) in &orders {
        let request = CancelOrderRequest {
            symbol: cfg.bitget_symbol.clone(),
            product_type: cfg.product_type.clone(),
            margin_coin: cfg.margin_coin.clone(),
            client_oid: client_oid.clone(),
        };
        if let Err(err) = rest.cancel_order(&request).await {
            crate::db::write_event(
                conn,
                "warning",
                "control",
                &format!("cancel_all failed for {client_oid}: {err}"),
                &serde_json::json!({"command_id": command_id, "client_oid": client_oid})
                    .to_string(),
            )?;
            failed += 1;
            continue;
        }
        requested.push((client_oid.clone(), order_id.clone(), *ordered_size));
    }

    let live = if requested.is_empty() {
        std::collections::HashSet::new()
    } else {
        exchange_pending_client_oids(cfg, rest).await?
    };
    for (client_oid, order_id, ordered_size) in requested {
        if live.contains(&client_oid) {
            crate::db::write_event(
                conn,
                "warning",
                "control",
                &format!("cancel_all left {client_oid} live on exchange"),
                &serde_json::json!({"command_id": command_id, "client_oid": client_oid})
                    .to_string(),
            )?;
            failed += 1;
            left_live = true;
            continue;
        }
        let detail = rest.get_order_detail(&client_oid).await?;
        match crate::reconcile::classify_missing_pending_order(&detail, ordered_size) {
            crate::reconcile::MissingOrderVerdict::Cancelled => {
                crate::db::mark_system_order_cancelled_by_command(conn, &client_oid)?;
                cancelled += 1;
            }
            crate::reconcile::MissingOrderVerdict::CancelledWithPartialFill(filled) => {
                crate::db::set_order_filled_from_detail(conn, &order_id, filled)?;
                crate::db::mark_system_order_cancelled_by_command(conn, &client_oid)?;
                cancelled += 1;
            }
            crate::reconcile::MissingOrderVerdict::Filled(filled) => {
                crate::db::set_order_filled_from_detail(conn, &order_id, filled)?;
            }
            crate::reconcile::MissingOrderVerdict::Unknown => {
                crate::db::write_event(
                    conn,
                    "warning",
                    "control",
                    &format!("cancel_all could not confirm {client_oid} cancelled"),
                    &serde_json::json!({"command_id": command_id, "client_oid": client_oid})
                        .to_string(),
                )?;
                failed += 1;
            }
        }
    }
    crate::db::write_event(
        conn,
        "info",
        "control",
        "cancel_all processed",
        &serde_json::json!({
            "command_id": command_id,
            "attempted": orders.len(),
            "cancelled": cancelled
        })
        .to_string(),
    )?;
    if failed > 0 {
        if left_live {
            return Err(anyhow::anyhow!(
                "cancel_all left {} working system orders",
                crate::db::local_working_system_orders(conn, &cfg.bitget_symbol)?.len()
            ));
        }
        return Err(anyhow::anyhow!(
            "cancel_all failed for {failed} of {} system orders",
            orders.len()
        ));
    }
    let working = crate::db::local_working_system_orders(conn, &cfg.bitget_symbol)?;
    if !working.is_empty() {
        return Err(anyhow::anyhow!(
            "cancel_all left {} working system orders",
            working.len()
        ));
    }
    Ok(())
}

async fn exchange_pending_client_oids(
    cfg: &ExecutorConfig,
    rest: &BitgetRestClient,
) -> Result<std::collections::HashSet<String>> {
    let pending = rest
        .get(
            "/api/v2/mix/order/orders-pending",
            &[
                ("productType", cfg.product_type.clone()),
                ("marginCoin", cfg.margin_coin.clone()),
            ],
        )
        .await?;
    let mut out = std::collections::HashSet::new();
    for row in pending
        .get("data")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default()
    {
        if row.get("symbol").and_then(serde_json::Value::as_str) != Some(cfg.bitget_symbol.as_str())
        {
            continue;
        }
        if let Some(client_oid) = row.get("clientOid").and_then(serde_json::Value::as_str) {
            out.insert(client_oid.to_string());
        }
    }
    Ok(out)
}

async fn reconcile_control(
    conn: &rusqlite::Connection,
    cfg: &ExecutorConfig,
    rest: &BitgetRestClient,
) -> Result<()> {
    crate::reconcile::reconcile_once(
        conn,
        rest,
        "now",
        !cfg.test_reset_demo_state,
        cfg.telegram_bot_token.as_deref(),
        cfg.telegram_chat_id.as_deref(),
    )
    .await
}

async fn apply_close_all(
    conn: &rusqlite::Connection,
    cfg: &ExecutorConfig,
    rest: &BitgetRestClient,
    market_cache: &mut MarketCache,
    command: &ControlCommand,
) -> Result<()> {
    apply_cancel_all(conn, cfg, rest, &command.command_id).await?;
    reconcile_control(conn, cfg, rest).await?;
    let queued = enqueue_close_all_intents(
        conn,
        &command.command_id,
        &command.requested_by,
        &cfg.bitget_symbol,
    )?;
    crate::db::write_event(
        conn,
        "info",
        "control",
        "close_all intents queued",
        &serde_json::json!({"command_id": command.command_id, "queued": queued}).to_string(),
    )?;

    if queued > 0 {
        let market = crate::executor::fetch_market_snapshot(cfg, rest).await?;
        market_cache.update(market.clone());
        let account = crate::executor::fetch_account_snapshot(rest, true).await?;
        let prefix = format!("control-close:{}:", command.command_id);
        for intent in crate::db::pending_intents(conn)?
            .into_iter()
            .filter(|intent| intent.intent_id.starts_with(&prefix))
        {
            if let Err(err) = crate::executor::process_one_intent(
                conn,
                cfg,
                rest,
                intent.clone(),
                market.clone(),
                account,
                market_cache,
            )
            .await
            {
                crate::executor::fail_intent_after_infra_error(conn, &intent.intent_id, &err);
            }
        }
    }
    reconcile_control(conn, cfg, rest).await?;
    ensure_close_all_finished(conn, cfg, &command.command_id)
}

fn ensure_close_all_finished(
    conn: &rusqlite::Connection,
    cfg: &ExecutorConfig,
    command_id: &str,
) -> Result<()> {
    let prefix = format!("control-close:{command_id}:");
    let mut stmt = conn.prepare(
        "select intent_id, status
         from trade_intents
         where intent_id like ?
         order by intent_id",
    )?;
    let rows = stmt.query_map(params![format!("{prefix}%")], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;
    let mut unfinished = Vec::new();
    for row in rows {
        let (intent_id, status) = row?;
        if status != "executed" {
            unfinished.push(format!("{intent_id}:{status}"));
        }
    }
    if !unfinished.is_empty() {
        return Err(anyhow::anyhow!(
            "close_all did not execute all close intents: {}",
            unfinished.join(", ")
        ));
    }

    let working = crate::db::local_working_system_orders(conn, &cfg.bitget_symbol)?;
    if !working.is_empty() {
        return Err(anyhow::anyhow!(
            "close_all left {} working system orders",
            working.len()
        ));
    }

    let positions: Vec<_> = crate::db::system_positions(conn)?
        .into_iter()
        .filter(|position| position.symbol == cfg.bitget_symbol)
        .collect();
    if !positions.is_empty() {
        return Err(anyhow::anyhow!(
            "close_all left {} system positions",
            positions.len()
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::time::{Duration, Instant};

    fn conn() -> rusqlite::Connection {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute_batch(include_str!("../../../schema/001_initial.sql"))
            .unwrap();
        conn.execute_batch(include_str!("../../../schema/002_execution.sql"))
            .unwrap();
        conn
    }

    #[test]
    fn stop_and_resume_update_operator_stop_state() {
        let conn = conn();
        apply_stop(&conn, "cmd-stop").unwrap();
        assert_eq!(
            crate::db::get_executor_state(&conn, "operator_stop:global")
                .unwrap()
                .as_deref(),
            Some("active")
        );

        apply_resume(&conn, "cmd-resume").unwrap();
        assert_eq!(
            crate::db::get_executor_state(&conn, "operator_stop:global")
                .unwrap()
                .as_deref(),
            Some("cleared")
        );
    }

    #[test]
    fn close_all_intents_are_created_only_for_configured_system_symbol() {
        let conn = conn();
        conn.execute(
            "insert into positions (
              symbol, side, notional, entry_price, unrealized_pnl, updated_at,
              ownership, opened_at, adopted_at, source_intent_id, raw_json
            ) values
            ('ETHUSDT', 'long', 100, 2000, 1, 'now', 'system', 'now', null, 'i1', '{}'),
            ('BTCUSDT', 'short', 100, 30000, 1, 'now', 'system', 'now', null, 'i2', '{}'),
            ('SOLUSDT', 'short', 100, 2000, 1, 'now', 'imported', 'now', 'now', null, '{}')",
            [],
        )
        .unwrap();

        enqueue_close_all_intents(&conn, "cmd-1", "123", "ETHUSDT").unwrap();

        let rows: Vec<(String, String, String)> = conn
            .prepare("select symbol, side, action from trade_intents order by symbol")
            .unwrap()
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            rows,
            vec![(
                "ETHUSDT".to_string(),
                "long".to_string(),
                "close".to_string()
            )]
        );
        let skipped_symbols: Vec<String> = conn
            .prepare(
                "select json_extract(payload_json, '$.symbol')
                 from events
                 where message = 'close_all skipped unsupported system position'
                 order by event_id",
            )
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(skipped_symbols, vec!["BTCUSDT".to_string()]);
    }

    #[test]
    fn close_all_enqueue_audits_skipped_non_system_positions() {
        let conn = conn();
        conn.execute(
            "insert into positions (
              symbol, side, notional, entry_price, unrealized_pnl, updated_at,
              ownership, opened_at, adopted_at, source_intent_id, raw_json
            ) values
            ('ETHUSDT', 'long', 100, 2000, 1, 'now', 'system', 'now', null, 'i1', '{}'),
            ('BTCUSDT', 'short', 100, 2000, 1, 'now', 'imported', 'now', 'now', null, '{}')",
            [],
        )
        .unwrap();

        enqueue_close_all_intents(&conn, "cmd-1", "123", "ETHUSDT").unwrap();

        let events: Vec<(String, String)> = conn
            .prepare("select message, payload_json from events order by created_at")
            .unwrap()
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].0, "close_all skipped non-system position");
        assert!(events[0].1.contains("\"symbol\":\"BTCUSDT\""));
        assert!(events[0].1.contains("\"ownership\":\"imported\""));
        assert!(events[0].1.contains("\"requested_by\":\"123\""));
    }

    fn http_stub() -> String {
        http_stub_with_cancel(true)
    }

    fn http_stub_with_cancel(cancel_ok: bool) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            let start = Instant::now();
            let mut handled = 0usize;
            while start.elapsed() < Duration::from_secs(2) {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        let mut buf = [0u8; 4096];
                        let n = stream.read(&mut buf).unwrap_or(0);
                        let request = String::from_utf8_lossy(&buf[..n]);
                        let (status, body) = if request
                            .starts_with("POST /api/v2/mix/order/cancel-order")
                        {
                            if cancel_ok {
                                ("200 OK", r#"{"code":"00000","data":{}}"#)
                            } else {
                                (
                                    "500 Internal Server Error",
                                    r#"{"code":"500","msg":"cancel failed"}"#,
                                )
                            }
                        } else if cancel_ok
                            && request.starts_with("GET /api/v2/mix/order/orders-pending")
                        {
                            ("200 OK", r#"{"code":"00000","data":[]}"#)
                        } else if cancel_ok && request.starts_with("GET /api/v2/mix/order/detail") {
                            (
                                "200 OK",
                                r#"{"code":"00000","data":{"status":"canceled","baseVolume":"0"}}"#,
                            )
                        } else {
                            (
                                "500 Internal Server Error",
                                r#"{"code":"500","msg":"stop"}"#,
                            )
                        };
                        let response = format!(
                            "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                            body.len()
                        );
                        stream.write_all(response.as_bytes()).unwrap();
                        handled += 1;
                    }
                    Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                        if handled > 0 && start.elapsed() > Duration::from_millis(300) {
                            break;
                        }
                        std::thread::sleep(Duration::from_millis(10));
                    }
                    Err(_) => break,
                }
            }
        });
        format!("http://{addr}")
    }

    fn close_missing_http_stub() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            let start = Instant::now();
            let mut handled = 0usize;
            let mut positions_calls = 0usize;
            while start.elapsed() < Duration::from_secs(3) {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        let mut buf = [0u8; 4096];
                        let n = stream.read(&mut buf).unwrap_or(0);
                        let request = String::from_utf8_lossy(&buf[..n]);
                        let body = if request.starts_with("GET /api/v2/mix/order/orders-pending") {
                            r#"{"code":"00000","data":[]}"#.to_string()
                        } else if request.starts_with("GET /api/v2/mix/order/fills") {
                            r#"{"code":"00000","data":{"fillList":[]}}"#.to_string()
                        } else if request.starts_with("GET /api/v2/mix/position/all-position") {
                            positions_calls += 1;
                            if positions_calls == 1 {
                                r#"{"code":"00000","data":[{"symbol":"ETHUSDT","holdSide":"long","total":"0.05","available":"0.05","averageOpenPrice":"2000","unrealizedPL":"0"}]}"#.to_string()
                            } else {
                                r#"{"code":"00000","data":[]}"#.to_string()
                            }
                        } else if request.starts_with("GET /api/v2/mix/market/ticker") {
                            r#"{"code":"00000","data":[{"bidPr":"1999","askPr":"2001"}]}"#
                                .to_string()
                        } else if request.starts_with("GET /api/v2/mix/account/account") {
                            r#"{"code":"00000","data":{"accountEquity":"1000","available":"900","unrealizedPL":"0"}}"#.to_string()
                        } else {
                            r#"{"code":"00000","data":{}}"#.to_string()
                        };
                        let response = format!(
                            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                            body.len()
                        );
                        stream.write_all(response.as_bytes()).unwrap();
                        handled += 1;
                    }
                    Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                        if handled > 0 && start.elapsed() > Duration::from_millis(500) {
                            break;
                        }
                        std::thread::sleep(Duration::from_millis(10));
                    }
                    Err(_) => break,
                }
            }
        });
        format!("http://{addr}")
    }

    fn cancel_still_live_http_stub() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            let start = Instant::now();
            let mut handled = 0usize;
            while start.elapsed() < Duration::from_secs(2) {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        let mut buf = [0u8; 4096];
                        let n = stream.read(&mut buf).unwrap_or(0);
                        let request = String::from_utf8_lossy(&buf[..n]);
                        let body = if request.starts_with("POST /api/v2/mix/order/cancel-order") {
                            r#"{"code":"00000","data":{}}"#
                        } else if request.starts_with("GET /api/v2/mix/order/orders-pending") {
                            r#"{"code":"00000","data":[{"symbol":"ETHUSDT","clientOid":"oid-1"}]}"#
                        } else if request.starts_with("GET /api/v2/mix/order/detail") {
                            r#"{"code":"00000","data":{"status":"live","baseVolume":"0"}}"#
                        } else {
                            r#"{"code":"00000","data":{}}"#
                        };
                        let response = format!(
                            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                            body.len()
                        );
                        stream.write_all(response.as_bytes()).unwrap();
                        handled += 1;
                    }
                    Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                        if handled > 0 && start.elapsed() > Duration::from_millis(300) {
                            break;
                        }
                        std::thread::sleep(Duration::from_millis(10));
                    }
                    Err(_) => break,
                }
            }
        });
        format!("http://{addr}")
    }

    fn cancel_partial_live_detail_http_stub() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            let start = Instant::now();
            let mut handled = 0usize;
            while start.elapsed() < Duration::from_secs(2) {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        let mut buf = [0u8; 4096];
                        let n = stream.read(&mut buf).unwrap_or(0);
                        let request = String::from_utf8_lossy(&buf[..n]);
                        let body = if request.starts_with("POST /api/v2/mix/order/cancel-order") {
                            r#"{"code":"00000","data":{}}"#
                        } else if request.starts_with("GET /api/v2/mix/order/orders-pending") {
                            r#"{"code":"00000","data":[]}"#
                        } else if request.starts_with("GET /api/v2/mix/order/detail") {
                            r#"{"code":"00000","data":{"status":"live","baseVolume":"0.04"}}"#
                        } else {
                            r#"{"code":"00000","data":{}}"#
                        };
                        let response = format!(
                            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                            body.len()
                        );
                        stream.write_all(response.as_bytes()).unwrap();
                        handled += 1;
                    }
                    Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                        if handled > 0 && start.elapsed() > Duration::from_millis(300) {
                            break;
                        }
                        std::thread::sleep(Duration::from_millis(10));
                    }
                    Err(_) => break,
                }
            }
        });
        format!("http://{addr}")
    }

    fn close_infra_error_after_accept_http_stub() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            let start = Instant::now();
            let mut handled = 0usize;
            let mut positions_calls = 0usize;
            while start.elapsed() < Duration::from_secs(3) {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        let mut buf = [0u8; 4096];
                        let n = stream.read(&mut buf).unwrap_or(0);
                        let request = String::from_utf8_lossy(&buf[..n]);
                        let (status, body) = if request
                            .starts_with("GET /api/v2/mix/order/orders-pending")
                        {
                            ("200 OK", r#"{"code":"00000","data":[]}"#.to_string())
                        } else if request.starts_with("GET /api/v2/mix/order/fills") {
                            (
                                "200 OK",
                                r#"{"code":"00000","data":{"fillList":[]}}"#.to_string(),
                            )
                        } else if request.starts_with("GET /api/v2/mix/position/all-position") {
                            positions_calls += 1;
                            if positions_calls <= 2 {
                                (
                                        "200 OK",
                                        r#"{"code":"00000","data":[{"symbol":"ETHUSDT","holdSide":"long","total":"0.05","available":"0.05","averageOpenPrice":"2000","unrealizedPL":"0"}]}"#.to_string(),
                                    )
                            } else {
                                (
                                    "500 Internal Server Error",
                                    r#"{"code":"500","msg":"position read failed"}"#.to_string(),
                                )
                            }
                        } else if request.starts_with("GET /api/v2/mix/market/ticker") {
                            (
                                "200 OK",
                                r#"{"code":"00000","data":[{"bidPr":"1999","askPr":"2001"}]}"#
                                    .to_string(),
                            )
                        } else if request.starts_with("GET /api/v2/mix/account/account") {
                            (
                                    "200 OK",
                                    r#"{"code":"00000","data":{"accountEquity":"1000","available":"900","unrealizedPL":"0"}}"#.to_string(),
                                )
                        } else {
                            ("200 OK", r#"{"code":"00000","data":{}}"#.to_string())
                        };
                        let response = format!(
                            "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                            body.len()
                        );
                        stream.write_all(response.as_bytes()).unwrap();
                        handled += 1;
                    }
                    Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                        if handled > 0 && start.elapsed() > Duration::from_millis(500) {
                            break;
                        }
                        std::thread::sleep(Duration::from_millis(10));
                    }
                    Err(_) => break,
                }
            }
        });
        format!("http://{addr}")
    }

    fn insert_executed_open_intent(conn: &rusqlite::Connection) {
        conn.execute(
            "insert into trade_intents (
              intent_id, created_at, symbol, side, action, target_notional,
              max_order_notional, status, source
            ) values (
              'intent-1', 'now', 'ETHUSDT', 'long', 'open', 100, 100,
              'executed', 'test'
            )",
            [],
        )
        .unwrap();
    }

    fn insert_working_order(conn: &rusqlite::Connection) {
        conn.execute(
            "insert into orders (
              order_id, client_oid, intent_id, symbol, side, action, order_type,
              status, price, size, filled_size, created_at, updated_at
            ) values (
              'o1', 'oid-1', 'intent-1', 'ETHUSDT', 'buy', 'open', 'limit',
              'submitted', 2000, 0.1, 0, 'now', 'now'
            )",
            [],
        )
        .unwrap();
    }

    #[tokio::test]
    async fn close_all_cancels_working_system_orders_before_processing_closes() {
        let conn = conn();
        insert_executed_open_intent(&conn);
        insert_working_order(&conn);
        conn.execute(
            "insert into positions (
              symbol, side, notional, entry_price, unrealized_pnl, updated_at,
              ownership, opened_at, adopted_at, source_intent_id, raw_json
            ) values
            ('ETHUSDT', 'long', 100, 2000, 1, 'now', 'system', 'now', null, 'i1', '{}')",
            [],
        )
        .unwrap();
        let cfg = crate::config::ExecutorConfig {
            rest_base_url: http_stub(),
            ..crate::config::ExecutorConfig::demo_for_tests()
        };
        let rest = crate::bitget::BitgetRestClient::new(cfg.clone()).unwrap();
        let command = ControlCommand {
            command_id: "cmd-close".to_string(),
            command: "close_all".to_string(),
            requested_by: "123".to_string(),
        };

        let err = apply_close_all(
            &conn,
            &cfg,
            &rest,
            &mut crate::executor::MarketCache::default(),
            &command,
        )
        .await
        .unwrap_err();

        assert!(err.to_string().contains("bitget"));
        assert_eq!(
            conn.query_row(
                "select status from orders where client_oid = 'oid-1'",
                [],
                |r| r.get::<_, String>(0)
            )
            .unwrap(),
            "cancelled"
        );
    }

    #[tokio::test]
    async fn cancel_all_returns_err_when_any_local_cancel_fails() {
        let conn = conn();
        insert_executed_open_intent(&conn);
        insert_working_order(&conn);
        let cfg = crate::config::ExecutorConfig {
            rest_base_url: http_stub_with_cancel(false),
            ..crate::config::ExecutorConfig::demo_for_tests()
        };
        let rest = crate::bitget::BitgetRestClient::new(cfg.clone()).unwrap();

        let err = apply_cancel_all(&conn, &cfg, &rest, "cmd-cancel")
            .await
            .unwrap_err();

        assert!(err.to_string().contains("cancel_all failed"));
        assert_eq!(
            conn.query_row(
                "select status from orders where client_oid = 'oid-1'",
                [],
                |r| r.get::<_, String>(0)
            )
            .unwrap(),
            "submitted"
        );
        let warnings: i64 = conn
            .query_row(
                "select count(*) from events
                 where severity = 'warning' and component = 'control'
                   and message like 'cancel_all failed for%'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(warnings, 1);
    }

    #[tokio::test]
    async fn cancel_all_fails_if_exchange_still_lists_order_after_cancel_ok() {
        let conn = conn();
        insert_executed_open_intent(&conn);
        insert_working_order(&conn);
        let cfg = crate::config::ExecutorConfig {
            rest_base_url: cancel_still_live_http_stub(),
            ..crate::config::ExecutorConfig::demo_for_tests()
        };
        let rest = crate::bitget::BitgetRestClient::new(cfg.clone()).unwrap();

        let err = apply_cancel_all(&conn, &cfg, &rest, "cmd-cancel")
            .await
            .unwrap_err();

        assert!(err.to_string().contains("working system orders"));
        assert_eq!(
            conn.query_row(
                "select status from orders where client_oid = 'oid-1'",
                [],
                |r| r.get::<_, String>(0)
            )
            .unwrap(),
            "submitted"
        );
    }

    #[tokio::test]
    async fn cancel_all_fails_if_partial_fill_detail_leaves_order_working_locally() {
        let conn = conn();
        insert_executed_open_intent(&conn);
        insert_working_order(&conn);
        let cfg = crate::config::ExecutorConfig {
            rest_base_url: cancel_partial_live_detail_http_stub(),
            ..crate::config::ExecutorConfig::demo_for_tests()
        };
        let rest = crate::bitget::BitgetRestClient::new(cfg.clone()).unwrap();

        let err = apply_cancel_all(&conn, &cfg, &rest, "cmd-cancel")
            .await
            .unwrap_err();

        assert!(err.to_string().contains("working system orders"));
        assert_eq!(
            conn.query_row(
                "select status from orders where client_oid = 'oid-1'",
                [],
                |r| r.get::<_, String>(0)
            )
            .unwrap(),
            "submitted"
        );
        assert_eq!(
            conn.query_row(
                "select filled_size from orders where client_oid = 'oid-1'",
                [],
                |r| r.get::<_, f64>(0)
            )
            .unwrap(),
            0.04
        );
    }

    #[tokio::test]
    async fn control_processor_audits_accepted_and_executed_commands() {
        let conn = conn();
        conn.execute(
            "insert into control_commands (
              command_id, created_at, command, status, requested_by
            ) values ('cmd-stop', 'now', 'stop', 'pending', '123')",
            [],
        )
        .unwrap();
        let cfg = crate::config::ExecutorConfig::demo_for_tests();
        let rest = crate::bitget::BitgetRestClient::new(cfg.clone()).unwrap();

        process_pending_control_commands_once(
            &conn,
            &cfg,
            &rest,
            &mut crate::executor::MarketCache::default(),
        )
        .await
        .unwrap();

        let messages: Vec<String> = conn
            .prepare("select message from events where component = 'control' order by created_at")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert!(messages.contains(&"control command accepted".to_string()));
        assert!(messages.contains(&"control command executed".to_string()));
    }

    #[tokio::test]
    async fn close_all_marks_prefixed_intent_failed_on_infra_error_after_accept() {
        let conn = conn();
        insert_executed_open_intent(&conn);
        conn.execute(
            "insert into orders (
              order_id, client_oid, intent_id, symbol, side, action, order_type,
              status, price, size, filled_size, created_at, updated_at
            ) values (
              'o1', 'oid-1', 'intent-1', 'ETHUSDT', 'buy', 'open', 'limit',
              'filled', 2000, 0.05, 0.05, 'now', 'now'
            )",
            [],
        )
        .unwrap();
        conn.execute(
            "insert into control_commands (
              command_id, created_at, command, status, requested_by
            ) values ('cmd-close', 'now', 'close_all', 'pending', '123')",
            [],
        )
        .unwrap();
        let cfg = crate::config::ExecutorConfig {
            rest_base_url: close_infra_error_after_accept_http_stub(),
            ..crate::config::ExecutorConfig::demo_for_tests()
        };
        let rest = crate::bitget::BitgetRestClient::new(cfg.clone()).unwrap();

        process_pending_control_commands_once(
            &conn,
            &cfg,
            &rest,
            &mut crate::executor::MarketCache::default(),
        )
        .await
        .unwrap();

        assert_eq!(
            conn.query_row(
                "select status from trade_intents where intent_id = 'control-close:cmd-close:ETHUSDT'",
                [],
                |r| r.get::<_, String>(0)
            )
            .unwrap(),
            "failed"
        );
    }

    #[tokio::test]
    async fn close_all_command_fails_when_prefixed_close_intent_failed() {
        let conn = conn();
        insert_executed_open_intent(&conn);
        conn.execute(
            "insert into orders (
              order_id, client_oid, intent_id, symbol, side, action, order_type,
              status, price, size, filled_size, created_at, updated_at
            ) values (
              'o1', 'oid-1', 'intent-1', 'ETHUSDT', 'buy', 'open', 'limit',
              'filled', 2000, 0.05, 0.05, 'now', 'now'
            )",
            [],
        )
        .unwrap();
        conn.execute(
            "insert into control_commands (
              command_id, created_at, command, status, requested_by
            ) values ('cmd-close', 'now', 'close_all', 'pending', '123')",
            [],
        )
        .unwrap();
        let cfg = crate::config::ExecutorConfig {
            rest_base_url: close_missing_http_stub(),
            ..crate::config::ExecutorConfig::demo_for_tests()
        };
        let rest = crate::bitget::BitgetRestClient::new(cfg.clone()).unwrap();

        process_pending_control_commands_once(
            &conn,
            &cfg,
            &rest,
            &mut crate::executor::MarketCache::default(),
        )
        .await
        .unwrap();

        assert_eq!(
            conn.query_row(
                "select status from control_commands where command_id = 'cmd-close'",
                [],
                |r| r.get::<_, String>(0)
            )
            .unwrap(),
            "failed"
        );
        assert_eq!(
            conn.query_row(
                "select status from trade_intents where intent_id = 'control-close:cmd-close:ETHUSDT'",
                [],
                |r| r.get::<_, String>(0)
            )
            .unwrap(),
            "failed"
        );
    }
}
