use anyhow::Result;
use std::sync::Arc;
use std::time::Duration;

use crate::config::ExecutorConfig;

#[derive(Debug, Clone, Default)]
pub struct DaemonOptions {
    pub max_runtime: Option<Duration>,
}

pub async fn run_daemon(cfg: ExecutorConfig, options: DaemonOptions) -> Result<()> {
    cfg.validate_demo_only()?;
    if let Some(max_runtime) = options.max_runtime {
        tokio::time::sleep(max_runtime).await;
        return Ok(());
    }
    futures_util::future::pending::<()>().await;
    Ok(())
}

/// Pure glue: stamp a parsed public-WS books5 update into the shared market cache
/// with the local-received time. Wraps `MarketCache::update_at` so the WS loop and
/// its test share one call site, and the freshness window stays LOCAL-received.
pub fn apply_public_market_update(
    cache: &mut crate::executor::MarketCache,
    update: crate::types::MarketUpdate,
    local_received_at_ms: i64,
) {
    cache.update_at(update, local_received_at_ms);
}

/// Pure glue: write a parsed private-WS update (orders/fills/positions) to SQLite
/// via the existing db upsert/insert helpers. Re-applying the same update is safe
/// (upserts by PK, fills insert-or-ignore). Wraps the three writes so the WS loop
/// and its test share one call site. Apply errors are surfaced (the loop logs them
/// and never crashes the daemon).
pub fn apply_private_ws_update(
    conn: &rusqlite::Connection,
    update: crate::types::PrivateWsUpdate,
) -> Result<()> {
    for order in update.orders {
        crate::db::upsert_order(conn, &order)?;
    }
    for fill in update.fills {
        crate::db::insert_fill(conn, &fill)?;
    }
    for position in update.positions {
        crate::db::upsert_position(conn, &position)?;
    }
    Ok(())
}

/// Long-running public-WS loop: connect, subscribe books5 for the configured
/// symbol, parse every incoming books5 message and refresh the shared market
/// cache with a LOCAL timestamp. Disconnects (or shutdown) reset the loop.
/// Spawned by `run_daemon`; demo-only invariant enforced at entry.
pub async fn run_public_ws_loop(
    cfg: ExecutorConfig,
    market_cache: Arc<tokio::sync::Mutex<crate::executor::MarketCache>>,
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> Result<()> {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;

    cfg.validate_demo_only()?;
    let mut shutdown = shutdown;
    loop {
        if *shutdown.borrow() {
            return Ok(());
        }
        match tokio_tungstenite::connect_async(&cfg.public_ws_url).await {
            Ok((mut socket, _)) => {
                socket
                    .send(Message::Text(
                        crate::bitget::public_books5_subscribe_message(&cfg).to_string(),
                    ))
                    .await?;
                'inner: loop {
                    tokio::select! {
                        _ = shutdown.changed() => {
                            if *shutdown.borrow() {
                                return Ok(());
                            }
                        }
                        msg = socket.next() => {
                            let Some(msg) = msg else { break 'inner; };
                            let Ok(msg) = msg else { break 'inner; };
                            let Ok(text) = msg.into_text() else { continue; };
                            match crate::bitget::parse_public_ws_message(&text) {
                                Ok(Some(update)) => {
                                    let now_ms = crate::bitget::now_ms().parse::<i64>().unwrap_or(0);
                                    let mut cache = market_cache.lock().await;
                                    apply_public_market_update(&mut cache, update, now_ms);
                                }
                                Ok(None) => {}
                                Err(err) => {
                                    eprintln!("public ws parse error: {err}");
                                }
                            }
                        }
                    }
                }
                eprintln!("public ws socket closed; reconnecting");
            }
            Err(err) => {
                eprintln!("public ws disconnected: {err}");
            }
        }
        // ponytail: fixed 1s reconnect backoff on EVERY reconnect path
        // (connect failure AND mid-stream socket close/error); exponential
        // backoff if disconnects become frequent (a flapping link would hammer
        // the endpoint and risk a temporary IP block). Shutdown exits return
        // above before reaching here, so they aren't delayed.
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// Long-running private-WS loop: connect, send the per-connection login (signed
/// `GET /user/verify`), then parse every incoming message and apply orders/fills/
/// positions to SQLite via `apply_private_ws_update`. A parse error or a SQLite
/// apply error is logged and the loop continues — the daemon must not crash on a
/// bad update. Disconnects (or shutdown) reset the loop. Spawned by `run_daemon`
/// (Task 7 wires it); demo-only invariant enforced at entry.
pub async fn run_private_ws_loop(
    cfg: ExecutorConfig,
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> Result<()> {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;

    cfg.validate_demo_only()?;
    let mut shutdown = shutdown;
    loop {
        if *shutdown.borrow() {
            return Ok(());
        }
        match tokio_tungstenite::connect_async(&cfg.private_ws_url).await {
            Ok((mut socket, _)) => {
                let timestamp = crate::bitget::now_seconds();
                socket
                    .send(Message::Text(
                        crate::bitget::private_login_message(&cfg, &timestamp).to_string(),
                    ))
                    .await?;
                loop {
                    tokio::select! {
                        _ = shutdown.changed() => {
                            if *shutdown.borrow() {
                                return Ok(());
                            }
                        }
                        msg = socket.next() => {
                            let Some(msg) = msg else { break; };
                            let Ok(msg) = msg else { break; };
                            let Ok(text) = msg.into_text() else { continue; };
                            let update = match crate::bitget::parse_private_ws_message(&text) {
                                Ok(update) => update,
                                Err(err) => {
                                    eprintln!("private ws parse error: {err}");
                                    continue;
                                }
                            };
                            if update.orders.is_empty()
                                && update.fills.is_empty()
                                && update.positions.is_empty()
                            {
                                continue;
                            }
                            match rusqlite::Connection::open(&cfg.db_path) {
                                Ok(conn) => {
                                    conn.busy_timeout(std::time::Duration::from_secs(5))?;
                                    if let Err(err) = apply_private_ws_update(&conn, update) {
                                        eprintln!("private ws sqlite apply error: {err}");
                                    }
                                }
                                Err(err) => eprintln!("private ws sqlite open error: {err}"),
                            }
                        }
                    }
                }
                eprintln!("private ws socket closed; reconnecting");
            }
            Err(err) => {
                eprintln!("private ws disconnected: {err}");
            }
        }
        // ponytail: fixed 1s reconnect backoff on EVERY reconnect path — hoisted
        // to the outer loop so BOTH connect failure and mid-stream close back off
        // (the Task 4 bug only slept the connect-err arm, letting a flapping
        // mid-stream disconnect hammer the endpoint). Add exponential backoff if
        // disconnects become frequent. Shutdown exits return above before here.
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ExecutorConfig, TradingMode};

    #[test]
    fn daemon_options_default_runs_forever() {
        let options = DaemonOptions::default();

        assert!(options.max_runtime.is_none());
    }

    #[tokio::test]
    async fn daemon_rejects_non_demo_mode_before_opening_db() {
        let cfg = ExecutorConfig {
            mode: TradingMode::Live,
            ..ExecutorConfig::demo_for_tests()
        };

        let err = run_daemon(
            cfg,
            DaemonOptions {
                max_runtime: Some(std::time::Duration::from_millis(1)),
            },
        )
        .await
        .unwrap_err();

        assert!(err.to_string().contains("demo"));
    }

    #[test]
    fn public_ws_update_refreshes_market_cache() {
        let mut cache = crate::executor::MarketCache::default();

        apply_public_market_update(
            &mut cache,
            crate::types::MarketUpdate {
                symbol: "ETHUSDT".to_string(),
                best_bid: 100.0,
                best_ask: 101.0,
                exchange_ts_ms: 10,
            },
            1_000,
        );

        assert!(cache.latest_fresh(1_500, 3).is_some());
    }

    #[test]
    fn private_ws_update_upserts_orders_and_positions() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute_batch(include_str!("../../../schema/001_initial.sql")).unwrap();
        conn.execute_batch(include_str!("../../../schema/002_execution.sql")).unwrap();

        let update = crate::types::PrivateWsUpdate {
            orders: vec![crate::types::OrderRecord {
                order_id: "local-order-1".to_string(),
                exchange_order_id: Some("ex-1".to_string()),
                client_oid: "client-1".to_string(),
                intent_id: None,
                symbol: "ETHUSDT".to_string(),
                side: "buy".to_string(),
                action: "open".to_string(),
                order_type: "market".to_string(),
                status: "filled".to_string(),
                price: Some(100.0),
                size: 0.1,
                filled_size: 0.1,
                attempt: 1,
                raw_json: "{}".to_string(),
                last_error: None,
            }],
            positions: vec![crate::types::PositionRecord {
                symbol: "ETH/USDT:USDT".to_string(),
                side: "long".to_string(),
                notional: 10.0,
                entry_price: 100.0,
                unrealized_pnl: 1.0,
                ownership: "system".to_string(),
                opened_at: Some("now".to_string()),
                adopted_at: None,
                source_intent_id: None,
                raw_json: "{}".to_string(),
            }],
            fills: vec![],
        };

        apply_private_ws_update(&conn, update).unwrap();

        let order_count: i64 = conn.query_row("select count(*) from orders", [], |r| r.get(0)).unwrap();
        let position_count: i64 = conn.query_row("select count(*) from positions", [], |r| r.get(0)).unwrap();
        assert_eq!(order_count, 1);
        assert_eq!(position_count, 1);
    }
}
