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
}
