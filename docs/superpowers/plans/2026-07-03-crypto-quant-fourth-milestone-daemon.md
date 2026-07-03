# Crypto Quant Fourth Milestone Daemon Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a demo-only long-running Rust executor daemon while preserving the existing one-shot execution path.

**Architecture:** Keep one Rust process. `--once` keeps the M3 flow. `--daemon` starts public/private WS loops, an intent loop, a periodic reconcile loop, and read-only Telegram query handling. REST remains the source of truth for order/account/position state; WS is only a fast cache/update source; SQLite stays the durable queue and audit log.

**Tech Stack:** Rust 2021, Tokio, tokio-tungstenite, reqwest, rusqlite, SQLite WAL, Python pytest with the existing `quantmamba` environment.

---

## Source Spec

Implement exactly this spec:

- `docs/superpowers/specs/2026-07-03-crypto-quant-fourth-milestone-daemon-design.md`

Do not implement live trading, live key loading, model training, signal generation, Telegram `/stop`, Telegram `/resume`, or Telegram `/close_all`.

## Worktree And Baseline

- Use `superpowers:using-git-worktrees` before implementation.
- Create or reuse a feature branch named `crypto-quant-fourth-milestone-daemon`.
- Use the local Python environment through `mamba run -n quantmamba ...`.
- Do not install Python packages globally.
- Do not add new Rust crates unless a task explicitly says so. The only expected dependency change is adding Tokio's `signal` feature to the existing Tokio dependency.

Baseline commands before Task 1:

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test -q
mamba run -n quantmamba python -m pytest -q
git status --short --branch
```

Expected baseline:

- Rust format passes.
- Rust clippy passes.
- Rust tests pass.
- Python tests pass.
- Worktree has no unexpected edits before implementation.

## File Structure

Modify:

- `crates/executor/Cargo.toml` - add Tokio `signal` feature only.
- `crates/executor/src/lib.rs` - export new daemon and telegram query modules.
- `crates/executor/src/main.rs` - parse `--once`, `--daemon`, and bounded runtime for tests.
- `crates/executor/src/executor.rs` - keep one-shot behavior and expose the minimal helpers daemon needs.
- `crates/executor/src/bitget.rs` - add reusable WS subscribe/login helpers and streaming loop helpers only if needed.
- `crates/executor/src/db.rs` - add read-only query helpers for Telegram and daemon status.
- `crates/executor/src/notify.rs` - keep push notification filtering; do not mix read-only command formatting here.
- `tests/test_executor_integration.py` - add daemon integration coverage.

Create:

- `crates/executor/src/daemon.rs` - daemon runtime loops and loop orchestration.
- `crates/executor/src/telegram_query.rs` - pure SQLite-backed read-only Telegram query formatting and optional polling loop.

Do not create a separate Telegram execution service. Do not introduce an actor framework, event bus, Redis, Kafka, or FastAPI.

## Task 1: CLI Run Mode

**Files:**

- Modify: `crates/executor/src/main.rs`
- Modify: `crates/executor/src/lib.rs`
- Create: `crates/executor/src/daemon.rs`
- Test: unit tests in `crates/executor/src/main.rs`

- [ ] **Step 1: Write the failing tests**

Add a `#[cfg(test)]` module to `main.rs` that drives argument parsing without reading real env secrets:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_once_mode_by_default() {
        let parsed = parse_args_from(["prodigy-executor"]).unwrap();

        assert_eq!(parsed.run_mode, RunMode::Once);
        assert_eq!(parsed.cfg.db_path, std::path::PathBuf::from("var/prodigy.sqlite"));
    }

    #[test]
    fn parses_daemon_mode_and_db_path() {
        let parsed = parse_args_from([
            "prodigy-executor",
            "--daemon",
            "--db",
            "/tmp/prodigy-test.sqlite",
        ])
        .unwrap();

        assert_eq!(parsed.run_mode, RunMode::Daemon);
        assert_eq!(parsed.cfg.db_path, std::path::PathBuf::from("/tmp/prodigy-test.sqlite"));
    }

    #[test]
    fn rejects_once_and_daemon_together() {
        let err = parse_args_from(["prodigy-executor", "--once", "--daemon"]).unwrap_err();

        assert!(err.to_string().contains("cannot use --once and --daemon together"));
    }

    #[test]
    fn parses_bounded_daemon_runtime_for_tests() {
        let parsed = parse_args_from([
            "prodigy-executor",
            "--daemon",
            "--max-runtime-ms",
            "1500",
        ])
        .unwrap();

        assert_eq!(parsed.run_mode, RunMode::Daemon);
        assert_eq!(parsed.max_runtime_ms, Some(1500));
    }

    #[test]
    fn rejects_live_mode_before_execution() {
        let err = parse_args_from(["prodigy-executor", "--mode", "live"]).unwrap_err();

        assert!(err.to_string().contains("only supports --mode demo"));
    }
}
```

- [ ] **Step 2: Run the test and verify RED**

Run:

```bash
cargo test -q -p prodigy-executor parses_daemon_mode_and_db_path
```

Expected: FAIL because `parse_args_from`, `RunMode`, and the parsed wrapper do not exist yet.

- [ ] **Step 3: Implement the minimal CLI split**

Refactor `main.rs` to introduce:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RunMode {
    Once,
    Daemon,
}

#[derive(Debug)]
struct ParsedExecutorArgs {
    cfg: ExecutorConfig,
    run_mode: RunMode,
    max_runtime_ms: Option<u64>,
}
```

Add:

```rust
fn parse_args_and_config() -> Result<ParsedExecutorArgs> {
    parse_args_from(env::args())
}
```

Add `parse_args_from<I, S>(args: I) -> Result<ParsedExecutorArgs>` where:

- default mode is `RunMode::Once`;
- `--once` sets `RunMode::Once`;
- `--daemon` sets `RunMode::Daemon`;
- using both returns `bail!("cannot use --once and --daemon together")`;
- `--max-runtime-ms <n>` parses to `Some(n)`;
- `--mode live` still returns an error before any execution.

Update `main` to dispatch:

```rust
#[tokio::main]
async fn main() -> Result<()> {
    let parsed = parse_args_and_config()?;
    parsed.cfg.validate_demo_only()?;
    match parsed.run_mode {
        RunMode::Once => executor::run_once_or_loop(parsed.cfg).await,
        RunMode::Daemon => {
            prodigy_executor::daemon::run_daemon(
                parsed.cfg,
                prodigy_executor::daemon::DaemonOptions {
                    max_runtime: parsed
                        .max_runtime_ms
                        .map(std::time::Duration::from_millis),
                },
            )
            .await
        }
    }
}
```

Add the smallest temporary daemon stub so `main.rs` compiles:

```rust
// crates/executor/src/lib.rs
pub mod daemon;
```

```rust
// crates/executor/src/daemon.rs
use anyhow::Result;
use std::time::Duration;

use crate::config::ExecutorConfig;

#[derive(Debug, Clone)]
pub struct DaemonOptions {
    pub max_runtime: Option<Duration>,
}

pub async fn run_daemon(_cfg: ExecutorConfig, options: DaemonOptions) -> Result<()> {
    if let Some(max_runtime) = options.max_runtime {
        tokio::time::sleep(max_runtime).await;
    }
    Ok(())
}
```

This stub intentionally does not validate or run loops yet. Task 2 adds the startup guard and default behavior with failing tests.

- [ ] **Step 4: Run focused tests and verify GREEN**

Run:

```bash
cargo test -q -p prodigy-executor parse
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/executor/src/main.rs crates/executor/src/lib.rs crates/executor/src/daemon.rs
git commit -m "feat: add executor daemon CLI mode"
```

## Task 2: Daemon Module Skeleton And Startup Guard

**Files:**

- Modify: `crates/executor/src/daemon.rs`
- Test: unit tests in `crates/executor/src/daemon.rs`

- [ ] **Step 1: Write the failing tests**

Add tests to the existing `daemon.rs` stub:

```rust
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
}
```

- [ ] **Step 2: Run the test and verify RED**

Run:

```bash
cargo test -q -p prodigy-executor daemon_rejects_non_demo_mode_before_opening_db
```

Expected: FAIL because `DaemonOptions` does not implement `Default` and `run_daemon` does not reject non-demo mode yet.

- [ ] **Step 3: Implement daemon startup guard**

Replace the temporary `daemon.rs` stub with:

```rust
use anyhow::Result;
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
```

This is intentionally tiny. Later tasks replace the sleep/pending body with real startup and loops.

- [ ] **Step 4: Run focused tests and verify GREEN**

Run:

```bash
cargo test -q -p prodigy-executor daemon_
```

Expected: PASS.

- [ ] **Step 5: Run CLI tests from Task 1**

Run:

```bash
cargo test -q -p prodigy-executor parse
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/executor/src/daemon.rs
git commit -m "feat: add daemon startup guard"
```

## Task 3: Market Cache Freshness For Daemon

**Files:**

- Modify: `crates/executor/src/executor.rs`
- Modify: `crates/executor/src/types.rs`
- Test: unit tests in `crates/executor/src/executor.rs`

- [ ] **Step 1: Write the failing tests**

Add or replace `MarketCache` tests in `executor.rs`:

```rust
#[test]
fn market_cache_uses_local_received_time_for_freshness() {
    let mut cache = MarketCache::default();
    cache.update_at(
        MarketUpdate {
            symbol: "ETHUSDT".to_string(),
            best_bid: 100.0,
            best_ask: 101.0,
            exchange_ts_ms: 10,
        },
        1_000,
    );

    assert!(cache.latest_fresh(3_999, 3).is_some());
    assert!(cache.latest_fresh(4_001, 3).is_none());
}

#[test]
fn market_cache_rejects_missing_snapshot() {
    let cache = MarketCache::default();

    assert!(cache.latest_fresh(1_000, 3).is_none());
}
```

- [ ] **Step 2: Run the test and verify RED**

Run:

```bash
cargo test -q -p prodigy-executor market_cache_uses_local_received_time_for_freshness
```

Expected: FAIL because `MarketCache::update_at` does not exist and freshness currently uses exchange time only.

- [ ] **Step 3: Implement local-received freshness**

Change `MarketCache` in `executor.rs` to:

```rust
#[derive(Debug, Clone, Default)]
pub struct MarketCache {
    latest: Option<MarketUpdate>,
    local_received_at_ms: Option<i64>,
}

impl MarketCache {
    pub fn update(&mut self, update: MarketUpdate) {
        let now_ms = crate::bitget::now_ms().parse::<i64>().unwrap_or(0);
        self.update_at(update, now_ms);
    }

    pub fn update_at(&mut self, update: MarketUpdate, local_received_at_ms: i64) {
        self.latest = Some(update);
        self.local_received_at_ms = Some(local_received_at_ms);
    }

    pub fn latest_fresh(&self, now_ms: i64, stale_after_secs: u64) -> Option<MarketUpdate> {
        let update = self.latest.clone()?;
        let received_at = self.local_received_at_ms?;
        let age_ms = now_ms.saturating_sub(received_at);
        if age_ms <= (stale_after_secs as i64) * 1000 {
            Some(update)
        } else {
            None
        }
    }
}
```

Do not add `local_received_at` to `MarketUpdate`; keeping it in the cache avoids rewriting WS parser structs and keeps exchange timestamps separate from local staleness.

- [ ] **Step 4: Run focused tests and verify GREEN**

Run:

```bash
cargo test -q -p prodigy-executor market_cache
```

Expected: PASS.

- [ ] **Step 5: Run existing Rust tests**

Run:

```bash
cargo test -q -p prodigy-executor
```

Expected: PASS. If existing `MarketUpdate` literals need no change, this task is correctly scoped.

- [ ] **Step 6: Commit**

```bash
git add crates/executor/src/executor.rs crates/executor/src/types.rs
git commit -m "feat: track local market cache freshness"
```

## Task 4: Public WebSocket Loop

**Files:**

- Modify: `crates/executor/src/bitget.rs`
- Modify: `crates/executor/src/daemon.rs`
- Test: unit tests in `crates/executor/src/bitget.rs` and `crates/executor/src/daemon.rs`

- [ ] **Step 1: Write failing tests for subscribe payload**

Add pure tests in `bitget.rs`:

```rust
#[test]
fn public_books5_subscribe_payload_targets_demo_symbol() {
    let cfg = ExecutorConfig::demo_for_tests();
    let msg = public_books5_subscribe_message(&cfg);
    let text = msg.to_string();

    assert!(text.contains("\"op\":\"subscribe\""));
    assert!(text.contains("\"channel\":\"books5\""));
    assert!(text.contains("\"instId\":\"ETHUSDT\""));
    assert!(text.contains("\"instType\":\"USDT-FUTURES\""));
}
```

- [ ] **Step 2: Run the test and verify RED**

Run:

```bash
cargo test -q -p prodigy-executor public_books5_subscribe_payload_targets_demo_symbol
```

Expected: FAIL because `public_books5_subscribe_message` does not exist.

- [ ] **Step 3: Extract reusable subscribe payload**

Add in `bitget.rs`:

```rust
pub fn public_books5_subscribe_message(cfg: &ExecutorConfig) -> serde_json::Value {
    serde_json::json!({
        "op": "subscribe",
        "args": [{
            "instType": cfg.product_type,
            "channel": "books5",
            "instId": cfg.bitget_symbol
        }]
    })
}
```

Update `verify_public_ws_connects` to call this helper instead of constructing the JSON inline.

- [ ] **Step 4: Add daemon public WS cache tests**

In `daemon.rs`, add a pure helper:

```rust
pub fn apply_public_market_update(
    cache: &mut crate::executor::MarketCache,
    update: crate::types::MarketUpdate,
    local_received_at_ms: i64,
) {
    cache.update_at(update, local_received_at_ms);
}
```

Add test:

```rust
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
```

- [ ] **Step 5: Implement the public WS loop**

In `daemon.rs`, add:

```rust
pub async fn run_public_ws_loop(
    cfg: ExecutorConfig,
    market_cache: std::sync::Arc<tokio::sync::Mutex<crate::executor::MarketCache>>,
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
                    .send(Message::Text(crate::bitget::public_books5_subscribe_message(&cfg).to_string()))
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
            }
            Err(err) => {
                eprintln!("public ws disconnected: {err}");
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        }
    }
}
```

Keep the reconnect backoff simple: fixed 1 second. Add a `// ponytail:` comment if you keep fixed backoff.

- [ ] **Step 6: Run tests and verify GREEN**

Run:

```bash
cargo test -q -p prodigy-executor public_
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add crates/executor/src/bitget.rs crates/executor/src/daemon.rs
git commit -m "feat: add daemon public websocket cache loop"
```

## Task 5: Private WebSocket Loop And SQLite Application

**Files:**

- Modify: `crates/executor/src/bitget.rs`
- Modify: `crates/executor/src/daemon.rs`
- Test: unit tests in `crates/executor/src/bitget.rs` and `crates/executor/src/daemon.rs`

- [ ] **Step 1: Write failing test for private login payload**

Add in `bitget.rs` tests:

```rust
#[test]
fn private_login_payload_uses_websocket_signature() {
    let cfg = ExecutorConfig::demo_for_tests();
    let msg = private_login_message(&cfg, "1538054050");
    let text = msg.to_string();

    assert!(text.contains("\"op\":\"login\""));
    assert!(text.contains("\"apiKey\":\"key\""));
    assert!(text.contains("\"passphrase\":\"pass\""));
    assert!(text.contains("\"timestamp\":\"1538054050\""));
    assert!(text.contains(&websocket_sign("secret", "1538054050")));
}
```

- [ ] **Step 2: Run test and verify RED**

Run:

```bash
cargo test -q -p prodigy-executor private_login_payload_uses_websocket_signature
```

Expected: FAIL because `private_login_message` does not exist.

- [ ] **Step 3: Extract reusable private login payload**

Add in `bitget.rs`:

```rust
pub fn private_login_message(cfg: &ExecutorConfig, timestamp: &str) -> serde_json::Value {
    serde_json::json!({
        "op": "login",
        "args": [{
            "apiKey": cfg.secrets.api_key,
            "passphrase": cfg.secrets.passphrase,
            "timestamp": timestamp,
            "sign": websocket_sign(&cfg.secrets.api_secret, timestamp)
        }]
    })
}
```

Update `verify_private_ws_connects` to call this helper.

- [ ] **Step 4: Write failing test for applying private updates**

Add in `daemon.rs` tests:

```rust
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
```

- [ ] **Step 5: Implement private update application**

Add in `daemon.rs`:

```rust
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
```

- [ ] **Step 6: Implement private WS loop**

Add in `daemon.rs`:

```rust
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
                    .send(Message::Text(crate::bitget::private_login_message(&cfg, &timestamp).to_string()))
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
                            if update.orders.is_empty() && update.fills.is_empty() && update.positions.is_empty() {
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
            }
            Err(err) => {
                eprintln!("private ws disconnected: {err}");
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        }
    }
}
```

Keep this loop simple. Open a SQLite connection per batch if needed; replace with a long-lived loop-owned connection only if tests show lock churn.

- [ ] **Step 7: Run tests and verify GREEN**

Run:

```bash
cargo test -q -p prodigy-executor private_
```

Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add crates/executor/src/bitget.rs crates/executor/src/daemon.rs
git commit -m "feat: apply private websocket updates"
```

## Task 6: Reusable Intent Processing For Daemon

**Files:**

- Modify: `crates/executor/src/executor.rs`
- Modify: `crates/executor/src/daemon.rs`
- Test: unit tests in `crates/executor/src/executor.rs`

- [ ] **Step 1: Write failing test for stale market behavior through shared helper**

If `require_fresh_market` is private, add a crate-visible wrapper test by making it `pub(crate)` and test:

```rust
#[test]
fn shared_market_requirement_rejects_stale_cache() {
    let err = require_fresh_market(None).unwrap_err();

    assert!(err.to_string().contains("market data is stale"));
}
```

- [ ] **Step 2: Run test and verify RED**

Run:

```bash
cargo test -q -p prodigy-executor shared_market_requirement_rejects_stale_cache
```

Expected: FAIL if the function is still private to tests or the test does not exist.

- [ ] **Step 3: Extract pending-intent processing without changing behavior**

Create a reusable function in `executor.rs`:

```rust
pub async fn process_pending_intents_once(
    conn: &rusqlite::Connection,
    cfg: &ExecutorConfig,
    rest: &BitgetRestClient,
    market_cache: &mut MarketCache,
) -> Result<usize> {
    let intents = crate::db::pending_intents(conn)?;
    let mut processed = 0usize;
    for intent in intents {
        let account = fetch_account_snapshot(rest).await?;
        crate::db::insert_equity_snapshot(
            conn,
            account.equity,
            account.available_margin,
            account.unrealized_pnl_24h,
            0.0,
        )?;
        let now_ms = crate::bitget::now_ms().parse::<i64>().unwrap_or(0);
        let market = require_fresh_market(
            market_cache.latest_fresh(now_ms, cfg.stale_market_data_secs),
        )?;
        process_one_intent(conn, cfg, rest, intent.clone(), market, account, market_cache).await?;
        crate::db::write_event(conn, "info", "executor", "processed intent", "{}")?;
        println!("processed {}", intent.intent_id);
        processed += 1;
    }
    Ok(processed)
}
```

Then simplify `run_once_or_loop`:

- keep demo validation, DB open, REST client, optional test reset, WS verification, leverage setting, startup reconcile;
- seed `MarketCache` from `fetch_market_snapshot`;
- call `process_pending_intents_once`.

Do not alter `process_one_intent` behavior.

- [ ] **Step 4: Run existing one-shot integration test**

Run:

```bash
mamba run -n quantmamba python -m pytest -q tests/test_executor_integration.py::test_rust_demo_executor_processes_pending_intent
```

Expected: PASS. If demo book is phantom, honest `failed` status is still acceptable per existing test.

- [ ] **Step 5: Run Rust executor tests**

Run:

```bash
cargo test -q -p prodigy-executor
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/executor/src/executor.rs crates/executor/src/daemon.rs
git commit -m "refactor: reuse pending intent processing"
```

## Task 7: Daemon Loop Orchestration And Shutdown

**Files:**

- Modify: `crates/executor/Cargo.toml`
- Modify: `crates/executor/src/daemon.rs`
- Modify: `crates/executor/src/executor.rs`
- Test: unit tests in `crates/executor/src/daemon.rs`

- [ ] **Step 1: Write failing tests for loop timing**

Add pure tests in `daemon.rs`:

```rust
#[test]
fn should_run_reconcile_when_interval_elapsed() {
    assert!(should_run_reconcile(10_000, 0, 10));
    assert!(!should_run_reconcile(9_999, 0, 10));
}

#[test]
fn daemon_allows_bounded_runtime_for_tests() {
    let options = DaemonOptions {
        max_runtime: Some(std::time::Duration::from_millis(5)),
    };

    assert_eq!(options.max_runtime.unwrap(), std::time::Duration::from_millis(5));
}
```

- [ ] **Step 2: Run test and verify RED**

Run:

```bash
cargo test -q -p prodigy-executor should_run_reconcile_when_interval_elapsed
```

Expected: FAIL because `should_run_reconcile` does not exist.

- [ ] **Step 3: Add Tokio signal feature**

Modify `crates/executor/Cargo.toml`:

```toml
tokio = { version = "1.40", features = ["macros", "rt-multi-thread", "time", "sync", "signal"] }
```

Do not add a new crate for signal handling.

- [ ] **Step 4: Implement daemon orchestration**

Add in `daemon.rs`:

```rust
pub fn should_run_reconcile(now_ms: i64, last_reconcile_ms: i64, interval_secs: u64) -> bool {
    now_ms.saturating_sub(last_reconcile_ms) >= (interval_secs as i64) * 1000
}
```

Replace the `run_daemon` skeleton with:

```rust
pub async fn run_daemon(cfg: ExecutorConfig, options: DaemonOptions) -> Result<()> {
    cfg.validate_demo_only()?;
    let conn = rusqlite::Connection::open(&cfg.db_path)?;
    conn.busy_timeout(std::time::Duration::from_secs(5))?;
    let rest = crate::bitget::BitgetRestClient::new(cfg.clone())?;

    if cfg.test_reset_demo_state {
        crate::db::write_event(&conn, "warning", "daemon", "test reset requested in daemon mode", "{}")?;
    }

    rest.set_leverage(cfg.leverage).await?;
    crate::reconcile::reconcile_once(
        &conn,
        &rest,
        "daemon-startup",
        !cfg.test_reset_demo_state,
        cfg.telegram_bot_token.as_deref(),
        cfg.telegram_chat_id.as_deref(),
    )
    .await?;
    crate::db::write_event(&conn, "info", "daemon", "daemon started", "{}")?;

    let market_cache = std::sync::Arc::new(tokio::sync::Mutex::new(crate::executor::MarketCache::default()));
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    let public_task = tokio::spawn(run_public_ws_loop(cfg.clone(), market_cache.clone(), shutdown_rx.clone()));
    let private_task = tokio::spawn(run_private_ws_loop(cfg.clone(), shutdown_rx.clone()));

    let started = tokio::time::Instant::now();
    let mut poll = tokio::time::interval(std::time::Duration::from_millis(250));
    let mut last_reconcile_ms = crate::bitget::now_ms().parse::<i64>().unwrap_or(0);

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                crate::db::write_event(&conn, "info", "daemon", "shutdown requested", "{}")?;
                break;
            }
            _ = poll.tick() => {
                if options.max_runtime.is_some_and(|max| started.elapsed() >= max) {
                    crate::db::write_event(&conn, "info", "daemon", "bounded daemon runtime elapsed", "{}")?;
                    break;
                }
                let now_ms = crate::bitget::now_ms().parse::<i64>().unwrap_or(0);
                if should_run_reconcile(now_ms, last_reconcile_ms, cfg.reconcile_interval_secs) {
                    if let Err(err) = crate::reconcile::reconcile_once(
                        &conn,
                        &rest,
                        "daemon-periodic",
                        !cfg.test_reset_demo_state,
                        cfg.telegram_bot_token.as_deref(),
                        cfg.telegram_chat_id.as_deref(),
                    ).await {
                        crate::db::write_event(&conn, "warning", "reconcile", &format!("reconcile failed: {err}"), "{}")?;
                    }
                    last_reconcile_ms = now_ms;
                }

                let mut local_cache = {
                    let cache = market_cache.lock().await;
                    cache.clone()
                };
                if let Err(err) = crate::executor::process_pending_intents_once(
                    &conn,
                    &cfg,
                    &rest,
                    &mut local_cache,
                ).await {
                    crate::db::write_event(&conn, "error", "intent_loop", &format!("intent loop failed: {err}"), "{}")?;
                }
            }
        }
    }

    let _ = shutdown_tx.send(true);
    public_task.abort();
    private_task.abort();
    crate::db::write_event(&conn, "info", "daemon", "daemon stopped", "{}")?;
    Ok(())
}
```

If clippy rejects `is_some_and` for toolchain compatibility, replace it with a `match`. Do not change behavior.

- [ ] **Step 5: Run tests and verify GREEN**

Run:

```bash
cargo test -q -p prodigy-executor reconcile
```

Expected: PASS.

- [ ] **Step 6: Run clippy**

Run:

```bash
cargo clippy --all-targets --all-features -- -D warnings
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add crates/executor/Cargo.toml crates/executor/src/daemon.rs crates/executor/src/executor.rs
git commit -m "feat: orchestrate demo executor daemon"
```

## Task 8: Telegram Read-Only Query Formatting

**Files:**

- Create: `crates/executor/src/telegram_query.rs`
- Modify: `crates/executor/src/lib.rs`
- Modify: `crates/executor/src/daemon.rs`
- Modify: `crates/executor/src/db.rs`
- Test: unit tests in `crates/executor/src/telegram_query.rs`

- [ ] **Step 1: Write failing tests for read-only query responses**

Create `telegram_query.rs` with tests first:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn test_conn() -> rusqlite::Connection {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute_batch(include_str!("../../../schema/001_initial.sql")).unwrap();
        conn.execute_batch(include_str!("../../../schema/002_execution.sql")).unwrap();
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
    fn remote_control_commands_are_not_supported_in_m4() {
        for command in ["/stop", "/resume", "/close_all"] {
            let conn = test_conn();
            let response = query_response(&conn, command).unwrap().unwrap();
            assert!(response.contains("not supported in M4"));
        }
    }
}
```

- [ ] **Step 2: Run tests and verify RED**

Run:

```bash
cargo test -q -p prodigy-executor status_query_reads_sqlite_without_side_effects
```

Expected: FAIL because `telegram_query.rs` and `query_response` do not exist.

- [ ] **Step 3: Add query module**

Add to `lib.rs`:

```rust
pub mod telegram_query;
```

Implement in `telegram_query.rs`:

```rust
use anyhow::Result;
use rusqlite::Connection;

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
    Ok(format!("status: daemon\npending_intents: {pending}\nevents: {events}"))
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
    Ok(if lines.is_empty() { "positions: none".to_string() } else { lines.join("\n") })
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
    Ok(if lines.is_empty() { "orders: none".to_string() } else { lines.join("\n") })
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
    Ok(format!("pnl:\nunrealized={unrealized}\nequity={}", equity.unwrap_or(0.0)))
}

fn risk_response(conn: &Connection) -> Result<String> {
    let manual_overrides: i64 = conn.query_row(
        "select count(*) from executor_state where key like 'manual_override:%' and value = 'active'",
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
```

- [ ] **Step 4: Run query tests and verify GREEN**

Run:

```bash
cargo test -q -p prodigy-executor query
```

Expected: PASS.

- [ ] **Step 5: Add optional Telegram polling loop**

Add in `daemon.rs` a small loop that runs only when token and chat id exist:

```rust
pub async fn run_telegram_query_loop(
    cfg: ExecutorConfig,
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> Result<()> {
    let (Some(token), Some(chat_id)) = (cfg.telegram_bot_token.clone(), cfg.telegram_chat_id.clone()) else {
        return Ok(());
    };
    let client = reqwest::Client::new();
    let mut offset: i64 = 0;
    let mut shutdown = shutdown;
    loop {
        if *shutdown.borrow() {
            return Ok(());
        }
        let url = format!("https://api.telegram.org/bot{token}/getUpdates");
        let response = client
            .get(&url)
            .query(&[("timeout", "10".to_string()), ("offset", offset.to_string())])
            .send()
            .await;
        if let Ok(resp) = response {
            if let Ok(value) = resp.json::<serde_json::Value>().await {
                if let Some(updates) = value.get("result").and_then(serde_json::Value::as_array) {
                    for update in updates {
                        if let Some(id) = update.get("update_id").and_then(serde_json::Value::as_i64) {
                            offset = id + 1;
                        }
                        let message = update.get("message").unwrap_or(&serde_json::Value::Null);
                        let chat = message
                            .get("chat")
                            .and_then(|c| c.get("id"))
                            .and_then(serde_json::Value::as_i64)
                            .map(|v| v.to_string());
                        if chat.as_deref() != Some(chat_id.as_str()) {
                            continue;
                        }
                        let Some(text) = message.get("text").and_then(serde_json::Value::as_str) else {
                            continue;
                        };
                        let conn = rusqlite::Connection::open(&cfg.db_path)?;
                        conn.busy_timeout(std::time::Duration::from_secs(5))?;
                        if let Some(reply) = crate::telegram_query::query_response(&conn, text)? {
                            let send_url = format!("https://api.telegram.org/bot{token}/sendMessage");
                            let _ = client
                                .post(send_url)
                                .form(&[("chat_id", chat_id.as_str()), ("text", reply.as_str())])
                                .send()
                                .await;
                        }
                    }
                }
            }
        }
        tokio::select! {
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    return Ok(());
                }
            }
            _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {}
        }
    }
}
```

Spawn this loop from `run_daemon` alongside public/private WS loops. Telegram errors must not break daemon execution.

- [ ] **Step 6: Run tests and clippy**

Run:

```bash
cargo test -q -p prodigy-executor
cargo clippy --all-targets --all-features -- -D warnings
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add crates/executor/src/lib.rs crates/executor/src/telegram_query.rs crates/executor/src/daemon.rs
git commit -m "feat: add read-only telegram queries"
```

## Task 9: Daemon Integration Tests

**Files:**

- Modify: `tests/test_executor_integration.py`
- Test: `tests/test_executor_integration.py`

- [ ] **Step 1: Write failing integration tests**

Add a second Python test:

```python
def test_rust_demo_daemon_processes_pending_intent_once(tmp_path):
    _demo_depth_diagnostic()
    db_path = tmp_path / "prodigy.sqlite"

    with connect(db_path) as conn:
        init_db(conn)
        write_trade_intent(
            conn,
            TradeIntent(
                intent_id="daemon-intent-1",
                created_at="2026-07-03T00:00:00Z",
                symbol="ETH/USDT:USDT",
                side="long",
                action="open",
                target_notional=100.0,
                max_order_notional=100.0,
                source="test",
                reason="daemon integration",
                model_version="smoke-test",
            ),
        )

    result = subprocess.run(
        [
            "cargo",
            "run",
            "-q",
            "-p",
            "prodigy-executor",
            "--",
            "--db",
            str(db_path),
            "--daemon",
            "--max-runtime-ms",
            "5000",
            "--test-reset-demo-state",
        ],
        check=True,
        text=True,
        capture_output=True,
        timeout=30,
    )

    with connect(db_path) as conn:
        intent = conn.execute(
            "select status, error from trade_intents where intent_id = 'daemon-intent-1'"
        ).fetchone()
        order_count = conn.execute("select count(*) from orders").fetchone()[0]
        event_count = conn.execute("select count(*) from events").fetchone()[0]

    assert intent["status"] in ("executed", "failed")
    if intent["status"] == "failed":
        assert intent["error"]
    assert order_count >= 1
    assert event_count >= 1
    assert "daemon" in result.stdout or result.stderr == ""
```

Add a no-live-path test:

```python
def test_rust_daemon_rejects_live_mode(tmp_path):
    db_path = tmp_path / "prodigy.sqlite"
    result = subprocess.run(
        [
            "cargo",
            "run",
            "-q",
            "-p",
            "prodigy-executor",
            "--",
            "--db",
            str(db_path),
            "--daemon",
            "--mode",
            "live",
        ],
        check=False,
        text=True,
        capture_output=True,
    )

    assert result.returncode != 0
    assert "only supports --mode demo" in result.stderr
```

- [ ] **Step 2: Run integration tests and verify RED**

Run:

```bash
mamba run -n quantmamba python -m pytest -q tests/test_executor_integration.py::test_rust_demo_daemon_processes_pending_intent_once
```

Expected before daemon is fully wired: FAIL by timeout, no intent processing, or missing `--daemon` behavior.

- [ ] **Step 3: Fix daemon wiring until integration is honest**

Required behavior:

- daemon validates demo-only mode;
- daemon starts public/private WS loops;
- daemon runs startup reconcile before pending intents;
- daemon processes pending intents;
- bounded runtime exits cleanly;
- if demo book is phantom, intent may end `failed` with diagnostic;
- if demo book is tradable, intent may end `executed`;
- no zero-fill order may be marked `filled`.

Do not weaken the test to accept `pending` or `accepted` after bounded daemon runtime.

- [ ] **Step 4: Run integration tests and verify GREEN**

Run:

```bash
mamba run -n quantmamba python -m pytest -q tests/test_executor_integration.py
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add tests/test_executor_integration.py crates/executor/src/daemon.rs crates/executor/src/main.rs crates/executor/src/executor.rs
git commit -m "test: cover demo daemon intent processing"
```

## Task 10: Final Verification And Readiness Review

**Files:**

- Modify only if verification reveals a real issue.
- Test: full project verification.

- [ ] **Step 1: Run full verification**

Run:

```bash
cargo fmt --check
git diff --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test -q
mamba run -n quantmamba python -m pytest -q
```

Expected:

- format check passes;
- whitespace check passes;
- clippy passes with `-D warnings`;
- all Rust tests pass;
- all Python tests pass.

- [ ] **Step 2: Search for forbidden M4 scope creep**

Run:

```bash
rg -n "close_all|/stop|/resume|TradingMode::Live|live API|actor|event bus|redis|kafka|fastapi" crates tests docs
```

Expected:

- Existing schema references to `control_commands` may appear.
- Existing `TradingMode::Live` enum and live-rejection tests may appear.
- There must be no implemented live trading path.
- There must be no Telegram remote-control implementation.
- There must be no Redis, Kafka, FastAPI, or actor/event-bus implementation.

- [ ] **Step 3: Search for secrets**

Run:

```bash
git grep -n "BITGET_DEMO_API_KEY\\|BITGET_DEMO_API_SECRET\\|BITGET_DEMO_API_PASSPHRASE\\|TELEGRAM_BOT_TOKEN" -- ':!.env.local'
```

Expected: only code/docs references to env var names, no real secret values.

- [ ] **Step 4: Review acceptance criteria against spec**

Open:

```bash
sed -n '273,292p' docs/superpowers/specs/2026-07-03-crypto-quant-fourth-milestone-daemon-design.md
```

For each numbered criterion, identify the test or code path that covers it. If any criterion has no coverage, add the smallest missing test first, watch it fail, implement the fix, and rerun full verification.

- [ ] **Step 5: Commit final cleanup if needed**

If fixes were needed:

```bash
git add <changed-files>
git commit -m "chore: finalize fourth milestone daemon"
```

If no fixes were needed, do not create an empty commit.

- [ ] **Step 6: Report final status**

Report:

- branch name;
- final commit hash;
- verification command results;
- whether live mode remains rejected;
- whether Telegram controls remain excluded;
- any known demo-environment limitation, especially phantom Bitget demo book behavior.
