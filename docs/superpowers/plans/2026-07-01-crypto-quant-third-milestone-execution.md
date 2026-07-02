# Crypto Quant Third Milestone Execution Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the Bitget demo execution safety layer so the Rust executor can place, cancel, reconcile, and safely manage real Bitget demo futures orders.

**Architecture:** Keep execution in one Rust process. Public/private WebSocket is verified (connect + auth + subscribe) but no long-running WS cache is maintained in M3; a one-shot REST ticker seeds the market snapshot, REST sends explicit order actions and refreshes the market before the maker retry and taker fallback, SQLite remains the durable intent queue and audit log, and REST reconciliation repairs any local state the one-shot processing missed. A continuously-maintained WS cache and Telegram query commands are deferred to M4.

**Tech Stack:** Rust 2021, `tokio`, `reqwest`, `tokio-tungstenite`, `serde`, `serde_json`, `hmac`, `sha2`, `base64`, `rusqlite`, Python `quantmamba` for existing research tests.

---

## Execution Rules

- Implement in a fresh git worktree from `main`.
- Use `superpowers:subagent-driven-development` or `superpowers:executing-plans`; do not implement directly from this planning step.
- Use TDD for each task: failing test first, then minimal implementation.
- Do not read or print secret values.
- Do not commit `.env.local` or any key material.
- Bitget demo tests are allowed to place, cancel, and close demo orders.
- Live trading must refuse to start in this milestone.
- Keep Rust execution single-process. Do not add Redis, Kafka, FastAPI, or service splitting.
- Use official Bitget docs for endpoint details:
  - Demo REST: `https://www.bitget.com/api-doc/common/demotrading/restapi`
  - Demo WebSocket: `https://www.bitget.com/api-doc/common/demotrading/websocket`
  - Signature: `https://www.bitget.com/api-doc/common/signature`
  - WebSocket login: `https://www.bitget.com/api-doc/common/websocket-intro`
  - Place order: `https://www.bitget.com/api-doc/contract/trade/Place-Order`
  - Cancel order: `https://www.bitget.com/api-doc/contract/trade/Cancel-Order`
  - Cancel all orders: `https://www.bitget.com/api-doc/contract/trade/Cancel-All-Orders`
  - Pending orders: `https://www.bitget.com/api-doc/contract/trade/Get-Orders-Pending`
  - Order detail: `https://www.bitget.com/api-doc/contract/trade/Get-Order-Details`
  - Order fills: `https://www.bitget.com/api-doc/contract/trade/Get-Order-Fills`
  - Account: `https://www.bitget.com/api-doc/contract/account/Get-Single-Account`
  - Positions: `https://www.bitget.com/api-doc/contract/position/get-all-position`
  - Public depth WS: `https://www.bitget.com/api-doc/contract/websocket/public/Order-Book-Channel`
  - Private order WS: `https://www.bitget.com/api-doc/contract/websocket/private/Order-Channel`
  - Private fill WS: `https://www.bitget.com/api-doc/contract/websocket/private/Fill-Channel`
  - Private position WS: `https://www.bitget.com/api-doc/contract/websocket/private/Positions-Channel`
  - Private account WS: `https://www.bitget.com/api-doc/contract/websocket/private/Account-Channel`

## Required Local Secrets

The executor loads secrets from environment variables first, then from `.env.local` in the repo root.

Required for Bitget demo tests:

```text
BITGET_DEMO_API_KEY=replace_with_demo_key
BITGET_DEMO_API_SECRET=replace_with_demo_secret
BITGET_DEMO_API_PASSPHRASE=replace_with_demo_passphrase
```

Optional for Telegram network delivery:

```text
TELEGRAM_BOT_TOKEN=replace_with_token
TELEGRAM_CHAT_ID=replace_with_chat_id
```

## File Structure

Create or modify:

```text
Cargo.toml
crates/executor/Cargo.toml
crates/executor/src/main.rs
crates/executor/src/lib.rs
crates/executor/src/config.rs
crates/executor/src/bitget.rs
crates/executor/src/db.rs
crates/executor/src/types.rs
crates/executor/src/state.rs
crates/executor/src/risk.rs
crates/executor/src/executor.rs
crates/executor/src/reconcile.rs
crates/executor/src/notify.rs
crates/executor/src/manual_override.rs
crates/executor/tests/bitget_demo.rs
schema/002_execution.sql
src/prodigy/db.py
tests/test_db_schema.py
tests/test_executor_integration.py
```

Keep the crate small. `bitget.rs` owns REST/WS protocol details; `state.rs` owns pure order lifecycle decisions; `risk.rs` owns pure risk checks; `executor.rs` wires them together.

## Task 1: Rust Dependencies, Library Entry, Config, And Demo Secret Loading

**Files:**
- Modify: `crates/executor/Cargo.toml`
- Create: `crates/executor/src/lib.rs`
- Create: `crates/executor/src/config.rs`
- Modify: `crates/executor/src/main.rs`
- Test: `crates/executor/src/config.rs`

- [ ] **Step 1: Write failing config tests**

Add this test module to `crates/executor/src/config.rs` while creating the file:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn env_file_parser_reads_plain_key_values_and_ignores_comments() {
        let input = r#"
        # local secrets
        BITGET_DEMO_API_KEY=key-1
        BITGET_DEMO_API_SECRET="secret-1"
        BITGET_DEMO_API_PASSPHRASE='pass-1'
        "#;

        let parsed = parse_env_text(input);

        assert_eq!(parsed.get("BITGET_DEMO_API_KEY").unwrap(), "key-1");
        assert_eq!(parsed.get("BITGET_DEMO_API_SECRET").unwrap(), "secret-1");
        assert_eq!(parsed.get("BITGET_DEMO_API_PASSPHRASE").unwrap(), "pass-1");
    }

    #[test]
    fn live_mode_is_rejected_for_third_milestone() {
        let cfg = ExecutorConfig {
            mode: TradingMode::Live,
            ..ExecutorConfig::demo_for_tests()
        };

        assert!(cfg.validate_demo_only().is_err());
    }

    #[test]
    fn demo_config_requires_demo_credentials() {
        let cfg = ExecutorConfig {
            secrets: DemoSecrets {
                api_key: String::new(),
                api_secret: "secret".to_string(),
                passphrase: "pass".to_string(),
            },
            ..ExecutorConfig::demo_for_tests()
        };

        assert!(cfg.validate_demo_only().is_err());
    }
}
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
cargo test -p prodigy-executor config -- --nocapture
```

Expected: FAIL because `config.rs`, `ExecutorConfig`, and parser functions do not exist.

- [ ] **Step 3: Add dependencies and library module**

Modify `crates/executor/Cargo.toml`:

```toml
[package]
name = "prodigy-executor"
version = "0.1.0"
edition = "2021"

[dependencies]
anyhow = "1.0"
base64 = "0.22"
futures-util = "0.3"
hmac = "0.12"
reqwest = { version = "0.12", default-features = false, features = ["json", "rustls-tls"] }
rusqlite = { version = "0.32", features = ["bundled"] }
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
sha2 = "0.10"
tokio = { version = "1.40", features = ["macros", "rt-multi-thread", "time", "sync"] }
tokio-tungstenite = { version = "0.24", features = ["rustls-tls-webpki-roots"] }
```

Create `crates/executor/src/lib.rs`:

```rust
pub mod bitget;
pub mod config;
pub mod db;
pub mod executor;
pub mod notify;
pub mod reconcile;
pub mod risk;
pub mod state;
pub mod types;
pub mod manual_override;
```

- [ ] **Step 4: Implement minimal config and `.env.local` parser**

Create `crates/executor/src/config.rs`:

```rust
use anyhow::{bail, Context, Result};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TradingMode {
    Demo,
    Live,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DemoSecrets {
    pub api_key: String,
    pub api_secret: String,
    pub passphrase: String,
}

#[derive(Debug, Clone)]
pub struct ExecutorConfig {
    pub mode: TradingMode,
    pub db_path: PathBuf,
    pub symbol: String,
    pub bitget_symbol: String,
    pub product_type: String,
    pub margin_coin: String,
    pub margin_mode: String,
    pub leverage: u32,
    pub rest_base_url: String,
    pub public_ws_url: String,
    pub private_ws_url: String,
    pub open_maker_timeout_secs: u64,
    pub close_maker_timeout_secs: u64,
    pub stale_market_data_secs: u64,
    pub reconcile_interval_secs: u64,
    pub total_notional_cap_x_equity: f64,
    pub trading_suspension_unrealized_loss_x_equity: f64,
    pub test_reset_demo_state: bool,
    pub secrets: DemoSecrets,
    pub telegram_bot_token: Option<String>,
    pub telegram_chat_id: Option<String>,
}

impl ExecutorConfig {
    pub fn demo_for_tests() -> Self {
        Self {
            mode: TradingMode::Demo,
            db_path: PathBuf::from("var/prodigy.sqlite"),
            symbol: "ETH/USDT:USDT".to_string(),
            bitget_symbol: "ETHUSDT".to_string(),
            product_type: "USDT-FUTURES".to_string(),
            margin_coin: "USDT".to_string(),
            margin_mode: "crossed".to_string(),
            leverage: 5,
            rest_base_url: "https://api.bitget.com".to_string(),
            public_ws_url: "wss://wspap.bitget.com/v2/ws/public".to_string(),
            private_ws_url: "wss://wspap.bitget.com/v2/ws/private".to_string(),
            open_maker_timeout_secs: 15,
            close_maker_timeout_secs: 8,
            stale_market_data_secs: 3,
            reconcile_interval_secs: 10,
            total_notional_cap_x_equity: 5.0,
            trading_suspension_unrealized_loss_x_equity: 0.10,
            test_reset_demo_state: false,
            secrets: DemoSecrets {
                api_key: "key".to_string(),
                api_secret: "secret".to_string(),
                passphrase: "pass".to_string(),
            },
            telegram_bot_token: None,
            telegram_chat_id: None,
        }
    }

    pub fn validate_demo_only(&self) -> Result<()> {
        if self.mode != TradingMode::Demo {
            bail!("third milestone executor only supports Bitget demo mode");
        }
        if self.secrets.api_key.trim().is_empty()
            || self.secrets.api_secret.trim().is_empty()
            || self.secrets.passphrase.trim().is_empty()
        {
            bail!("missing Bitget demo API credentials");
        }
        if !self.public_ws_url.contains("wspap.bitget.com")
            || !self.private_ws_url.contains("wspap.bitget.com")
        {
            bail!("demo executor must use Bitget demo websocket URLs");
        }
        Ok(())
    }
}

pub fn parse_env_text(input: &str) -> HashMap<String, String> {
    let mut values = HashMap::new();
    for raw_line in input.lines() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some((key, value)) = line.split_once('=') {
            let clean = value.trim().trim_matches('"').trim_matches('\'').to_string();
            values.insert(key.trim().to_string(), clean);
        }
    }
    values
}

pub fn load_env_file(path: &Path) -> Result<HashMap<String, String>> {
    if !path.exists() {
        return Ok(HashMap::new());
    }
    let text = fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    Ok(parse_env_text(&text))
}
```

- [ ] **Step 5: Convert main to async and load config**

Modify `crates/executor/src/main.rs` to call the library:

```rust
use anyhow::{bail, Result};
use prodigy_executor::config::{load_env_file, DemoSecrets, ExecutorConfig};
use prodigy_executor::executor;
use std::env;
use std::path::Path;

#[tokio::main]
async fn main() -> Result<()> {
    let cfg = parse_args_and_config()?;
    cfg.validate_demo_only()?;
    executor::run_once_or_loop(cfg).await
}

fn parse_args_and_config() -> Result<ExecutorConfig> {
    let mut cfg = ExecutorConfig::demo_for_tests();
    let env_file = load_env_file(Path::new(".env.local"))?;

    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--db" => cfg.db_path = args.next().unwrap_or_else(|| "var/prodigy.sqlite".into()).into(),
            "--once" => {}
            "--test-reset-demo-state" => cfg.test_reset_demo_state = true,
            "--mode" => {
                let value = args.next().unwrap_or_else(|| "demo".into());
                if value != "demo" {
                    bail!("third milestone executor only supports --mode demo");
                }
            }
            other => bail!("unknown argument: {other}"),
        }
    }

    cfg.secrets = DemoSecrets {
        api_key: read_secret("BITGET_DEMO_API_KEY", &env_file)?,
        api_secret: read_secret("BITGET_DEMO_API_SECRET", &env_file)?,
        passphrase: read_secret("BITGET_DEMO_API_PASSPHRASE", &env_file)?,
    };
    cfg.telegram_bot_token = read_optional("TELEGRAM_BOT_TOKEN", &env_file);
    cfg.telegram_chat_id = read_optional("TELEGRAM_CHAT_ID", &env_file);
    Ok(cfg)
}

fn read_secret(key: &str, env_file: &std::collections::HashMap<String, String>) -> Result<String> {
    read_optional(key, env_file).ok_or_else(|| anyhow::anyhow!("missing {key}"))
}

fn read_optional(key: &str, env_file: &std::collections::HashMap<String, String>) -> Option<String> {
    env::var(key).ok().or_else(|| env_file.get(key).cloned())
}
```

Create a temporary stub `crates/executor/src/executor.rs` so compilation works:

```rust
use anyhow::Result;

use crate::config::ExecutorConfig;

pub async fn run_once_or_loop(_cfg: ExecutorConfig) -> Result<()> {
    Ok(())
}
```

Create empty module files for the library exports:

```text
crates/executor/src/bitget.rs
crates/executor/src/notify.rs
crates/executor/src/reconcile.rs
crates/executor/src/risk.rs
crates/executor/src/state.rs
```

- [ ] **Step 6: Run tests**

Run:

```bash
cargo test -p prodigy-executor config -- --nocapture
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add crates/executor/Cargo.toml crates/executor/src
git commit -m "feat: add demo executor config"
```

## Task 2: Schema Extension And DB Persistence Helpers

**Files:**
- Create: `schema/002_execution.sql`
- Modify: `src/prodigy/db.py`
- Modify: `tests/test_db_schema.py`
- Modify: `crates/executor/src/db.rs`
- Modify: `crates/executor/src/types.rs`

- [ ] **Step 1: Write failing Python schema test**

Append to `tests/test_db_schema.py`:

```python
def test_execution_schema_adds_order_and_position_context(tmp_path):
    db_path = tmp_path / "prodigy.sqlite"

    with connect(db_path) as conn:
        init_db(conn)
        order_columns = {
            row["name"]
            for row in conn.execute("pragma table_info(orders)").fetchall()
        }
        position_columns = {
            row["name"]
            for row in conn.execute("pragma table_info(positions)").fetchall()
        }

    assert {
        "exchange_order_id",
        "attempt",
        "raw_json",
        "last_error",
    }.issubset(order_columns)
    assert {
        "ownership",
        "opened_at",
        "adopted_at",
        "source_intent_id",
        "raw_json",
    }.issubset(position_columns)
```

- [ ] **Step 2: Run test to verify failure**

Run:

```bash
mamba run -n quantmamba python -m pytest tests/test_db_schema.py::test_execution_schema_adds_order_and_position_context -v
```

Expected: FAIL because `schema/002_execution.sql` is not applied.

- [ ] **Step 3: Add schema migration**

Create `schema/002_execution.sql`:

```sql
alter table orders add column exchange_order_id text;
alter table orders add column attempt integer not null default 1;
alter table orders add column raw_json text not null default '{}';
alter table orders add column last_error text;

alter table fills add column trade_id text;
alter table fills add column client_oid text;
alter table fills add column raw_json text not null default '{}';

alter table positions add column ownership text not null default 'system'
  check (ownership in ('system', 'imported'));
alter table positions add column opened_at text;
alter table positions add column adopted_at text;
alter table positions add column source_intent_id text;
alter table positions add column raw_json text not null default '{}';

create table if not exists executor_state (
  key text primary key,
  value text not null,
  updated_at text not null
);

create index if not exists idx_orders_intent_status
  on orders(intent_id, status, updated_at);

create index if not exists idx_fills_order_symbol
  on fills(order_id, symbol, created_at);
```

- [ ] **Step 4: Apply all schema files in Python**

Modify `src/prodigy/db.py`:

```python
from __future__ import annotations

import sqlite3
from pathlib import Path


SCHEMA_DIR = Path(__file__).resolve().parents[2] / "schema"
SCHEMA_PATH = SCHEMA_DIR / "001_initial.sql"


def connect(path: str | Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("pragma foreign_keys = on")
    conn.execute("pragma journal_mode = wal")
    conn.execute("pragma busy_timeout = 5000")
    return conn


def init_db(conn: sqlite3.Connection, schema_path: Path = SCHEMA_PATH) -> None:
    if schema_path == SCHEMA_PATH:
        conn.executescript(SCHEMA_PATH.read_text())
        _ensure_execution_schema(conn)
    else:
        conn.executescript(schema_path.read_text())
    conn.commit()


def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {row["name"] for row in conn.execute(f"pragma table_info({table})")}


def _add_column_if_missing(
    conn: sqlite3.Connection, table: str, column: str, definition: str
) -> None:
    if column not in _columns(conn, table):
        conn.execute(f"alter table {table} add column {definition}")


def _ensure_execution_schema(conn: sqlite3.Connection) -> None:
    _add_column_if_missing(conn, "orders", "exchange_order_id", "exchange_order_id text")
    _add_column_if_missing(conn, "orders", "attempt", "attempt integer not null default 1")
    _add_column_if_missing(conn, "orders", "raw_json", "raw_json text not null default '{}'")
    _add_column_if_missing(conn, "orders", "last_error", "last_error text")
    _add_column_if_missing(conn, "fills", "trade_id", "trade_id text")
    _add_column_if_missing(conn, "fills", "client_oid", "client_oid text")
    _add_column_if_missing(conn, "fills", "raw_json", "raw_json text not null default '{}'")
    _add_column_if_missing(
        conn,
        "positions",
        "ownership",
        "ownership text not null default 'system' check (ownership in ('system', 'imported'))",
    )
    _add_column_if_missing(conn, "positions", "opened_at", "opened_at text")
    _add_column_if_missing(conn, "positions", "adopted_at", "adopted_at text")
    _add_column_if_missing(conn, "positions", "source_intent_id", "source_intent_id text")
    _add_column_if_missing(conn, "positions", "raw_json", "raw_json text not null default '{}'")
    conn.executescript(
        """
        create table if not exists executor_state (
          key text primary key,
          value text not null,
          updated_at text not null
        );
        create index if not exists idx_orders_intent_status
          on orders(intent_id, status, updated_at);
        create index if not exists idx_fills_order_symbol
          on fills(order_id, symbol, created_at);
        """
    )
```

- [ ] **Step 5: Run Python schema tests**

Run:

```bash
mamba run -n quantmamba python -m pytest tests/test_db_schema.py -v
```

Expected: PASS.

- [ ] **Step 6: Write failing Rust DB helper tests**

Append to `crates/executor/src/db.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    fn memory_db() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(include_str!("../../../schema/001_initial.sql")).unwrap();
        conn.execute_batch(include_str!("../../../schema/002_execution.sql")).unwrap();
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
            intent_id: Some("intent-1".to_string()),
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
        upsert_order(&conn, &OrderRecord { status: "filled".to_string(), ..order }).unwrap();

        let status: String = conn
            .query_row(
                "select status from orders where client_oid = 'client-1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(status, "filled");
    }
}
```

- [ ] **Step 7: Add Rust DB types and helpers**

Modify `crates/executor/src/types.rs`:

```rust
#[derive(Debug, Clone, PartialEq)]
pub struct TradeIntent {
    pub intent_id: String,
    pub symbol: String,
    pub side: String,
    pub action: String,
    pub target_notional: f64,
    pub max_order_notional: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderRecord {
    pub order_id: String,
    pub exchange_order_id: Option<String>,
    pub client_oid: String,
    pub intent_id: Option<String>,
    pub symbol: String,
    pub side: String,
    pub action: String,
    pub order_type: String,
    pub status: String,
    pub price: Option<f64>,
    pub size: f64,
    pub filled_size: f64,
    pub attempt: i64,
    pub raw_json: String,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FillRecord {
    pub fill_id: String,
    pub order_id: String,
    pub trade_id: Option<String>,
    pub client_oid: Option<String>,
    pub symbol: String,
    pub side: String,
    pub price: f64,
    pub size: f64,
    pub fee: f64,
    pub created_at: String,
    pub raw_json: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PositionRecord {
    pub symbol: String,
    pub side: String,
    pub notional: f64,
    pub entry_price: f64,
    pub unrealized_pnl: f64,
    pub ownership: String,
    pub opened_at: Option<String>,
    pub adopted_at: Option<String>,
    pub source_intent_id: Option<String>,
    pub raw_json: String,
}
```

Modify `crates/executor/src/db.rs` by importing the new types and adding helpers:

```rust
use anyhow::Result;
use rusqlite::{params, Connection};

use crate::types::{OrderRecord, TradeIntent};

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
```

- [ ] **Step 8: Run Rust DB tests**

Run:

```bash
cargo test -p prodigy-executor db -- --nocapture
```

Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add schema/002_execution.sql src/prodigy/db.py tests/test_db_schema.py crates/executor/src/db.rs crates/executor/src/types.rs
git commit -m "feat: extend execution persistence schema"
```

## Task 3: Bitget Auth, REST Client, And Demo Guard

**Files:**
- Modify: `crates/executor/src/bitget.rs`
- Test: `crates/executor/src/bitget.rs`

- [ ] **Step 1: Write failing unit tests for signing and demo header**

Create `crates/executor/src/bitget.rs` with tests:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rest_signature_matches_hmac_sha256_base64() {
        let sig = sign(
            "secret",
            "16273667805456",
            "POST",
            "/api/v2/mix/order/place-order",
            r#"{"symbol":"ETHUSDT"}"#,
        );

        assert_eq!(sig, "7q0EikaFI6vj9FuQRddouLkfjADl2NLTCej2t5t/QZY=");
    }

    #[test]
    fn websocket_signature_uses_user_verify_path() {
        let sig = websocket_sign("secret", "1538054050");

        assert_eq!(sig, "QW9NDIxQTfljkfSeZydUIsfx+5D1GgkIbDzvrCplpp4=");
    }

    #[test]
    fn demo_rest_headers_include_paptrading() {
        let cfg = crate::config::ExecutorConfig::demo_for_tests();
        let headers = signed_headers(&cfg, "1", "GET", "/api/v2/mix/account/account", "").unwrap();

        assert_eq!(headers.get("PAPTRADING").unwrap(), "1");
        assert_eq!(headers.get("ACCESS-KEY").unwrap(), "key");
    }
}
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
cargo test -p prodigy-executor bitget::tests -- --nocapture
```

Expected: FAIL because Bitget signing functions do not exist.

- [ ] **Step 3: Implement signing, request types, and REST skeleton**

Implement `crates/executor/src/bitget.rs`:

```rust
use anyhow::{bail, Context, Result};
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use hmac::{Hmac, Mac};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue, CONTENT_TYPE};
use serde::Serialize;
use serde_json::Value;
use sha2::Sha256;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::config::ExecutorConfig;

type HmacSha256 = Hmac<Sha256>;

pub fn now_ms() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_millis()
        .to_string()
}

pub fn now_seconds() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_secs()
        .to_string()
}

pub fn sign(secret: &str, timestamp: &str, method: &str, path: &str, body: &str) -> String {
    let payload = format!("{timestamp}{method}{path}{body}");
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).expect("hmac accepts key");
    mac.update(payload.as_bytes());
    STANDARD.encode(mac.finalize().into_bytes())
}

pub fn websocket_sign(secret: &str, timestamp: &str) -> String {
    sign(secret, timestamp, "GET", "/user/verify", "")
}

pub fn signed_headers(
    cfg: &ExecutorConfig,
    timestamp: &str,
    method: &str,
    path: &str,
    body: &str,
) -> Result<HashMap<String, String>> {
    cfg.validate_demo_only()?;
    let mut headers = HashMap::new();
    headers.insert("ACCESS-KEY".to_string(), cfg.secrets.api_key.clone());
    headers.insert(
        "ACCESS-SIGN".to_string(),
        sign(&cfg.secrets.api_secret, timestamp, method, path, body),
    );
    headers.insert("ACCESS-PASSPHRASE".to_string(), cfg.secrets.passphrase.clone());
    headers.insert("ACCESS-TIMESTAMP".to_string(), timestamp.to_string());
    headers.insert("locale".to_string(), "en-US".to_string());
    headers.insert("Content-Type".to_string(), "application/json".to_string());
    headers.insert("PAPTRADING".to_string(), "1".to_string());
    Ok(headers)
}

fn to_headermap(headers: HashMap<String, String>) -> Result<HeaderMap> {
    let mut map = HeaderMap::new();
    for (key, value) in headers {
        let name = HeaderName::from_bytes(key.as_bytes()).with_context(|| key.clone())?;
        map.insert(name, HeaderValue::from_str(&value)?);
    }
    map.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    Ok(map)
}

#[derive(Debug, Clone)]
pub struct BitgetRestClient {
    cfg: ExecutorConfig,
    client: reqwest::Client,
}

impl BitgetRestClient {
    pub fn new(cfg: ExecutorConfig) -> Result<Self> {
        cfg.validate_demo_only()?;
        Ok(Self {
            cfg,
            client: reqwest::Client::builder().build()?,
        })
    }

    pub async fn get(&self, path: &str, query: &[(&str, String)]) -> Result<Value> {
        let query_string = if query.is_empty() {
            String::new()
        } else {
            let joined = query
                .iter()
                .map(|(k, v)| format!("{k}={v}"))
                .collect::<Vec<_>>()
                .join("&");
            format!("?{joined}")
        };
        let request_path = format!("{path}{query_string}");
        let timestamp = now_ms();
        let headers = to_headermap(signed_headers(
            &self.cfg,
            &timestamp,
            "GET",
            &request_path,
            "",
        )?)?;
        let url = format!("{}{}", self.cfg.rest_base_url, request_path);
        let response = self.client.get(url).headers(headers).send().await?;
        parse_bitget_response(response).await
    }

    pub async fn post_json<T: Serialize>(&self, path: &str, body: &T) -> Result<Value> {
        let body_text = serde_json::to_string(body)?;
        let timestamp = now_ms();
        let headers = to_headermap(signed_headers(
            &self.cfg,
            &timestamp,
            "POST",
            path,
            &body_text,
        )?)?;
        let url = format!("{}{}", self.cfg.rest_base_url, path);
        let response = self.client.post(url).headers(headers).body(body_text).send().await?;
        parse_bitget_response(response).await
    }
}

async fn parse_bitget_response(response: reqwest::Response) -> Result<Value> {
    let status = response.status();
    let value: Value = response.json().await?;
    if !status.is_success() {
        bail!("bitget http status {status}: {value}");
    }
    let code = value.get("code").and_then(|v| v.as_str()).unwrap_or("");
    if code != "00000" && code != "0" {
        bail!("bitget api error: {value}");
    }
    Ok(value)
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PlaceOrderRequest {
    pub symbol: String,
    pub product_type: String,
    pub margin_mode: String,
    pub margin_coin: String,
    pub size: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub price: Option<String>,
    pub side: String,
    pub order_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub force: Option<String>,
    pub client_oid: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reduce_only: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CancelOrderRequest {
    pub symbol: String,
    pub product_type: String,
    pub margin_coin: String,
    pub client_oid: String,
}
```

- [ ] **Step 4: Run unit tests**

Run:

```bash
cargo test -p prodigy-executor bitget::tests -- --nocapture
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/executor/src/bitget.rs crates/executor/src/config.rs
git commit -m "feat: add bitget demo rest client"
```

## Task 4: Public And Private WebSocket Cache

**Files:**
- Modify: `crates/executor/src/bitget.rs`
- Modify: `crates/executor/src/types.rs`
- Test: `crates/executor/src/bitget.rs`

- [ ] **Step 1: Write failing WS parser tests**

Append to the `tests` module in `crates/executor/src/bitget.rs`:

```rust
#[test]
fn parses_books5_snapshot_into_best_bid_ask() {
    let raw = r#"{
      "action":"snapshot",
      "arg":{"instType":"USDT-FUTURES","channel":"books5","instId":"ETHUSDT"},
      "data":[{"bids":[["3000.1","2"]],"asks":[["3000.2","3"]],"ts":"1760461517285"}],
      "ts":1760461517285
    }"#;

    let update = parse_public_ws_message(raw).unwrap().unwrap();

    assert_eq!(update.symbol, "ETHUSDT");
    assert_eq!(update.best_bid, 3000.1);
    assert_eq!(update.best_ask, 3000.2);
}

#[test]
fn parses_private_order_message() {
    let raw = r#"{
      "action":"snapshot",
      "arg":{"instType":"USDT-FUTURES","instId":"default","channel":"orders"},
      "data":[{
        "orderId":"123",
        "clientOid":"client-1",
        "instId":"ETHUSDT",
        "side":"buy",
        "orderType":"limit",
        "status":"live",
        "price":"3000",
        "size":"0.01",
        "accBaseVolume":"0"
      }]
    }"#;

    let update = parse_private_ws_message(raw).unwrap();

    assert_eq!(update.orders.len(), 1);
    assert_eq!(update.orders[0].client_oid, "client-1");
    assert_eq!(update.orders[0].status, "live");
}
```

- [ ] **Step 2: Run parser tests to verify failure**

Run:

```bash
cargo test -p prodigy-executor bitget::tests::parses_ -- --nocapture
```

Expected: FAIL because WS parsers do not exist.

- [ ] **Step 3: Add WS update types and parsers**

Append to `crates/executor/src/types.rs`:

```rust
#[derive(Debug, Clone, PartialEq)]
pub struct MarketUpdate {
    pub symbol: String,
    pub best_bid: f64,
    pub best_ask: f64,
    pub exchange_ts_ms: i64,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct PrivateWsUpdate {
    pub orders: Vec<OrderRecord>,
    pub fills: Vec<FillRecord>,
    pub positions: Vec<PositionRecord>,
}
```

Append parser functions to `crates/executor/src/bitget.rs`:

```rust
use crate::types::{MarketUpdate, OrderRecord, PrivateWsUpdate};

pub fn parse_public_ws_message(text: &str) -> Result<Option<MarketUpdate>> {
    if text == "pong" {
        return Ok(None);
    }
    let value: Value = serde_json::from_str(text)?;
    let channel = value
        .pointer("/arg/channel")
        .and_then(Value::as_str)
        .unwrap_or("");
    if channel != "books5" && channel != "books1" {
        return Ok(None);
    }
    let data = value
        .get("data")
        .and_then(Value::as_array)
        .and_then(|rows| rows.first())
        .context("missing books data")?;
    let bid = data
        .get("bids")
        .and_then(Value::as_array)
        .and_then(|rows| rows.first())
        .and_then(Value::as_array)
        .context("missing best bid")?;
    let ask = data
        .get("asks")
        .and_then(Value::as_array)
        .and_then(|rows| rows.first())
        .and_then(Value::as_array)
        .context("missing best ask")?;
    Ok(Some(MarketUpdate {
        symbol: value.pointer("/arg/instId").and_then(Value::as_str).unwrap_or("").to_string(),
        best_bid: parse_f64(bid.first(), "best bid")?,
        best_ask: parse_f64(ask.first(), "best ask")?,
        exchange_ts_ms: value.get("ts").and_then(Value::as_i64).unwrap_or(0),
    }))
}

pub fn parse_private_ws_message(text: &str) -> Result<PrivateWsUpdate> {
    if text == "pong" {
        return Ok(PrivateWsUpdate::default());
    }
    let value: Value = serde_json::from_str(text)?;
    let channel = value
        .pointer("/arg/channel")
        .and_then(Value::as_str)
        .unwrap_or("");
    let data = value.get("data").and_then(Value::as_array).cloned().unwrap_or_default();
    let mut update = PrivateWsUpdate::default();

    if channel == "orders" {
        for row in data {
            let order_id = str_field(&row, "orderId");
            let client_oid = str_field(&row, "clientOid");
            update.orders.push(OrderRecord {
                order_id: order_id.clone(),
                exchange_order_id: Some(order_id),
                client_oid,
                intent_id: None,
                symbol: "ETH/USDT:USDT".to_string(),
                side: str_field(&row, "side"),
                action: str_field(&row, "tradeSide"),
                order_type: str_field(&row, "orderType"),
                status: str_field(&row, "status"),
                price: row.get("price").and_then(Value::as_str).and_then(|v| v.parse().ok()),
                size: row.get("size").and_then(Value::as_str).and_then(|v| v.parse().ok()).unwrap_or(0.0),
                filled_size: row.get("accBaseVolume").and_then(Value::as_str).and_then(|v| v.parse().ok()).unwrap_or(0.0),
                attempt: 1,
                raw_json: row.to_string(),
                last_error: None,
            });
        }
    }

    Ok(update)
}

fn str_field(value: &Value, key: &str) -> String {
    value.get(key).and_then(Value::as_str).unwrap_or("").to_string()
}

fn parse_f64(value: Option<&Value>, name: &str) -> Result<f64> {
    value
        .and_then(Value::as_str)
        .context(name.to_string())?
        .parse::<f64>()
        .with_context(|| format!("parse {name}"))
}
```

- [ ] **Step 4: Add WS connection functions**

Add these public functions to `crates/executor/src/bitget.rs`:

```rust
pub async fn verify_public_ws_connects(cfg: &ExecutorConfig) -> Result<()> {
    cfg.validate_demo_only()?;
    let (mut socket, _) = tokio_tungstenite::connect_async(&cfg.public_ws_url).await?;
    use futures_util::{SinkExt, StreamExt};
    let msg = serde_json::json!({
        "op": "subscribe",
        "args": [{
            "instType": cfg.product_type,
            "channel": "books5",
            "instId": cfg.bitget_symbol
        }]
    });
    socket.send(tokio_tungstenite::tungstenite::Message::Text(msg.to_string())).await?;
    let msg = tokio::time::timeout(std::time::Duration::from_secs(10), socket.next())
        .await?
        .ok_or_else(|| anyhow::anyhow!("public websocket closed"))??;
    let text = msg.into_text()?;
    if text.contains("\"event\":\"error\"") {
        bail!("public websocket subscription failed: {text}");
    }
    Ok(())
}

pub async fn verify_private_ws_connects(cfg: &ExecutorConfig) -> Result<()> {
    cfg.validate_demo_only()?;
    let (mut socket, _) = tokio_tungstenite::connect_async(&cfg.private_ws_url).await?;
    use futures_util::{SinkExt, StreamExt};
    let timestamp = now_seconds();
    let login = serde_json::json!({
        "op": "login",
        "args": [{
            "apiKey": cfg.secrets.api_key,
            "passphrase": cfg.secrets.passphrase,
            "timestamp": timestamp,
            "sign": websocket_sign(&cfg.secrets.api_secret, &timestamp)
        }]
    });
    socket.send(tokio_tungstenite::tungstenite::Message::Text(login.to_string())).await?;
    let msg = tokio::time::timeout(std::time::Duration::from_secs(10), socket.next())
        .await?
        .ok_or_else(|| anyhow::anyhow!("private websocket closed"))??;
    let text = msg.into_text()?;
    if text.contains("\"event\":\"error\"") || !text.contains("\"login\"") {
        bail!("private websocket login failed: {text}");
    }
    Ok(())
}
```

- [ ] **Step 5: Run WS parser tests**

Run:

```bash
cargo test -p prodigy-executor bitget::tests -- --nocapture
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/executor/src/bitget.rs crates/executor/src/types.rs
git commit -m "feat: add bitget websocket parsers"
```

## Task 5: Pure State Machine For Maker Retry, Cancel, And Taker Fallback

**Files:**
- Modify: `crates/executor/src/state.rs`
- Test: `crates/executor/src/state.rs`

- [ ] **Step 1: Write failing state machine tests**

Create `crates/executor/src/state.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_order_retries_maker_once_then_taker() {
        let policy = ExecutionPolicy {
            max_maker_attempts_before_taker: 2,
        };
        let mut state = IntentExecution::new("intent-1", "open");

        assert_eq!(state.next_command(&policy), ExecutionCommand::PlaceMaker { attempt: 1 });
        state.on_order_timeout();
        assert_eq!(state.next_command(&policy), ExecutionCommand::CancelCurrent);
        state.on_order_cancelled();
        assert_eq!(state.next_command(&policy), ExecutionCommand::PlaceMaker { attempt: 2 });
        state.on_order_timeout();
        assert_eq!(state.next_command(&policy), ExecutionCommand::CancelCurrent);
        state.on_order_cancelled();
        assert_eq!(state.next_command(&policy), ExecutionCommand::PlaceTaker);
    }

    #[test]
    fn filled_order_marks_execution_done() {
        let policy = ExecutionPolicy {
            max_maker_attempts_before_taker: 2,
        };
        let mut state = IntentExecution::new("intent-1", "open");

        state.on_order_placed("client-1");
        state.on_order_filled();

        assert_eq!(state.next_command(&policy), ExecutionCommand::MarkIntentExecuted);
    }
}
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
cargo test -p prodigy-executor state -- --nocapture
```

Expected: FAIL because state types do not exist.

- [ ] **Step 3: Implement pure state machine**

Replace `crates/executor/src/state.rs` with:

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutionCommand {
    PlaceMaker { attempt: u32 },
    CancelCurrent,
    PlaceTaker,
    MarkIntentExecuted,
    Wait,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionPolicy {
    pub max_maker_attempts_before_taker: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IntentExecution {
    pub intent_id: String,
    pub action: String,
    pub maker_attempts: u32,
    pub has_live_order: bool,
    pub needs_cancel_confirmation: bool,
    pub filled: bool,
    pub taker_sent: bool,
}

impl IntentExecution {
    pub fn new(intent_id: impl Into<String>, action: impl Into<String>) -> Self {
        Self {
            intent_id: intent_id.into(),
            action: action.into(),
            maker_attempts: 0,
            has_live_order: false,
            needs_cancel_confirmation: false,
            filled: false,
            taker_sent: false,
        }
    }

    pub fn next_command(&self, policy: &ExecutionPolicy) -> ExecutionCommand {
        if self.filled {
            return ExecutionCommand::MarkIntentExecuted;
        }
        if self.needs_cancel_confirmation {
            return ExecutionCommand::CancelCurrent;
        }
        if self.has_live_order {
            return ExecutionCommand::Wait;
        }
        if self.maker_attempts < policy.max_maker_attempts_before_taker {
            return ExecutionCommand::PlaceMaker {
                attempt: self.maker_attempts + 1,
            };
        }
        if !self.taker_sent {
            return ExecutionCommand::PlaceTaker;
        }
        ExecutionCommand::Wait
    }

    pub fn on_order_placed(&mut self, _client_oid: &str) {
        self.has_live_order = true;
        self.needs_cancel_confirmation = false;
        self.maker_attempts += 1;
    }

    pub fn on_taker_sent(&mut self) {
        self.taker_sent = true;
        self.has_live_order = true;
    }

    pub fn on_order_timeout(&mut self) {
        if self.has_live_order {
            self.needs_cancel_confirmation = true;
        }
    }

    pub fn on_order_cancelled(&mut self) {
        self.has_live_order = false;
        self.needs_cancel_confirmation = false;
    }

    pub fn on_order_filled(&mut self) {
        self.filled = true;
        self.has_live_order = false;
        self.needs_cancel_confirmation = false;
    }
}
```

- [ ] **Step 4: Run state tests**

Run:

```bash
cargo test -p prodigy-executor state -- --nocapture
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/executor/src/state.rs
git commit -m "feat: add executor order state machine"
```

## Task 6: Risk Gate

**Files:**
- Modify: `crates/executor/src/risk.rs`
- Test: `crates/executor/src/risk.rs`

- [ ] **Step 1: Write failing risk tests**

Create `crates/executor/src/risk.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::TradeIntent;

    fn intent(action: &str, target: f64, max_order: f64) -> TradeIntent {
        TradeIntent {
            intent_id: "i1".to_string(),
            symbol: "ETH/USDT:USDT".to_string(),
            side: "long".to_string(),
            action: action.to_string(),
            target_notional: target,
            max_order_notional: max_order,
        }
    }

    #[test]
    fn clips_order_notional_to_intent_and_cap() {
        let account = AccountRiskSnapshot {
            equity: 1_000.0,
            available_margin: 500.0,
            unrealized_pnl_24h: 0.0,
            gross_notional: 1_000.0,
            market_is_fresh: true,
            private_state_is_ready: true,
        };
        let params = RiskParams::default();

        let decision = check_intent(&intent("open", 900.0, 600.0), &account, &params);

        assert_eq!(decision.unwrap().approved_notional, 600.0);
    }

    #[test]
    fn blocks_new_opening_when_unrealized_loss_threshold_hit() {
        let account = AccountRiskSnapshot {
            equity: 1_000.0,
            available_margin: 500.0,
            unrealized_pnl_24h: -100.0,
            gross_notional: 0.0,
            market_is_fresh: true,
            private_state_is_ready: true,
        };

        let err = check_intent(&intent("open", 100.0, 100.0), &account, &RiskParams::default())
            .unwrap_err();

        assert!(err.contains("trading suspended"));
    }

    #[test]
    fn allows_close_during_trading_suspension() {
        let account = AccountRiskSnapshot {
            equity: 1_000.0,
            available_margin: 500.0,
            unrealized_pnl_24h: -100.0,
            gross_notional: 500.0,
            market_is_fresh: true,
            private_state_is_ready: true,
        };

        let decision = check_intent(&intent("close", 100.0, 100.0), &account, &RiskParams::default());

        assert!(decision.is_ok());
    }
}
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
cargo test -p prodigy-executor risk -- --nocapture
```

Expected: FAIL because risk types do not exist.

- [ ] **Step 3: Implement risk gate**

Replace `crates/executor/src/risk.rs` with:

```rust
use crate::types::TradeIntent;

#[derive(Debug, Clone, Copy)]
pub struct RiskParams {
    pub total_notional_cap_x_equity: f64,
    pub trading_suspension_unrealized_loss_x_equity: f64,
    pub min_available_margin_fraction: f64,
}

impl Default for RiskParams {
    fn default() -> Self {
        Self {
            total_notional_cap_x_equity: 5.0,
            trading_suspension_unrealized_loss_x_equity: 0.10,
            min_available_margin_fraction: 0.05,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct AccountRiskSnapshot {
    pub equity: f64,
    pub available_margin: f64,
    pub unrealized_pnl_24h: f64,
    pub gross_notional: f64,
    pub market_is_fresh: bool,
    pub private_state_is_ready: bool,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RiskDecision {
    pub approved_notional: f64,
}

pub fn check_intent(
    intent: &TradeIntent,
    account: &AccountRiskSnapshot,
    params: &RiskParams,
) -> Result<RiskDecision, String> {
    if !account.private_state_is_ready {
        return Err("private account state is not ready".to_string());
    }
    if !account.market_is_fresh && intent.action == "open" {
        return Err("market data is stale".to_string());
    }
    if account.equity <= 0.0 {
        return Err("equity is not positive".to_string());
    }
    if account.available_margin < account.equity * params.min_available_margin_fraction {
        return Err("available margin is too low".to_string());
    }

    let suspended = account.unrealized_pnl_24h <= -account.equity
        * params.trading_suspension_unrealized_loss_x_equity;
    if suspended && intent.action == "open" {
        return Err("trading suspended by 24h unrealized loss".to_string());
    }

    let total_cap = account.equity * params.total_notional_cap_x_equity;
    let remaining = (total_cap - account.gross_notional).max(0.0);
    let approved = intent.target_notional.min(intent.max_order_notional).min(remaining);
    if approved <= 0.0 && intent.action == "open" {
        return Err("notional cap reached".to_string());
    }

    Ok(RiskDecision {
        approved_notional: approved.max(0.0),
    })
}
```

- [ ] **Step 4: Run risk tests**

Run:

```bash
cargo test -p prodigy-executor risk -- --nocapture
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/executor/src/risk.rs
git commit -m "feat: add executor risk gate"
```

## Task 7: REST Order Manager With Maker Timeout And Taker Fallback

**Files:**
- Modify: `crates/executor/src/executor.rs`
- Modify: `crates/executor/src/bitget.rs`
- Modify: `crates/executor/src/db.rs`
- Test: `crates/executor/src/executor.rs`

- [ ] **Step 1: Write failing unit tests for order construction**

Append tests to `crates/executor/src/executor.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{MarketUpdate, TradeIntent};

    #[test]
    fn maker_open_long_uses_best_bid() {
        let intent = TradeIntent {
            intent_id: "intent-1".to_string(),
            symbol: "ETH/USDT:USDT".to_string(),
            side: "long".to_string(),
            action: "open".to_string(),
            target_notional: 300.0,
            max_order_notional: 300.0,
        };
        let market = MarketUpdate {
            symbol: "ETHUSDT".to_string(),
            best_bid: 3000.0,
            best_ask: 3000.5,
            exchange_ts_ms: 1,
        };

        let order = build_order_request(&ExecutorConfig::demo_for_tests(), &intent, &market, 300.0, OrderMode::Maker, 1);

        assert_eq!(order.side, "buy");
        assert_eq!(order.order_type, "limit");
        assert_eq!(order.price.as_deref(), Some("3000"));
        assert_eq!(order.size, "0.1");
    }

    #[test]
    fn taker_close_long_is_reduce_only_sell_market() {
        let intent = TradeIntent {
            intent_id: "intent-1".to_string(),
            symbol: "ETH/USDT:USDT".to_string(),
            side: "long".to_string(),
            action: "close".to_string(),
            target_notional: 300.0,
            max_order_notional: 300.0,
        };
        let market = MarketUpdate {
            symbol: "ETHUSDT".to_string(),
            best_bid: 3000.0,
            best_ask: 3000.5,
            exchange_ts_ms: 1,
        };

        let order = build_order_request(&ExecutorConfig::demo_for_tests(), &intent, &market, 300.0, OrderMode::Taker, 1);

        assert_eq!(order.side, "sell");
        assert_eq!(order.order_type, "market");
        assert_eq!(order.reduce_only.as_deref(), Some("YES"));
        assert!(order.price.is_none());
    }
}
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
cargo test -p prodigy-executor executor::tests -- --nocapture
```

Expected: FAIL because order construction does not exist.

- [ ] **Step 3: Implement order construction and REST actions**

Add to `crates/executor/src/executor.rs`:

```rust
use anyhow::Result;
use rusqlite::Connection;

use crate::bitget::{BitgetRestClient, PlaceOrderRequest};
use crate::config::ExecutorConfig;
use crate::db;
use crate::risk::{check_intent, AccountRiskSnapshot, RiskParams};
use crate::types::{MarketUpdate, OrderRecord, TradeIntent};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderMode {
    Maker,
    Taker,
}

pub fn build_order_request(
    cfg: &ExecutorConfig,
    intent: &TradeIntent,
    market: &MarketUpdate,
    approved_notional: f64,
    mode: OrderMode,
    attempt: u32,
) -> PlaceOrderRequest {
    let side = match (intent.action.as_str(), intent.side.as_str()) {
        ("open", "long") => "buy",
        ("open", "short") => "sell",
        ("close", "long") => "sell",
        ("close", "short") => "buy",
        _ => "sell",
    };
    let price = match mode {
        OrderMode::Maker if side == "buy" => Some(format_price(market.best_bid)),
        OrderMode::Maker => Some(format_price(market.best_ask)),
        OrderMode::Taker => None,
    };
    let reference_price = match (mode, side) {
        (OrderMode::Maker, "buy") => market.best_bid,
        (OrderMode::Maker, _) => market.best_ask,
        (OrderMode::Taker, "buy") => market.best_ask,
        (OrderMode::Taker, _) => market.best_bid,
    };
    let size = format_size(approved_notional / reference_price);
    let client_oid = format!("pdgy-{}-{attempt}", intent.intent_id);

    PlaceOrderRequest {
        symbol: cfg.bitget_symbol.clone(),
        product_type: cfg.product_type.clone(),
        margin_mode: cfg.margin_mode.clone(),
        margin_coin: cfg.margin_coin.clone(),
        size,
        price,
        side: side.to_string(),
        order_type: if mode == OrderMode::Maker { "limit" } else { "market" }.to_string(),
        force: if mode == OrderMode::Maker { Some("post_only".to_string()) } else { None },
        client_oid,
        reduce_only: if intent.action == "close" { Some("YES".to_string()) } else { None },
    }
}

fn format_price(value: f64) -> String {
    format!("{value:.2}").trim_end_matches('0').trim_end_matches('.').to_string()
}

fn format_size(value: f64) -> String {
    format!("{value:.4}").trim_end_matches('0').trim_end_matches('.').to_string()
}

pub async fn process_one_intent(
    conn: &Connection,
    cfg: &ExecutorConfig,
    rest: &BitgetRestClient,
    intent: TradeIntent,
    market: MarketUpdate,
    account: AccountRiskSnapshot,
) -> Result<()> {
    if !db::accept_intent(conn, &intent.intent_id)? {
        return Ok(());
    }
    let risk = check_intent(
        &intent,
        &account,
        &RiskParams {
            total_notional_cap_x_equity: cfg.total_notional_cap_x_equity,
            trading_suspension_unrealized_loss_x_equity: cfg.trading_suspension_unrealized_loss_x_equity,
            ..RiskParams::default()
        },
    );
    let approved = match risk {
        Ok(decision) => decision.approved_notional,
        Err(reason) => {
            db::fail_intent(conn, &intent.intent_id, &reason)?;
            return Ok(());
        }
    };

    let order = build_order_request(cfg, &intent, &market, approved, OrderMode::Maker, 1);
    let response = rest
        .post_json("/api/v2/mix/order/place-order", &order)
        .await?;
    let exchange_order_id = response
        .pointer("/data/orderId")
        .and_then(serde_json::Value::as_str)
        .unwrap_or(&order.client_oid)
        .to_string();
    db::upsert_order(
        conn,
        &OrderRecord {
            order_id: exchange_order_id.clone(),
            exchange_order_id: Some(exchange_order_id),
            client_oid: order.client_oid.clone(),
            intent_id: Some(intent.intent_id.clone()),
            symbol: intent.symbol.clone(),
            side: order.side.clone(),
            action: intent.action.clone(),
            order_type: order.order_type.clone(),
            status: "submitted".to_string(),
            price: order.price.as_ref().and_then(|v| v.parse().ok()),
            size: order.size.parse().unwrap_or(0.0),
            filled_size: 0.0,
            attempt: 1,
            raw_json: response.to_string(),
            last_error: None,
        },
    )?;
    Ok(())
}
```

- [ ] **Step 4: Add cancel helper to REST client**

Append to `impl BitgetRestClient` in `crates/executor/src/bitget.rs`:

```rust
pub async fn cancel_order(&self, request: &CancelOrderRequest) -> Result<Value> {
    self.post_json("/api/v2/mix/order/cancel-order", request).await
}

pub async fn cancel_all_orders(&self) -> Result<Value> {
    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct CancelAll<'a> {
        product_type: &'a str,
        margin_coin: &'a str,
    }
    self.post_json(
        "/api/v2/mix/order/cancel-all-order",
        &CancelAll {
            product_type: &self.cfg.product_type,
            margin_coin: &self.cfg.margin_coin,
        },
    )
    .await
}
```

- [ ] **Step 5: Run order construction tests**

Run:

```bash
cargo test -p prodigy-executor executor::tests -- --nocapture
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/executor/src/executor.rs crates/executor/src/bitget.rs
git commit -m "feat: add demo order request builder"
```

## Task 8: Reconciliation And Startup Adoption

**Files:**
- Modify: `crates/executor/src/reconcile.rs`
- Modify: `crates/executor/src/bitget.rs`
- Modify: `crates/executor/src/db.rs`
- Test: `crates/executor/src/reconcile.rs`

- [ ] **Step 1: Write failing reconciliation tests**

Create `crates/executor/src/reconcile.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::PositionRecord;

    #[test]
    fn exchange_position_without_local_order_is_imported() {
        let local_order_intents = std::collections::HashSet::new();
        let exchange = PositionRecord {
            symbol: "ETH/USDT:USDT".to_string(),
            side: "long".to_string(),
            notional: 1000.0,
            entry_price: 3000.0,
            unrealized_pnl: 0.0,
            ownership: "system".to_string(),
            opened_at: None,
            adopted_at: None,
            source_intent_id: None,
            raw_json: "{}".to_string(),
        };

        let adopted = classify_position(exchange, &local_order_intents, "2026-07-01T00:00:00Z");

        assert_eq!(adopted.ownership, "imported");
        assert_eq!(adopted.adopted_at.as_deref(), Some("2026-07-01T00:00:00Z"));
    }
}
```

- [ ] **Step 2: Run reconciliation test to verify failure**

Run:

```bash
cargo test -p prodigy-executor reconcile -- --nocapture
```

Expected: FAIL because reconciliation functions do not exist.

- [ ] **Step 3: Implement classification and REST query wrappers**

Replace `crates/executor/src/reconcile.rs`:

```rust
use anyhow::Result;
use rusqlite::Connection;
use std::collections::HashSet;

use crate::bitget::BitgetRestClient;
use crate::db;
use crate::types::PositionRecord;

pub fn classify_position(
    mut exchange_position: PositionRecord,
    local_order_intents: &HashSet<String>,
    now: &str,
) -> PositionRecord {
    if exchange_position
        .source_intent_id
        .as_ref()
        .map(|id| local_order_intents.contains(id))
        .unwrap_or(false)
    {
        exchange_position.ownership = "system".to_string();
    } else {
        exchange_position.ownership = "imported".to_string();
        exchange_position.adopted_at = Some(now.to_string());
        exchange_position.opened_at = Some(now.to_string());
    }
    exchange_position
}

pub async fn reconcile_once(conn: &Connection, rest: &BitgetRestClient, now: &str) -> Result<()> {
    let _open_orders = rest
        .get(
            "/api/v2/mix/order/orders-pending",
            &[
                ("productType", "USDT-FUTURES".to_string()),
                ("marginCoin", "USDT".to_string()),
            ],
        )
        .await?;
    let _positions = rest
        .get(
            "/api/v2/mix/position/all-position",
            &[
                ("productType", "USDT-FUTURES".to_string()),
                ("marginCoin", "USDT".to_string()),
            ],
        )
        .await?;
    db::write_event(conn, "info", "executor", "reconciliation completed", "{}")?;
    let _ = now;
    Ok(())
}
```

- [ ] **Step 4: Add event writer**

Append to `crates/executor/src/db.rs`:

```rust
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
```

- [ ] **Step 5: Run reconciliation tests**

Run:

```bash
cargo test -p prodigy-executor reconcile -- --nocapture
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/executor/src/reconcile.rs crates/executor/src/db.rs crates/executor/src/bitget.rs
git commit -m "feat: add execution reconciliation skeleton"
```

## Task 9: Telegram Notification Sender

**Files:**
- Modify: `crates/executor/src/notify.rs`
- Modify: `crates/executor/src/db.rs`
- Test: `crates/executor/src/notify.rs`

- [ ] **Step 1: Write failing notification tests**

Create `crates/executor/src/notify.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_major_events_are_active_notifications() {
        assert!(should_send_telegram("fill"));
        assert!(should_send_telegram("critical"));
        assert!(!should_send_telegram("heartbeat"));
        assert!(!should_send_telegram("info"));
    }
}
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
cargo test -p prodigy-executor notify -- --nocapture
```

Expected: FAIL because notification helpers do not exist.

- [ ] **Step 3: Implement minimal Telegram sender**

Replace `crates/executor/src/notify.rs`:

```rust
use anyhow::Result;
use reqwest::Client;

pub fn should_send_telegram(kind: &str) -> bool {
    matches!(
        kind,
        "fill" | "position_closed" | "intent_rejected" | "critical" | "margin_danger"
    )
}

pub async fn send_telegram(
    bot_token: Option<&str>,
    chat_id: Option<&str>,
    kind: &str,
    text: &str,
) -> Result<()> {
    if !should_send_telegram(kind) {
        return Ok(());
    }
    let (Some(token), Some(chat)) = (bot_token, chat_id) else {
        return Ok(());
    };
    let url = format!("https://api.telegram.org/bot{token}/sendMessage");
    Client::new()
        .post(url)
        .form(&[("chat_id", chat), ("text", text)])
        .send()
        .await?
        .error_for_status()?;
    Ok(())
}
```

- [ ] **Step 4: Run notification tests**

Run:

```bash
cargo test -p prodigy-executor notify -- --nocapture
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/executor/src/notify.rs
git commit -m "feat: add executor telegram notifications"
```

## Task 9.5: Manual Client Intervention And Mode-Aware Telegram Policy

**Files:**
- Create: `crates/executor/src/manual_override.rs`
- Modify: `crates/executor/src/lib.rs`
- Modify: `crates/executor/src/notify.rs`
- Modify: `crates/executor/src/db.rs`
- Test: `crates/executor/src/manual_override.rs`
- Test: `crates/executor/src/notify.rs`

This task can be implemented after Task 9 if Tasks 1-9 are already complete.

- [ ] **Step 1: Write failing manual override tests**

Create `crates/executor/src/manual_override.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unmatched_exchange_change_enters_symbol_manual_override() {
        let mut state = ManualOverrideState::default();
        let event = ExchangeIntervention {
            symbol: "ETH/USDT:USDT".to_string(),
            matched_local_client_oid: false,
            kind: InterventionKind::Open,
        };

        let decision = apply_exchange_intervention(&mut state, event);

        assert_eq!(decision, ManualOverrideDecision::Entered("ETH/USDT:USDT".to_string()));
        assert!(state.is_blocked_for_open("ETH/USDT:USDT"));
    }

    #[test]
    fn matched_system_change_does_not_enter_manual_override() {
        let mut state = ManualOverrideState::default();
        let event = ExchangeIntervention {
            symbol: "ETH/USDT:USDT".to_string(),
            matched_local_client_oid: true,
            kind: InterventionKind::Open,
        };

        let decision = apply_exchange_intervention(&mut state, event);

        assert_eq!(decision, ManualOverrideDecision::NoChange);
        assert!(!state.is_blocked_for_open("ETH/USDT:USDT"));
    }

    #[test]
    fn override_clears_only_when_position_and_open_orders_are_zero() {
        let mut state = ManualOverrideState::default();
        state.enter("ETH/USDT:USDT");

        assert_eq!(
            maybe_clear_manual_override(&mut state, "ETH/USDT:USDT", 10.0, 0),
            ManualOverrideDecision::NoChange
        );
        assert!(state.is_blocked_for_open("ETH/USDT:USDT"));

        assert_eq!(
            maybe_clear_manual_override(&mut state, "ETH/USDT:USDT", 0.0, 0),
            ManualOverrideDecision::Cleared("ETH/USDT:USDT".to_string())
        );
        assert!(!state.is_blocked_for_open("ETH/USDT:USDT"));
    }

    #[test]
    fn cap_breach_alone_does_not_force_reduce_manual_position() {
        let action = risk_action_for_manual_position(20_000.0, 5_000.0, false);

        assert_eq!(action, ManualRiskAction::DoNothing);
    }

    #[test]
    fn margin_danger_can_force_reduce_manual_position() {
        let action = risk_action_for_manual_position(20_000.0, 5_000.0, true);

        assert_eq!(action, ManualRiskAction::AllowEmergencyReduce);
    }

    #[test]
    fn system_order_manual_cancel_is_external_cancel() {
        assert_eq!(
            classify_external_status(InterventionKind::Cancel, true),
            Some("externally_cancelled")
        );
    }

    #[test]
    fn system_position_manual_close_is_external_close() {
        assert_eq!(
            classify_external_status(InterventionKind::Close, true),
            Some("externally_closed")
        );
    }
}
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
cargo test -p prodigy-executor manual_override -- --nocapture
```

Expected: FAIL because manual override types do not exist.

- [ ] **Step 3: Implement manual override state**

Replace `crates/executor/src/manual_override.rs`:

```rust
use std::collections::HashSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InterventionKind {
    Open,
    Add,
    Reduce,
    Close,
    Cancel,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExchangeIntervention {
    pub symbol: String,
    pub matched_local_client_oid: bool,
    pub kind: InterventionKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManualOverrideDecision {
    Entered(String),
    Cleared(String),
    NoChange,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManualRiskAction {
    DoNothing,
    AllowEmergencyReduce,
}

#[derive(Debug, Default, Clone)]
pub struct ManualOverrideState {
    blocked_symbols: HashSet<String>,
}

impl ManualOverrideState {
    pub fn enter(&mut self, symbol: &str) {
        self.blocked_symbols.insert(symbol.to_string());
    }

    pub fn is_blocked_for_open(&self, symbol: &str) -> bool {
        self.blocked_symbols.contains(symbol)
    }

    fn clear(&mut self, symbol: &str) -> bool {
        self.blocked_symbols.remove(symbol)
    }
}

pub fn apply_exchange_intervention(
    state: &mut ManualOverrideState,
    event: ExchangeIntervention,
) -> ManualOverrideDecision {
    if event.matched_local_client_oid {
        return ManualOverrideDecision::NoChange;
    }
    if state.is_blocked_for_open(&event.symbol) {
        return ManualOverrideDecision::NoChange;
    }
    state.enter(&event.symbol);
    ManualOverrideDecision::Entered(event.symbol)
}

pub fn maybe_clear_manual_override(
    state: &mut ManualOverrideState,
    symbol: &str,
    position_notional: f64,
    open_order_count: usize,
) -> ManualOverrideDecision {
    if position_notional == 0.0 && open_order_count == 0 && state.clear(symbol) {
        return ManualOverrideDecision::Cleared(symbol.to_string());
    }
    ManualOverrideDecision::NoChange
}

pub fn risk_action_for_manual_position(
    manual_notional: f64,
    normal_cap: f64,
    margin_danger: bool,
) -> ManualRiskAction {
    if margin_danger {
        return ManualRiskAction::AllowEmergencyReduce;
    }
    let _ = (manual_notional, normal_cap);
    ManualRiskAction::DoNothing
}

pub fn classify_external_status(
    kind: InterventionKind,
    was_system_owned: bool,
) -> Option<&'static str> {
    if !was_system_owned {
        return None;
    }
    match kind {
        InterventionKind::Cancel => Some("externally_cancelled"),
        InterventionKind::Close | InterventionKind::Reduce => Some("externally_closed"),
        InterventionKind::Open | InterventionKind::Add => None,
    }
}
```

Modify `crates/executor/src/lib.rs`:

```rust
pub mod manual_override;
```

- [ ] **Step 4: Persist manual override state in SQLite**

Append to `crates/executor/src/db.rs`:

```rust
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
```

Use key format `manual_override:<symbol>` and value `active` or delete/clear by setting `cleared` if deletion helper is not yet needed. This is intentionally minimal; the active check reads only `active`.

- [ ] **Step 5: Write failing notification mode tests**

Append to `crates/executor/src/notify.rs` tests:

```rust
#[test]
fn demo_mode_suppresses_normal_trade_notifications_but_allows_manual_override() {
    assert!(!should_send_telegram_for_mode(NotificationMode::Demo, "fill"));
    assert!(!should_send_telegram_for_mode(NotificationMode::Demo, "position_closed"));
    assert!(should_send_telegram_for_mode(NotificationMode::Demo, "manual_override_entered"));
    assert!(should_send_telegram_for_mode(NotificationMode::Demo, "manual_override_cleared"));
    assert!(should_send_telegram_for_mode(NotificationMode::Demo, "critical"));
}

#[test]
fn live_mode_sends_trade_and_manual_override_notifications() {
    assert!(should_send_telegram_for_mode(NotificationMode::Live, "fill"));
    assert!(should_send_telegram_for_mode(NotificationMode::Live, "position_closed"));
    assert!(should_send_telegram_for_mode(NotificationMode::Live, "manual_override_entered"));
    assert!(should_send_telegram_for_mode(NotificationMode::Live, "manual_override_cleared"));
}
```

- [ ] **Step 6: Implement mode-aware notification policy**

Modify `crates/executor/src/notify.rs`:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotificationMode {
    Demo,
    Live,
}

pub fn should_send_telegram_for_mode(mode: NotificationMode, kind: &str) -> bool {
    match mode {
        NotificationMode::Demo => matches!(
            kind,
            "critical"
                | "margin_danger"
                | "manual_override_entered"
                | "manual_override_cleared"
                | "websocket_auth_failed"
                | "rest_order_failed"
        ),
        NotificationMode::Live => matches!(
            kind,
            "fill"
                | "position_closed"
                | "intent_rejected"
                | "critical"
                | "margin_danger"
                | "manual_override_entered"
                | "manual_override_cleared"
                | "websocket_auth_failed"
                | "rest_order_failed"
        ),
    }
}

pub fn should_send_telegram(kind: &str) -> bool {
    should_send_telegram_for_mode(NotificationMode::Demo, kind)
}
```

Keep `send_telegram` using `should_send_telegram` for now because third milestone is demo-only. Future live mode should call `should_send_telegram_for_mode(NotificationMode::Live, kind)`.

- [ ] **Step 7: Run tests**

Run:

```bash
cargo test -p prodigy-executor manual_override notify -- --nocapture
```

Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add crates/executor/src/manual_override.rs crates/executor/src/lib.rs crates/executor/src/notify.rs crates/executor/src/db.rs
git commit -m "feat: handle manual client intervention"
```

## Task 10: Bitget Demo Integration Tests

**Files:**
- Create: `crates/executor/tests/bitget_demo.rs`
- Modify: `crates/executor/src/bitget.rs`
- Modify: `crates/executor/src/executor.rs`

- [ ] **Step 1: Write failing demo integration tests**

Create `crates/executor/tests/bitget_demo.rs`:

```rust
use prodigy_executor::bitget::{
    verify_private_ws_connects, verify_public_ws_connects, BitgetRestClient, CancelOrderRequest,
    PlaceOrderRequest,
};
use prodigy_executor::config::{load_env_file, DemoSecrets, ExecutorConfig};
use std::env;
use std::path::Path;

fn demo_config() -> ExecutorConfig {
    let file = load_env_file(Path::new(".env.local")).unwrap();
    let get = |key: &str| env::var(key).ok().or_else(|| file.get(key).cloned()).unwrap();
    let mut cfg = ExecutorConfig::demo_for_tests();
    cfg.secrets = DemoSecrets {
        api_key: get("BITGET_DEMO_API_KEY"),
        api_secret: get("BITGET_DEMO_API_SECRET"),
        passphrase: get("BITGET_DEMO_API_PASSPHRASE"),
    };
    cfg.test_reset_demo_state = true;
    cfg
}

#[tokio::test]
async fn bitget_demo_public_and_private_ws_connect() {
    let cfg = demo_config();

    verify_public_ws_connects(&cfg).await.unwrap();
    verify_private_ws_connects(&cfg).await.unwrap();
}

#[tokio::test]
async fn bitget_demo_can_place_and_cancel_limit_order() {
    let cfg = demo_config();
    let rest = BitgetRestClient::new(cfg.clone()).unwrap();
    let client_oid = format!("pdgy-test-cancel-{}", prodigy_executor::bitget::now_ms());

    let request = PlaceOrderRequest {
        symbol: cfg.bitget_symbol.clone(),
        product_type: cfg.product_type.clone(),
        margin_mode: cfg.margin_mode.clone(),
        margin_coin: cfg.margin_coin.clone(),
        size: "0.01".to_string(),
        price: Some("100".to_string()),
        side: "buy".to_string(),
        order_type: "limit".to_string(),
        force: Some("gtc".to_string()),
        client_oid: client_oid.clone(),
        reduce_only: None,
    };

    let placed = rest.post_json("/api/v2/mix/order/place-order", &request).await.unwrap();
    assert_eq!(placed.get("code").and_then(|v| v.as_str()), Some("00000"));

    let cancelled = rest
        .cancel_order(&CancelOrderRequest {
            symbol: cfg.bitget_symbol.clone(),
            product_type: cfg.product_type.clone(),
            margin_coin: cfg.margin_coin.clone(),
            client_oid,
        })
        .await
        .unwrap();
    assert_eq!(cancelled.get("code").and_then(|v| v.as_str()), Some("00000"));
}

#[tokio::test]
async fn bitget_demo_can_open_and_reduce_only_close_market_order() {
    let cfg = demo_config();
    let rest = BitgetRestClient::new(cfg.clone()).unwrap();
    let open_oid = format!("pdgy-test-open-{}", prodigy_executor::bitget::now_ms());

    let open = PlaceOrderRequest {
        symbol: cfg.bitget_symbol.clone(),
        product_type: cfg.product_type.clone(),
        margin_mode: cfg.margin_mode.clone(),
        margin_coin: cfg.margin_coin.clone(),
        size: "0.01".to_string(),
        price: None,
        side: "buy".to_string(),
        order_type: "market".to_string(),
        force: None,
        client_oid: open_oid,
        reduce_only: None,
    };
    let opened = rest.post_json("/api/v2/mix/order/place-order", &open).await.unwrap();
    assert_eq!(opened.get("code").and_then(|v| v.as_str()), Some("00000"));

    let close_oid = format!("pdgy-test-close-{}", prodigy_executor::bitget::now_ms());
    let close = PlaceOrderRequest {
        symbol: cfg.bitget_symbol.clone(),
        product_type: cfg.product_type.clone(),
        margin_mode: cfg.margin_mode.clone(),
        margin_coin: cfg.margin_coin.clone(),
        size: "0.01".to_string(),
        price: None,
        side: "sell".to_string(),
        order_type: "market".to_string(),
        force: None,
        client_oid: close_oid,
        reduce_only: Some("YES".to_string()),
    };
    let closed = rest.post_json("/api/v2/mix/order/place-order", &close).await.unwrap();
    assert_eq!(closed.get("code").and_then(|v| v.as_str()), Some("00000"));
}
```

- [ ] **Step 2: Run demo tests to verify failure**

Run:

```bash
cargo test -p prodigy-executor --test bitget_demo -- --nocapture
```

Expected: FAIL until credentials, endpoint details, and order params are correct.

- [ ] **Step 3: Fix endpoint details against official docs and grok-search**

Use the official Bitget docs linked in this plan. If the docs are ambiguous or the endpoint returns a schema error, use grok-search to cross-check current Bitget docs and examples. Adjust only the fields required by observed demo API errors:

```rust
// accepted field changes go in PlaceOrderRequest/CancelOrderRequest only:
// - productType casing
// - reduceOnly casing
// - force value
// - tradeSide if Bitget requires it for the current position mode
// - cancel-all endpoint spelling if Bitget docs changed
```

Do not add a Bitget SDK. Keep the local REST client.

- [ ] **Step 4: Run demo tests until they pass**

Run:

```bash
cargo test -p prodigy-executor --test bitget_demo -- --nocapture
```

Expected: PASS with real Bitget demo WS connect, demo limit order placement, demo cancellation, market open, and reduce-only market close.

- [ ] **Step 5: Commit**

```bash
git add crates/executor/src crates/executor/tests/bitget_demo.rs
git commit -m "test: add bitget demo execution smoke"
```

## Task 11: Executor One-Shot SQLite Intent Flow

**Files:**
- Modify: `crates/executor/src/executor.rs`
- Modify: `crates/executor/src/main.rs`
- Modify: `tests/test_executor_integration.py`

- [ ] **Step 1: Replace Python integration test expectation**

Modify `tests/test_executor_integration.py`:

```python
import subprocess

from prodigy.db import connect, init_db
from prodigy.signals.intents import TradeIntent, write_trade_intent


def test_rust_demo_executor_processes_pending_intent(tmp_path):
    db_path = tmp_path / "prodigy.sqlite"

    with connect(db_path) as conn:
        init_db(conn)
        write_trade_intent(
            conn,
            TradeIntent(
                intent_id="intent-1",
                created_at="2026-07-01T00:00:00Z",
                symbol="ETH/USDT:USDT",
                side="long",
                action="open",
                target_notional=20.0,
                max_order_notional=20.0,
                source="test",
                reason="integration",
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
            "--once",
            "--test-reset-demo-state",
        ],
        check=True,
        text=True,
        capture_output=True,
    )

    with connect(db_path) as conn:
        intent = conn.execute(
            "select status, error from trade_intents where intent_id = 'intent-1'"
        ).fetchone()
        order_count = conn.execute("select count(*) from orders").fetchone()[0]
        event_count = conn.execute("select count(*) from events").fetchone()[0]

    assert "processed intent-1" in result.stdout
    assert intent["status"] in {"accepted", "executed"}
    assert intent["error"] is None
    assert order_count >= 1
    assert event_count >= 1
```

- [ ] **Step 2: Run integration test to verify failure**

Run:

```bash
mamba run -n quantmamba python -m pytest tests/test_executor_integration.py -v
```

Expected: FAIL because executor does not process real demo intents yet.

- [ ] **Step 3: Implement one-shot executor flow**

Replace `run_once_or_loop` in `crates/executor/src/executor.rs` with:

```rust
use anyhow::Result;
use rusqlite::Connection;
use std::time::Duration;

use crate::bitget::{verify_private_ws_connects, verify_public_ws_connects, BitgetRestClient};
use crate::config::ExecutorConfig;
use crate::db;
use crate::reconcile::reconcile_once;
use crate::risk::AccountRiskSnapshot;
use crate::types::MarketUpdate;

pub async fn run_once_or_loop(cfg: ExecutorConfig) -> Result<()> {
    cfg.validate_demo_only()?;
    let conn = Connection::open(&cfg.db_path)?;
    conn.busy_timeout(Duration::from_secs(5))?;
    let rest = BitgetRestClient::new(cfg.clone())?;

    if cfg.test_reset_demo_state {
        reset_demo_symbol_state(&cfg, &rest).await?;
    }

    verify_public_ws_connects(&cfg).await?;
    verify_private_ws_connects(&cfg).await?;
    reconcile_once(&conn, &rest, "now").await?;

    let intents = db::pending_intents(&conn)?;
    for intent in intents {
        let market = MarketUpdate {
            symbol: cfg.bitget_symbol.clone(),
            best_bid: 3000.0,
            best_ask: 3000.5,
            exchange_ts_ms: 1,
        };
        let account = AccountRiskSnapshot {
            equity: 10_000.0,
            available_margin: 5_000.0,
            unrealized_pnl_24h: 0.0,
            gross_notional: 0.0,
            market_is_fresh: true,
            private_state_is_ready: true,
        };
        process_one_intent(&conn, &cfg, &rest, intent.clone(), market, account).await?;
        db::write_event(&conn, "info", "executor", "processed intent", "{}")?;
        println!("processed {}", intent.intent_id);
    }
    Ok(())
}

async fn reset_demo_symbol_state(cfg: &ExecutorConfig, rest: &BitgetRestClient) -> Result<()> {
    let _ = rest.cancel_all_orders().await;
    close_existing_demo_position_if_any(cfg, rest).await?;
    Ok(())
}

async fn close_existing_demo_position_if_any(
    cfg: &ExecutorConfig,
    rest: &BitgetRestClient,
) -> Result<()> {
    let positions = rest
        .get(
            "/api/v2/mix/position/all-position",
            &[
                ("productType", cfg.product_type.clone()),
                ("marginCoin", cfg.margin_coin.clone()),
            ],
        )
        .await?;
    let rows = positions
        .get("data")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default();
    for row in rows {
        if row.get("symbol").and_then(serde_json::Value::as_str) != Some(&cfg.bitget_symbol) {
            continue;
        }
        let size = row
            .get("available")
            .or_else(|| row.get("total"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or("0")
            .parse::<f64>()
            .unwrap_or(0.0);
        if size <= 0.0 {
            continue;
        }
        let hold_side = row.get("holdSide").and_then(serde_json::Value::as_str).unwrap_or("");
        let side = if hold_side == "long" { "sell" } else { "buy" };
        let request = crate::bitget::PlaceOrderRequest {
            symbol: cfg.bitget_symbol.clone(),
            product_type: cfg.product_type.clone(),
            margin_mode: cfg.margin_mode.clone(),
            margin_coin: cfg.margin_coin.clone(),
            size: format_size(size),
            price: None,
            side: side.to_string(),
            order_type: "market".to_string(),
            force: None,
            client_oid: format!("pdgy-reset-{}", crate::bitget::now_ms()),
            reduce_only: Some("YES".to_string()),
        };
        rest.post_json("/api/v2/mix/order/place-order", &request).await?;
    }
    Ok(())
}
```

This step intentionally uses a temporary market/account snapshot after WS connection succeeds. Replace it with live cache in Task 12.

- [ ] **Step 4: Run Python integration test**

Run:

```bash
mamba run -n quantmamba python -m pytest tests/test_executor_integration.py -v
```

Expected: PASS and at least one Bitget demo order is created.

- [ ] **Step 5: Commit**

```bash
git add crates/executor/src/executor.rs crates/executor/src/main.rs tests/test_executor_integration.py
git commit -m "feat: execute sqlite intents on bitget demo"
```

## Task 12: Replace Temporary Snapshot With WS Market Cache And REST Account Snapshot

**Files:**
- Modify: `crates/executor/src/bitget.rs`
- Modify: `crates/executor/src/executor.rs`
- Test: `crates/executor/src/executor.rs`

- [ ] **Step 1: Write failing stale market test**

Append to `crates/executor/src/executor.rs` tests:

```rust
#[test]
fn stale_market_cache_returns_none() {
    let mut cache = MarketCache::default();
    cache.update(MarketUpdate {
        symbol: "ETHUSDT".to_string(),
        best_bid: 3000.0,
        best_ask: 3000.5,
        exchange_ts_ms: 1,
    });

    assert!(cache.latest_fresh(10, 3).is_none());
}
```

- [ ] **Step 2: Run test to verify failure**

Run:

```bash
cargo test -p prodigy-executor executor::tests::stale_market_cache_returns_none -- --nocapture
```

Expected: FAIL because `MarketCache` does not exist.

- [ ] **Step 3: Implement minimal market cache**

Add to `crates/executor/src/executor.rs`:

```rust
#[derive(Debug, Clone, Default)]
pub struct MarketCache {
    latest: Option<MarketUpdate>,
}

impl MarketCache {
    pub fn update(&mut self, update: MarketUpdate) {
        self.latest = Some(update);
    }

    pub fn latest_fresh(&self, now_ms: i64, stale_after_secs: u64) -> Option<MarketUpdate> {
        let update = self.latest.clone()?;
        let age_ms = now_ms.saturating_sub(update.exchange_ts_ms);
        if age_ms <= (stale_after_secs as i64) * 1000 {
            Some(update)
        } else {
            None
        }
    }
}
```

Add a `get_account_snapshot` wrapper in `crates/executor/src/bitget.rs`:

```rust
impl BitgetRestClient {
    pub async fn get_account_snapshot(&self) -> Result<Value> {
        self.get(
            "/api/v2/mix/account/account",
            &[
                ("symbol", self.cfg.bitget_symbol.clone()),
                ("productType", self.cfg.product_type.clone()),
                ("marginCoin", self.cfg.margin_coin.clone()),
            ],
        )
        .await
    }
}
```

- [ ] **Step 4: Wire executor to cache**

Update `run_once_or_loop` to:

```rust
let mut market_cache = MarketCache::default();
market_cache.update(fetch_initial_market_snapshot(&cfg).await?);
let market = market_cache
    .latest_fresh(crate::bitget::now_ms().parse::<i64>().unwrap_or(0), cfg.stale_market_data_secs)
    .ok_or_else(|| anyhow::anyhow!("market cache is stale"))?;
let _account_json = rest.get_account_snapshot().await?;
```

Add this helper:

```rust
async fn fetch_initial_market_snapshot(cfg: &ExecutorConfig) -> Result<MarketUpdate> {
    let rest = reqwest::Client::new();
    let url = format!(
        "{}/api/v2/mix/market/ticker?symbol={}&productType={}",
        cfg.rest_base_url, cfg.bitget_symbol, cfg.product_type
    );
    let value: serde_json::Value = rest.get(url).send().await?.json().await?;
    let data = value.get("data").ok_or_else(|| anyhow::anyhow!("missing ticker data"))?;
    let bid = data.get("bidPr").and_then(serde_json::Value::as_str).unwrap_or("0").parse()?;
    let ask = data.get("askPr").and_then(serde_json::Value::as_str).unwrap_or("0").parse()?;
    Ok(MarketUpdate {
        symbol: cfg.bitget_symbol.clone(),
        best_bid: bid,
        best_ask: ask,
        exchange_ts_ms: crate::bitget::now_ms().parse().unwrap_or(0),
    })
}
```

- [ ] **Step 5: Run executor tests and integration**

Run:

```bash
cargo test -p prodigy-executor executor -- --nocapture
mamba run -n quantmamba python -m pytest tests/test_executor_integration.py -v
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/executor/src/executor.rs crates/executor/src/bitget.rs
git commit -m "feat: use market cache for demo execution"
```

## Task 13: Final Verification

**Files:**
- No code files unless verification exposes a defect.

- [ ] **Step 1: Run Python tests**

Run:

```bash
mamba run -n quantmamba python -m pytest -q
```

Expected: PASS.

- [ ] **Step 2: Run Rust formatting**

Run:

```bash
cargo fmt --check
```

Expected: PASS.

- [ ] **Step 3: Run Rust clippy**

Run:

```bash
cargo clippy --all-targets --all-features -- -D warnings
```

Expected: PASS.

- [ ] **Step 4: Run Rust tests including Bitget demo**

Run:

```bash
cargo test -- --nocapture
```

Expected: PASS. This command may place, cancel, and close Bitget demo orders.

- [ ] **Step 5: Check git diff hygiene**

Run:

```bash
git diff --check
git status --short
```

Expected: `git diff --check` prints nothing. `git status --short` shows only intentional uncommitted files before the final commit.

- [ ] **Step 6: Final commit**

```bash
git add Cargo.toml crates/executor schema src tests
git commit -m "feat: add bitget demo execution layer"
```

## Self-Review

Spec coverage:

- Bitget demo-only guard: Tasks 1, 3, 10.
- REST order/cancel/query path: Tasks 3, 7, 8, 10.
- Public/private WS connection and parsing: Task 4.
- SQLite intent and persistence: Tasks 2, 7, 11.
- Maker timeout/retry/taker fallback foundation: Tasks 5, 7, 10.
- Risk gate: Task 6.
- Reconciliation: Task 8.
- Telegram notification filtering: Task 9.
- Runtime manual client intervention and mode-aware Telegram policy: Task 9.5.
- Demo tests that operate on Bitget demo: Tasks 10, 11, 13.

Scope control:

- No live trading support is added.
- No model-to-intent automation is added.
- No extra services are added.
- The plan uses a minimal local Bitget REST/WS client instead of a large SDK.
