# Crypto Quant Sixth Milestone Telegram Operator Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build M6: Telegram operator control + observability + a light demo smoke report, while keeping execution demo-only and exchange actions inside Rust.

**Architecture:** Telegram reads real Bot API updates, authorizes by `message.from.id`, writes SQLite commands/events only, and replies to `message.chat.id`. Rust consumes `control_commands` before opening trade intents and performs stop/resume/cancel/close actions through existing REST execution paths. A Python smoke runner starts the demo daemons for 30-120 minutes, records observations, and writes a Markdown report.

**Tech Stack:** Rust `prodigy-executor`, `rusqlite`, existing `reqwest` Telegram polling, Python stdlib + existing `prodigy.db`, SQLite WAL, Bitget demo only.

---

## File Map

- `schema/001_initial.sql` - add `cancel_all` to new databases.
- `src/prodigy/db.py` - migrate existing `control_commands` check constraint by rebuilding the table.
- `tests/test_db_schema.py` - prove migrated databases accept `cancel_all`.
- `crates/executor/src/config.rs` - store `telegram_allowed_user_ids`.
- `crates/executor/src/main.rs` - load `TELEGRAM_ALLOWED_USER_IDS`; stop requiring `TELEGRAM_CHAT_ID` for the bot loop.
- `crates/executor/src/types.rs` - add `ControlCommand`.
- `crates/executor/src/db.rs` - add control-command helpers and small order/position query helpers.
- `crates/executor/src/telegram_query.rs` - turn read-only query formatter into authorized operator command handling.
- `crates/executor/src/control.rs` - process pending control commands; keep this separate from `executor.rs` because it is a different queue.
- `crates/executor/src/executor.rs` - expose existing snapshot helpers to `control.rs`; add operator stop open gate.
- `crates/executor/src/daemon.rs` - process control commands before `trade_intents`; use user-id authorization in Telegram polling.
- `crates/executor/src/lib.rs` - export `control`.
- `src/prodigy/smoke/report.py` - build the smoke report from SQLite.
- `src/prodigy/cli/smoke.py` - run bounded demo smoke and write the report.
- `pyproject.toml` - add `prodigy-smoke` CLI.
- `tests/test_smoke_report.py` - test report generation without starting real daemons.
- `tests/test_executor_integration.py` - add small integration checks for operator stop and demo-only constraints.

No new third-party dependency is needed.

---

## Task 1: Schema Migration For `cancel_all`

**Files:**
- Modify: `schema/001_initial.sql`
- Modify: `src/prodigy/db.py`
- Modify: `tests/test_db_schema.py`

- [ ] **Step 1: Write failing Python schema tests**

Add these tests to `tests/test_db_schema.py`:

```python
import sqlite3

from prodigy.db import connect, init_db


def test_control_commands_accept_cancel_all_on_new_db(tmp_path):
    db_path = tmp_path / "prodigy.sqlite"
    with connect(db_path) as conn:
        init_db(conn)
        conn.execute(
            """
            insert into control_commands (
              command_id, created_at, command, status, requested_by
            ) values ('c1', '2026-07-06T00:00:00Z', 'cancel_all', 'pending', '123')
            """
        )
        row = conn.execute("select command from control_commands").fetchone()

    assert row["command"] == "cancel_all"


def test_init_db_migrates_old_control_commands_check_constraint(tmp_path):
    db_path = tmp_path / "prodigy.sqlite"
    raw = sqlite3.connect(db_path)
    raw.executescript(
        """
        create table control_commands (
          command_id text primary key,
          created_at text not null,
          command text not null check (command in ('stop', 'resume', 'close_all')),
          status text not null check (status in ('pending', 'accepted', 'rejected', 'executed', 'failed')),
          requested_by text not null,
          processed_at text,
          error text
        );
        insert into control_commands (
          command_id, created_at, command, status, requested_by
        ) values ('old-stop', '2026-07-06T00:00:00Z', 'stop', 'pending', '123');
        """
    )
    raw.commit()
    raw.close()

    with connect(db_path) as conn:
        init_db(conn)
        conn.execute(
            """
            insert into control_commands (
              command_id, created_at, command, status, requested_by
            ) values ('new-cancel', '2026-07-06T00:01:00Z', 'cancel_all', 'pending', '123')
            """
        )
        commands = [
            row["command"]
            for row in conn.execute("select command from control_commands order by command_id")
        ]

    assert commands == ["cancel_all", "stop"]
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
mamba run -n quantmamba python -m pytest -q tests/test_db_schema.py::test_control_commands_accept_cancel_all_on_new_db tests/test_db_schema.py::test_init_db_migrates_old_control_commands_check_constraint
```

Expected: both tests fail because the old `CHECK` constraint rejects `cancel_all`.

- [ ] **Step 3: Update the base schema**

Change `schema/001_initial.sql`:

```sql
command text not null check (command in ('stop', 'resume', 'close_all', 'cancel_all')),
```

- [ ] **Step 4: Add the table rebuild migration**

In `src/prodigy/db.py`, call a new helper from `_ensure_execution_schema(conn)`:

```python
def _ensure_execution_schema(conn: sqlite3.Connection) -> None:
    _ensure_control_commands_support_cancel_all(conn)
    _add_column_if_missing(conn, "orders", "exchange_order_id", "exchange_order_id text")
    ...
```

Add this helper below `_add_column_if_missing`:

```python
def _control_commands_sql(conn: sqlite3.Connection) -> str:
    row = conn.execute(
        "select sql from sqlite_master where type = 'table' and name = 'control_commands'"
    ).fetchone()
    return "" if row is None else str(row["sql"] or "")


def _ensure_control_commands_support_cancel_all(conn: sqlite3.Connection) -> None:
    sql = _control_commands_sql(conn)
    if "cancel_all" in sql:
        return
    conn.executescript(
        """
        alter table control_commands rename to control_commands_old;
        create table control_commands (
          command_id text primary key,
          created_at text not null,
          command text not null check (command in ('stop', 'resume', 'close_all', 'cancel_all')),
          status text not null check (status in ('pending', 'accepted', 'rejected', 'executed', 'failed')),
          requested_by text not null,
          processed_at text,
          error text
        );
        insert into control_commands (
          command_id, created_at, command, status, requested_by, processed_at, error
        )
        select command_id, created_at, command, status, requested_by, processed_at, error
        from control_commands_old;
        drop table control_commands_old;
        create index if not exists idx_control_commands_status_created
          on control_commands(status, created_at);
        """
    )
```

- [ ] **Step 5: Run schema tests**

Run:

```bash
mamba run -n quantmamba python -m pytest -q tests/test_db_schema.py
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add schema/001_initial.sql src/prodigy/db.py tests/test_db_schema.py
git commit -m "feat: migrate control commands for cancel_all"
```

---

## Task 2: Load Telegram Allowed User IDs

**Files:**
- Modify: `crates/executor/src/config.rs`
- Modify: `crates/executor/src/main.rs`

- [ ] **Step 1: Write failing Rust config tests**

In `crates/executor/src/main.rs` tests, add:

```rust
#[test]
fn parses_telegram_allowed_user_ids_without_chat_id() {
    let mut env = fake_env();
    env.insert("TELEGRAM_BOT_TOKEN".into(), "test-token".into());
    env.insert("TELEGRAM_ALLOWED_USER_IDS".into(), "123, 456".into());

    let parsed = parse_args_from_env(["prodigy-executor", "--daemon"], &env).unwrap();

    assert_eq!(parsed.cfg.telegram_bot_token.as_deref(), Some("test-token"));
    assert_eq!(parsed.cfg.telegram_allowed_user_ids, vec!["123", "456"]);
    assert!(parsed.cfg.telegram_chat_id.is_none());
}
```

In `crates/executor/src/config.rs` tests, add:

```rust
#[test]
fn allowed_user_ids_parser_trims_and_drops_empty_values() {
    assert_eq!(
        parse_allowed_user_ids(" 123, ,456,789 "),
        vec!["123".to_string(), "456".to_string(), "789".to_string()]
    );
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
cargo test -q -p prodigy-executor
```

Expected: FAIL because `telegram_allowed_user_ids` and parser do not exist.

- [ ] **Step 3: Add config field and parser**

In `crates/executor/src/config.rs`, add to `ExecutorConfig`:

```rust
pub telegram_allowed_user_ids: Vec<String>,
```

Set the test default:

```rust
telegram_allowed_user_ids: Vec::new(),
```

Add:

```rust
pub fn parse_allowed_user_ids(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(ToString::to_string)
        .collect()
}
```

- [ ] **Step 4: Load env keys in CLI**

In `crates/executor/src/main.rs`, add `"TELEGRAM_ALLOWED_USER_IDS"` to the overlay list.

Import parser:

```rust
use prodigy_executor::config::{
    load_env_file, parse_allowed_user_ids, DemoSecrets, ExecutorConfig,
};
```

After `cfg.telegram_bot_token = ...`, add:

```rust
cfg.telegram_allowed_user_ids = read_optional(&["TELEGRAM_ALLOWED_USER_IDS"], env_file)
    .map(|v| parse_allowed_user_ids(&v))
    .unwrap_or_default();
```

Keep `cfg.telegram_chat_id = read_optional(&["TELEGRAM_CHAT_ID"], env_file);` for legacy notifications only. Do not require it for Telegram polling.

- [ ] **Step 5: Run Rust tests**

Run:

```bash
cargo test -q -p prodigy-executor
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/executor/src/config.rs crates/executor/src/main.rs
git commit -m "feat: load telegram allowed user ids"
```

---

## Task 3: Telegram Operator Queries And Authorization

**Files:**
- Modify: `crates/executor/src/telegram_query.rs`

- [ ] **Step 1: Write failing tests for authorization and new query commands**

Replace the old M4 remote-control refusal test with M6 behavior, and add:

```rust
#[test]
fn unauthorized_user_gets_no_sqlite_state_and_no_control() {
    let conn = test_conn();
    let response = operator_response(&conn, "/status", "999", &["123".to_string()], 1_000)
        .unwrap()
        .unwrap();

    let command_count: i64 = conn
        .query_row("select count(*) from control_commands", [], |r| r.get(0))
        .unwrap();

    assert!(response.contains("unauthorized"));
    assert_eq!(command_count, 0);
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
cargo test -q -p prodigy-executor
```

Expected: FAIL because `operator_response` and new query commands do not exist.

- [ ] **Step 3: Add the M6 operator entry point**

In `crates/executor/src/telegram_query.rs`, keep `query_response` for internal read-only formatting if useful, and add:

```rust
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
            &format!(
                r#"{{"from_user_id":"{}","command":"{}"}}"#,
                from_user_id, command
            ),
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
```

Add short read-only helpers:

```rust
fn help_response() -> String {
    "/help /status /positions /orders /trades /pnl /risk /events /smoke_status\ncontrols: /stop /resume /cancel_all /close_all /confirm <code>".to_string()
}

fn trades_response(conn: &Connection) -> Result<String> {
    let mut stmt = conn.prepare(
        "select symbol, side, price, size, fee, created_at
         from fills order by created_at desc limit 10",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok(format!(
            "{} {} size={} price={} fee={} at={}",
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, f64>(3)?,
            row.get::<_, f64>(2)?,
            row.get::<_, f64>(4)?,
            row.get::<_, String>(5)?,
        ))
    })?;
    let lines = rows.collect::<Result<Vec<_>, _>>()?;
    Ok(if lines.is_empty() { "trades: none".to_string() } else { lines.join("\n") })
}

fn events_response(conn: &Connection) -> Result<String> {
    let mut stmt = conn.prepare(
        "select severity, component, message, created_at
         from events
         where severity in ('warning', 'error', 'critical')
         order by created_at desc limit 10",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok(format!(
            "{} {} {} at={}",
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
        ))
    })?;
    let lines = rows.collect::<Result<Vec<_>, _>>()?;
    Ok(if lines.is_empty() { "events: none".to_string() } else { lines.join("\n") })
}

fn smoke_status_response(conn: &Connection) -> Result<String> {
    let value = crate::db::get_executor_state(conn, "smoke:status")?
        .unwrap_or_else(|| "none".to_string());
    Ok(format!("smoke_status: {value}"))
}

```

Update `pnl_response` to include:

```rust
realized=n/a
total=n/a
```

- [ ] **Step 4: Run tests**

Run:

```bash
cargo test -q -p prodigy-executor
```

Expected: PASS except control commands still fail if `control_response` is not implemented. If so, add a temporary `control_response` that returns `"control not implemented"` and let Task 4 replace it.

- [ ] **Step 5: Commit**

```bash
git add crates/executor/src/telegram_query.rs
git commit -m "feat: add authorized telegram operator queries"
```

---

## Task 4: Telegram Control Queue And Close-All Confirmation

**Files:**
- Modify: `crates/executor/src/telegram_query.rs`

- [ ] **Step 1: Write failing control tests**

Add:

```rust
#[test]
fn stop_resume_and_cancel_all_queue_commands_and_events() {
    for (text, command) in [("/stop", "stop"), ("/resume", "resume"), ("/cancel_all", "cancel_all")] {
        let conn = test_conn();
        let response = operator_response(&conn, text, "123", &["123".to_string()], 1_000)
            .unwrap()
            .unwrap();
        let row = conn
            .query_row(
                "select command, status, requested_by from control_commands",
                [],
                |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?, r.get::<_, String>(2)?)),
            )
            .unwrap();
        let event_count: i64 = conn
            .query_row("select count(*) from events where component = 'telegram'", [], |r| r.get(0))
            .unwrap();

        assert!(response.contains("queued"));
        assert_eq!(row, (command.to_string(), "pending".to_string(), "123".to_string()));
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
        conn.query_row("select count(*) from control_commands", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        0
    );

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
        .query_row("select command from control_commands", [], |r| r.get::<_, String>(0))
        .unwrap();
    assert!(second.contains("queued"));
    assert_eq!(command, "close_all");
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
    let wrong_code = operator_response(&conn, "/confirm badbad", "123", &["123".to_string()], 20_000)
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
cargo test -q -p prodigy-executor
```

Expected: FAIL because control queueing is not implemented.

- [ ] **Step 3: Implement queueing and audit**

In `telegram_query.rs`, add imports:

```rust
use sha2::{Digest, Sha256};
```

Add helpers:

```rust
fn audit(conn: &Connection, message: &str, payload_json: &str) -> Result<()> {
    crate::db::write_event(conn, "info", "telegram", message, payload_json)
}

fn queue_control_command(conn: &Connection, command: &str, requested_by: &str) -> Result<String> {
    let command_id: String = conn.query_row("select lower(hex(randomblob(16)))", [], |r| r.get(0))?;
    conn.execute(
        "insert into control_commands (
           command_id, created_at, command, status, requested_by
         ) values (?, datetime('now'), ?, 'pending', ?)",
        rusqlite::params![command_id, command, requested_by],
    )?;
    audit(
        conn,
        "telegram control command queued",
        &format!(
            r#"{{"command_id":"{}","command":"{}","requested_by":"{}"}}"#,
            command_id, command, requested_by
        ),
    )?;
    Ok(command_id)
}

fn sha256_hex(value: &str) -> String {
    let digest = Sha256::digest(value.as_bytes());
    digest.iter().map(|b| format!("{b:02x}")).collect()
}
```

Add confirmation helpers:

```rust
fn start_close_all_confirmation(conn: &Connection, requested_by: &str, now_ms: i64) -> Result<String> {
    let code: String = conn.query_row("select lower(hex(randomblob(3)))", [], |r| r.get(0))?;
    let value = format!(
        r#"{{"requested_by":"{}","code_hash":"{}","expires_ms":{}}}"#,
        requested_by,
        sha256_hex(&code),
        now_ms + 60_000
    );
    crate::db::set_executor_state(conn, &format!("close_all_confirm:{requested_by}"), &value)?;
    audit(
        conn,
        "telegram close_all confirmation generated",
        &format!(r#"{{"requested_by":"{}","expires_ms":{}}}"#, requested_by, now_ms + 60_000),
    )?;
    Ok(format!("confirm close_all with /confirm {code}"))
}

fn confirm_close_all(conn: &Connection, text: &str, requested_by: &str, now_ms: i64) -> Result<String> {
    let code = text.split_whitespace().nth(1).unwrap_or("");
    let key = format!("close_all_confirm:{requested_by}");
    let Some(raw) = crate::db::get_executor_state(conn, &key)? else {
        audit(conn, "telegram close_all confirmation rejected", r#"{"reason":"missing"}"#)?;
        return Ok("confirmation rejected".to_string());
    };
    let value: serde_json::Value = serde_json::from_str(&raw)?;
    let expires_ms = value.get("expires_ms").and_then(serde_json::Value::as_i64).unwrap_or(0);
    let expected = value.get("code_hash").and_then(serde_json::Value::as_str).unwrap_or("");
    if now_ms > expires_ms {
        audit(conn, "telegram close_all confirmation expired", r#"{"reason":"expired"}"#)?;
        return Ok("confirmation expired".to_string());
    }
    if expected != sha256_hex(code) {
        audit(conn, "telegram close_all confirmation rejected", r#"{"reason":"bad_code"}"#)?;
        return Ok("confirmation rejected".to_string());
    }
    let command_id = queue_control_command(conn, "close_all", requested_by)?;
    crate::db::set_executor_state(conn, &key, "used")?;
    Ok(format!("close_all queued command_id={command_id}"))
}
```

Implement:

```rust
fn control_response(conn: &Connection, text: &str, from_user_id: &str, now_ms: i64) -> Result<Option<String>> {
    let command = text.split_whitespace().next().unwrap_or("");
    match command {
        "/stop" => Ok(Some(format!("stop queued command_id={}", queue_control_command(conn, "stop", from_user_id)?))),
        "/resume" => Ok(Some(format!("resume queued command_id={}", queue_control_command(conn, "resume", from_user_id)?))),
        "/cancel_all" => Ok(Some(format!("cancel_all queued command_id={}", queue_control_command(conn, "cancel_all", from_user_id)?))),
        "/close_all" => Ok(Some(start_close_all_confirmation(conn, from_user_id, now_ms)?)),
        "/confirm" => Ok(Some(confirm_close_all(conn, text, from_user_id, now_ms)?)),
        _ => Ok(None),
    }
}
```

- [ ] **Step 4: Run control tests**

Run:

```bash
cargo test -q -p prodigy-executor
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/executor/src/telegram_query.rs
git commit -m "feat: queue telegram control commands"
```

---

## Task 5: Rust Control Command DB Helpers

**Files:**
- Modify: `crates/executor/src/types.rs`
- Modify: `crates/executor/src/db.rs`

- [ ] **Step 1: Write failing tests**

In `crates/executor/src/db.rs` tests, add:

```rust
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

    let pending = pending_control_commands(&conn).unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].command, "stop");
    assert!(accept_control_command(&conn, "cmd-1").unwrap());
    assert!(!accept_control_command(&conn, "cmd-1").unwrap());
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
        ('BTCUSDT', 'long', 100, 2000, 1, 'now', 'imported', 'now', 'now', null, '{}')",
        [],
    )
    .unwrap();

    let positions = system_positions(&conn).unwrap();

    assert_eq!(positions.len(), 1);
    assert_eq!(positions[0].symbol, "ETHUSDT");
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
cargo test -q -p prodigy-executor
```

Expected: FAIL because helpers and type do not exist.

- [ ] **Step 3: Add `ControlCommand`**

In `crates/executor/src/types.rs`:

```rust
#[derive(Debug, Clone, PartialEq)]
pub struct ControlCommand {
    pub command_id: String,
    pub command: String,
    pub requested_by: String,
}
```

- [ ] **Step 4: Add DB helpers**

In `crates/executor/src/db.rs`, import `ControlCommand`:

```rust
use crate::types::{ControlCommand, FillRecord, OrderRecord, PositionRecord, TradeIntent};
```

Add:

```rust
pub fn pending_control_commands(conn: &Connection) -> Result<Vec<ControlCommand>> {
    let mut stmt = conn.prepare(
        "select command_id, command, requested_by
         from control_commands
         where status = 'pending'
         order by created_at asc",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok(ControlCommand {
            command_id: row.get(0)?,
            command: row.get(1)?,
            requested_by: row.get(2)?,
        })
    })?;
    let mut out = Vec::new();
    for row in rows {
        out.push(row?);
    }
    Ok(out)
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

pub fn mark_system_order_cancelled_by_command(conn: &Connection, client_oid: &str) -> Result<()> {
    conn.execute(
        "update orders set status = 'cancelled', updated_at = datetime('now')
         where client_oid = ? and intent_id is not null",
        params![client_oid],
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
    let mut out = Vec::new();
    for row in rows {
        out.push(row?);
    }
    Ok(out)
}
```

- [ ] **Step 5: Run tests**

Run:

```bash
cargo test -q -p prodigy-executor
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/executor/src/types.rs crates/executor/src/db.rs
git commit -m "feat: add control command db helpers"
```

---

## Task 6: Rust Control Command Processor

**Files:**
- Create: `crates/executor/src/control.rs`
- Modify: `crates/executor/src/lib.rs`
- Modify: `crates/executor/src/executor.rs`
- Modify: `crates/executor/src/db.rs`

- [ ] **Step 1: Write failing control processor tests**

Create `crates/executor/src/control.rs` with tests first:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn conn() -> rusqlite::Connection {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute_batch(include_str!("../../../schema/001_initial.sql")).unwrap();
        conn.execute_batch(include_str!("../../../schema/002_execution.sql")).unwrap();
        conn
    }

    #[test]
    fn stop_and_resume_update_operator_stop_state() {
        let conn = conn();
        apply_stop(&conn, "cmd-stop").unwrap();
        assert_eq!(
            crate::db::get_executor_state(&conn, "operator_stop:global").unwrap().as_deref(),
            Some("active")
        );

        apply_resume(&conn, "cmd-resume").unwrap();
        assert_eq!(
            crate::db::get_executor_state(&conn, "operator_stop:global").unwrap().as_deref(),
            Some("cleared")
        );
    }

    #[test]
    fn close_all_intents_are_created_only_for_system_positions() {
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

        enqueue_close_all_intents(&conn, "cmd-1", "123").unwrap();

        let rows: Vec<(String, String, String)> = conn
            .prepare("select symbol, side, action from trade_intents order by symbol")
            .unwrap()
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(rows, vec![("ETHUSDT".to_string(), "long".to_string(), "close".to_string())]);
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
cargo test -q -p prodigy-executor
```

Expected: FAIL because `control` module and helpers do not exist.

- [ ] **Step 3: Export the module**

In `crates/executor/src/lib.rs`:

```rust
pub mod control;
```

- [ ] **Step 4: Add pure control helpers**

In `crates/executor/src/control.rs`:

```rust
use anyhow::Result;
use rusqlite::params;

pub const OPERATOR_STOP_KEY: &str = "operator_stop:global";

pub fn apply_stop(conn: &rusqlite::Connection, command_id: &str) -> Result<()> {
    crate::db::set_executor_state(conn, OPERATOR_STOP_KEY, "active")?;
    crate::db::write_event(
        conn,
        "info",
        "control",
        "operator stop activated",
        &format!(r#"{{"command_id":"{}"}}"#, command_id),
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
        &format!(r#"{{"command_id":"{}"}}"#, command_id),
    )?;
    Ok(())
}

pub fn enqueue_close_all_intents(
    conn: &rusqlite::Connection,
    command_id: &str,
    requested_by: &str,
) -> Result<usize> {
    let positions = crate::db::system_positions(conn)?;
    for position in &positions {
        let intent_id = format!("control-close:{command_id}:{}", position.symbol);
        conn.execute(
            "insert or ignore into trade_intents (
              intent_id, created_at, symbol, side, action, target_notional,
              max_order_notional, status, source, reason
            ) values (?, datetime('now'), ?, ?, 'close', 0, 0, 'pending', 'telegram-control', ?)",
            params![
                intent_id,
                position.symbol,
                position.side,
                format!("close_all requested_by={requested_by}")
            ],
        )?;
    }
    Ok(positions.len())
}
```

- [ ] **Step 5: Add control command processor shell**

Still in `control.rs`, add:

```rust
pub async fn process_pending_control_commands_once(
    conn: &rusqlite::Connection,
    cfg: &crate::config::ExecutorConfig,
    rest: &crate::bitget::BitgetRestClient,
    market_cache: &mut crate::executor::MarketCache,
) -> Result<()> {
    let commands = crate::db::pending_control_commands(conn)?;
    for command in commands {
        if !crate::db::accept_control_command(conn, &command.command_id)? {
            continue;
        }
        let result = match command.command.as_str() {
            "stop" => apply_stop(conn, &command.command_id),
            "resume" => apply_resume(conn, &command.command_id),
            "cancel_all" => apply_cancel_all(conn, cfg, rest, &command.command_id).await,
            "close_all" => apply_close_all(conn, cfg, rest, market_cache, &command).await,
            other => Err(anyhow::anyhow!("unsupported control command: {other}")),
        };
        match result {
            Ok(()) => crate::db::mark_control_command_executed(conn, &command.command_id)?,
            Err(err) => {
                crate::db::fail_control_command(conn, &command.command_id, &err.to_string())?;
                crate::db::write_event(
                    conn,
                    "error",
                    "control",
                    &format!("control command failed: {err}"),
                    &format!(r#"{{"command_id":"{}","command":"{}"}}"#, command.command_id, command.command),
                )?;
            }
        }
    }
    Ok(())
}
```

- [ ] **Step 6: Add `cancel_all` implementation using SQLite working orders**

Add:

```rust
async fn apply_cancel_all(
    conn: &rusqlite::Connection,
    cfg: &crate::config::ExecutorConfig,
    rest: &crate::bitget::BitgetRestClient,
    command_id: &str,
) -> Result<()> {
    let orders = crate::db::local_working_system_orders(conn, &cfg.bitget_symbol)?;
    for (client_oid, _, _) in orders {
        let req = crate::bitget::CancelOrderRequest {
            symbol: cfg.bitget_symbol.clone(),
            product_type: cfg.product_type.clone(),
            margin_coin: cfg.margin_coin.clone(),
            client_oid: client_oid.clone(),
        };
        match rest.cancel_order(&req).await {
            Ok(_) => crate::db::mark_system_order_cancelled_by_command(conn, &client_oid)?,
            Err(err) => crate::db::write_event(
                conn,
                "warning",
                "control",
                &format!("cancel_all cancel failed: {err}"),
                &format!(r#"{{"command_id":"{}","client_oid":"{}"}}"#, command_id, client_oid),
            )?,
        }
    }
    crate::db::write_event(
        conn,
        "info",
        "control",
        "cancel_all processed",
        &format!(r#"{{"command_id":"{}"}}"#, command_id),
    )?;
    Ok(())
}
```

This deliberately does not call `rest.cancel_all_orders()`.

- [ ] **Step 7: Add `close_all` implementation by reusing close intents**

Expose these existing functions in `crates/executor/src/executor.rs` by changing them to `pub(crate)`:

```rust
pub(crate) async fn fetch_account_snapshot(...)
pub(crate) async fn fetch_market_snapshot(...)
```

Then add to `control.rs`:

```rust
async fn apply_close_all(
    conn: &rusqlite::Connection,
    cfg: &crate::config::ExecutorConfig,
    rest: &crate::bitget::BitgetRestClient,
    market_cache: &mut crate::executor::MarketCache,
    command: &crate::types::ControlCommand,
) -> Result<()> {
    let count = enqueue_close_all_intents(conn, &command.command_id, &command.requested_by)?;
    crate::db::write_event(
        conn,
        "info",
        "control",
        "close_all close intents queued",
        &format!(r#"{{"command_id":"{}","system_positions":{}}}"#, command.command_id, count),
    )?;
    let market = crate::executor::fetch_market_snapshot(cfg, rest).await?;
    market_cache.update(market.clone());
    let account = crate::executor::fetch_account_snapshot(rest, true).await?;
    let intents = crate::db::pending_intents(conn)?;
    for intent in intents.into_iter().filter(|i| i.intent_id.starts_with(&format!("control-close:{}:", command.command_id))) {
        crate::executor::process_one_intent(conn, cfg, rest, market.clone(), account, market_cache, intent).await?;
    }
    crate::reconcile::reconcile_once(
        conn,
        rest,
        "control-close-all",
        true,
        cfg.telegram_bot_token.as_deref(),
        cfg.telegram_chat_id.as_deref(),
    )
    .await?;
    Ok(())
}
```

If the exact `process_one_intent` signature differs, adapt only the call site; do not write a second close order state machine.

- [ ] **Step 8: Run Rust tests**

Run:

```bash
cargo test -q -p prodigy-executor
```

Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add crates/executor/src/control.rs crates/executor/src/lib.rs crates/executor/src/executor.rs crates/executor/src/db.rs
git commit -m "feat: process executor control commands"
```

---

## Task 7: Operator Stop Gate And Daemon Ordering

**Files:**
- Modify: `crates/executor/src/executor.rs`
- Modify: `crates/executor/src/daemon.rs`

- [ ] **Step 1: Write failing unit test for stop gate**

In `crates/executor/src/executor.rs` tests, add a pure helper test. If no helper exists, add the helper in Step 3.

```rust
#[test]
fn operator_stop_blocks_opening_actions_only() {
    assert!(operator_stop_blocks_action(Some("active"), "open"));
    assert!(operator_stop_blocks_action(Some("active"), "reverse"));
    assert!(!operator_stop_blocks_action(Some("active"), "close"));
    assert!(!operator_stop_blocks_action(Some("cleared"), "open"));
    assert!(!operator_stop_blocks_action(None, "open"));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
cargo test -q -p prodigy-executor operator_stop_blocks_opening_actions_only
```

Expected: FAIL because the helper does not exist.

- [ ] **Step 3: Add operator stop helper and gate**

In `crates/executor/src/executor.rs`:

```rust
pub fn operator_stop_blocks_action(state: Option<&str>, action: &str) -> bool {
    state == Some("active") && is_opening_action(action)
}
```

In `process_one_intent`, before placing orders:

```rust
if operator_stop_blocks_action(
    db::get_executor_state(conn, crate::control::OPERATOR_STOP_KEY)?.as_deref(),
    &intent.action,
) {
    db::fail_intent(conn, &intent.intent_id, "operator stop active")?;
    return Ok(());
}
```

Keep this gate open for `close` actions.

- [ ] **Step 4: Process control commands before intents in daemon**

In `crates/executor/src/daemon.rs`, inside the tick after reconcile and before `process_pending_intents_once`, call:

```rust
if let Err(err) = crate::control::process_pending_control_commands_once(
    &conn,
    &cfg,
    &rest,
    &mut local_cache,
)
.await
{
    crate::db::write_event(
        &conn,
        "error",
        "control_loop",
        &format!("control loop failed: {err}"),
        "{}",
    )?;
}
```

Then call `process_pending_intents_once` as it already does. The order matters.

- [ ] **Step 5: Run targeted tests**

Run:

```bash
cargo test -q -p prodigy-executor operator_stop_blocks_opening_actions_only
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/executor/src/executor.rs crates/executor/src/daemon.rs
git commit -m "feat: prioritize operator control commands"
```

---

## Task 8: Telegram Polling Uses `from.id`, Not `chat_id`

**Files:**
- Modify: `crates/executor/src/daemon.rs`

- [ ] **Step 1: Write failing parser tests**

In `crates/executor/src/daemon.rs` tests, add:

```rust
#[test]
fn telegram_update_parts_use_from_id_for_auth_and_chat_id_for_reply() {
    let update = serde_json::json!({
        "update_id": 10,
        "message": {
            "from": {"id": 123},
            "chat": {"id": 999},
            "text": "/status"
        }
    });

    let parts = telegram_update_parts(&update).unwrap();

    assert_eq!(parts.update_id, 10);
    assert_eq!(parts.from_user_id, "123");
    assert_eq!(parts.reply_chat_id, "999");
    assert_eq!(parts.text, "/status");
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
cargo test -q -p prodigy-executor telegram_update_parts_use_from_id_for_auth_and_chat_id_for_reply
```

Expected: FAIL because parser does not exist.

- [ ] **Step 3: Add parser**

In `daemon.rs`:

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TelegramUpdateParts {
    pub update_id: i64,
    pub from_user_id: String,
    pub reply_chat_id: String,
    pub text: String,
}

pub fn telegram_update_parts(update: &serde_json::Value) -> Option<TelegramUpdateParts> {
    let message = update.get("message")?;
    Some(TelegramUpdateParts {
        update_id: update.get("update_id")?.as_i64()?,
        from_user_id: message.get("from")?.get("id")?.as_i64()?.to_string(),
        reply_chat_id: message.get("chat")?.get("id")?.as_i64()?.to_string(),
        text: message.get("text")?.as_str()?.to_string(),
    })
}
```

- [ ] **Step 4: Update `run_telegram_query_loop`**

Change startup guard from token + chat id to token + allowed users:

```rust
let Some(token) = cfg.telegram_bot_token.clone() else {
    return Ok(());
};
if cfg.telegram_allowed_user_ids.is_empty() {
    return Ok(());
}
```

Inside update loop, replace chat-id filtering with:

```rust
let Some(parts) = telegram_update_parts(update) else {
    continue;
};
offset = parts.update_id + 1;
...
let reply = crate::telegram_query::operator_response(
    &conn,
    &parts.text,
    &parts.from_user_id,
    &cfg.telegram_allowed_user_ids,
    crate::bitget::now_ms().parse::<i64>().unwrap_or(0),
)?;
...
.form(&[
    ("chat_id", parts.reply_chat_id.as_str()),
    ("text", reply.as_str()),
])
```

Do not compare `parts.reply_chat_id` to a configured `TELEGRAM_CHAT_ID`.

- [ ] **Step 5: Run tests**

Run:

```bash
cargo test -q -p prodigy-executor telegram_update_parts_use_from_id_for_auth_and_chat_id_for_reply
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/executor/src/daemon.rs
git commit -m "feat: authorize telegram by user id"
```

---

## Task 9: Light Demo Smoke Report CLI

**Files:**
- Create: `src/prodigy/smoke/__init__.py`
- Create: `src/prodigy/smoke/report.py`
- Create: `src/prodigy/cli/smoke.py`
- Modify: `pyproject.toml`
- Create: `tests/test_smoke_report.py`

- [ ] **Step 1: Write failing smoke report tests**

Create `tests/test_smoke_report.py`:

```python
from pathlib import Path

from prodigy.db import connect, init_db
from prodigy.smoke.report import build_smoke_report, write_smoke_report


def test_build_smoke_report_summarizes_sqlite_state(tmp_path):
    db_path = tmp_path / "prodigy.sqlite"
    with connect(db_path) as conn:
        init_db(conn)
        conn.execute(
            "insert into events (event_id, created_at, severity, component, message, payload_json) "
            "values ('e1', '2026-07-06T00:00:00Z', 'error', 'test', 'boom', '{}')"
        )
        conn.execute(
            "insert into control_commands (command_id, created_at, command, status, requested_by) "
            "values ('c1', '2026-07-06T00:00:00Z', 'stop', 'executed', '123')"
        )
        conn.commit()

    text = build_smoke_report(
        db_path=db_path,
        started_at="2026-07-06T00:00:00Z",
        ended_at="2026-07-06T01:00:00Z",
        duration_minutes=60,
        issues=["telegram reply delayed"],
    )

    assert "# M6 Demo Smoke Report" in text
    assert "duration_minutes: 60" in text
    assert "control_commands_total: 1" in text
    assert "telegram reply delayed" in text
    assert "boom" in text


def test_write_smoke_report_creates_markdown_and_state_marker(tmp_path):
    db_path = tmp_path / "prodigy.sqlite"
    report_dir = tmp_path / "reports"
    with connect(db_path) as conn:
        init_db(conn)

    path = write_smoke_report(
        db_path=db_path,
        report_dir=report_dir,
        started_at="2026-07-06T00:00:00Z",
        ended_at="2026-07-06T01:00:00Z",
        duration_minutes=60,
        issues=[],
    )

    assert Path(path).exists()
    assert Path(path).read_text().startswith("# M6 Demo Smoke Report")
    with connect(db_path) as conn:
        row = conn.execute(
            "select value from executor_state where key = 'smoke:last_report'"
        ).fetchone()
    assert row["value"] == str(path)
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
mamba run -n quantmamba python -m pytest -q tests/test_smoke_report.py
```

Expected: FAIL because the smoke package does not exist.

- [ ] **Step 3: Add report builder**

Create `src/prodigy/smoke/__init__.py`:

```python
"""M6 smoke reporting helpers."""
```

Create `src/prodigy/smoke/report.py`:

```python
from __future__ import annotations

from pathlib import Path

from prodigy.db import connect
from prodigy.signals.state import set_executor_state


def _count(conn, table: str, where: str = "1=1") -> int:
    return int(conn.execute(f"select count(*) from {table} where {where}").fetchone()[0])


def _recent_events(conn) -> list[str]:
    rows = conn.execute(
        """
        select severity, component, message, created_at
        from events
        where severity in ('warning', 'error', 'critical')
        order by created_at desc
        limit 20
        """
    ).fetchall()
    return [
        f"- {row['created_at']} {row['severity']} {row['component']}: {row['message']}"
        for row in rows
    ]


def build_smoke_report(
    *,
    db_path: str | Path,
    started_at: str,
    ended_at: str,
    duration_minutes: int,
    issues: list[str],
) -> str:
    with connect(db_path) as conn:
        lines = [
            "# M6 Demo Smoke Report",
            "",
            f"started_at: {started_at}",
            f"ended_at: {ended_at}",
            f"duration_minutes: {duration_minutes}",
            "",
            "## SQLite Summary",
            "",
            f"- trade_intents_total: {_count(conn, 'trade_intents')}",
            f"- control_commands_total: {_count(conn, 'control_commands')}",
            f"- open_orders: {_count(conn, 'orders', \"status in ('submitted', 'live')\")}",
            f"- fills_total: {_count(conn, 'fills')}",
            f"- positions_total: {_count(conn, 'positions')}",
            f"- warning_error_critical_events: {_count(conn, 'events', \"severity in ('warning', 'error', 'critical')\")}",
            "",
            "## Issues Recorded During Run",
            "",
        ]
        lines.extend(f"- {issue}" for issue in issues)
        if not issues:
            lines.append("- none recorded")
        lines.extend(["", "## Recent Important Events", ""])
        events = _recent_events(conn)
        lines.extend(events if events else ["- none"])
    return "\n".join(lines) + "\n"


def write_smoke_report(
    *,
    db_path: str | Path,
    report_dir: str | Path,
    started_at: str,
    ended_at: str,
    duration_minutes: int,
    issues: list[str],
) -> Path:
    report_dir = Path(report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    safe_stamp = ended_at.replace(":", "").replace("-", "").replace("T", "-").replace("Z", "")
    path = report_dir / f"m6-demo-smoke-{safe_stamp}.md"
    path.write_text(
        build_smoke_report(
            db_path=db_path,
            started_at=started_at,
            ended_at=ended_at,
            duration_minutes=duration_minutes,
            issues=issues,
        )
    )
    with connect(db_path) as conn:
        set_executor_state(conn, "smoke:last_report", str(path), ended_at)
        set_executor_state(conn, "smoke:status", "completed", ended_at)
        conn.commit()
    return path
```

- [ ] **Step 4: Add CLI**

Create `src/prodigy/cli/smoke.py`:

```python
from __future__ import annotations

import argparse
import subprocess
import time
from datetime import UTC, datetime
from pathlib import Path

from prodigy.db import connect, init_db
from prodigy.signals.state import set_executor_state
from prodigy.smoke.report import write_smoke_report


def _now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _start(cmd: list[str]) -> subprocess.Popen:
    return subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default="var/prodigy.sqlite")
    parser.add_argument("--duration-minutes", type=int, default=60)
    parser.add_argument("--report-dir", default="reports")
    parser.add_argument("--skip-start", action="store_true")
    args = parser.parse_args()

    if not 30 <= args.duration_minutes <= 120:
        raise SystemExit("--duration-minutes must be between 30 and 120")

    db_path = Path(args.db)
    with connect(db_path) as conn:
        init_db(conn)
        started_at = _now()
        set_executor_state(conn, "smoke:status", "running", started_at)
        conn.commit()

    processes: list[subprocess.Popen] = []
    issues: list[str] = []
    try:
        if not args.skip_start:
            processes.append(_start(["cargo", "run", "-q", "-p", "prodigy-executor", "--", "--daemon", "--db", str(db_path)]))
            processes.append(_start(["prodigy-signal", "--daemon", "--db", str(db_path)]))
        deadline = time.monotonic() + args.duration_minutes * 60
        while time.monotonic() < deadline:
            for proc in processes:
                if proc.poll() is not None:
                    issues.append(f"process exited early: returncode={proc.returncode}")
            time.sleep(30)
    finally:
        for proc in processes:
            if proc.poll() is None:
                proc.terminate()
        for proc in processes:
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
                issues.append("process killed after terminate timeout")

    ended_at = _now()
    path = write_smoke_report(
        db_path=db_path,
        report_dir=args.report_dir,
        started_at=started_at,
        ended_at=ended_at,
        duration_minutes=args.duration_minutes,
        issues=issues,
    )
    print(path)


if __name__ == "__main__":
    main()
```

`--skip-start` is for tests and manual report generation against already-running daemons.

- [ ] **Step 5: Add CLI entry point**

In `pyproject.toml`:

```toml
prodigy-smoke = "prodigy.cli.smoke:main"
```

- [ ] **Step 6: Run smoke report tests**

Run:

```bash
mamba run -n quantmamba python -m pytest -q tests/test_smoke_report.py
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src/prodigy/smoke src/prodigy/cli/smoke.py pyproject.toml tests/test_smoke_report.py
git commit -m "feat: add M6 demo smoke report"
```

---

## Task 10: Integration Checks And Scope Scan

**Files:**
- Modify: `tests/test_executor_integration.py`

- [ ] **Step 1: Add non-live scope tests**

Append:

```python
import subprocess


def test_m6_scope_scan_has_no_remote_open_or_live_enablement():
    result = subprocess.run(
        [
            "rg",
            "-n",
            "remote_open|open_from_telegram|TELEGRAM_LIVE|BITGET_LIVE|ws.bitget.com",
            "src",
            "crates",
            "tests",
        ],
        check=False,
        text=True,
        capture_output=True,
    )

    assert result.stdout == ""
```

- [ ] **Step 2: Run Python integration scope test**

Run:

```bash
mamba run -n quantmamba python -m pytest -q tests/test_executor_integration.py::test_m6_scope_scan_has_no_remote_open_or_live_enablement
```

Expected: PASS. If it fails on a legitimate existing rejection-path string, narrow the regex to the dangerous path only; do not delete live rejection tests.

- [ ] **Step 3: Run full verification**

Run:

```bash
mamba run -n quantmamba python -m pytest -q
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test -q
git diff --check
```

Expected:

- Python tests pass.
- Rust formatting is clean.
- Rust clippy is clean.
- Rust tests pass.
- No whitespace errors.

- [ ] **Step 4: Commit**

```bash
git add tests/test_executor_integration.py
git commit -m "test: verify M6 scope boundaries"
```

---

## Task 11: 30-120 Minute Demo Smoke Run

**Files:**
- Generated: `reports/m6-demo-smoke-*.md`

- [ ] **Step 1: Confirm demo-only environment**

Run:

```bash
rg -n "BITGET_LIVE|TRADING_MODE=live|--mode live" .env.local configs src crates || true
```

Expected: no live enablement. Environment may contain demo Bitget keys, `TELEGRAM_BOT_TOKEN`, and `TELEGRAM_ALLOWED_USER_IDS`.

- [ ] **Step 2: Start a 30-minute smoke run**

Run:

```bash
mamba run -n quantmamba prodigy-smoke --db var/prodigy.sqlite --duration-minutes 30
```

During the run, from an allowed Telegram user, send:

```text
/help
/status
/pnl
/risk
/stop
/resume
/cancel_all
/close_all
/confirm <code returned by the bot>
```

Do not fix issues during the run. Record observations in the final report or in a separate note, then fix after the run ends.

- [ ] **Step 3: Inspect smoke report**

Run:

```bash
ls -t reports/m6-demo-smoke-*.md | head -1
sed -n '1,220p' "$(ls -t reports/m6-demo-smoke-*.md | head -1)"
```

Expected: the report includes run duration, command results, important events, positions, orders, fills, and issues.

- [ ] **Step 4: Commit report only if useful**

If the report is useful as an artifact and contains no secrets:

```bash
git add reports/m6-demo-smoke-*.md
git commit -m "test: add M6 demo smoke report"
```

If it contains account-sensitive values, do not commit it. Instead commit only fixes found from the report.

---

## Final Verification

Run after all implementation tasks:

```bash
mamba run -n quantmamba python -m pytest -q
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test -q
git diff --check
git status --short --branch
```

Expected:

- all Python tests pass;
- Rust format, clippy, and tests pass;
- worktree contains only intentional files;
- no live trading path is enabled;
- Telegram controls write SQLite commands/events only;
- Rust executes commands in demo mode only.

## Spec Coverage Checklist

- Telegram authorization by `message.from.id`: Tasks 2, 3, 8.
- No `TELEGRAM_CHAT_ID` permission gate: Tasks 2, 8.
- Real Telegram bot testing in demo: Tasks 8, 11.
- `cancel_all` schema migration: Task 1.
- Rust `process_pending_control_commands_once()`: Task 6.
- Control commands before opening intents: Task 7.
- `operator_stop:global=active`: Tasks 6, 7.
- `/stop` only blocks new openings: Task 7.
- `/cancel_all` cancels SQLite system working orders only: Task 6.
- `/close_all` closes system positions only: Task 6.
- Conservative `/pnl`: Task 3.
- Audit events for controls: Tasks 4, 6.
- No remote open/parameter/model/shell: Task 10.
- Light 30-120 minute smoke report: Tasks 9, 11.
