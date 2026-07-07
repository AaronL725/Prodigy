# Crypto Quant Eighth Milestone Live Integration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a complete Bitget live API profile and final live startup safety gates while preserving the existing demo trading system.

**Architecture:** Keep one Rust executor and one SQLite-backed Telegram control path. Add a small mode/profile layer for demo vs live credentials, headers, and WebSocket URLs; add SQLite mode/instance isolation for operator commands; add an active executor lock and live clean-state gates before any private exchange call. Live dry validation proves the live path without requiring demo keys, live keys, funds, active locks, private REST, or orders.

**Tech Stack:** Rust executor (`anyhow`, `rusqlite`, `reqwest`, `tokio`), SQLite schema files, Python DB initializer/tests, existing pytest/cargo verification.

---

## File Structure

- `crates/executor/src/config.rs` - replace demo-only validation with mode-aware profile validation and redacted `BitgetSecrets`.
- `crates/executor/src/main.rs` - parse `--mode demo|live`, parse `--dry-validate`, load demo or live env vars only when needed.
- `crates/executor/src/bitget.rs` - make signed headers mode-aware; demo adds `PAPTRADING`, live does not.
- `schema/001_initial.sql` - new DBs get `control_commands.mode` and `control_commands.instance_id`.
- `src/prodigy/db.py` - Python initializer migrates old DBs by adding the two columns.
- `crates/executor/src/types.rs` - `ControlCommand` carries `mode` and `instance_id`.
- `crates/executor/src/db.rs` - query only mode/instance-matching controls; add active lock helpers; add live clean-state helpers.
- `crates/executor/src/control.rs` - pass mode/instance into control processing and audit payloads.
- `crates/executor/src/telegram_query.rs` - queue controls for active mode/instance only; bind close-all confirmations to mode/instance; `/status` reports active mode.
- `crates/executor/src/daemon.rs` - acquire heartbeat lock before daemon loops, release on clean shutdown, takeover stale lock with audit event.
- `crates/executor/src/executor.rs` - run live startup gates before any private REST, set-leverage, private WS login, account, position, or order call.
- `tests/test_db_schema.py` and Rust unit tests - prove migrations and safety gates.
- `tests/test_executor_integration.py` - update scope scan and live dry validation integration checks.

Do not add a general exchange adapter, a second Telegram service, remote open, live-only risk logic, or another strategy path.

---

## Task 1: Mode-Aware Config And CLI Parsing

**Files:**
- Modify: `crates/executor/src/config.rs`
- Modify: `crates/executor/src/main.rs`
- Test: `crates/executor/src/config.rs`
- Test: `crates/executor/src/main.rs`

- [ ] **Step 1: Write failing config tests**

Add these tests to `crates/executor/src/config.rs`:

```rust
#[test]
fn demo_runtime_validation_requires_demo_ws_and_demo_creds() {
    let cfg = ExecutorConfig::demo_for_tests();

    cfg.validate_for_runtime().unwrap();

    let live_ws = ExecutorConfig {
        public_ws_url: "wss://ws.bitget.com/v2/ws/public".to_string(),
        ..ExecutorConfig::demo_for_tests()
    };
    assert!(live_ws.validate_for_runtime().unwrap_err().to_string().contains("demo websocket"));
}

#[test]
fn live_runtime_validation_requires_enable_confirm_and_live_creds() {
    let cfg = ExecutorConfig::live_for_tests();
    let err = cfg.validate_for_runtime().unwrap_err().to_string();
    assert!(err.contains("live credentials") || err.contains("live trading enable"));

    let enabled = ExecutorConfig {
        live_safety: LiveSafety {
            enabled: true,
            confirm_phrase: Some("I_UNDERSTAND_THIS_CAN_TRADE_REAL_MONEY".to_string()),
        },
        secrets: BitgetSecrets {
            api_key: "live-key".to_string(),
            api_secret: "live-secret".to_string(),
            passphrase: "live-pass".to_string(),
        },
        ..ExecutorConfig::live_for_tests()
    };
    enabled.validate_for_runtime().unwrap();
}

#[test]
fn live_dry_validation_requires_no_demo_or_live_credentials() {
    let cfg = ExecutorConfig {
        secrets: BitgetSecrets {
            api_key: String::new(),
            api_secret: String::new(),
            passphrase: String::new(),
        },
        ..ExecutorConfig::live_for_tests()
    };

    cfg.validate_for_dry_validate().unwrap();
}

#[test]
fn bitget_secrets_debug_redacts_values_for_live_too() {
    let secrets = BitgetSecrets {
        api_key: "real-key".to_string(),
        api_secret: "real-secret".to_string(),
        passphrase: "real-pass".to_string(),
    };
    let formatted = format!("{:?}", secrets);
    assert!(!formatted.contains("real-key"));
    assert!(!formatted.contains("real-secret"));
    assert!(!formatted.contains("real-pass"));
    assert!(formatted.contains("<redacted>"));
}
```

Add these tests to `crates/executor/src/main.rs`:

```rust
#[test]
fn parses_live_mode_with_live_credentials_and_enable_flags() {
    let mut env = fake_env();
    env.insert("BITGET_LIVE_API_KEY".into(), "live-key".into());
    env.insert("BITGET_LIVE_API_SECRET".into(), "live-secret".into());
    env.insert("BITGET_LIVE_API_PASSPHRASE".into(), "live-pass".into());
    env.insert("PRODIGY_LIVE_TRADING_ENABLED".into(), "1".into());
    env.insert(
        "PRODIGY_LIVE_CONFIRM".into(),
        "I_UNDERSTAND_THIS_CAN_TRADE_REAL_MONEY".into(),
    );

    let parsed = parse_args_from_env(["prodigy-executor", "--mode", "live"], &env).unwrap();

    assert_eq!(parsed.cfg.mode, TradingMode::Live);
    assert_eq!(parsed.cfg.secrets.api_key, "live-key");
    assert!(parsed.cfg.live_safety.enabled);
}

#[test]
fn live_dry_validate_parses_without_any_bitget_credentials() {
    let env = std::collections::HashMap::new();

    let parsed = parse_args_from_env(
        ["prodigy-executor", "--mode", "live", "--dry-validate"],
        &env,
    )
    .unwrap();

    assert_eq!(parsed.cfg.mode, TradingMode::Live);
    assert!(parsed.dry_validate);
    assert!(parsed.cfg.secrets.api_key.is_empty());
}
```

- [ ] **Step 2: Run tests and verify they fail**

Run:

```bash
cargo test -q -p prodigy-executor live_runtime_validation_requires_enable_confirm_and_live_creds live_dry_validate_parses_without_any_bitget_credentials
```

Expected: compile/test failure because `BitgetSecrets`, `LiveSafety`, `live_for_tests`, `validate_for_runtime`, `validate_for_dry_validate`, and `dry_validate` do not exist yet.

- [ ] **Step 3: Implement minimal config changes**

In `crates/executor/src/config.rs`, rename `DemoSecrets` to `BitgetSecrets`, add `LiveSafety`, and replace `validate_demo_only` with mode-aware methods. Keep a compatibility alias only if it avoids a huge one-commit rename:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TradingMode {
    Demo,
    Live,
}

#[derive(Clone, PartialEq, Eq)]
pub struct BitgetSecrets {
    pub api_key: String,
    pub api_secret: String,
    pub passphrase: String,
}

pub type DemoSecrets = BitgetSecrets;

impl std::fmt::Debug for BitgetSecrets {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BitgetSecrets")
            .field("api_key", &"<redacted>")
            .field("api_secret", &"<redacted>")
            .field("passphrase", &"<redacted>")
            .finish()
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LiveSafety {
    pub enabled: bool,
    pub confirm_phrase: Option<String>,
}

pub const LIVE_CONFIRM_PHRASE: &str = "I_UNDERSTAND_THIS_CAN_TRADE_REAL_MONEY";
```

Add `pub live_safety: LiveSafety` to `ExecutorConfig`.

Add this live test constructor:

```rust
pub fn live_for_tests() -> Self {
    Self {
        mode: TradingMode::Live,
        public_ws_url: "wss://ws.bitget.com/v2/ws/public".to_string(),
        private_ws_url: "wss://ws.bitget.com/v2/ws/private".to_string(),
        secrets: BitgetSecrets {
            api_key: String::new(),
            api_secret: String::new(),
            passphrase: String::new(),
        },
        live_safety: LiveSafety::default(),
        ..Self::demo_for_tests()
    }
}
```

Implement:

```rust
pub fn validate_for_runtime(&self) -> Result<()> {
    self.validate_urls_for_mode()?;
    if self.secrets.api_key.trim().is_empty()
        || self.secrets.api_secret.trim().is_empty()
        || self.secrets.passphrase.trim().is_empty()
    {
        bail!(
            "missing Bitget {} API credentials",
            match self.mode {
                TradingMode::Demo => "demo",
                TradingMode::Live => "live",
            }
        );
    }
    if self.mode == TradingMode::Live {
        if !self.live_safety.enabled {
            bail!("live trading enable flag is required");
        }
        if self.live_safety.confirm_phrase.as_deref() != Some(LIVE_CONFIRM_PHRASE) {
            bail!("live confirmation phrase is required");
        }
    }
    Ok(())
}

pub fn validate_for_dry_validate(&self) -> Result<()> {
    if self.mode != TradingMode::Live {
        bail!("dry validation is only for live mode");
    }
    self.validate_urls_for_mode()
}

pub fn validate_urls_for_mode(&self) -> Result<()> {
    match self.mode {
        TradingMode::Demo => {
            if !self.public_ws_url.contains("wspap.bitget.com")
                || !self.private_ws_url.contains("wspap.bitget.com")
            {
                bail!("demo profile must use Bitget demo websocket URLs");
            }
        }
        TradingMode::Live => {
            if !self.public_ws_url.contains("ws.bitget.com")
                || !self.private_ws_url.contains("ws.bitget.com")
                || self.public_ws_url.contains("wspap.bitget.com")
                || self.private_ws_url.contains("wspap.bitget.com")
            {
                bail!("live profile must use Bitget live websocket URLs");
            }
        }
    }
    Ok(())
}
```

Keep this wrapper temporarily so old call sites compile until later tasks update them:

```rust
pub fn validate_demo_only(&self) -> Result<()> {
    if self.mode != TradingMode::Demo {
        bail!("prodigy executor only supports Bitget demo mode");
    }
    self.validate_for_runtime()
}
```

- [ ] **Step 4: Implement minimal CLI parsing**

In `crates/executor/src/main.rs`, add `dry_validate: bool` to `ParsedExecutorArgs`.

Parse:

```rust
let mut dry_validate = false;
```

Add args:

```rust
"--dry-validate" => dry_validate = true,
"--mode" => {
    let value = args.next().unwrap_or_else(|| "demo".to_string());
    cfg = match value.as_str() {
        "demo" => ExecutorConfig::demo_for_tests(),
        "live" => ExecutorConfig::live_for_tests(),
        other => bail!("unsupported mode: {other}"),
    };
}
```

When overlaying env, include live keys and live enable flags:

```rust
"BITGET_LIVE_API_KEY",
"BITGET_LIVE_API_SECRET",
"BITGET_LIVE_API_PASSPHRASE",
"PRODIGY_LIVE_TRADING_ENABLED",
"PRODIGY_LIVE_CONFIRM",
```

After args are parsed, resolve secrets by mode:

```rust
cfg.secrets = match cfg.mode {
    TradingMode::Demo => BitgetSecrets {
        api_key: read_secret(&["BITGET_DEMO_API_KEY"], env_file)?,
        api_secret: read_secret(&["BITGET_DEMO_API_SECRET", "BITGET_DEMO_SECRET_KEY"], env_file)?,
        passphrase: read_secret(
            &["BITGET_DEMO_API_PASSPHRASE", "BITGET_DEMO_PASSPHRASE"],
            env_file,
        )?,
    },
    TradingMode::Live if dry_validate => BitgetSecrets {
        api_key: read_optional(&["BITGET_LIVE_API_KEY"], env_file).unwrap_or_default(),
        api_secret: read_optional(&["BITGET_LIVE_API_SECRET"], env_file).unwrap_or_default(),
        passphrase: read_optional(&["BITGET_LIVE_API_PASSPHRASE"], env_file).unwrap_or_default(),
    },
    TradingMode::Live => BitgetSecrets {
        api_key: read_secret(&["BITGET_LIVE_API_KEY"], env_file)?,
        api_secret: read_secret(&["BITGET_LIVE_API_SECRET"], env_file)?,
        passphrase: read_secret(&["BITGET_LIVE_API_PASSPHRASE"], env_file)?,
    },
};
cfg.live_safety = LiveSafety {
    enabled: read_optional(&["PRODIGY_LIVE_TRADING_ENABLED"], env_file).as_deref() == Some("1"),
    confirm_phrase: read_optional(&["PRODIGY_LIVE_CONFIRM"], env_file),
};
```

In `main()`, validate by path:

```rust
if parsed.dry_validate {
    parsed.cfg.validate_for_dry_validate()?;
    prodigy_executor::daemon::run_live_dry_validate(parsed.cfg).await
} else {
    parsed.cfg.validate_for_runtime()?;
    match parsed.run_mode {
        RunMode::Once => executor::run_once_or_loop(parsed.cfg).await,
        RunMode::Daemon => {
            prodigy_executor::daemon::run_daemon(
                parsed.cfg,
                prodigy_executor::daemon::DaemonOptions {
                    max_runtime: parsed.max_runtime_ms.map(std::time::Duration::from_millis),
                },
            )
            .await
        }
    }
}
```

Stub `run_live_dry_validate` in `crates/executor/src/daemon.rs` for now:

```rust
pub async fn run_live_dry_validate(cfg: ExecutorConfig) -> Result<()> {
    cfg.validate_for_dry_validate()?;
    Ok(())
}
```

- [ ] **Step 5: Run tests**

Run:

```bash
cargo test -q -p prodigy-executor live_runtime_validation_requires_enable_confirm_and_live_creds live_dry_validate_parses_without_any_bitget_credentials
```

Expected: pass.

- [ ] **Step 6: Commit**

```bash
git add crates/executor/src/config.rs crates/executor/src/main.rs crates/executor/src/daemon.rs
git commit -m "feat: add mode-aware executor config"
```

---

## Task 2: Bitget Headers And Client Validation By Mode

**Files:**
- Modify: `crates/executor/src/bitget.rs`
- Test: `crates/executor/src/bitget.rs`

- [ ] **Step 1: Write failing header tests**

In `crates/executor/src/bitget.rs`, replace/extend the existing `signed_headers_include_auth_and_demo_header` test with:

```rust
#[test]
fn signed_headers_include_paptrading_only_for_demo() {
    let demo = ExecutorConfig::demo_for_tests();
    let demo_headers =
        signed_headers(&demo, "1", "GET", "/api/v2/mix/account/account", "").unwrap();
    assert_eq!(demo_headers.get("PAPTRADING").map(String::as_str), Some("1"));

    let live = ExecutorConfig {
        secrets: crate::config::BitgetSecrets {
            api_key: "live-key".to_string(),
            api_secret: "live-secret".to_string(),
            passphrase: "live-pass".to_string(),
        },
        live_safety: crate::config::LiveSafety {
            enabled: true,
            confirm_phrase: Some(crate::config::LIVE_CONFIRM_PHRASE.to_string()),
        },
        ..ExecutorConfig::live_for_tests()
    };
    let live_headers =
        signed_headers(&live, "1", "GET", "/api/v2/mix/account/account", "").unwrap();
    assert!(!live_headers.contains_key("PAPTRADING"));
    assert_eq!(live_headers.get("ACCESS-KEY").map(String::as_str), Some("live-key"));
}
```

- [ ] **Step 2: Run test and verify it fails**

Run:

```bash
cargo test -q -p prodigy-executor signed_headers_include_paptrading_only_for_demo
```

Expected: fail because `signed_headers` still validates demo-only and always inserts `PAPTRADING`.

- [ ] **Step 3: Make `signed_headers` mode-aware**

In `signed_headers`, replace:

```rust
cfg.validate_demo_only()?;
```

with:

```rust
cfg.validate_for_runtime()?;
```

Then only add `PAPTRADING` for demo:

```rust
if cfg.mode == crate::config::TradingMode::Demo {
    headers.insert("PAPTRADING".to_string(), "1".to_string());
}
```

In `BitgetRestClient::new`, replace `cfg.validate_demo_only()?` with
`cfg.validate_for_runtime()?`.

- [ ] **Step 4: Run test**

Run:

```bash
cargo test -q -p prodigy-executor signed_headers_include_paptrading_only_for_demo
```

Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add crates/executor/src/bitget.rs
git commit -m "feat: make Bitget headers profile-aware"
```

---

## Task 3: Control Command Schema And Struct Migration

**Files:**
- Modify: `schema/001_initial.sql`
- Modify: `src/prodigy/db.py`
- Modify: `tests/test_db_schema.py`
- Modify: `crates/executor/src/types.rs`
- Modify: `crates/executor/src/db.rs`

- [ ] **Step 1: Write failing Python migration tests**

Add to `tests/test_db_schema.py`:

```python
def test_control_commands_has_mode_and_instance_on_new_db(tmp_path):
    db_path = tmp_path / "prodigy.sqlite"
    with connect(db_path) as conn:
        init_db(conn)
        columns = {
            row["name"]: row
            for row in conn.execute("pragma table_info(control_commands)").fetchall()
        }

    assert columns["mode"]["notnull"] == 1
    assert columns["mode"]["dflt_value"] == "'demo'"
    assert "instance_id" in columns


def test_init_db_migrates_control_commands_mode_instance(tmp_path):
    db_path = tmp_path / "prodigy.sqlite"
    raw = sqlite3.connect(db_path)
    raw.executescript(
        """
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
          command_id, created_at, command, status, requested_by
        ) values ('old-stop', '2026-07-06T00:00:00Z', 'stop', 'pending', '123');
        """
    )
    raw.commit()
    raw.close()

    with connect(db_path) as conn:
        init_db(conn)
        row = conn.execute(
            "select command, mode, instance_id from control_commands where command_id = 'old-stop'"
        ).fetchone()

    assert dict(row) == {"command": "stop", "mode": "demo", "instance_id": None}
```

- [ ] **Step 2: Write failing Rust query test**

In `crates/executor/src/db.rs`, update `pending_control_commands_are_accepted_idempotently` or add:

```rust
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
```

- [ ] **Step 3: Run tests and verify they fail**

Run:

```bash
mamba run -n quantmamba python -m pytest -q tests/test_db_schema.py::test_control_commands_has_mode_and_instance_on_new_db tests/test_db_schema.py::test_init_db_migrates_control_commands_mode_instance
cargo test -q -p prodigy-executor pending_control_commands_filter_by_mode_and_instance
```

Expected: fail because columns and query signature do not exist.

- [ ] **Step 4: Update SQL schema**

In `schema/001_initial.sql`, add:

```sql
  mode text not null default 'demo',
  instance_id text,
```

inside `control_commands` before `processed_at`.

- [ ] **Step 5: Update Python migration**

In `src/prodigy/db.py`, inside `_ensure_execution_schema`, add:

```python
    _add_column_if_missing(
        conn,
        "control_commands",
        "mode",
        "mode text not null default 'demo'",
    )
    _add_column_if_missing(conn, "control_commands", "instance_id", "instance_id text")
```

Keep `_ensure_control_commands_support_cancel_all(conn)` before these calls.

- [ ] **Step 6: Update Rust type and query**

In `crates/executor/src/types.rs`:

```rust
pub struct ControlCommand {
    pub command_id: String,
    pub command: String,
    pub requested_by: String,
    pub mode: String,
    pub instance_id: Option<String>,
}
```

In `crates/executor/src/db.rs`, change:

```rust
pub fn pending_control_commands(
    conn: &Connection,
    mode: &str,
    instance_id: &str,
) -> Result<Vec<ControlCommand>> {
    let mut stmt = conn.prepare(
        "select command_id, command, requested_by, mode, instance_id
         from control_commands
         where status = 'pending' and mode = ? and instance_id = ?
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
```

- [ ] **Step 7: Fix compile call sites minimally**

Where tests construct `ControlCommand`, add:

```rust
mode: "demo".to_string(),
instance_id: Some("test-instance".to_string()),
```

Do not change behavior yet; full control processing isolation is Task 6.

- [ ] **Step 8: Run tests**

Run:

```bash
mamba run -n quantmamba python -m pytest -q tests/test_db_schema.py
cargo test -q -p prodigy-executor pending_control_commands_filter_by_mode_and_instance
```

Expected: pass.

- [ ] **Step 9: Commit**

```bash
git add schema/001_initial.sql src/prodigy/db.py tests/test_db_schema.py crates/executor/src/types.rs crates/executor/src/db.rs crates/executor/src/control.rs
git commit -m "feat: add mode and instance to control commands"
```

---

## Task 4: Active Executor Lock Helpers

**Files:**
- Modify: `crates/executor/src/db.rs`
- Modify: `crates/executor/src/daemon.rs`

- [ ] **Step 1: Write failing lock tests**

Add to `crates/executor/src/db.rs` tests:

```rust
#[test]
fn active_executor_lock_blocks_non_stale_second_instance() {
    let conn = memory_db();

    acquire_active_executor_lock(
        &conn,
        "demo",
        "inst-a",
        1_000,
        60_000,
    )
    .unwrap();

    let err = acquire_active_executor_lock(
        &conn,
        "live",
        "inst-b",
        31_000,
        60_000,
    )
    .unwrap_err();

    assert!(err.to_string().contains("active executor"));
}

#[test]
fn stale_active_executor_lock_can_be_taken_over_and_audited() {
    let conn = memory_db();
    acquire_active_executor_lock(
        &conn,
        "demo",
        "inst-a",
        1_000,
        60_000,
    )
    .unwrap();

    acquire_active_executor_lock(
        &conn,
        "live",
        "inst-b",
        122_000,
        60_000,
    )
    .unwrap();

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
fn release_active_executor_lock_only_releases_matching_instance() {
    let conn = memory_db();
    acquire_active_executor_lock(
        &conn,
        "demo",
        "inst-a",
        1_000,
        60_000,
    )
    .unwrap();

    release_active_executor_lock(&conn, "demo", "other").unwrap();
    assert_eq!(
        get_executor_state(&conn, "active_instance_id").unwrap().as_deref(),
        Some("inst-a")
    );

    release_active_executor_lock(&conn, "demo", "inst-a").unwrap();
    assert_eq!(get_executor_state(&conn, "active_instance_id").unwrap(), None);
}
```

- [ ] **Step 2: Run tests and verify they fail**

Run:

```bash
cargo test -q -p prodigy-executor active_executor_lock
```

Expected: fail because helper functions do not exist.

- [ ] **Step 3: Implement DB helpers**

Add to `crates/executor/src/db.rs`:

```rust
pub const ACTIVE_MODE_KEY: &str = "active_mode";
pub const ACTIVE_INSTANCE_ID_KEY: &str = "active_instance_id";
pub const ACTIVE_STARTED_AT_KEY: &str = "active_started_at";
pub const ACTIVE_HEARTBEAT_AT_KEY: &str = "active_heartbeat_at";

pub fn acquire_active_executor_lock(
    conn: &Connection,
    mode: &str,
    instance_id: &str,
    now_ms: i64,
    stale_timeout_ms: i64,
) -> Result<()> {
    let active_mode = get_executor_state(conn, ACTIVE_MODE_KEY)?;
    let active_instance = get_executor_state(conn, ACTIVE_INSTANCE_ID_KEY)?;
    let heartbeat = get_executor_state(conn, ACTIVE_HEARTBEAT_AT_KEY)?;
    if let (Some(old_mode), Some(old_instance), Some(old_heartbeat)) =
        (active_mode, active_instance, heartbeat)
    {
        let old_heartbeat_ms = old_heartbeat.parse::<i64>().unwrap_or(0);
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
    set_executor_state(conn, ACTIVE_MODE_KEY, mode)?;
    set_executor_state(conn, ACTIVE_INSTANCE_ID_KEY, instance_id)?;
    set_executor_state(conn, ACTIVE_STARTED_AT_KEY, &now_ms.to_string())?;
    set_executor_state(conn, ACTIVE_HEARTBEAT_AT_KEY, &now_ms.to_string())?;
    Ok(())
}

pub fn heartbeat_active_executor_lock(
    conn: &Connection,
    mode: &str,
    instance_id: &str,
    now_ms: i64,
) -> Result<()> {
    if get_executor_state(conn, ACTIVE_MODE_KEY)?.as_deref() == Some(mode)
        && get_executor_state(conn, ACTIVE_INSTANCE_ID_KEY)?.as_deref() == Some(instance_id)
    {
        set_executor_state(conn, ACTIVE_HEARTBEAT_AT_KEY, &now_ms.to_string())?;
    }
    Ok(())
}

pub fn release_active_executor_lock(
    conn: &Connection,
    mode: &str,
    instance_id: &str,
) -> Result<()> {
    if get_executor_state(conn, ACTIVE_MODE_KEY)?.as_deref() == Some(mode)
        && get_executor_state(conn, ACTIVE_INSTANCE_ID_KEY)?.as_deref() == Some(instance_id)
    {
        for key in [
            ACTIVE_MODE_KEY,
            ACTIVE_INSTANCE_ID_KEY,
            ACTIVE_STARTED_AT_KEY,
            ACTIVE_HEARTBEAT_AT_KEY,
        ] {
            conn.execute("delete from executor_state where key = ?", params![key])?;
        }
    }
    Ok(())
}
```

Store lock timestamps as Unix epoch milliseconds in string form. Do not add a time dependency.

- [ ] **Step 4: Run tests**

Run:

```bash
cargo test -q -p prodigy-executor active_executor_lock
```

Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add crates/executor/src/db.rs
git commit -m "feat: add active executor lock helpers"
```

---

## Task 5: Live Startup Clean-State Gate

**Files:**
- Modify: `crates/executor/src/db.rs`
- Modify: `crates/executor/src/executor.rs`
- Modify: `crates/executor/src/daemon.rs`

- [ ] **Step 1: Write failing clean-state tests**

Add to `crates/executor/src/db.rs` tests:

```rust
#[test]
fn live_clean_state_rejects_pending_intents_working_orders_and_positions() {
    let conn = memory_db();
    assert!(live_startup_clean_state(&conn, "live", "inst-live").is_ok());

    conn.execute(
        "insert into trade_intents (
          intent_id, created_at, symbol, side, action, target_notional,
          max_order_notional, status, source
        ) values ('i1', '2026-07-07T00:00:00Z', 'ETHUSDT', 'long', 'open', 1, 1, 'pending', 'test')",
        [],
    )
    .unwrap();
    assert!(live_startup_clean_state(&conn, "live", "inst-live")
        .unwrap_err()
        .to_string()
        .contains("pending trade intents"));
}

#[test]
fn live_clean_state_rejects_other_instance_pending_controls() {
    let conn = memory_db();
    conn.execute(
        "insert into control_commands (
          command_id, created_at, command, status, requested_by, mode, instance_id
        ) values ('c1', '2026-07-07T00:00:00Z', 'stop', 'pending', '123', 'demo', 'inst-demo')",
        [],
    )
    .unwrap();

    assert!(live_startup_clean_state(&conn, "live", "inst-live")
        .unwrap_err()
        .to_string()
        .contains("pending control commands"));
}
```

- [ ] **Step 2: Run tests and verify they fail**

Run:

```bash
cargo test -q -p prodigy-executor live_clean_state
```

Expected: fail because `live_startup_clean_state` does not exist.

- [ ] **Step 3: Implement clean-state helper**

Add to `crates/executor/src/db.rs`:

```rust
pub fn live_startup_clean_state(conn: &Connection, mode: &str, instance_id: &str) -> Result<()> {
    let pending_intents: i64 = conn.query_row(
        "select count(*) from trade_intents where status in ('pending', 'accepted')",
        [],
        |r| r.get(0),
    )?;
    if pending_intents > 0 {
        anyhow::bail!("live startup blocked: {pending_intents} pending trade intents");
    }

    let foreign_controls: i64 = conn.query_row(
        "select count(*) from control_commands
         where status in ('pending', 'accepted')
           and (mode != ?1 or coalesce(instance_id, '') != ?2)",
        params![mode, instance_id],
        |r| r.get(0),
    )?;
    if foreign_controls > 0 {
        anyhow::bail!("live startup blocked: {foreign_controls} pending control commands for other mode/instance");
    }

    let working_orders: i64 = conn.query_row(
        "select count(*) from orders
         where intent_id is not null and status in ('submitted', 'live')",
        [],
        |r| r.get(0),
    )?;
    if working_orders > 0 {
        anyhow::bail!("live startup blocked: {working_orders} working system orders");
    }

    let system_positions: i64 = conn.query_row(
        "select count(*) from positions where ownership = 'system'",
        [],
        |r| r.get(0),
    )?;
    if system_positions > 0 {
        anyhow::bail!("live startup blocked: {system_positions} system positions");
    }

    Ok(())
}
```

- [ ] **Step 4: Wire gate before private exchange calls**

In `crates/executor/src/executor.rs::run_once_or_loop` and `crates/executor/src/daemon.rs::run_daemon`, ensure the order is:

1. open SQLite;
2. set WAL/busy timeout;
3. acquire active lock where applicable;
4. for live normal mode, run `db::live_startup_clean_state(&conn, cfg.mode.as_str(), &instance_id)`;
5. only then create `BitgetRestClient`, call `set_leverage`, verify private WS, account, position, order, or reconcile.

The code must not do this before the gate:

```rust
let rest = BitgetRestClient::new(cfg.clone())?;
rest.set_leverage(cfg.leverage).await?;
verify_private_ws_connects(&cfg).await?;
```

- [ ] **Step 5: Add source-order regression test**

Add to `crates/executor/src/daemon.rs` tests:

```rust
#[test]
fn live_clean_state_gate_appears_before_private_exchange_calls() {
    let source = include_str!("daemon.rs");
    let clean_state = source.find("live_startup_clean_state").expect("clean-state gate exists");
    let rest_new = source.find("BitgetRestClient::new").expect("REST client exists");
    let set_leverage = source.find("set_leverage").expect("set leverage exists");

    assert!(clean_state < rest_new, "clean-state gate must precede REST client creation");
    assert!(clean_state < set_leverage, "clean-state gate must precede set-leverage");
}
```

If implementation moves code into helper functions, update the test to inspect the helper source instead of weakening the ordering assertion.

- [ ] **Step 6: Run tests**

Run:

```bash
cargo test -q -p prodigy-executor live_clean_state live_clean_state_gate_appears_before_private_exchange_calls
```

Expected: pass.

- [ ] **Step 7: Commit**

```bash
git add crates/executor/src/db.rs crates/executor/src/executor.rs crates/executor/src/daemon.rs
git commit -m "feat: gate live startup on clean state"
```

---

## Task 6: Mode/Instance-Isolated Control Processing

**Files:**
- Modify: `crates/executor/src/control.rs`
- Modify: `crates/executor/src/db.rs`
- Modify: `crates/executor/src/daemon.rs`

- [ ] **Step 1: Write failing control processing test**

Add to `crates/executor/src/control.rs` tests:

```rust
#[tokio::test]
async fn process_controls_ignores_other_mode_and_instance() {
    let conn = conn();
    conn.execute_batch(
        "
        insert into control_commands (
          command_id, created_at, command, status, requested_by, mode, instance_id
        ) values
          ('demo-stop', '2026-07-07T00:00:00Z', 'stop', 'pending', '123', 'demo', 'inst-demo'),
          ('live-stop', '2026-07-07T00:00:01Z', 'stop', 'pending', '123', 'live', 'inst-live');
        ",
    )
    .unwrap();
    let cfg = ExecutorConfig::demo_for_tests();
    let rest = BitgetRestClient::new(cfg.clone()).unwrap();
    let mut market_cache = MarketCache::default();

    process_pending_control_commands_once(
        &conn,
        &cfg,
        "inst-demo",
        &rest,
        &mut market_cache,
    )
    .await
    .unwrap();

    let statuses: Vec<(String, String)> = conn
        .prepare("select command_id, status from control_commands order by command_id")
        .unwrap()
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();

    assert_eq!(
        statuses,
        vec![
            ("demo-stop".to_string(), "executed".to_string()),
            ("live-stop".to_string(), "pending".to_string()),
        ]
    );
}
```

- [ ] **Step 2: Run test and verify it fails**

Run:

```bash
cargo test -q -p prodigy-executor process_controls_ignores_other_mode_and_instance
```

Expected: fail because `process_pending_control_commands_once` does not accept an instance id and queries all controls.

- [ ] **Step 3: Update control processing signature**

In `crates/executor/src/control.rs`:

```rust
pub async fn process_pending_control_commands_once(
    conn: &rusqlite::Connection,
    cfg: &ExecutorConfig,
    instance_id: &str,
    rest: &BitgetRestClient,
    market_cache: &mut MarketCache,
) -> Result<()> {
    cfg.validate_for_runtime()?;
    let mode = cfg.mode.as_str();
    let commands = crate::db::pending_control_commands(conn, mode, instance_id)?;
    // existing match over stop/resume/cancel_all/close_all stays here
}
```

Add `TradingMode::as_str()` in `config.rs`:

```rust
impl TradingMode {
    pub fn as_str(self) -> &'static str {
        match self {
            TradingMode::Demo => "demo",
            TradingMode::Live => "live",
        }
    }
}
```

Include mode/instance in accepted/executed/failed audit payloads:

```rust
"mode": command.mode,
"instance_id": command.instance_id,
```

- [ ] **Step 4: Update daemon call site**

Where `process_pending_control_commands_once` is called in `daemon.rs`, pass the daemon `instance_id`.

Use a temporary constant in tests only if the instance plumbing lands in Task 7:

```rust
let instance_id = "test-instance";
```

Production daemon must use its generated instance id after Task 7.

- [ ] **Step 5: Run tests**

Run:

```bash
cargo test -q -p prodigy-executor process_controls_ignores_other_mode_and_instance
```

Expected: pass.

- [ ] **Step 6: Commit**

```bash
git add crates/executor/src/config.rs crates/executor/src/control.rs crates/executor/src/daemon.rs crates/executor/src/db.rs
git commit -m "feat: isolate control commands by mode and instance"
```

---

## Task 7: Telegram Queueing, Status, And Close-All Confirmation Binding

**Files:**
- Modify: `crates/executor/src/telegram_query.rs`
- Test: `crates/executor/src/telegram_query.rs`

- [ ] **Step 1: Write failing Telegram queue tests**

Add to `crates/executor/src/telegram_query.rs` tests:

```rust
#[test]
fn telegram_control_requires_active_executor_and_writes_mode_instance() {
    let conn = memory_db();
    let reply = operator_command_reply(&conn, "/stop", "123", 1_000).unwrap().unwrap();
    assert!(reply.text.contains("no active executor"));
    let queued: i64 = conn
        .query_row("select count(*) from control_commands", [], |r| r.get(0))
        .unwrap();
    assert_eq!(queued, 0);

    crate::db::set_executor_state(&conn, crate::db::ACTIVE_MODE_KEY, "live").unwrap();
    crate::db::set_executor_state(&conn, crate::db::ACTIVE_INSTANCE_ID_KEY, "inst-live").unwrap();
    let reply = operator_command_reply(&conn, "/stop", "123", 2_000).unwrap().unwrap();
    assert!(reply.text.contains("queued"));

    let row: (String, Option<String>) = conn
        .query_row(
            "select mode, instance_id from control_commands where command = 'stop'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(row, ("live".to_string(), Some("inst-live".to_string())));
}

#[test]
fn close_all_confirmation_rejects_stale_mode_instance() {
    let conn = memory_db();
    crate::db::set_executor_state(&conn, crate::db::ACTIVE_MODE_KEY, "demo").unwrap();
    crate::db::set_executor_state(&conn, crate::db::ACTIVE_INSTANCE_ID_KEY, "inst-demo").unwrap();

    let reply = start_close_all_confirmation_reply(&conn, "123", 1_000).unwrap();
    assert!(reply.text.contains("confirm"));

    crate::db::set_executor_state(&conn, crate::db::ACTIVE_MODE_KEY, "live").unwrap();
    crate::db::set_executor_state(&conn, crate::db::ACTIVE_INSTANCE_ID_KEY, "inst-live").unwrap();

    let code = extract_code_for_test(&reply.text);
    let rejected = confirm_close_all(&conn, code, "123", 2_000).unwrap();
    assert!(rejected.contains("stale") || rejected.contains("expired"));
    let queued: i64 = conn
        .query_row("select count(*) from control_commands where command = 'close_all'", [], |r| r.get(0))
        .unwrap();
    assert_eq!(queued, 0);
}
```

If there is no public `operator_command_reply` or `extract_code_for_test`, use the existing internal helper names in this file. Keep tests inside the same module so private helpers are callable.

- [ ] **Step 2: Run tests and verify they fail**

Run:

```bash
cargo test -q -p prodigy-executor telegram_control_requires_active_executor_and_writes_mode_instance close_all_confirmation_rejects_stale_mode_instance
```

Expected: fail because queueing does not read active mode/instance and confirmation state does not bind to them.

- [ ] **Step 3: Add active target helper**

In `telegram_query.rs`:

```rust
#[derive(Debug, Clone)]
struct ActiveExecutorTarget {
    mode: String,
    instance_id: String,
}

fn active_executor_target(conn: &Connection) -> Result<Option<ActiveExecutorTarget>> {
    let Some(mode) = crate::db::get_executor_state(conn, crate::db::ACTIVE_MODE_KEY)? else {
        return Ok(None);
    };
    let Some(instance_id) =
        crate::db::get_executor_state(conn, crate::db::ACTIVE_INSTANCE_ID_KEY)?
    else {
        return Ok(None);
    };
    Ok(Some(ActiveExecutorTarget { mode, instance_id }))
}
```

- [ ] **Step 4: Update `queue_control_command`**

Replace the insert with:

```rust
let Some(target) = active_executor_target(conn)? else {
    audit(
        conn,
        "telegram control command rejected",
        &serde_json::json!({
            "command": command,
            "requested_by": requested_by,
            "error": "no_active_executor",
        })
        .to_string(),
    )?;
    anyhow::bail!("no active executor");
};

conn.execute(
    "insert into control_commands (
       command_id, created_at, command, status, requested_by, mode, instance_id
     ) values (?, datetime('now'), ?, 'pending', ?, ?, ?)",
    rusqlite::params![
        command_id,
        command,
        requested_by,
        target.mode,
        target.instance_id
    ],
)?;
```

Include `mode` and `instance_id` in the audit JSON.

- [ ] **Step 5: Bind close-all confirmation to active mode/instance**

When storing confirmation state, include `mode` and `instance_id`:

```json
{
  "code": "A1B2C3",
  "requested_by": "123",
  "expires_at_ms": 123456,
  "status": "pending",
  "mode": "demo",
  "instance_id": "inst-demo"
}
```

During confirmation, compare current active mode/instance to the stored values before queueing. If mismatched, reject and audit as stale.

- [ ] **Step 6: Update `/status` mode display**

Use `active_executor_target(conn)` in `status_reply`. Add one row:

```rust
row(
    "MODE",
    active_executor_target(conn)?
        .map(|t| t.mode.to_uppercase())
        .unwrap_or_else(|| "NO ACTIVE EXECUTOR".to_string()),
),
```

If an existing `MODE DEMO` row is hardcoded, replace it rather than adding a duplicate.

- [ ] **Step 7: Run tests**

Run:

```bash
cargo test -q -p prodigy-executor telegram_control_requires_active_executor_and_writes_mode_instance close_all_confirmation_rejects_stale_mode_instance
```

Expected: pass.

- [ ] **Step 8: Commit**

```bash
git add crates/executor/src/telegram_query.rs
git commit -m "feat: bind Telegram controls to active executor"
```

---

## Task 8: Daemon Active Lock, Heartbeat, And Clean Shutdown

**Files:**
- Modify: `crates/executor/src/daemon.rs`
- Modify: `crates/executor/src/db.rs`

- [ ] **Step 1: Write failing daemon helper tests**

Add to `crates/executor/src/daemon.rs` tests:

```rust
#[test]
fn daemon_release_helper_clears_only_matching_active_lock() {
    let conn = test_conn();
    crate::db::acquire_active_executor_lock(
        &conn,
        "demo",
        "inst-demo",
        1_000,
        60_000,
    )
    .unwrap();

    let cfg = ExecutorConfig {
        ..ExecutorConfig::demo_for_tests()
    };
    release_lock_on_shutdown(&conn, &cfg, "other").unwrap();
    assert_eq!(
        crate::db::get_executor_state(&conn, crate::db::ACTIVE_INSTANCE_ID_KEY)
            .unwrap()
            .as_deref(),
        Some("inst-demo")
    );

    release_lock_on_shutdown(&conn, &cfg, "inst-demo").unwrap();
    assert_eq!(
        crate::db::get_executor_state(&conn, crate::db::ACTIVE_INSTANCE_ID_KEY).unwrap(),
        None
    );
}
```

- [ ] **Step 2: Run test and verify it fails**

Run:

```bash
cargo test -q -p prodigy-executor releases_active_lock
```

Expected: fail because `release_lock_on_shutdown` does not exist yet.

- [ ] **Step 3: Add small daemon lock helpers**

In `daemon.rs`:

```rust
fn new_instance_id(conn: &rusqlite::Connection) -> Result<String> {
    Ok(conn.query_row("select lower(hex(randomblob(16)))", [], |r| r.get(0))?)
}

fn now_ms_i64() -> i64 {
    crate::bitget::now_ms().parse().unwrap_or(0)
}

fn release_lock_on_shutdown(
    conn: &rusqlite::Connection,
    cfg: &ExecutorConfig,
    instance_id: &str,
) -> Result<()> {
    crate::db::release_active_executor_lock(conn, cfg.mode.as_str(), instance_id)
}
```

- [ ] **Step 4: Acquire lock before exchange calls**

In `run_daemon`, after opening SQLite and setting busy timeout, before `BitgetRestClient::new`:

```rust
let instance_id = new_instance_id(&conn)?;
crate::db::acquire_active_executor_lock(
    &conn,
    cfg.mode.as_str(),
    &instance_id,
    now_ms_i64(),
    30_000,
)?;
```

- [ ] **Step 5: Heartbeat each daemon tick**

In the daemon main loop, call:

```rust
crate::db::heartbeat_active_executor_lock(
    &conn,
    cfg.mode.as_str(),
    &instance_id,
    now_ms_i64(),
)?;
```

- [ ] **Step 6: Release lock on clean shutdown**

Before returning from `run_daemon`, call:

```rust
release_lock_on_shutdown(&conn, &cfg, &instance_id)?;
```

If `run_daemon` has multiple return paths, use one final cleanup block rather than duplicating release in several branches.

- [ ] **Step 7: Pass instance id into control processing**

Update daemon calls:

```rust
crate::control::process_pending_control_commands_once(
    conn,
    cfg,
    &instance_id,
    rest,
    &mut control_cache,
)
.await
```

- [ ] **Step 8: Run tests**

Run:

```bash
cargo test -q -p prodigy-executor active_executor_lock private_state_ready daemon_allows_bounded_runtime_for_tests
```

Expected: pass.

- [ ] **Step 9: Commit**

```bash
git add crates/executor/src/daemon.rs crates/executor/src/db.rs crates/executor/src/control.rs
git commit -m "feat: add daemon active executor lock"
```

---

## Task 9: Live Dry Validation With No Side Effects

**Files:**
- Modify: `crates/executor/src/daemon.rs`
- Modify: `crates/executor/src/main.rs`
- Test: `crates/executor/src/daemon.rs`
- Test: `tests/test_executor_integration.py`

- [ ] **Step 1: Write failing dry validation tests**

Add to `crates/executor/src/daemon.rs` tests:

```rust
#[tokio::test]
async fn live_dry_validate_leaves_no_active_lock() {
    let db_path = std::env::temp_dir().join(format!(
        "prodigy-live-dry-validate-{}.sqlite",
        std::process::id()
    ));
    {
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.execute_batch(include_str!("../../../schema/001_initial.sql"))
            .unwrap();
        conn.execute_batch(include_str!("../../../schema/002_execution.sql"))
            .unwrap();
    }
    let cfg = ExecutorConfig {
        db_path: db_path.clone(),
        ..ExecutorConfig::live_for_tests()
    };

    run_live_dry_validate(cfg).await.unwrap();

    let conn = rusqlite::Connection::open(&db_path).unwrap();
    assert_eq!(
        crate::db::get_executor_state(&conn, crate::db::ACTIVE_MODE_KEY).unwrap(),
        None
    );
}
```

Add to `tests/test_executor_integration.py`:

```python
def test_live_dry_validate_needs_no_keys_and_leaves_no_active_executor(tmp_path):
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
            "--mode",
            "live",
            "--dry-validate",
        ],
        check=True,
        text=True,
        capture_output=True,
        env={},
    )

    assert "live dry validation passed" in result.stdout
    with connect(db_path) as conn:
        init_db(conn)
        active = conn.execute(
            "select value from executor_state where key = 'active_mode'"
        ).fetchone()
    assert active is None
```

- [ ] **Step 2: Run tests and verify they fail**

Run:

```bash
cargo test -q -p prodigy-executor live_dry_validate_leaves_no_active_lock
mamba run -n quantmamba python -m pytest -q tests/test_executor_integration.py::test_live_dry_validate_needs_no_keys_and_leaves_no_active_executor
```

Expected: fail because dry validation is still a stub or tries to load keys.

- [ ] **Step 3: Implement dry validation**

In `daemon.rs`:

```rust
pub async fn run_live_dry_validate(cfg: ExecutorConfig) -> Result<()> {
    cfg.validate_for_dry_validate()?;
    let conn = rusqlite::Connection::open(&cfg.db_path)?;
    conn.busy_timeout(Duration::from_secs(5))?;
    crate::db::live_startup_clean_state(&conn, cfg.mode.as_str(), "dry-validate")?;

    if crate::bitget::should_send_paptrading(&cfg) {
        anyhow::bail!("live dry validation generated PAPTRADING header");
    }
    println!("live dry validation passed");
    Ok(())
}
```

Do not add a fake active lock. Add a pure helper in `crates/executor/src/bitget.rs`:

```rust
pub fn should_send_paptrading(cfg: &ExecutorConfig) -> bool {
    cfg.mode == TradingMode::Demo
}
```

Dry validation must use this helper and must not sign headers without credentials.

- [ ] **Step 4: Ensure no private REST is reachable**

Do not construct `BitgetRestClient` in `run_live_dry_validate`. Do not call:

```rust
BitgetRestClient::new(cfg.clone())?;
rest.set_leverage(cfg.leverage).await?;
verify_private_ws_connects(&cfg).await?;
reconcile_once(&conn, &rest, "live-dry-validate", true, None, None).await?;
```

- [ ] **Step 5: Run tests**

Run:

```bash
cargo test -q -p prodigy-executor live_dry_validate_leaves_no_active_lock
mamba run -n quantmamba python -m pytest -q tests/test_executor_integration.py::test_live_dry_validate_needs_no_keys_and_leaves_no_active_executor
```

Expected: pass.

- [ ] **Step 6: Commit**

```bash
git add crates/executor/src/daemon.rs crates/executor/src/main.rs crates/executor/src/bitget.rs tests/test_executor_integration.py
git commit -m "feat: add live dry validation"
```

---

## Task 10: Live Normal Startup Gates Before Private Calls

**Files:**
- Modify: `crates/executor/src/executor.rs`
- Modify: `crates/executor/src/daemon.rs`
- Modify: `crates/executor/src/main.rs`
- Test: `crates/executor/src/daemon.rs`
- Test: `tests/test_executor_integration.py`

- [ ] **Step 1: Write failing integration tests**

Add to `tests/test_executor_integration.py`:

```python
def test_live_daemon_without_keys_fails_before_private_calls(tmp_path):
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
            "--mode",
            "live",
            "--daemon",
            "--max-runtime-ms",
            "1",
        ],
        check=False,
        text=True,
        capture_output=True,
        env={},
    )

    assert result.returncode != 0
    assert "live" in result.stderr.lower()
    assert "credential" in result.stderr.lower()
```

Add another test with fake live keys but missing enable:

```python
def test_live_daemon_with_keys_without_enable_fails_before_private_calls(tmp_path):
    db_path = tmp_path / "prodigy.sqlite"
    env = {
        "BITGET_LIVE_API_KEY": "live-key",
        "BITGET_LIVE_API_SECRET": "live-secret",
        "BITGET_LIVE_API_PASSPHRASE": "live-pass",
    }
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
            "--mode",
            "live",
            "--daemon",
            "--max-runtime-ms",
            "1",
        ],
        check=False,
        text=True,
        capture_output=True,
        env=env,
    )

    assert result.returncode != 0
    assert "enable" in result.stderr.lower()
```

- [ ] **Step 2: Run tests and verify they fail if gates are missing**

Run:

```bash
mamba run -n quantmamba python -m pytest -q tests/test_executor_integration.py::test_live_daemon_without_keys_fails_before_private_calls tests/test_executor_integration.py::test_live_daemon_with_keys_without_enable_fails_before_private_calls
```

Expected: fail until parser/startup gates are correctly wired.

- [ ] **Step 3: Wire runtime validation before execution**

In `main.rs`, ensure `parsed.cfg.validate_for_runtime()?` happens before dispatching once/daemon when not dry-validating.

In `daemon.rs` and `executor.rs`, keep defense-in-depth:

```rust
cfg.validate_for_runtime()?;
```

at the top, but ensure live clean-state gate comes before private exchange calls.

- [ ] **Step 4: Ensure `BitgetRestClient::new` remains a private-call boundary**

`BitgetRestClient::new` can validate runtime mode, but live startup must fail before this constructor when DB clean-state fails. Keep the source-order test from Task 5.

- [ ] **Step 5: Run tests**

Run:

```bash
mamba run -n quantmamba python -m pytest -q tests/test_executor_integration.py::test_live_daemon_without_keys_fails_before_private_calls tests/test_executor_integration.py::test_live_daemon_with_keys_without_enable_fails_before_private_calls
cargo test -q -p prodigy-executor live_clean_state_gate_appears_before_private_exchange_calls
```

Expected: pass.

- [ ] **Step 6: Commit**

```bash
git add crates/executor/src/main.rs crates/executor/src/daemon.rs crates/executor/src/executor.rs tests/test_executor_integration.py
git commit -m "feat: enforce live startup safety gates"
```

---

## Task 11: Demo Behavior Regression Coverage

**Files:**
- Modify: `crates/executor/src/main.rs`
- Modify: `crates/executor/tests/bitget_demo.rs`
- Test: existing Python/Rust tests

- [ ] **Step 1: Add demo regression tests if missing**

Ensure these existing tests still pass; do not duplicate them if Task 1/Task 2 already updated them:

- `parses_once_mode_by_default`
- `parses_daemon_mode_and_db_path`
- `signed_headers_include_paptrading_only_for_demo`

- [ ] **Step 2: Run demo-focused tests**

Run:

```bash
cargo test -q -p prodigy-executor parses_once_mode_by_default parses_daemon_mode_and_db_path signed_headers_include_paptrading_only_for_demo
cargo test -q --test bitget_demo
```

Expected: pass, or Bitget demo tests fail only for existing honest demo liquidity constraints. Do not weaken false-fill assertions.

- [ ] **Step 3: Fix only regressions caused by M8**

If demo tests fail due renamed `DemoSecrets`, keep the alias:

```rust
pub type DemoSecrets = BitgetSecrets;
```

If demo runtime now needs live safety values, fix validation so demo ignores `LiveSafety`.

- [ ] **Step 4: Commit**

```bash
git add crates/executor/src/config.rs crates/executor/src/main.rs crates/executor/src/bitget.rs crates/executor/tests/bitget_demo.rs
git commit -m "test: preserve demo profile behavior"
```

---

## Task 12: Scope Scan And Switch Procedure Documentation

**Files:**
- Modify: `tests/test_executor_integration.py`
- Create: `docs/superpowers/checklists/2026-07-07-m8-demo-to-live-switch.md`

- [ ] **Step 1: Add scope scan test**

Append to `tests/test_executor_integration.py`:

```python
def test_m8_scope_scan_has_no_remote_open_or_live_bypass():
    repo_root = Path(__file__).resolve().parents[1]
    dangerous = subprocess.run(
        [
            "rg",
            "-n",
            "remote_open|open_from_telegram|TELEGRAM_PARAM|remote_param|"
            "set_param_from_telegram|remote_shell|shell_from_telegram|"
            "model_debug_from_telegram|remote_model_debug|"
            "LIVE_TRADING_ENABLED\\s*=\\s*true|allow_live_without_confirm",
            "src",
            "crates",
        ],
        check=False,
        text=True,
        capture_output=True,
        cwd=repo_root,
    )
    assert dangerous.returncode in (0, 1), dangerous.stdout + dangerous.stderr

    production_hits = []
    for hit in dangerous.stdout.splitlines():
        if "/tests/" in hit or "#[cfg(test)]" in hit:
            continue
        production_hits.append(hit)
    assert not production_hits, "\\n".join(production_hits)
```

If this flags safe test strings in Rust `#[cfg(test)]` modules, reuse the existing `rust_cfg_test_ranges` helper from the M7 scan instead of weakening the pattern.

- [ ] **Step 2: Write switch procedure doc**

Create `docs/superpowers/checklists/2026-07-07-m8-demo-to-live-switch.md`:

```markdown
# M8 Demo To Live Switch

1. In demo, run `/stop`.
2. Run `/cancel_all`.
3. If `/status` shows system positions, run `/close_all` and confirm.
4. Run `/status`.
5. Confirm:
   - no pending or accepted intents;
   - no pending or accepted control commands;
   - no working system orders;
   - no system positions;
   - mode is still `MODE DEMO`.
6. Stop the demo executor cleanly.
7. Add live credentials to `.env.local` or the process environment:
   - `BITGET_LIVE_API_KEY`
   - `BITGET_LIVE_API_SECRET`
   - `BITGET_LIVE_API_PASSPHRASE`
8. Set:
   - `PRODIGY_LIVE_TRADING_ENABLED=1`
   - `PRODIGY_LIVE_CONFIRM=I_UNDERSTAND_THIS_CAN_TRADE_REAL_MONEY`
9. Start:
   `prodigy-executor --mode live --daemon`
10. Run `/status` and verify `MODE LIVE`.

If the DB is not clean, live startup must fail even if this checklist is wrong.
```

- [ ] **Step 3: Run tests and inspect doc**

Run:

```bash
mamba run -n quantmamba python -m pytest -q tests/test_executor_integration.py::test_m8_scope_scan_has_no_remote_open_or_live_bypass
rg -n "MODE LIVE|PRODIGY_LIVE_CONFIRM|--mode live" docs/superpowers/checklists/2026-07-07-m8-demo-to-live-switch.md
```

Expected: test passes; `rg` shows the explicit switch procedure.

- [ ] **Step 4: Commit**

```bash
git add tests/test_executor_integration.py docs/superpowers/checklists/2026-07-07-m8-demo-to-live-switch.md
git commit -m "docs: add M8 demo to live switch procedure"
```

---

## Task 13: Final Verification And Review

**Files:**
- Review all files changed by this plan.

- [ ] **Step 1: Run complete verification**

Run:

```bash
mamba run -n quantmamba python -m pytest -q
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test -q
git diff --check main...HEAD
git status --short --branch
```

Expected:

- Python tests pass.
- Rust formatting passes.
- Clippy passes with no warnings.
- Rust tests pass.
- Diff check prints nothing.
- Worktree has only intentional committed changes or is clean.

- [ ] **Step 2: Run targeted safety scans**

Run:

```bash
rg -n "PAPTRADING" crates/executor/src crates/executor/tests
rg -n "BITGET_LIVE|PRODIGY_LIVE|LIVE_CONFIRM|ws.bitget.com|wspap.bitget.com" crates/executor/src tests docs/superpowers
rg -n "remote_open|open_from_telegram|remote_param|remote_shell|model_debug_from_telegram|allow_live_without_confirm" src crates tests docs/superpowers
```

Expected:

- `PAPTRADING` is conditional on demo or appears in tests/docs.
- live env vars appear only in config/parser/tests/docs.
- no production remote-open/remote-param/shell/model-debug path exists.

- [ ] **Step 3: Manual code review checklist**

Check these before final commit/report:

- live dry validation does not construct `BitgetRestClient`;
- live normal startup gates run before private REST and private WS login;
- demo `--once` and `--daemon` still use demo keys and `wspap`;
- live normal startup cannot pass without live key, enable, and exact confirm phrase;
- control command SQL inserts always include mode and instance id;
- executor control query filters by mode and instance id;
- `/close_all` confirmation payload includes mode and instance id;
- clean shutdown releases active lock;
- stale takeover writes an event;
- no live secret appears in test output or `Debug`.

- [ ] **Step 4: Commit final review fixes**

If final review finds changes:

```bash
git add <changed-files>
git commit -m "fix: address M8 final review"
```

If no fixes are needed, do not create an empty commit.
