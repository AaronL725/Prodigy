# Crypto Quant Seventh Milestone Live Readiness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add live-readiness safety tests and a short M8 checklist without enabling live trading.

**Architecture:** M7 is a safety-test and documentation milestone. It strengthens existing tests around demo-only execution, Telegram control boundaries, and live-enablement scope scans, while leaving runtime behavior unchanged.

**Tech Stack:** Rust executor tests, Python pytest scope scans, Markdown checklist, SQLite-backed command/audit boundaries.

---

## File Structure

- Modify: `tests/test_executor_integration.py`
  - Refine the M6 scope scan into an M7 live-readiness scan that targets dangerous live-enablement patterns, not every `live` string.
- Modify: `crates/executor/src/main.rs`
  - Add a Rust parser/config regression test proving live key names are ignored and demo credentials remain the only loaded credentials.
- Modify: `crates/executor/src/telegram_query.rs`
  - Add a Rust Telegram regression test proving remote open, remote parameter edit, model debug, shell, and live-enable commands are not accepted operator commands.
- Create: `docs/superpowers/checklists/2026-07-06-m8-prelive-readiness.md`
  - Static, concise checklist for the future M8 live integration milestone.

No new CLI, no generated report, no live API call, and no runtime live behavior change.

---

### Task 1: Refine Live-Readiness Scope Scan

**Files:**
- Modify: `tests/test_executor_integration.py`

- [ ] **Step 1: Replace the existing M6 scope scan test with an M7-specific scan**

Replace `test_m6_scope_scan_has_no_remote_open_or_live_enablement` with:

```python
def test_m7_live_readiness_scope_scan_targets_dangerous_patterns_only():
    repo_root = Path(__file__).resolve().parents[1]
    dangerous_patterns = (
        "remote_open|open_from_telegram|"
        "TELEGRAM_LIVE|BITGET_LIVE|ENABLE_LIVE_TRADING|LIVE_TRADING_ENABLED|"
        "allow_live_trading|live_trading_enabled|live_order_execution_enabled|"
        "TELEGRAM_PARAM|remote_param|set_param_from_telegram|"
        "remote_shell|shell_from_telegram|"
        "model_debug_from_telegram|remote_model_debug"
    )
    dangerous = subprocess.run(
        ["rg", "-n", dangerous_patterns, "src", "crates"],
        check=False,
        text=True,
        capture_output=True,
        cwd=repo_root,
    )
    assert dangerous.returncode == 1, dangerous.stdout + dangerous.stderr

    config_rs = (repo_root / "crates/executor/src/config.rs").read_text()
    production_config, test_config = config_rs.split("#[cfg(test)]", 1)

    # M7 deliberately allows safe live-rejection code; do not ban every "live"
    # string. The dangerous scan above is targeted at enablement patterns.
    assert "TradingMode::Live" in config_rs
    assert "ws.bitget.com" not in production_config
    assert "wss://ws.bitget.com/v2/ws/public" in test_config
    assert "wss://ws.bitget.com/v2/ws/private" in test_config

    telegram_query = (repo_root / "crates/executor/src/telegram_query.rs").read_text()
    for forbidden in ("BitgetRestClient", "/api/v2", "place-order", "cancel-order"):
        assert forbidden not in telegram_query
```

- [ ] **Step 2: Run the focused Python test**

Run:

```bash
mamba run -n quantmamba python -m pytest -q tests/test_executor_integration.py::test_m7_live_readiness_scope_scan_targets_dangerous_patterns_only
```

Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/test_executor_integration.py
git commit -m "test: add M7 live readiness scope scan"
```

---

### Task 2: Prove Live Key Names Are Not Loaded In M7

**Files:**
- Modify: `crates/executor/src/main.rs`

- [ ] **Step 1: Add the failing parser/config test**

In `crates/executor/src/main.rs`, inside `#[cfg(test)] mod tests`, add:

```rust
    #[test]
    fn ignores_live_key_names_and_loads_demo_credentials_only() {
        let mut env = fake_env();
        env.insert("BITGET_LIVE_API_KEY".into(), "live-key".into());
        env.insert("BITGET_LIVE_API_SECRET".into(), "live-secret".into());
        env.insert("BITGET_LIVE_API_PASSPHRASE".into(), "live-pass".into());

        let parsed = parse_args_from_env(["prodigy-executor", "--daemon"], &env).unwrap();

        assert_eq!(parsed.cfg.mode, TradingMode::Demo);
        assert_eq!(parsed.cfg.secrets.api_key, "test-key");
        assert_eq!(parsed.cfg.secrets.api_secret, "test-secret");
        assert_eq!(parsed.cfg.secrets.passphrase, "test-pass");
    }
```

- [ ] **Step 2: Run the focused Rust test**

Run:

```bash
cargo test -q -p prodigy-executor ignores_live_key_names_and_loads_demo_credentials_only
```

Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add crates/executor/src/main.rs
git commit -m "test: prove live keys are ignored in M7"
```

---

### Task 3: Prove Telegram Cannot Queue Remote Open Or Debug Commands

**Files:**
- Modify: `crates/executor/src/telegram_query.rs`

- [ ] **Step 1: Add the failing Telegram boundary test**

In `crates/executor/src/telegram_query.rs`, inside `#[cfg(test)] mod tests`, add:

```rust
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
            let response = operator_response(&conn, text, "123", &["123".to_string()], 1_000)
                .unwrap();
            assert!(response.is_none(), "{text} should not be a Telegram operator command");
        }

        let command_count: i64 = conn
            .query_row("select count(*) from control_commands", [], |r| r.get(0))
            .unwrap();
        assert_eq!(command_count, 0);
    }
```

- [ ] **Step 2: Run the focused Rust test**

Run:

```bash
cargo test -q -p prodigy-executor remote_open_param_model_shell_and_live_commands_are_not_operator_commands
```

Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add crates/executor/src/telegram_query.rs
git commit -m "test: reject unsafe telegram operator commands"
```

---

### Task 4: Add The M8 Pre-Live Readiness Checklist

**Files:**
- Create: `docs/superpowers/checklists/2026-07-06-m8-prelive-readiness.md`

- [ ] **Step 1: Create the checklist**

Create `docs/superpowers/checklists/2026-07-06-m8-prelive-readiness.md`:

```markdown
# M8 Pre-Live Readiness Checklist

Before M8 live integration starts:

- [ ] M7 full test suite passes on `main`.
- [ ] `prodigy-executor --mode live` is still rejected.
- [ ] Telegram `/stop`, `/resume`, `/cancel_all`, and `/close_all` have been tested in demo.
- [ ] SQLite has no unexpected system working orders.
- [ ] SQLite has no unexpected system positions.
- [ ] Recent `events` contain no unresolved `critical` execution errors.
- [ ] M8 has an explicit live-enable design before any live key is used.
- [ ] Live API keys are prepared outside M7 and are not committed.
- [ ] M8 rollback plan is written before small-capital launch.
```

- [ ] **Step 2: Verify the checklist is concise and not a generated report**

Run:

```bash
wc -l docs/superpowers/checklists/2026-07-06-m8-prelive-readiness.md
rg -n "report|preflight|CLI|soak" docs/superpowers/checklists/2026-07-06-m8-prelive-readiness.md
```

Expected:

- `wc -l` reports fewer than 20 lines.
- `rg` exits with code 1, meaning none of those out-of-scope words appear.

- [ ] **Step 3: Commit**

```bash
git add docs/superpowers/checklists/2026-07-06-m8-prelive-readiness.md
git commit -m "docs: add M8 pre-live readiness checklist"
```

---

### Task 5: Final Verification

**Files:**
- No code changes.

- [ ] **Step 1: Run Python tests**

```bash
mamba run -n quantmamba python -m pytest -q
```

Expected: all tests pass.

- [ ] **Step 2: Run Rust formatting and linting**

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
```

Expected: both commands exit 0.

- [ ] **Step 3: Run Rust tests**

```bash
cargo test -q
```

Expected: all tests pass.

- [ ] **Step 4: Run diff whitespace check**

```bash
git diff --check main...HEAD
```

Expected: no output and exit 0.

- [ ] **Step 5: Confirm no out-of-scope feature was added**

Run:

```bash
rg -n "prodigy-prelive|prelive-check|remote_open|open_from_telegram|TELEGRAM_LIVE|BITGET_LIVE|ENABLE_LIVE_TRADING|LIVE_TRADING_ENABLED|remote_shell|shell_from_telegram|model_debug_from_telegram" src crates docs/superpowers/checklists
```

Expected: no matches except the checklist/spec/plan text documenting prohibited patterns. If matches appear in production code under `src` or `crates`, stop and remove the out-of-scope path.

- [ ] **Step 6: Commit verification-only fixes if any**

If Task 5 reveals a small documentation or test naming issue, fix it and commit:

```bash
git add <changed-files>
git commit -m "chore: finalize M7 live readiness checks"
```

If Task 5 has no changes, do not create an empty commit.
