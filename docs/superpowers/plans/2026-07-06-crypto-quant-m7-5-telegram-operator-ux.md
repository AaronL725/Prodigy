# Crypto Quant M7.5 Telegram Operator UX Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Upgrade Telegram operator replies and buttons into a polished dark editorial mobile dashboard without changing trading semantics or slash command names.

**Architecture:** Keep Telegram as a SQLite-backed operator interface. `telegram_query.rs` owns response formatting, keyboards, callbacks, and control confirmation semantics; `daemon.rs` owns Telegram HTTP plumbing: getUpdates parsing, sendMessage payloads, answerCallbackQuery, and best-effort setMyCommands. Telegram still never calls Bitget and never executes trades directly.

**Tech Stack:** Rust, rusqlite, serde_json, reqwest, Telegram Bot API `sendMessage` HTML parse mode, inline keyboards, callback queries, setMyCommands.

---

## File Structure

- Modify: `crates/executor/src/telegram_query.rs`
  - Add a small `TelegramReply` value type.
  - Add HTML escaping and refined dark editorial formatting helpers.
  - Add inline keyboard builders, including bounded pagination controls.
  - Preserve existing `query_response()` and `operator_response()` string APIs for tests/backward compatibility.
  - Add richer reply APIs for the daemon loop.
  - Add callback handling for read-only, paginated list, and control buttons.
  - Add button-based `/close_all` confirmation and cancellation.
  - Add a pure `bot_commands_payload()` helper for setMyCommands.
- Modify: `crates/executor/src/daemon.rs`
  - Parse Telegram `callback_query` updates in addition to text messages.
  - Send HTML parse mode and inline keyboard reply markup.
  - Answer callback queries promptly.
  - Register bot commands once at Telegram loop startup, best effort, with a short request timeout.
- Modify: `tests/test_executor_integration.py`
  - Extend the existing dangerous-scope scan so M7.5 still forbids remote open, live enablement, remote parameter editing, model debug, shell, and direct Telegram-to-Bitget paths.

No schema migration, no new dependencies, no new command names, no live path, no remote open, no parameter editing, no model debug, no shell.

---

## Current Version Notes From Operator Feedback

The task list below records the original TDD implementation path. The current
branch also includes post-plan operator feedback commits; where older snippets
below still show all-uppercase labels, unpaged list callbacks, `/confirm` in
the help body, or stale M4 refusal copy, this section and the design spec are
the source of truth.

Reviewed unplanned commits:

- `46acb8c fix: tighten telegram operator compatibility edges`
  - `query_response()` now rejects `/stop`, `/resume`, `/cancel_all`, and
    `/close_all` with current authorization wording.
  - `setMyCommands` remains best effort but uses a short request timeout so
    Telegram polling is not delayed by the long HTTP client timeout.
- `457ba33 fix: polish telegram operator reply formatting`
  - All `◆` headings render as bold Title Case.
  - `/help` no longer advertises `/confirm <code>`; `/confirm` remains a
    fallback command path.
  - Status-style replies use bold Title Case labels and unbold values.
  - Position/order/trade/PnL replies use spaced multi-line rows; numeric
    fields are bold.
  - PnL and UPnL values show green/red/neutral markers.
  - Orders include price and position size; trades include position size.
  - Realized and total PnL stay `n/a` until a reliable realized-PnL ledger
    exists.
- `894a2a9 fix: paginate telegram operator lists`
  - Orders, trades, and events are paginated at 8 rows per page, capped at 5
    pages / 40 displayed rows.
  - Callback data supports `tgux:orders:<page>`, `tgux:trades:<page>`, and
    `tgux:events:<page>`; slash commands still open page 1.
- `2bffbea fix: keep pagination label in buttons only`
  - Page numbers are shown in inline keyboard buttons only, not in the message
    body.

Additional current-version tests should cover help text, Title Case/bold label
formatting, dense numeric rows, PnL markers, bounded pagination, page labels in
buttons only, compatibility control refusal, and the short command-registration
timeout.

---

### Task 1: Add Telegram Reply Model, HTML Escaping, And Keyboard Builders

**Files:**
- Modify: `crates/executor/src/telegram_query.rs`

- [ ] **Step 1: Write failing tests for HTML escaping and keyboard payloads**

Add these tests inside `#[cfg(test)] mod tests` in `crates/executor/src/telegram_query.rs`:

```rust
    #[test]
    fn html_escape_escapes_dynamic_telegram_values() {
        assert_eq!(
            html_escape("ETH<&>USDT"),
            "ETH&lt;&amp;&gt;USDT"
        );
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
        assert!(!text.contains("open"));
        assert!(!text.contains("set_param"));
        assert!(!text.contains("live"));
    }
```

- [ ] **Step 2: Run tests and verify RED**

Run:

```bash
cargo test -q -p prodigy-executor html_escape_escapes_dynamic_telegram_values navigation_keyboard_contains_query_and_control_buttons control_keyboard_contains_only_existing_control_commands
```

Expected: FAIL because `html_escape`, `navigation_keyboard`, and `control_keyboard` do not exist.

- [ ] **Step 3: Add the minimal reply and keyboard helpers**

Near the top of `crates/executor/src/telegram_query.rs`, after imports, add:

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TelegramReply {
    pub text: String,
    pub parse_mode: Option<&'static str>,
    pub reply_markup: Option<serde_json::Value>,
}

impl TelegramReply {
    fn html(text: String, reply_markup: Option<serde_json::Value>) -> Self {
        Self {
            text,
            parse_mode: Some("HTML"),
            reply_markup,
        }
    }

    fn plain(text: String) -> Self {
        Self {
            text,
            parse_mode: None,
            reply_markup: None,
        }
    }
}

fn html_escape(value: &str) -> String {
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

fn navigation_keyboard() -> serde_json::Value {
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

fn control_keyboard() -> serde_json::Value {
    inline_keyboard(vec![
        vec![
            button("Stop", "tgux:stop"),
            button("Resume", "tgux:resume"),
        ],
        vec![
            button("Cancel All", "tgux:cancel_all"),
            button("Close All", "tgux:close_all"),
        ],
        vec![button("Back", "tgux:status")],
    ])
}

fn close_all_confirm_keyboard() -> serde_json::Value {
    inline_keyboard(vec![
        vec![button("Confirm Close All", "tgux:confirm_close_all")],
        vec![button("Cancel", "tgux:cancel_close_all")],
    ])
}
```

- [ ] **Step 4: Run tests and verify GREEN**

Run:

```bash
cargo test -q -p prodigy-executor html_escape_escapes_dynamic_telegram_values navigation_keyboard_contains_query_and_control_buttons control_keyboard_contains_only_existing_control_commands
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/executor/src/telegram_query.rs
git commit -m "feat: add telegram UX reply primitives"
```

---

### Task 2: Format Read-Only Replies In Dark Editorial HTML

**Files:**
- Modify: `crates/executor/src/telegram_query.rs`

- [ ] **Step 1: Write failing tests for editorial reply layout**

Add these tests:

```rust
    #[test]
    fn status_reply_uses_editorial_html_layout_and_navigation_keyboard() {
        let conn = test_conn();
        crate::db::write_event(&conn, "info", "daemon", "daemon started", "{}").unwrap();

        let reply = query_reply(&conn, "/status").unwrap().unwrap();

        assert_eq!(reply.parse_mode, Some("HTML"));
        assert!(reply.text.contains("◆ <b>Prodigy Operator</b>"));
        assert!(reply.text.contains("<b>Mode</b> — DEMO"));
        assert!(reply.text.contains("<b>Daemon</b>"));
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
        assert!(reply.text.contains("◆ <b>Pnl</b>"));
        assert!(reply.text.contains("Realized — n/a"));
        assert!(reply.text.contains("Total — n/a"));
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
```

- [ ] **Step 2: Run tests and verify RED**

Run:

```bash
cargo test -q -p prodigy-executor status_reply_uses_editorial_html_layout_and_navigation_keyboard pnl_reply_keeps_realized_and_total_conservative dynamic_values_are_escaped_in_position_reply
```

Expected: FAIL because `query_reply` and the new HTML layout do not exist.

- [ ] **Step 3: Add `query_reply` and preserve `query_response`**

Replace the current `query_response` body with a wrapper, and add `query_reply`:

```rust
pub fn query_response(conn: &Connection, text: &str) -> Result<Option<String>> {
    Ok(query_reply(conn, text)?.map(|reply| reply.text))
}

pub fn query_reply(conn: &Connection, text: &str) -> Result<Option<TelegramReply>> {
    let command = text.split_whitespace().next().unwrap_or("");
    match command {
        "/status" => Ok(Some(status_reply(conn)?)),
        "/positions" => Ok(Some(positions_reply(conn)?)),
        "/orders" => Ok(Some(orders_reply(conn, 1)?)),
        "/pnl" => Ok(Some(pnl_reply(conn)?)),
        "/risk" => Ok(Some(risk_reply(conn)?)),
        "/trades" => Ok(Some(trades_reply(conn, 1)?)),
        "/events" => Ok(Some(events_reply(conn, 1)?)),
        "/smoke_status" => Ok(Some(smoke_status_reply(conn)?)),
        "/help" => Ok(Some(help_reply())),
        "/stop" | "/resume" | "/cancel_all" | "/close_all" => Ok(Some(TelegramReply::plain(
            "operator controls require authorized Telegram access".to_string(),
        ))),
        _ => Ok(None),
    }
}
```

- [ ] **Step 4: Add minimal editorial formatting helpers**

Add:

```rust
fn row(label: &str, value: impl ToString) -> String {
    format!(
        "<b>{}</b> — {}",
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
    let mut text = format!("◆ <b>{}</b>\n\n{}", html_escape(title), rows.join("\n"));
    if let Some(footer) = footer {
        text.push_str("\n\n— ");
        text.push_str(&html_escape(&footer));
    }
    TelegramReply::html(text, Some(navigation_keyboard()))
}
```

- [ ] **Step 5: Convert read-only response functions**

Keep the existing SQL, but change the string construction. Rename old functions or replace them with these names:

```rust
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
            row("MANUAL", manual_overrides),
            row("PENDING INTENTS", pending),
            row("PENDING CONTROLS", pending_controls),
            row("LATEST ERROR", latest_critical_error(conn)?),
        ],
        None,
    ))
}
```

Use the same pattern for:

- `positions_reply`
- `orders_reply`
- `trades_reply`
- `pnl_reply`
- `risk_reply`
- `events_reply`
- `smoke_status_reply`
- `help_reply`

For empty lists, use rows like `row("POSITIONS", "NONE")`.

For `/pnl`, keep:

```rust
row("REALIZED", "n/a")
row("TOTAL", "n/a")
```

Do not introduce realized PnL calculations.

- [ ] **Step 6: Update `operator_response` to continue returning text**

After Task 3 adds `operator_reply`, `operator_response` will wrap that. For this task, keep `operator_response` compiling by calling the new read-only reply functions and returning `.text`.

- [ ] **Step 7: Run focused tests**

Run:

```bash
cargo test -q -p prodigy-executor status_reply_uses_editorial_html_layout_and_navigation_keyboard pnl_reply_keeps_realized_and_total_conservative dynamic_values_are_escaped_in_position_reply
```

Expected: PASS.

- [ ] **Step 8: Run existing Telegram query tests**

Run:

```bash
cargo test -q -p prodigy-executor telegram_query
```

Expected: PASS. If old assertions fail only because text labels changed, update assertions to check stable semantics, not exact plain-text formatting.

- [ ] **Step 9: Commit**

```bash
git add crates/executor/src/telegram_query.rs
git commit -m "feat: format telegram replies as editorial HTML"
```

---

### Task 3: Add Operator Reply API And Read-Only Callback Handling

**Files:**
- Modify: `crates/executor/src/telegram_query.rs`

- [ ] **Step 1: Write failing tests for callback handling**

Add:

```rust
    #[test]
    fn read_only_callbacks_return_same_dashboard_replies() {
        let conn = test_conn();
        crate::db::write_event(&conn, "info", "daemon", "daemon started", "{}").unwrap();

        let callback = operator_callback_reply(
            &conn,
            "tgux:status",
            "123",
            &["123".to_string()],
            1_000,
        )
        .unwrap()
        .unwrap();
        let slash = operator_reply(&conn, "/status", "123", &["123".to_string()], 1_000)
            .unwrap()
            .unwrap();

        assert_eq!(callback.text, slash.text);
        assert_eq!(callback.parse_mode, Some("HTML"));
        assert!(callback.reply_markup.is_some());
    }

    #[test]
    fn unauthorized_callback_gets_no_sqlite_state_and_no_control() {
        let conn = test_conn();
        let response = operator_callback_reply(
            &conn,
            "tgux:status",
            "999",
            &["123".to_string()],
            1_000,
        )
        .unwrap()
        .unwrap();

        let command_count: i64 = conn
            .query_row("select count(*) from control_commands", [], |r| r.get(0))
            .unwrap();

        assert!(response.text.contains("unauthorized"));
        assert_eq!(command_count, 0);
    }

    #[test]
    fn unknown_callback_does_not_queue_any_command() {
        let conn = test_conn();
        let response = operator_callback_reply(
            &conn,
            "tgux:open",
            "123",
            &["123".to_string()],
            1_000,
        )
        .unwrap()
        .unwrap();

        let command_count: i64 = conn
            .query_row("select count(*) from control_commands", [], |r| r.get(0))
            .unwrap();

        assert!(response.text.contains("unsupported"));
        assert_eq!(command_count, 0);
    }
```

- [ ] **Step 2: Run tests and verify RED**

Run:

```bash
cargo test -q -p prodigy-executor read_only_callbacks_return_same_dashboard_replies unauthorized_callback_gets_no_sqlite_state_and_no_control unknown_callback_does_not_queue_any_command
```

Expected: FAIL because `operator_reply` and `operator_callback_reply` do not exist.

- [ ] **Step 3: Add `operator_reply` and keep `operator_response`**

Replace `operator_response` with a wrapper and add:

```rust
pub fn operator_response(
    conn: &Connection,
    text: &str,
    from_user_id: &str,
    allowed_user_ids: &[String],
    now_ms: i64,
) -> Result<Option<String>> {
    Ok(operator_reply(conn, text, from_user_id, allowed_user_ids, now_ms)?
        .map(|reply| reply.text))
}

pub fn operator_reply(
    conn: &Connection,
    text: &str,
    from_user_id: &str,
    allowed_user_ids: &[String],
    now_ms: i64,
) -> Result<Option<TelegramReply>> {
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
        return Ok(Some(TelegramReply::plain("unauthorized".to_string())));
    }
    match command {
        "/help" => Ok(Some(help_reply())),
        "/status" | "/positions" | "/orders" | "/trades" | "/pnl" | "/risk" | "/events"
        | "/smoke_status" => query_reply(conn, command),
        "/stop" | "/resume" | "/cancel_all" | "/close_all" | "/confirm" => {
            match control_reply(conn, text, from_user_id, now_ms) {
                Ok(reply) => Ok(reply),
                Err(err) => Ok(Some(TelegramReply::plain(control_failure_response(err)))),
            }
        }
        _ => Ok(None),
    }
}
```

Rename the current `control_response` to `control_reply` and make it return `Result<Option<TelegramReply>>`. For direct queue replies, wrap the existing text with `TelegramReply::html(..., Some(control_keyboard()))` or `TelegramReply::plain(...)` if the text contains no markup.

- [ ] **Step 4: Add callback mapping**

Add:

```rust
pub fn operator_callback_reply(
    conn: &Connection,
    callback_data: &str,
    from_user_id: &str,
    allowed_user_ids: &[String],
    now_ms: i64,
) -> Result<Option<TelegramReply>> {
    if !allowed_user_ids.iter().any(|id| id == from_user_id) {
        crate::db::write_event(
            conn,
            "warning",
            "telegram",
            "unauthorized telegram callback",
            &serde_json::json!({
                "from_user_id": from_user_id,
                "callback_data": callback_data,
            })
            .to_string(),
        )
        .ok();
        return Ok(Some(TelegramReply::plain("unauthorized".to_string())));
    }

    if let Some(page) = callback_page(callback_data, "tgux:orders:") {
        return orders_reply(conn, page).map(Some);
    }
    if let Some(page) = callback_page(callback_data, "tgux:trades:") {
        return trades_reply(conn, page).map(Some);
    }
    if let Some(page) = callback_page(callback_data, "tgux:events:") {
        return events_reply(conn, page).map(Some);
    }

    match callback_data {
        "tgux:status" => query_reply(conn, "/status"),
        "tgux:pnl" => query_reply(conn, "/pnl"),
        "tgux:risk" => query_reply(conn, "/risk"),
        "tgux:positions" => query_reply(conn, "/positions"),
        "tgux:orders" => orders_reply(conn, 1).map(Some),
        "tgux:trades" => trades_reply(conn, 1).map(Some),
        "tgux:events" => events_reply(conn, 1).map(Some),
        "tgux:smoke" => query_reply(conn, "/smoke_status"),
        "tgux:help" => Ok(Some(help_reply())),
        "tgux:control" => Ok(Some(control_panel_reply())),
        "tgux:stop" => control_reply(conn, "/stop", from_user_id, now_ms),
        "tgux:resume" => control_reply(conn, "/resume", from_user_id, now_ms),
        "tgux:cancel_all" => control_reply(conn, "/cancel_all", from_user_id, now_ms),
        "tgux:close_all" => control_reply(conn, "/close_all", from_user_id, now_ms),
        _ => Ok(Some(TelegramReply::plain("unsupported button".to_string()))),
    }
}

fn control_panel_reply() -> TelegramReply {
    TelegramReply::html(
        "◆ <b>CONTROL</b>\n\n<b>STOP</b> — block new opening exposure\n<b>RESUME</b> — clear operator stop\n<b>CANCEL ALL</b> — system working orders only\n<b>CLOSE ALL</b> — system positions only".to_string(),
        Some(control_keyboard()),
    )
}
```

Task 4 adds `tgux:confirm_close_all` and `tgux:cancel_close_all`. Until then,
those callbacks intentionally fall through to `"unsupported button"`; do not
commit any placeholder branch for them.

- [ ] **Step 5: Run tests and verify GREEN for read-only callback behavior**

Run:

```bash
cargo test -q -p prodigy-executor read_only_callbacks_return_same_dashboard_replies unauthorized_callback_gets_no_sqlite_state_and_no_control unknown_callback_does_not_queue_any_command
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/executor/src/telegram_query.rs
git commit -m "feat: add telegram callback response mapping"
```

---

### Task 4: Add Button-Based Close-All Confirmation

**Files:**
- Modify: `crates/executor/src/telegram_query.rs`

- [ ] **Step 1: Write failing tests for button confirmation and cancel**

Add:

```rust
    #[test]
    fn close_all_button_requires_button_confirmation_before_queueing() {
        let conn = test_conn();
        let first = operator_callback_reply(
            &conn,
            "tgux:close_all",
            "123",
            &["123".to_string()],
            10_000,
        )
        .unwrap()
        .unwrap();

        assert!(first.text.contains("CONFIRM CLOSE ALL"));
        assert!(serde_json::to_string(&first.reply_markup).unwrap().contains("tgux:confirm_close_all"));
        assert_eq!(
            conn.query_row("select count(*) from control_commands", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            0
        );

        let second = operator_callback_reply(
            &conn,
            "tgux:confirm_close_all",
            "123",
            &["123".to_string()],
            20_000,
        )
        .unwrap()
        .unwrap();

        let command = conn
            .query_row("select command from control_commands", [], |r| r.get::<_, String>(0))
            .unwrap();
        assert!(second.text.contains("queued"));
        assert_eq!(command, "close_all");
    }

    #[test]
    fn close_all_button_confirmation_rejects_wrong_user_expiry_and_replay() {
        let conn = test_conn();
        operator_callback_reply(&conn, "tgux:close_all", "123", &["123".to_string(), "456".to_string()], 10_000)
            .unwrap()
            .unwrap();

        let wrong_user = operator_callback_reply(
            &conn,
            "tgux:confirm_close_all",
            "456",
            &["123".to_string(), "456".to_string()],
            20_000,
        )
        .unwrap()
        .unwrap();
        assert!(wrong_user.text.contains("rejected"));

        let expired = operator_callback_reply(
            &conn,
            "tgux:confirm_close_all",
            "123",
            &["123".to_string()],
            70_000,
        )
        .unwrap()
        .unwrap();
        assert!(expired.text.contains("expired"));

        operator_callback_reply(&conn, "tgux:close_all", "123", &["123".to_string()], 100_000)
            .unwrap()
            .unwrap();
        let accepted = operator_callback_reply(
            &conn,
            "tgux:confirm_close_all",
            "123",
            &["123".to_string()],
            110_000,
        )
        .unwrap()
        .unwrap();
        let replay = operator_callback_reply(
            &conn,
            "tgux:confirm_close_all",
            "123",
            &["123".to_string()],
            111_000,
        )
        .unwrap()
        .unwrap();

        assert!(accepted.text.contains("queued"));
        assert!(replay.text.contains("rejected"));
        assert_eq!(
            conn.query_row("select count(*) from control_commands where command = 'close_all'", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            1
        );
    }

    #[test]
    fn cancel_close_all_button_clears_pending_confirmation_without_queueing() {
        let conn = test_conn();
        operator_callback_reply(&conn, "tgux:close_all", "123", &["123".to_string()], 10_000)
            .unwrap()
            .unwrap();

        let cancelled = operator_callback_reply(
            &conn,
            "tgux:cancel_close_all",
            "123",
            &["123".to_string()],
            20_000,
        )
        .unwrap()
        .unwrap();

        assert!(cancelled.text.contains("cancelled"));
        assert_eq!(
            conn.query_row("select count(*) from control_commands", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            0
        );
    }
```

- [ ] **Step 2: Run tests and verify RED**

Run:

```bash
cargo test -q -p prodigy-executor close_all_button_requires_button_confirmation_before_queueing close_all_button_confirmation_rejects_wrong_user_expiry_and_replay cancel_close_all_button_clears_pending_confirmation_without_queueing
```

Expected: FAIL because button confirmation support is still missing.

- [ ] **Step 3: Update close-all confirmation start reply**

Change `start_close_all_confirmation` to return `TelegramReply`, not `String`, through a new function:

```rust
fn start_close_all_confirmation_reply(
    conn: &Connection,
    requested_by: &str,
    now_ms: i64,
) -> Result<TelegramReply> {
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
    Ok(TelegramReply::html(
        "◆ <b>CONFIRM CLOSE ALL</b>\n\n<b>SCOPE</b> — system positions only\n<b>MANUAL</b> — skipped and audited\n<b>EXPIRES</b> — 60s\n\nUse the button below, or fallback to /confirm ".to_string()
            + &html_escape(&code),
        Some(close_all_confirm_keyboard()),
    ))
}
```

Keep the old `/confirm <code>` fallback working by preserving the generated code and existing `confirm_close_all` logic.

- [ ] **Step 4: Add button confirm/cancel helpers**

Add:

```rust
fn confirm_close_all_button(
    conn: &Connection,
    requested_by: &str,
    now_ms: i64,
) -> Result<TelegramReply> {
    confirm_close_all_shared(conn, None, requested_by, now_ms)
}

fn confirm_close_all_code(
    conn: &Connection,
    text: &str,
    requested_by: &str,
    now_ms: i64,
) -> Result<TelegramReply> {
    confirm_close_all_shared(conn, text.split_whitespace().nth(1), requested_by, now_ms)
}

fn confirm_close_all_shared(
    conn: &Connection,
    code: Option<&str>,
    requested_by: &str,
    now_ms: i64,
) -> Result<TelegramReply> {
    let key = format!("close_all_confirm:{requested_by}");
    let Some(raw) = crate::db::get_executor_state(conn, &key)? else {
        audit(conn, "telegram close_all confirmation rejected", &serde_json::json!({
            "reason": "missing",
            "requested_by": requested_by,
        }).to_string())?;
        return Ok(TelegramReply::plain("confirmation rejected".to_string()));
    };
    let value: serde_json::Value = serde_json::from_str(&raw).unwrap_or_else(|_| serde_json::json!({}));
    if value.get("status").and_then(serde_json::Value::as_str) == Some("used") {
        audit(conn, "telegram close_all confirmation rejected", &serde_json::json!({
            "reason": "used",
            "requested_by": requested_by,
        }).to_string())?;
        return Ok(TelegramReply::plain("confirmation rejected".to_string()));
    }
    if value.get("status").and_then(serde_json::Value::as_str) == Some("cancelled") {
        audit(conn, "telegram close_all confirmation rejected", &serde_json::json!({
            "reason": "cancelled",
            "requested_by": requested_by,
        }).to_string())?;
        return Ok(TelegramReply::plain("confirmation rejected".to_string()));
    }
    let expires_ms = value.get("expires_ms").and_then(serde_json::Value::as_i64).unwrap_or(0);
    if now_ms >= expires_ms {
        audit(conn, "telegram close_all confirmation expired", &serde_json::json!({
            "reason": "expired",
            "requested_by": requested_by,
        }).to_string())?;
        return Ok(TelegramReply::plain("confirmation expired".to_string()));
    }
    if let Some(code) = code {
        let expected = value.get("code_hash").and_then(serde_json::Value::as_str).unwrap_or("");
        if expected != sha256_hex(code) {
            audit(conn, "telegram close_all confirmation rejected", &serde_json::json!({
                "reason": "bad_code",
                "requested_by": requested_by,
            }).to_string())?;
            return Ok(TelegramReply::plain("confirmation rejected".to_string()));
        }
    }
    let command_id = with_savepoint(conn, "telegram_confirm_close_all", |conn| {
        let command_id = queue_control_command(conn, "close_all", requested_by)?;
        audit(conn, "telegram close_all confirmation accepted", &serde_json::json!({
            "command_id": command_id,
            "requested_by": requested_by,
        }).to_string())?;
        crate::db::set_executor_state(conn, &key, &serde_json::json!({
            "status": "used",
            "requested_by": requested_by,
            "command_id": command_id,
            "used_ms": now_ms,
        }).to_string())?;
        Ok(command_id)
    })?;
    Ok(TelegramReply::html(
        format!("◆ <b>CLOSE ALL QUEUED</b>\n\n<b>COMMAND</b> — {}", html_escape(&command_id)),
        Some(navigation_keyboard()),
    ))
}

fn cancel_close_all_button(
    conn: &Connection,
    requested_by: &str,
    now_ms: i64,
) -> Result<TelegramReply> {
    let key = format!("close_all_confirm:{requested_by}");
    with_savepoint(conn, "telegram_cancel_close_all", |conn| {
        crate::db::set_executor_state(conn, &key, &serde_json::json!({
            "status": "cancelled",
            "requested_by": requested_by,
            "cancelled_ms": now_ms,
        }).to_string())?;
        audit(conn, "telegram close_all confirmation cancelled", &serde_json::json!({
            "requested_by": requested_by,
            "cancelled_ms": now_ms,
        }).to_string())
    })?;
    Ok(TelegramReply::html(
        "◆ <b>CLOSE ALL CANCELLED</b>".to_string(),
        Some(control_keyboard()),
    ))
}
```

Then update `/confirm` to call `confirm_close_all_code`.

- [ ] **Step 5: Wire confirm/cancel callback arms**

In `operator_callback_reply`, add these match arms above the unknown callback arm:

```rust
        "tgux:confirm_close_all" => confirm_close_all_button(conn, from_user_id, now_ms).map(Some),
        "tgux:cancel_close_all" => cancel_close_all_button(conn, from_user_id, now_ms).map(Some),
```

- [ ] **Step 6: Run tests and verify GREEN**

Run:

```bash
cargo test -q -p prodigy-executor close_all_button_requires_button_confirmation_before_queueing close_all_button_confirmation_rejects_wrong_user_expiry_and_replay cancel_close_all_button_clears_pending_confirmation_without_queueing
```

Expected: PASS.

- [ ] **Step 7: Run existing confirmation tests**

Run:

```bash
cargo test -q -p prodigy-executor close_all
```

Expected: PASS. Existing `/confirm <code>` tests must continue to pass.

- [ ] **Step 8: Commit**

```bash
git add crates/executor/src/telegram_query.rs
git commit -m "feat: add button confirmation for telegram close all"
```

---

### Task 5: Wire Callback Queries, Rich SendMessage, And Command Menu In Daemon

**Files:**
- Modify: `crates/executor/src/daemon.rs`
- Modify: `crates/executor/src/telegram_query.rs`

- [ ] **Step 1: Write failing daemon parser and payload tests**

Add tests in `crates/executor/src/daemon.rs`:

```rust
    #[test]
    fn telegram_update_parts_parse_callback_query_for_auth_reply_and_answer() {
        let update = serde_json::json!({
            "update_id": 43,
            "callback_query": {
                "id": "callback-1",
                "from": { "id": 123 },
                "data": "tgux:status",
                "message": {
                    "chat": { "id": 456 },
                    "message_id": 99
                }
            }
        });

        let parts = telegram_update_parts(&update).unwrap();

        assert_eq!(parts.update_id, 43);
        assert_eq!(parts.from_user_id, "123");
        assert_eq!(parts.reply_chat_id, "456");
        assert_eq!(parts.callback_query_id.as_deref(), Some("callback-1"));
        assert_eq!(parts.callback_data.as_deref(), Some("tgux:status"));
        assert!(parts.text.is_none());
    }

    #[test]
    fn telegram_send_message_form_includes_html_and_reply_markup() {
        let reply = crate::telegram_query::TelegramReply {
            text: "<b>Status</b>".to_string(),
            parse_mode: Some("HTML"),
            reply_markup: Some(serde_json::json!({"inline_keyboard": []})),
        };

        let form = telegram_send_message_form("456", &reply);

        assert!(form.contains(&("chat_id".to_string(), "456".to_string())));
        assert!(form.contains(&("text".to_string(), "<b>Status</b>".to_string())));
        assert!(form.contains(&("parse_mode".to_string(), "HTML".to_string())));
        assert!(form.iter().any(|(k, v)| k == "reply_markup" && v.contains("inline_keyboard")));
    }
```

Add tests in `crates/executor/src/telegram_query.rs`:

```rust
    #[test]
    fn bot_commands_payload_contains_existing_commands_only() {
        let payload = bot_commands_payload();
        let text = serde_json::to_string(&payload).unwrap();

        for command in [
            "help",
            "status",
            "positions",
            "orders",
            "trades",
            "pnl",
            "risk",
            "events",
            "smoke_status",
            "stop",
            "resume",
            "cancel_all",
            "close_all",
            "confirm",
        ] {
            assert!(text.contains(&format!("\"command\":\"{command}\"")), "missing {command}");
        }
        assert!(!text.contains("\"open\""));
        assert!(!text.contains("set_param"));
        assert!(!text.contains("model_debug"));
        assert!(!text.contains("shell"));
        assert!(!text.contains("live"));
    }
```

- [ ] **Step 2: Run tests and verify RED**

Run:

```bash
cargo test -q -p prodigy-executor telegram_update_parts_parse_callback_query_for_auth_reply_and_answer telegram_send_message_form_includes_html_and_reply_markup bot_commands_payload_contains_existing_commands_only
```

Expected: FAIL because callback fields, send form helper, and command payload do not exist.

- [ ] **Step 3: Extend Telegram update parsing**

In `daemon.rs`, change `TelegramUpdateParts`:

```rust
#[derive(Debug, Clone)]
struct TelegramUpdateParts {
    update_id: i64,
    from_user_id: String,
    reply_chat_id: String,
    text: Option<String>,
    callback_query_id: Option<String>,
    callback_data: Option<String>,
}
```

Replace `telegram_update_parts` with:

```rust
fn telegram_update_parts(update: &serde_json::Value) -> Option<TelegramUpdateParts> {
    let update_id = update.get("update_id")?.as_i64()?;
    if let Some(message) = update.get("message") {
        return Some(TelegramUpdateParts {
            update_id,
            from_user_id: message.get("from")?.get("id")?.as_i64()?.to_string(),
            reply_chat_id: message.get("chat")?.get("id")?.as_i64()?.to_string(),
            text: Some(message.get("text")?.as_str()?.to_string()),
            callback_query_id: None,
            callback_data: None,
        });
    }
    let callback = update.get("callback_query")?;
    Some(TelegramUpdateParts {
        update_id,
        from_user_id: callback.get("from")?.get("id")?.as_i64()?.to_string(),
        reply_chat_id: callback
            .get("message")?
            .get("chat")?
            .get("id")?
            .as_i64()?
            .to_string(),
        text: None,
        callback_query_id: Some(callback.get("id")?.as_str()?.to_string()),
        callback_data: Some(callback.get("data")?.as_str()?.to_string()),
    })
}
```

- [ ] **Step 4: Add sendMessage form and command payload helpers**

In `daemon.rs`, add:

```rust
fn telegram_send_message_form(
    chat_id: &str,
    reply: &crate::telegram_query::TelegramReply,
) -> Vec<(String, String)> {
    let mut form = vec![
        ("chat_id".to_string(), chat_id.to_string()),
        ("text".to_string(), reply.text.clone()),
    ];
    if let Some(parse_mode) = reply.parse_mode {
        form.push(("parse_mode".to_string(), parse_mode.to_string()));
    }
    if let Some(markup) = &reply.reply_markup {
        form.push(("reply_markup".to_string(), markup.to_string()));
    }
    form
}
```

In `telegram_query.rs`, add:

```rust
pub fn bot_commands_payload() -> serde_json::Value {
    serde_json::json!({
        "commands": [
            { "command": "help", "description": "Show commands and controls" },
            { "command": "status", "description": "System status summary" },
            { "command": "positions", "description": "Current positions" },
            { "command": "orders", "description": "Working and recent orders" },
            { "command": "trades", "description": "Recent fills" },
            { "command": "pnl", "description": "PnL summary" },
            { "command": "risk", "description": "Risk state" },
            { "command": "events", "description": "Recent warnings and errors" },
            { "command": "smoke_status", "description": "Smoke run status" },
            { "command": "stop", "description": "Stop new opening exposure" },
            { "command": "resume", "description": "Resume new opening exposure" },
            { "command": "cancel_all", "description": "Cancel system working orders" },
            { "command": "close_all", "description": "Confirm and close system positions" },
            { "command": "confirm", "description": "Confirm pending close-all fallback" }
        ]
    })
}
```

- [ ] **Step 5: Register commands best effort**

In `run_telegram_query_loop`, after creating `client`, call:

```rust
let set_commands_url = format!("https://api.telegram.org/bot{token}/setMyCommands");
let _ = client
    .post(set_commands_url)
    .timeout(telegram_command_registration_timeout())
    .json(&crate::telegram_query::bot_commands_payload())
    .send()
    .await;
```

Do not propagate errors.

- [ ] **Step 6: Route messages and callbacks through rich reply APIs**

Inside the update loop, replace the call to `operator_response` with:

```rust
let now_ms = crate::bitget::now_ms().parse::<i64>().unwrap_or(0);
let reply = if let Some(data) = parts.callback_data.as_deref() {
    crate::telegram_query::operator_callback_reply(
        &conn,
        data,
        &parts.from_user_id,
        &cfg.telegram_allowed_user_ids,
        now_ms,
    )
} else if let Some(text) = parts.text.as_deref() {
    crate::telegram_query::operator_reply(
        &conn,
        text,
        &parts.from_user_id,
        &cfg.telegram_allowed_user_ids,
        now_ms,
    )
} else {
    Ok(None)
};
```

If `parts.callback_query_id` is present, send `answerCallbackQuery` best effort:

```rust
if let Some(callback_id) = parts.callback_query_id.as_deref() {
    let answer_url = format!("https://api.telegram.org/bot{token}/answerCallbackQuery");
    let _ = client
        .post(answer_url)
        .form(&[("callback_query_id", callback_id)])
        .send()
        .await;
}
```

When sending a reply, use:

```rust
let form = telegram_send_message_form(&parts.reply_chat_id, &reply);
let _ = client.post(send_url).form(&form).send().await;
```

- [ ] **Step 7: Run focused tests and verify GREEN**

Run:

```bash
cargo test -q -p prodigy-executor telegram_update_parts_parse_callback_query_for_auth_reply_and_answer telegram_send_message_form_includes_html_and_reply_markup bot_commands_payload_contains_existing_commands_only
```

Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add crates/executor/src/daemon.rs crates/executor/src/telegram_query.rs
git commit -m "feat: wire telegram buttons and command menu"
```

---

### Task 6: Extend Safety Scope Scan And Final Verification

**Files:**
- Modify: `tests/test_executor_integration.py`

- [ ] **Step 1: Write failing scope scan assertions for M7.5 boundaries**

Update `test_m7_live_readiness_scope_scan_targets_dangerous_patterns_only` so it also scans for direct Bitget calls from Telegram UI code. Add this after the existing `telegram_query` forbidden scan:

```python
    daemon_rs = (repo_root / "crates/executor/src/daemon.rs").read_text()
    telegram_loop = daemon_rs.split("pub async fn run_telegram_query_loop", 1)[1]
    telegram_loop = telegram_loop.split("/// Record a `websocket_auth_failed`", 1)[0]
    for forbidden in ("BitgetRestClient", "/api/v2", "place-order", "cancel-order"):
        assert forbidden not in telegram_loop
```

Add a check that no remote open/control expansion pattern appears in production code:

```python
    m75_forbidden = subprocess.run(
        [
            "rg",
            "-n",
            "tgux:open|tgux:set_param|tgux:model_debug|tgux:shell|tgux:live|remote_shell|model_debug_from_telegram",
            "src",
            "crates",
        ],
        check=False,
        text=True,
        capture_output=True,
        cwd=repo_root,
    )
    assert m75_forbidden.returncode == 1, m75_forbidden.stdout + m75_forbidden.stderr
```

If this catches test strings in `#[cfg(test)]`, reuse the existing `rust_cfg_test_ranges` filtering helper instead of weakening the production scan.

- [ ] **Step 2: Run the focused Python scope scan**

Run:

```bash
mamba run -n quantmamba python -m pytest -q tests/test_executor_integration.py::test_m7_live_readiness_scope_scan_targets_dangerous_patterns_only
```

Expected: PASS.

- [ ] **Step 3: Run full Rust and Python verification**

Run:

```bash
mamba run -n quantmamba python -m pytest -q
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test -q
git diff --check main...HEAD
```

Expected: all commands exit 0.

- [ ] **Step 4: Manual out-of-scope scan**

Run:

```bash
rg -n "tgux:open|tgux:set_param|tgux:model_debug|tgux:shell|tgux:live|remote_open|open_from_telegram|TELEGRAM_LIVE|BITGET_LIVE|ENABLE_LIVE_TRADING|LIVE_TRADING_ENABLED|remote_shell|shell_from_telegram|model_debug_from_telegram|remote_model_debug" src crates tests docs/superpowers/specs docs/superpowers/plans
```

Expected:

- Matches in M7/M7.5 specs and plans are allowed.
- Matches in Rust `#[cfg(test)]` test strings are allowed.
- Matches in production code under `src` or `crates` are blockers unless they are safe rejection code.

- [ ] **Step 5: Commit**

```bash
git add tests/test_executor_integration.py
git commit -m "test: extend telegram UX safety scope scan"
```

If Step 3 required small fixes in Rust files, include them in the commit with the scope scan.

---

### Task 7: Apply Operator Feedback Refinements

**Files:**
- Modify: `crates/executor/src/telegram_query.rs`
- Modify: `crates/executor/src/daemon.rs`

- [x] **Step 1: Tighten compatibility edges**

  Add failing coverage for the legacy `query_response()` control-refusal path
  and command-registration timeout, then fix:

  - reject `/cancel_all` with the other controls;
  - replace stale M4 refusal copy with current authorization wording;
  - keep `setMyCommands` best effort but bound it with a short timeout.

  Commit: `46acb8c fix: tighten telegram operator compatibility edges`.

- [x] **Step 2: Apply live operator formatting feedback**

  Add failing formatting tests before changing output, then refine Telegram
  replies:

  - bold every `◆` heading and render it in Title Case;
  - remove `/confirm <code>` from `/help` while preserving the fallback;
  - render `/help` groups as bold `Read` and `Control`;
  - use bold Title Case labels and unbold values in status-style replies;
  - format positions, orders, trades, and PnL as spaced multi-line rows;
  - bold numeric values and add green/red/neutral markers for PnL/UPnL;
  - include price and position size in orders;
  - include position size in trades;
  - keep realized and total PnL as `n/a` until a reliable realized-PnL ledger
    exists.

  Commit: `457ba33 fix: polish telegram operator reply formatting`.

- [x] **Step 3: Paginate high-cardinality operator lists**

  Add failing tests for page callbacks and row bounds, then implement:

  - 8 rows per page;
  - maximum 5 pages / 40 displayed rows;
  - `tgux:orders:<page>`, `tgux:trades:<page>`, and `tgux:events:<page>`;
  - slash commands open page 1.

  Commit: `894a2a9 fix: paginate telegram operator lists`.

- [x] **Step 4: Keep pagination labels in buttons only**

  Add failing tests that reject body footer page labels, then keep page numbers
  only in inline keyboard buttons.

  Commit: `2bffbea fix: keep pagination label in buttons only`.

---

## Final Review Checklist

Before reporting completion:

- [ ] Slash commands still work.
- [ ] Buttons map to existing command semantics only.
- [ ] `/close_all` cannot queue without a second confirmation action.
- [ ] Button confirmation is same-user, expiring, one-use, and audited.
- [ ] Telegram callbacks never call Bitget.
- [ ] Telegram command menu contains existing commands only.
- [ ] Dynamic values are HTML-escaped.
- [ ] No new dependencies were added.
- [ ] No live trading path was added.
- [ ] Full verification commands pass.
