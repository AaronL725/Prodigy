# Crypto Quant M7.5 Telegram Operator UX Design

## Goal

M7.5 upgrades the Telegram operator experience while preserving the existing
command interface and safety boundary.

The bot should feel like a polished dark luxury operator dashboard on mobile:
editorial, restrained, high-end, and easy to scan. It must not look like a
generic emoji-heavy AI-generated dashboard.

M7.5 does not change trading logic. Telegram remains an operator interface that
reads SQLite and queues control commands through SQLite. Rust executor remains
the only component that executes controls.

## Style Direction

Use the selected **Editorial Symbols** direction, refined after operator
testing:

- Dark-client friendly message composition.
- Sparse luxury cues: `◆`, `—`, `●`, Title Case section labels.
- Minimal emoji. Use green/red status lights next to PnL/UPnL; reserve other
  symbols for risk or danger states.
- No fake branding, no logo imitation, and no excessive gold/ornament text.
- Headings use `◆ <b>Title Case</b>`.
- Status-style rows use a two-column label/value layout:
  - left: bold Title Case labels such as `Mode`, `Daemon`, `Risk`, `Equity`;
  - right: unbold values such as `Demo`, `Running`, `Clear`, `1,000.00`.
- Dense market rows may use multi-line label/value blocks; numeric fields
  should be bold.

Telegram cannot enforce custom fonts, background colors, or CSS. The
implementation should use Telegram-native capabilities only:

- `sendMessage` with HTML parse mode;
- safe HTML escaping for dynamic values;
- inline keyboards;
- callback query handling;
- optional `setMyCommands` registration for discoverability.

HTML is preferred over MarkdownV2 because dynamic Rust messages only need a
small escaping helper for `&`, `<`, and `>`. MarkdownV2 escaping is easier to
break and adds no meaningful value for this milestone.

## Commands Preserved

All current slash commands remain valid:

- `/help`
- `/status`
- `/positions`
- `/orders`
- `/trades`
- `/pnl`
- `/risk`
- `/events`
- `/smoke_status`
- `/stop`
- `/resume`
- `/cancel_all`
- `/close_all`
- `/confirm`

M7.5 adds button shortcuts for these commands. Buttons are convenience
wrappers, not new commands.

## Read-Only Query UX

Query replies should be formatted as concise HTML messages:

- `/status`: first-screen system summary with daemon, signal, reconcile,
  operator stop, manual override count, pending counts, and latest error.
- `/pnl`: conservative PnL card with unrealized, equity, realized `n/a`, total
  `n/a` unless reliable realized PnL exists; show a green/red/neutral marker
  next to PnL values.
- `/risk`: risk state summary first, then contributing states, using the same
  status-style row treatment.
- `/positions`: compact multi-line rows grouped by symbol, side, ownership,
  notional, entry, and unrealized PnL; use Title Case `Position` labels, bold
  numeric values, and green/red markers for UPnL.
- `/orders`: working system orders first, then recent orders, including price,
  order size, filled size, and status.
- `/trades`: recent fills with symbol, side, price, size, fee, position size,
  and timestamp.
- `/events`: warning/error/critical events only, newest first.
- `/smoke_status`: local smoke status only, using the same status-style row
  treatment.

High-cardinality read-only lists must paginate:

- `/orders`, `/trades`, and `/events` show 8 rows per page.
- Show at most 5 pages / 40 rows.
- Page labels belong in inline keyboard buttons only; do not add body footer
  text like `— Page 3`.

Every read-only reply should include an inline keyboard with the most useful
navigation buttons:

- row 1: `Status`, `PnL`, `Risk`;
- row 2: `Positions`, `Orders`, `Trades`;
- row 3: `Events`, `Smoke`, `Help`;
- row 4: `Control`.

Buttons should map to the same internal response functions used by slash
commands.

## Control UX

Control buttons should be available from the control panel:

- `Stop`
- `Resume`
- `Cancel All`
- `Close All`

Safety rules stay unchanged:

- Telegram never calls Bitget.
- Telegram only writes SQLite `control_commands`, `executor_state`, and
  `events`.
- Unauthorized users cannot queue commands.
- `/stop`, `/resume`, and `/cancel_all` may be queued directly by an allowed
  user.
- `/cancel_all` still means system-owned working orders only.
- `/close_all` still means system-owned positions only.
- Imported/manual positions must be skipped by Rust executor and audited.
- Every control path must write audit events.

Button presses must run through the same authorization check as slash commands.
The legacy read-only `query_response()` compatibility path must reject control
commands, including `/cancel_all`, with current non-milestone wording rather
than stale M4 copy.

## Close-All Confirmation

`Close All` should be optimized for mobile and use buttons rather than manual
code entry.

Flow:

1. User taps `Close All`.
2. Bot replies with a high-signal confirmation message:
   - clearly says this only queues a system-position close command;
   - says manual/imported positions are not closed;
   - shows expiration time or "expires in 60s";
   - includes `Confirm Close All` and `Cancel` buttons.
3. User taps `Confirm Close All`.
4. Bot verifies:
   - same Telegram `from.id`;
   - confirmation exists;
   - confirmation is not expired;
   - confirmation is not already used.
5. Bot queues the existing `close_all` control command and writes audit events.
6. Bot edits or replies with a final queued/rejected result.

The existing `/confirm <code>` path may remain for backwards compatibility, but
the preferred M7.5 path is button confirmation.

## Command Menu

M7.5 should register Telegram bot commands with `setMyCommands` so mobile users
can discover commands from Telegram's native command menu.

Use the existing command names and short descriptions. Do not add new
operator-only command names for remote open, parameter editing, model debug,
shell, or live enablement.

If command registration fails, write or log a warning but do not block the bot
loop or trading executor. Command registration should use a short request
timeout because it is best-effort startup decoration, not a prerequisite for
polling.

## Callback Handling

Callback data should be small and explicit:

- `tgux:status`
- `tgux:pnl`
- `tgux:risk`
- `tgux:positions`
- `tgux:orders`
- `tgux:orders:<page>`
- `tgux:trades`
- `tgux:trades:<page>`
- `tgux:events`
- `tgux:events:<page>`
- `tgux:smoke`
- `tgux:help`
- `tgux:control`
- `tgux:stop`
- `tgux:resume`
- `tgux:cancel_all`
- `tgux:close_all`
- `tgux:confirm_close_all`
- `tgux:cancel_close_all`

Unknown callback data should be ignored or answered with a short unsupported
message. It must not queue any command.

The bot should answer callback queries promptly so Telegram does not leave the
button in a loading state.

## Security Boundary

M7.5 must preserve M6 and M7 safety boundaries:

- No live trading.
- No live key loading.
- No live REST or WebSocket execution path.
- No remote open.
- No remote parameter editing.
- No remote model debug.
- No remote shell.
- No Telegram-to-Bitget direct call.
- No bypass around SQLite audit/control tables.

Buttons are UI affordances only. They must not create a second control path.

## Testing Focus

Add tests around behavior, not visual snapshots:

- HTML formatter escapes dynamic values.
- `/help` omits `/confirm <code>` from the command list while preserving the
  fallback path.
- `/status`, `/pnl`, and `/risk` contain the refined editorial layout markers:
  bold Title Case headings and labels, unbold values, and PnL markers where
  applicable.
- `/positions`, `/orders`, `/trades`, and `/pnl` use dense multi-line rows with
  bold numeric values.
- `/orders`, `/trades`, and `/events` paginate at 8 rows per page, cap at 5
  pages / 40 rows, and keep page labels in buttons only.
- Inline keyboard contains read-only and control buttons.
- Read-only callbacks return the same data as slash commands.
- Control callbacks queue the same commands as slash commands.
- Unauthorized callback users cannot query SQLite state or queue controls.
- `Close All` button requires confirmation before queueing.
- `Confirm Close All` callback is same-user, one-use, and expiry checked.
- `Cancel Close All` clears/rejects the pending confirmation without queueing.
- `setMyCommands` payload contains existing commands only.
- `setMyCommands` is best effort and uses a short timeout.
- Telegram HTTP failures do not block execution.
- The legacy `query_response()` compatibility path rejects `/cancel_all` with
  other controls.
- Scope scan still rejects remote open, live enablement, remote parameter
  editing, model debug, and shell paths.

## Non-Goals

M7.5 does not:

- add new trading commands;
- add live trading behavior;
- add new strategy/model/factor behavior;
- add a web app;
- add custom images, logos, generated art, or brand imitation;
- add a large UI framework;
- replace slash commands;
- change Rust executor control semantics.

## Success Criteria

- Existing slash commands continue to work.
- Telegram replies are easier to read on mobile and follow the approved dark
  editorial style.
- Query and control buttons are available.
- Orders, trades, and events remain usable with high row counts through bounded
  pagination.
- `Close All` is button-confirmed and still safe.
- Command menu registration is implemented or cleanly skipped on failure.
- All Telegram control paths still write audit events.
- No live trading capability, remote open capability, remote parameter editing
  capability, model debug path, shell path, or new live execution path is added.
- Full Python and Rust test suites pass.
