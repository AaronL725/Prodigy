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

Use the selected **Editorial Symbols** direction:

- Dark-client friendly message composition.
- Sparse luxury cues: `◆`, `—`, `●`, uppercase section labels.
- Minimal emoji. Use clear warning symbols only for risk or danger states.
- No fake branding, no logo imitation, and no excessive gold/ornament text.
- Headings use uppercase editorial labels.
- Data rows use a two-column label/value layout:
  - left: `MODE`, `DAEMON`, `RISK`, `EQUITY`;
  - right: `DEMO`, `RUNNING`, `CLEAR`, `1,000.00`.

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
  `n/a` unless reliable realized PnL exists.
- `/risk`: risk state summary first, then contributing states.
- `/positions`: compact list grouped by symbol, side, ownership, notional,
  entry, and unrealized PnL.
- `/orders`: working system orders first, then recent orders.
- `/trades`: recent fills with symbol, side, price, size, fee, and timestamp.
- `/events`: warning/error/critical events only, newest first.
- `/smoke_status`: local smoke status only.

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
loop or trading executor.

## Callback Handling

Callback data should be small and explicit:

- `tgux:status`
- `tgux:pnl`
- `tgux:risk`
- `tgux:positions`
- `tgux:orders`
- `tgux:trades`
- `tgux:events`
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
- `/status`, `/pnl`, and `/risk` contain the new editorial layout markers.
- Inline keyboard contains read-only and control buttons.
- Read-only callbacks return the same data as slash commands.
- Control callbacks queue the same commands as slash commands.
- Unauthorized callback users cannot query SQLite state or queue controls.
- `Close All` button requires confirmation before queueing.
- `Confirm Close All` callback is same-user, one-use, and expiry checked.
- `Cancel Close All` clears/rejects the pending confirmation without queueing.
- `setMyCommands` payload contains existing commands only.
- Telegram HTTP failures do not block execution.
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
- `Close All` is button-confirmed and still safe.
- Command menu registration is implemented or cleanly skipped on failure.
- All Telegram control paths still write audit events.
- No live trading capability, remote open capability, remote parameter editing
  capability, model debug path, shell path, or new live execution path is added.
- Full Python and Rust test suites pass.
