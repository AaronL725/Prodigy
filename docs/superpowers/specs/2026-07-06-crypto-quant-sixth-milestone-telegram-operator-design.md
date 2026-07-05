# Crypto Quant Sixth Milestone Telegram Operator Design

## Goal

M6 adds an operator-facing Telegram layer and a short demo reliability run.

M6 is not an alpha, model, or live-trading milestone. It proves that the demo
system can be monitored and controlled from a phone without giving Telegram any
direct exchange authority.

```text
Telegram operator
  -> SQLite control_commands / executor_state / events
  -> Rust prodigy-executor --daemon
  -> Bitget demo futures through REST execution
  -> SQLite orders/fills/positions/equity/events
  -> Telegram queries and local smoke report
```

## Scope

Included:

- Expand Telegram from read-only queries to operator observability plus control.
- Use one command set for demo and future live semantics.
- Keep M6 implementation demo-only.
- Keep Telegram away from Bitget APIs.
- Write every remote control action to SQLite audit events.
- Add `cancel_all` to `control_commands` through a schema migration.
- Make Rust executor consume `control_commands` and perform the actual action.
- Add a light demo smoke run that lasts 30-120 minutes and writes a local report.

Excluded:

- Live trading.
- Live API key loading.
- Remote opening orders.
- Remote parameter changes.
- Remote model debugging.
- Remote shell or arbitrary command execution.
- 24-72 hour soak testing.
- A separate Telegram control service, Redis, Kafka, FastAPI, actors, or an event bus.

## Live Boundary

M6 ships one Telegram command vocabulary. It does not create separate demo and
live commands.

The implementation remains demo-only: live mode is rejected and live keys are
not required or read.

Future live mode may reuse the same commands, but must add stricter gates:

- explicit live enable switch;
- stricter user whitelist;
- stronger confirmation for destructive actions;
- complete audit events;
- replay protection;
- reviewed failure handling.

## Command Ownership

Telegram never calls Bitget.

Telegram may only write:

- `control_commands`;
- `executor_state`;
- `events`.

Rust executor is the only component that executes commands. It reads pending
`control_commands`, applies risk and mode checks, calls Bitget through existing
REST execution helpers, reconciles, and writes results back to SQLite.

## Telegram Commands

### Read-Only Queries

- `/help` - show supported commands.
- `/status` - daemon, signal, pause, latest error, and freshness summary.
- `/positions` - current SQLite positions.
- `/orders` - open orders and recent orders.
- `/trades` - recent fills and trade flow.
- `/pnl` - realized, unrealized, and total PnL where available.
- `/risk` - risk state, margin state, manual override, and trading suspension.
- `/events` - recent important events.
- `/smoke_status` - active smoke run status.
- `/smoke_report` - latest smoke report summary and path.

Queries read SQLite only. Query failure must not affect execution.

### Control Commands

- `/stop` - queue a stop command that blocks new opening exposure.
- `/resume` - queue a resume command that clears the operator stop state.
- `/cancel_all` - queue a command for Rust to cancel system-owned open orders.
- `/close_all` - start a confirmation flow.
- `/confirm <code>` - confirm the pending `/close_all` code.

All control commands require a Telegram user-id whitelist.

`/stop`, `/resume`, and `/cancel_all` may be queued immediately by a whitelisted
user. `/close_all` must be confirmed before a `close_all` control command is
queued.

## Close-All Confirmation

`/close_all` is destructive and uses a short text confirmation flow.

1. Whitelisted user sends `/close_all`.
2. Telegram writes an audit event for the request.
3. Telegram stores a pending confirmation hash and expiry in `executor_state`.
4. Telegram replies with a one-time code.
5. The same whitelisted user sends `/confirm <code>` within 60 seconds.
6. Telegram writes the `close_all` row to `control_commands`.
7. Telegram writes audit events for confirmation success or failure.

Expired, wrong, replayed, or cross-user confirmation attempts are rejected and
audited. They do not write a `close_all` command.

The confirmation state can live in `executor_state`; no new table is required
unless implementation shows the existing key-value table cannot keep the state
clear and safe.

## Control Command Semantics

`stop`:

- Rust marks operator trading suspension in `executor_state`.
- New opening exposure is blocked.
- Close, cancel, reconcile, and emergency de-risking remain allowed.

`resume`:

- Rust clears the operator stop state.
- Other risk gates still apply.

`cancel_all`:

- Rust cancels system-owned open orders for enabled symbols.
- Manual or imported orders are not adopted or cancelled by assumption.
- Rust reconciles after the cancel attempt.

`close_all`:

- Rust cancels system-owned open orders first.
- Rust closes system-owned positions with reduce-only orders.
- Emergency close behavior may use taker execution.
- Rust reconciles after execution.
- If a position cannot be closed in demo liquidity conditions, Rust records the
  failure and leaves the command in a terminal failed state with diagnostics.

Telegram only queues the command. It never directly mutates trading state beyond
the command and audit rows.

## SQLite Changes

Existing schema already has `control_commands`, `executor_state`, and `events`.

M6 adds `cancel_all` to the `control_commands.command` check constraint through
a schema migration. Existing databases must be migrated safely.

`control_commands` remains the durable command queue:

```text
pending -> accepted -> executed
pending -> rejected
accepted -> failed
```

Commands are idempotent by `command_id`. Rust must not execute the same command
twice.

`executor_state` stores light operator state:

- operator stop state;
- pending close-all confirmation;
- active smoke run metadata;
- latest smoke report path.

`events` is the audit log.

## Audit Events

Every remote control action writes an event.

Audit payloads include:

- Telegram user id;
- command text;
- generated `command_id` when one exists;
- confirmation status when relevant;
- created timestamp;
- execution timestamp when relevant;
- result status;
- failure reason when relevant.

Audit events are required for:

- unauthorized command attempt;
- command queued;
- confirmation code generated;
- confirmation accepted;
- confirmation rejected or expired;
- Rust command accepted;
- Rust command executed;
- Rust command failed.

Audit write failure must not silently pretend a command succeeded. If Telegram
cannot write the command and its audit row, it returns an error to the operator
and writes no command.

## Observability Rules

Telegram responses should be short enough to read on a phone.

`/status` is the top-level health command. It should summarize:

- Rust daemon freshness;
- Python signal freshness;
- latest reconcile freshness;
- operator stop state;
- manual override count;
- pending intents;
- pending control commands;
- latest critical or error event.

`/pnl` uses SQLite state only. If realized PnL is incomplete, the response must
say so instead of implying precision.

`/trades` reads recent `fills` rows and shows symbol, side, size, price, fee,
and timestamp.

`/events` defaults to recent warning/error/critical events. It should not spam
normal polling noise.

## Light Demo Smoke Run

M6 includes a short smoke reliability workflow.

Duration:

- default: 60 minutes;
- allowed: 30-120 minutes.

The smoke run starts the existing demo components:

- Rust `prodigy-executor --daemon`;
- Python `prodigy-signal --daemon`;
- Telegram bot/query loop when credentials are configured.

During the run, the smoke workflow records observations and problems. It does
not stop mid-run to fix issues.

At the end, it writes a local Markdown report:

```text
reports/m6-demo-smoke-YYYYMMDD-HHMM.md
```

The report includes:

- run start and end time;
- configured duration;
- component startup status;
- Telegram query/control checks;
- pending and terminal intents;
- control command results;
- open orders;
- fills and trade flow;
- positions;
- realized/unrealized PnL snapshot where available;
- error/warning/critical events;
- WS/REST/SQLite/Telegram issues;
- residual positions or orders;
- recommended fixes.

After the run ends, discovered issues can be fixed in a separate pass.

## Error Handling

Telegram network failure must not block Rust execution or reconcile.

SQLite lock or write failures must be surfaced as operator-visible errors where
possible. Control commands must not be acknowledged as queued unless the command
row was written.

Rust control command processing must fail safe:

- no assumed order success without confirmation;
- reconcile after cancel or close attempts;
- terminal command status for success or failure;
- diagnostic event for every failed command.

## Testing Acceptance Criteria

1. Existing M5 signal daemon tests still pass.
2. Existing M4 executor daemon tests still pass.
3. M6 remains demo-only and rejects live mode.
4. Telegram unauthorized users cannot queue controls.
5. `/help` lists query and control commands.
6. `/status`, `/positions`, `/orders`, `/trades`, `/pnl`, `/risk`, `/events`,
   `/smoke_status`, and `/smoke_report` read SQLite only.
7. `/stop` writes a `stop` control command and audit event.
8. Rust consumes `stop` and blocks new opening exposure.
9. `/resume` writes a `resume` control command and audit event.
10. Rust consumes `resume` and clears operator stop state.
11. `/cancel_all` is accepted by the migrated schema.
12. Rust consumes `cancel_all`, cancels system-owned open orders, and audits the result.
13. `/close_all` without confirmation does not write a `close_all` command.
14. `/confirm <code>` writes `close_all` only for the same whitelisted user before expiry.
15. Wrong, expired, replayed, and cross-user confirmation attempts are rejected and audited.
16. Rust consumes `close_all`, cancels system-owned orders, closes system-owned positions, reconciles, and audits success or failure.
17. Control command processing is idempotent by `command_id`.
18. Telegram failure does not block execution.
19. The smoke workflow can run for a configured 30-120 minute window.
20. The smoke workflow writes a Markdown report with observations and failures.
21. Smoke run issue collection does not mutate strategy parameters or live settings.
22. No remote open, remote parameter edit, remote model debug, or remote shell path exists.

## Final M6 Shape

M6 should leave the system with:

```text
Python signal daemon
  -> writes trade_intents

Rust executor daemon
  -> reads trade_intents and control_commands
  -> executes through Bitget demo REST
  -> reconciles REST truth into SQLite
  -> writes audit events

Telegram operator interface
  -> reads SQLite for observability
  -> writes SQLite commands and audit events for controls
  -> never calls Bitget

Light smoke workflow
  -> runs demo for 30-120 minutes
  -> records issues
  -> writes local report
```
