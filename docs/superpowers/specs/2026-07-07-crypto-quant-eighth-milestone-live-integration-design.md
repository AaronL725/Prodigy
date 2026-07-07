# Crypto Quant Eighth Milestone Live Integration Design

## Summary

M8 is the final live-integration milestone.

The goal is to make the existing demo system able to run against Bitget live
APIs after the operator adds live credentials, funds the account, and sets
explicit live-enable switches. M8 does not create a second trading system. Demo
and live must use the same signal daemon, SQLite queue, Rust executor state
machine, risk checks, Telegram commands, manual override rules, and operator
controls.

The only runtime difference between demo and live is the Bitget API profile:

| Area | Demo | Live |
| --- | --- | --- |
| Credentials | `BITGET_DEMO_*` | `BITGET_LIVE_*` |
| REST trading header | `PAPTRADING: 1` | no `PAPTRADING` |
| WebSocket host | `wspap.bitget.com` | `ws.bitget.com` |
| Enable gate | default demo profile | explicit live enable + confirmation |

Without live keys and funds, M8 must still support live-path dry validation that
proves configuration, request construction, startup gates, mode isolation, and
control-command isolation without calling live private REST or placing orders.

## Official API Facts

The design relies on these Bitget API facts:

- Bitget demo REST uses the normal REST API with a demo API key and the
  `paptrading: 1` header:
  <https://www.bitget.com/api-doc/common/demotrading/restapi>
- Bitget demo WebSocket uses:
  - public: `wss://wspap.bitget.com/v2/ws/public`
  - private: `wss://wspap.bitget.com/v2/ws/private`
  <https://www.bitget.com/api-doc/common/demotrading/websocket>
- Bitget futures APIs use product types such as `USDT-FUTURES`:
  <https://www.bitget.com/api-doc/contract/intro>

M8 must not guess around these differences. REST demo/live split is header and
credential based; WebSocket demo/live split is host based.

## Goals

- Add a real `demo` / `live` Bitget API profile selection.
- Keep demo behavior unchanged by default.
- Allow `prodigy-executor --mode live` only behind explicit live gates.
- Add `--mode live --dry-validate` for no-key/no-fund live-path validation.
- Keep live and demo on the same execution logic and risk logic.
- Prevent demo and live executors from running active trading loops at the same
  time against the same SQLite database.
- Prevent Telegram commands from being queued or executed for the wrong runtime
  mode or stale executor instance.
- Document the exact demo-to-live switch procedure.

## Non-Goals

- No second Telegram bot service.
- No separate live strategy path.
- No remote open command.
- No remote parameter editing.
- No remote model debug.
- No remote shell.
- No live-only risk model.
- No new exchange abstraction framework.
- No large refactor of the existing executor.

## Shared System Semantics

Demo and live share:

- Python signal daemon;
- SQLite `trade_intents`;
- Rust executor state machine;
- Rust risk checks;
- Telegram commands;
- manual override semantics;
- operator stop/resume semantics;
- cancel-all and close-all semantics;
- `/status`, `/positions`, `/orders`, `/trades`, `/pnl`, `/risk`, and `/events`
  query behavior.

Live must not receive different hidden leverage, cap, or sizing defaults. If the
operator wants more conservative live sizing, they change the same config values
used by demo.

## API Profile

M8 replaces demo-only assumptions with a small API profile layer.

Minimal target shape:

```rust
enum TradingMode {
    Demo,
    Live,
}

struct BitgetSecrets {
    api_key: String,
    api_secret: String,
    passphrase: String,
}

struct LiveSafety {
    enabled: bool,
    confirm_phrase: Option<String>,
}
```

`ExecutorConfig` continues to hold the existing symbol, product type, margin
coin, leverage, risk, and Telegram fields. It also holds:

- `mode`;
- current profile `secrets`;
- `live_safety`;
- mode-specific public/private WebSocket URLs;
- a flag for live dry validation.

Validation rules:

- demo:
  - uses demo credentials;
  - public/private WS URLs must be `wspap.bitget.com`;
  - signed REST headers include `PAPTRADING: 1`.
- live:
  - normal startup requires live credentials;
  - normal startup requires explicit live enable;
  - normal startup requires an exact confirmation phrase;
  - public/private WS URLs must be `ws.bitget.com`;
  - signed REST headers must not include `PAPTRADING`.
- live dry validation:
  - does not require demo credentials;
  - does not require live credentials;
  - must not call live private REST;
  - must not place, cancel, or modify live orders;
  - must not leave an active executor lock;
  - must not make Telegram `/status` display a real `MODE LIVE` active executor;
  - can validate public live endpoints, config shape, request construction,
    schema, startup gates, and isolation rules.

Ponytail constraint: do not introduce a general exchange adapter. Bitget remains
the only exchange; the profile controls credentials, headers, and WS URLs.

## CLI And Environment

The executor supports:

- `prodigy-executor --mode demo --once`
- `prodigy-executor --mode demo --daemon`
- `prodigy-executor --mode live --daemon`
- `prodigy-executor --mode live --dry-validate`

Default mode remains demo.

Expected live environment variables:

- `BITGET_LIVE_API_KEY`
- `BITGET_LIVE_API_SECRET`
- `BITGET_LIVE_API_PASSPHRASE`
- `PRODIGY_LIVE_TRADING_ENABLED=1`
- `PRODIGY_LIVE_CONFIRM=I_UNDERSTAND_THIS_CAN_TRADE_REAL_MONEY`

The exact confirmation phrase is deliberately long and explicit. If it is
missing or different, live normal startup fails before any private API call.

Live secrets must be redacted in `Debug`, logs, events, and errors.

Live normal startup gates must run before any private exchange action. Missing
keys, missing live enable, missing confirm phrase, active-lock conflicts, and
live DB clean-state failures must be detected before any private REST call,
`set-leverage`, private WebSocket login, order call, account call, or position
call.

## Active Executor Lock

M8 adds an active executor lock in `executor_state`.

Required keys:

- `active_mode`
- `active_instance_id`
- `active_started_at`
- `active_heartbeat_at`

Startup behavior:

- each executor generates a fresh `instance_id`;
- startup acquires the active lock before starting loops;
- if an active lock exists and is not stale, startup fails;
- if an active lock exists and is stale, startup may take over and must write an
  audit event that records the old mode/instance and the new mode/instance.

Runtime behavior:

- daemon updates `active_heartbeat_at` periodically;
- clean shutdown releases the active lock;
- crash recovery uses stale timeout takeover;
- demo and live both use the same lock.

The lock prevents two executors from processing the same SQLite queue or
Telegram commands at the same time.

## Control Command Isolation

M8 migrates `control_commands`.

New columns:

- `mode text not null default 'demo'`
- `instance_id text`

SQLite cannot alter a `CHECK` constraint directly, so if the migration also
needs to preserve command constraints, it must rebuild the table safely.

The implementation must update every typed/query boundary that touches control
commands:

- database schema and migration;
- Rust `ControlCommand` struct;
- pending control command query;
- Telegram `queue_control_command`;
- tests for migration, queueing, and executor processing.

Telegram writes control commands by reading:

- `executor_state["active_mode"]`;
- `executor_state["active_instance_id"]`.

If no active executor exists, Telegram must not queue `/stop`, `/resume`,
`/cancel_all`, or `/close_all`. It replies with a no-active-executor message and
writes an audit event.

Rust executor processes only commands where:

- `command.mode == self.mode`;
- `command.instance_id == self.instance_id`.

Commands for other modes or stale instances must not execute. They may remain
pending or be audited as ignored, but they must never affect the current
executor.

`/close_all` confirmation state must also bind to mode and instance. Confirming
a stale code after an executor switch must fail safely.

## Trade Intent Boundary

M8 does not need to add `mode` to `trade_intents` if the active lock and startup
clean-state gate are implemented.

Before a live executor starts normal trading, it must hard fail if any of these
exist:

- `trade_intents.status in ('pending', 'accepted')`;
- `control_commands.status in ('pending', 'accepted')` for another mode or
  instance;
- working system orders;
- system positions.

This is not an operator checklist. It is a startup gate. The executor exits
before private live trading if the database is not clean.

## Telegram Semantics

Telegram keeps one command vocabulary for demo and live.

Telegram never calls Bitget. It only reads/writes SQLite and sends Telegram
replies.

`/status` must show the active mode:

- `MODE DEMO`
- `MODE LIVE`
- or no active executor / stale executor state.

Other replies do not need to repeat the mode at the top.

Control commands:

- `/stop`
- `/resume`
- `/cancel_all`
- `/close_all`

All control audit events must include:

- Telegram `user_id`;
- command;
- mode;
- instance_id;
- status or error.

`/stop`, `/resume`, `/cancel_all`, and `/close_all` affect only the active mode
and active instance.

## Demo To Live Switch Procedure

The documented operator procedure is:

1. In demo, run `/stop`.
2. Run `/cancel_all`.
3. If there are system positions, run `/close_all` and confirm.
4. Run `/status`.
5. Confirm no pending/accepted intents, no working system orders, and no system
   positions.
6. Stop the demo executor cleanly.
7. Add live credentials to the local environment.
8. Set:
   - `PRODIGY_LIVE_TRADING_ENABLED=1`
   - `PRODIGY_LIVE_CONFIRM=I_UNDERSTAND_THIS_CAN_TRADE_REAL_MONEY`
9. Start `prodigy-executor --mode live --daemon`.
10. Run `/status` and verify `MODE LIVE`.

If the database is not clean, live startup must fail even if the operator missed
step 5.

## Dry Validation

`--mode live --dry-validate` is for the current no-key/no-fund state.

It should validate:

- live mode parses;
- live dry validation does not require demo keys;
- live dry validation does not require live keys;
- live public WS URL uses `ws.bitget.com`;
- live REST request construction excludes `PAPTRADING`;
- live private/order/account operations are not sent;
- live normal startup would reject missing keys;
- live normal startup would reject missing enable/confirm;
- schema migration has run;
- active lock logic works;
- dry validation does not leave an active executor lock;
- dry validation does not make Telegram `/status` report a real `MODE LIVE`
  active executor;
- live startup clean-state checks work;
- Telegram control commands bind to active mode/instance.

It should return success only after all dry validations pass.

## Error Handling

- Missing live key: fail loud before private REST.
- Missing live enable: fail loud before private REST.
- Missing live confirm phrase: fail loud before private REST.
- Stale active lock takeover: allowed only after timeout and must write an audit
  event.
- Non-stale active lock: startup fails.
- Control command mode/instance mismatch: do not execute, audit or leave pending.
- Live dry validation accidentally reaching private REST/order code: test failure.

## Testing And Acceptance Criteria

1. `--mode demo` default behavior is unchanged.
2. Demo still uses `wspap.bitget.com`.
3. Demo signed headers still include `PAPTRADING: 1`.
4. `--mode live` parses.
5. `--mode live` without live keys fails loud before private REST.
6. `--mode live` with live keys but without explicit enable fails loud before
   private REST.
7. `--mode live` with enable but without the exact confirm phrase fails loud
   before private REST.
8. `--mode live --dry-validate` runs without demo keys, live keys, and funds.
9. Live dry validation does not call private REST and does not place/cancel
   orders.
10. Live signed headers do not include `PAPTRADING`.
11. Live WS URLs use `ws.bitget.com`.
12. Live secrets debug output redacts key, secret, and passphrase.
13. Live profile does not change executor state machine, risk checks, signal
    rules, or Telegram command semantics.
14. `control_commands` migration adds `mode` and `instance_id`, and updates the
    schema, `ControlCommand` struct, pending command query, Telegram
    `queue_control_command`, and related tests.
15. Existing control command rows migrate safely to `mode='demo'`.
16. Telegram queues control commands using current `active_mode` and
    `active_instance_id`.
17. Telegram does not queue control commands when no active executor exists.
18. Rust executor processes only matching `mode` and `instance_id` control
    commands.
19. Mismatched mode/instance commands never execute against the current
    executor.
20. Active executor lock writes mode, instance, started timestamp, and heartbeat
    timestamp.
21. A second executor cannot start while a non-stale lock exists.
22. Stale lock takeover works and writes an audit event.
23. Clean shutdown releases the active lock.
24. Heartbeat updates while daemon runs.
25. Live normal startup gates and clean-state checks run before private REST,
    `set-leverage`, private WS login, order, account, or position calls.
26. Live startup hard-fails if pending/accepted trade intents exist.
27. Live startup hard-fails if pending/accepted control commands for another
    mode or instance exist.
28. Live startup hard-fails if working system orders exist.
29. Live startup hard-fails if system positions exist.
30. Live dry validation leaves no active executor lock and does not make
    `/status` report a real `MODE LIVE` active executor.
31. `/status` displays `MODE DEMO`, `MODE LIVE`, or no-active-executor state.
32. `/stop`, `/resume`, `/cancel_all`, and `/close_all` affect only current
    active mode/instance.
33. `/close_all` confirmation binds to mode and instance.
34. Telegram audit events include user id, command, mode, instance id, status or
    error.
35. Scope scan shows no remote open, remote parameter edit, model debug, or
    remote shell.
36. Scope scan shows live path exists only behind explicit live enable and
    confirm gates.
37. No live key is printed, logged, committed, or exposed through `Debug`.
38. Existing full suite passes:
    - Python pytest;
    - Rust fmt;
    - Rust clippy with `-D warnings`;
    - Rust tests;
    - `git diff --check`.
39. Demo Bitget integration tests still pass or fail honestly under existing
    demo liquidity constraints.
40. Documentation includes the exact demo-to-live switch procedure.

## Final State

After M8, the operator should be able to:

1. run demo exactly as before;
2. run live dry validation without live keys or funds;
3. add live keys and funds;
4. set explicit live enable and confirmation;
5. start `prodigy-executor --mode live --daemon`;
6. verify `/status` shows `MODE LIVE`;
7. use the same Telegram controls and SQLite-backed audit trail in live mode.

No separate M9 is assumed.
