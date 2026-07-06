# Crypto Quant Seventh Milestone Live Readiness Design

## Goal

M7 is **Live-Ready Safety Gates + Final Pre-Live Readiness**.

M7 prepares the system for a future live integration milestone. It does not
enable live trading, does not connect to live Bitget APIs, and does not add any
new production trading capability.

M8 is the first milestone that may perform live integration or a small-capital
production launch. If that scope is too large, M9 can hold the final live
stability work.

## Scope

Included:

- Add live-readiness safety tests.
- Keep `--mode live` rejected at the executor entry point.
- Preserve the current demo-only runtime behavior.
- Verify Telegram operator control cannot bypass SQLite and Rust executor
  safety boundaries.
- Verify no remote open, remote parameter edit, remote model debug, remote
  shell, or live enablement path exists.
- Add a short readiness checklist for M8.

Excluded:

- Live trading.
- Live API key loading.
- Live REST or WebSocket connection tests.
- Live dry-run mode.
- Live order placement, cancellation, or reconciliation.
- New preflight CLI.
- Local readiness report generation.
- 12-24 hour soak testing.
- Signal, factor, or model quality work.
- Detection or rejection of accidentally configured live keys.

## Live Boundary

M7 keeps the existing hard boundary:

- `prodigy-executor --mode live` fails before execution.
- The executor does not read live credentials.
- The executor does not connect to live Bitget REST or WebSocket endpoints.
- Telegram commands cannot enable live mode.
- Python signal code cannot request live execution.

M7 does not inspect `.env.local` for live key names. If live keys are
accidentally present, M7 does not use, validate, or reject them. M8 will define
the real live-key handling rules.

## Safety Test Focus

M7 is test-first. The main output is safety coverage that prevents accidental
live-enablement regressions.

Test coverage should include:

- live mode still fails at the executor entry point;
- production config does not point to live WebSocket endpoints;
- Telegram query/control code does not call Bitget REST directly;
- Telegram control commands still write SQLite command/audit rows only;
- Rust remains the only component that executes control commands;
- `/stop`, `/resume`, `/cancel_all`, and `/close_all` safety boundaries do not
  regress;
- `/close_all` remains confirmed and system-position-only;
- `/cancel_all` remains system-working-order-only;
- no remote open, remote parameter edit, remote model debug, remote shell, or
  live enablement strings appear in production code paths;
- M6 smoke/operator tests continue to pass.

## Readiness Checklist

M7 adds one concise checklist for M8. It is documentation, not a generated
report.

The checklist should cover:

- required demo tests before M8 starts;
- required Telegram operator checks;
- required SQLite state checks for residual orders and positions;
- required manual confirmation that M8 has an explicit live-enable design;
- required live key preparation steps, without placing keys in M7;
- rollback expectations for the future small-capital launch.

The checklist should stay short enough to read before starting M8. It should
not become a full operating manual.

## Non-Goals

M7 does not make the strategy better. It does not change signals, factors,
models, risk sizing, or execution policy unless a safety test exposes a direct
live-readiness regression.

M7 does not add observability features beyond the minimal checklist and tests.
M6 already added Telegram operator observability and smoke reporting.

## Success Criteria

- All existing tests pass.
- New live-readiness safety tests pass.
- `--mode live` is still rejected.
- No live API call path is added.
- No live key is required or read.
- No new preflight command exists.
- The M8 readiness checklist is present and concise.
- M7 documentation clearly states that M8 is the first live integration
  milestone.
- There is no new live trading capability, remote opening capability, remote
  parameter editing capability, or new live execution path.
