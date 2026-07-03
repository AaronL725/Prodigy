# Crypto Quant Fourth Milestone Daemon Design

## Goal

Upgrade the third milestone one-shot demo executor into a stable demo-only daemon executor.

This milestone is a continuous demo execution and operations milestone. It is not a strategy, model, or live-trading milestone.

## Scope

Included:

- `prodigy-executor --once` keeps the existing M3 one-shot flow for tests, debugging, and CI.
- `prodigy-executor --daemon` starts a long-running demo executor.
- Rust remains responsible for execution.
- Python strategy and model processes remain responsible for writing `trade_intents` to SQLite.
- SQLite remains the durable command queue and audit log.
- REST remains the authority for orders, fills, account state, and positions.
- WebSocket is used only as a fast cache and state-update source.
- Telegram supports read-only query commands.

Excluded:

- Live trading.
- Loading live API keys.
- Generating trading signals.
- Training models.
- Strategy direction decisions inside Rust.
- Remote trading controls through Telegram.
- Splitting Telegram into a separate execution-control service.
- A full actor or event-bus architecture.
- `/stop`, `/resume`, or `/close_all`.

Remote trading controls need a separate design for user whitelist, confirmation, command idempotency, replay protection, failure handling, emergency behavior, and audit logging.

## Architecture

The final M4 architecture is:

```text
Python strategy/model process
        |
        v
SQLite trade_intents
        |
        v
Rust prodigy-executor --daemon
  - reads pending trade_intents
  - checks risk
  - uses WS cache for fresh market data
  - places/cancels orders through REST
  - reconciles through REST
  - writes orders/fills/positions/events to SQLite
        |
        v
Bitget demo futures

Telegram query interface
        |
        v
read-only SQLite queries
```

M4 stays in one Rust process. This avoids extra process supervision and keeps execution ownership simple.

## CLI

`prodigy-executor --once`:

- preserves the existing one-shot flow;
- validates demo-only mode;
- runs the M3 execution path;
- remains useful for tests, debugging, and CI.

`prodigy-executor --daemon`:

- validates demo-only mode;
- starts the public WS loop, private WS loop, intent loop, reconcile loop, and event/notification writer behavior;
- runs until Ctrl+C or SIGTERM.

Live mode is still rejected before intents are read.

## Runtime Loops

### Public WS Loop

The public WS loop:

- connects to Bitget demo public WS;
- subscribes to order book or best bid/ask data;
- maintains `MarketCache` with `best_bid`, `best_ask`, `exchange_ts_ms`, and `local_received_at`;
- reconnects automatically after disconnect;
- marks market state stale if no fresh update arrives within `stale_market_data_secs`.

New opening orders require fresh market data. Closing, cancelling, and safe de-risking actions may continue when market data is stale if their own safety checks pass.

### Private WS Loop

The private WS loop:

- connects to Bitget demo private WS;
- listens for order, account, and position updates;
- updates in-memory private state cache;
- writes important state changes and events to SQLite when appropriate;
- never becomes the final source of truth.

REST reconcile wins when private WS and REST disagree.

### Intent Loop

The intent loop:

- polls SQLite for pending `trade_intents`;
- uses the existing M3 execution state machine;
- runs existing risk checks before any order;
- refuses new opening exposure when market cache is stale;
- refuses new opening exposure when private state is not ready;
- preserves `intent_id` idempotency;
- never executes the same intent twice.

Trading suspension blocks new opening orders only. Close, cancel, stop-loss, and margin-safety actions remain allowed when safe.

### Reconcile Loop

The reconcile loop:

- runs once immediately at daemon startup;
- runs periodically using `reconcile_interval_secs`;
- runs after WS reconnect;
- may run after a batch of intent processing;
- uses REST as exchange truth;
- repairs missing orders, fills, positions, equity snapshots, and local state;
- records transient errors into SQLite events instead of crashing the daemon when continuing is safe.

Reconcile remains safe to run repeatedly.

### Event And Notification Writer

Important events are always written to SQLite.

Telegram delivery failure must not block execution, order management, or reconcile. A Telegram failure may write an event.

Demo mode suppresses ordinary open, close, and fill notifications. Demo mode may push only major events:

- `critical`;
- `margin_danger`;
- `manual_override_entered`;
- `manual_override_cleared`;
- `websocket_auth_failed`;
- `rest_order_failed`.

## Telegram Query Interface

M4 supports read-only Telegram queries:

- `/status`;
- `/positions`;
- `/orders`;
- `/pnl`;
- `/risk`.

Queries read from SQLite. They do not place orders, cancel orders, change strategy state, or alter risk state.

Telegram is not an execution dependency. If Telegram is unavailable, daemon execution continues.

## Execution Rules

- Placing and cancelling orders still goes through REST.
- WS never sends trading instructions.
- WS cache is used for fresh market-data checks, faster local state display, and reducing unnecessary REST reads.
- REST reconcile remains the final authority.
- SQLite remains the durable audit log.
- If WS and REST disagree, REST wins.
- If market data is stale, new opening orders are rejected.
- If private state is not ready, new opening orders are rejected.
- Close, cancel, and emergency de-risking actions remain allowed when safe.

## Risk Priority

Strategy signals cannot bypass Rust risk checks.

Priority order:

1. Margin safety and liquidation-risk prevention.
2. Cancel, close, reduce, stop-loss, and other de-risking actions.
3. Manual override restrictions.
4. Trading suspension restrictions.
5. New opening exposure.

Rules:

- `manual_override` blocks new opening exposure for that symbol.
- `manual_override` does not block emergency de-risking.
- `trading_suspension` blocks new opening orders only.
- Margin danger may cancel system orders and reduce or close positions.

## Manual Override

If exchange state shows user or manual intervention, the daemon enters `manual_override` for that symbol.

Manual override:

- prevents new system opens for that symbol;
- does not block system-owned close, cancel, reconcile, or emergency de-risking actions;
- clears only when no position and no open order remain for that symbol.

Exchange state wins over local state.

## Daemon Startup

Startup order:

1. Load config and demo secrets.
2. Validate demo-only mode.
3. Connect SQLite with `busy_timeout` and WAL-compatible behavior.
4. Create Bitget REST client.
5. Connect public WS.
6. Connect private WS.
7. Set leverage.
8. Run REST reconcile before consuming new intents.
9. Start daemon loops.

## Shutdown And Restart

On Ctrl+C or SIGTERM:

- stop accepting new intents;
- do not start new opening orders;
- allow the current order flow to converge where practical;
- write a shutdown event to SQLite;
- exit cleanly.

On restart:

- validate demo-only mode;
- reconnect WS;
- run REST reconcile first;
- consume pending intents only after reconcile completes.

## Error Handling

WS disconnect:

- reconnect automatically;
- mark cache stale while disconnected;
- pause new opening orders;
- run reconcile after reconnect.

Stale market data:

- reject new open intents;
- write an event;
- allow close, cancel, and risk-reducing paths when safe.

REST error:

- fail or defer the affected intent safely;
- write a SQLite event;
- do not assume order success without confirmation.

Reconcile error:

- write a warning or error event;
- continue daemon operation when safe;
- do not crash the whole daemon on one transient failure.

Telegram error:

- does not affect execution;
- may write an event;
- never blocks trading or reconcile.

## Testing Acceptance Criteria

1. Existing `--once` flow still works.
2. `--daemon` starts in demo mode only.
3. Non-demo/live mode is rejected.
4. Daemon connects to public WS and refreshes `MarketCache`.
5. Daemon connects to private WS and parses order, account, and position events.
6. Daemon processes SQLite pending `trade_intents`.
7. Stale market data blocks new opening orders.
8. Trading suspension blocks new opens but allows close and cancel.
9. `manual_override` blocks new opens but does not block emergency de-risking.
10. The same `intent_id` cannot be executed twice.
11. Daemon restart runs reconcile before processing new intents.
12. Reconcile repairs missing fills, orders, and positions from REST.
13. WS disconnect/reconnect does not cause duplicate orders.
14. Telegram read-only queries return status, positions, orders, PnL, and risk from SQLite.
15. Telegram failure does not affect order execution.
16. Abnormal shutdown/restart does not leave untracked system orders.
17. No live API key is required or read.
18. No live trading path is enabled.

