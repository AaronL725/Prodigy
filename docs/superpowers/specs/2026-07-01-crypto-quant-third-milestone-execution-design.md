# Crypto Quant Third Milestone Execution Design

## Goal

Build the Bitget demo execution safety layer.

This milestone makes the Rust executor capable of running real demo futures orders through Bitget. It does not connect the LightGBM model to automatic production trading. Python may write test or manual `trade_intents`; Rust is the only process allowed to place, cancel, reconcile, and risk-check exchange orders.

## Scope

Included:

- Bitget demo USDT-M futures execution for `ETH/USDT:USDT`.
- Single-process Rust executor.
- Public WebSocket connection verification (subscribe + auth check).
- Private WebSocket login verification (auth check).
- REST client for place order, cancel order, query orders, query positions, query account, and query open orders.
- REST market data snapshot (ticker) as the price source for order construction.
- SQLite intent consumption and order/fill/position/equity/event persistence.
- Local order state machine with idempotent intent processing.
- REST reconciliation to repair missed updates.
- Runtime manual intervention detection for client-side opens, closes, reductions, and cancellations.
- Mode-aware Telegram notification filtering for demo and future live runs.
- Demo integration tests that are allowed to place, cancel, and close Bitget demo orders.

Excluded:

- Live Bitget trading.
- Full model-to-intent automation.
- Production model publication.
- Multi-exchange execution.
- Redis, Kafka, FastAPI, or multiple execution services.
- Long-running WebSocket event loop with continuous cache maintenance (deferred to M4; M3 uses REST snapshot + one-shot processing).
- Telegram query commands (/positions, /trades, /pnl) as an interactive bot (deferred to M4; M3 has notification filtering only).

## Architecture

The executor remains one Rust process:

```text
SQLite trade_intents
        |
        v
Rust executor
  - public/private WS connect + auth + subscribe verification (no live cache)
  - REST action client (place / cancel / order-detail / account / position)
  - REST ticker one-shot market snapshot
  - local state machine
  - risk gate
  - REST reconciler
        |
        v
Bitget demo futures
```

The M3 fast path is:

1. At startup the executor verifies it can connect, authenticate, and subscribe to the public and private demo WS channels (no long-running cache is maintained — that is M4).
2. It seeds a market snapshot from a one-shot REST ticker and writes a real account equity snapshot.
3. Rust polls pending SQLite intents, one run through.
4. For each intent the risk gate runs on a freshly-fetched REST account snapshot and the seeded market snapshot; market data older than `stale_market_data_secs` blocks new openings.
5. REST sends place/cancel requests through the local state machine (maker → retry maker → taker); the market snapshot is refreshed from REST before the maker retry and the taker fallback so neither places on a stale price.
6. The local state machine records the order lifecycle, including fills (queried from REST order-detail for real price/fee/trade_id).
7. REST reconciliation repairs any local state the one-shot processing missed.

SQLite is the durable command queue and audit log. The in-memory market snapshot is a short-lived freshness check, not a continuously-maintained WS cache.

## Bitget Demo Safety

Demo mode is mandatory in this milestone.

The executor must refuse to start unless:

- config trading mode is `demo`;
- demo API key, secret, and passphrase are present;
- REST requests include Bitget's demo trading header `PAPTRADING: 1`;
- WebSocket URLs use Bitget demo endpoints;
- product type and symbol mapping target demo USDT futures.

Live trading config and live credentials are not used by this milestone. If a live mode value is passed, the executor exits before reading intents.

## Runtime Modes

### Test Mode

Integration tests may reset the demo account state for the tested symbol:

1. cancel all open `ETHUSDT` demo orders;
2. close existing `ETHUSDT` demo positions;
3. run the test scenario;
4. cancel and close again at test teardown.

This behavior is allowed only in explicit test commands.

### Strategy Run Mode

Normal demo strategy runs never clear positions or orders on startup.

At startup, the executor:

1. loads local SQLite orders, fills, positions, and active intents;
2. queries Bitget account, positions, and open orders;
3. verifies it can connect, authenticate, and subscribe to the public and private demo WS channels (verification only — no long-running cache in M3);
4. reconciles local and exchange state;
5. begins processing eligible intents.

If an existing position matches local orders/fills, it is treated as system-owned and its previous context is restored. If no local ownership can be found, it is marked as imported. Imported positions use exchange average entry price and first local adoption time as the baseline for stop-loss, trailing take-profit, and 24h holding review.

## Runtime Manual Intervention

The executor must handle manual actions made in the Bitget client while the program is running.

Manual intervention is detected when WebSocket or REST reconciliation sees an exchange order, fill, cancellation, or position change that cannot be matched to a local `client_oid`, local order, or local intent.

Rules:

- Manual override is per symbol, not global.
- When manual open, add, reduce, close, or cancel is detected for `ETHUSDT`, the executor enters `manual_override:ETHUSDT`.
- While a symbol is in manual override, new automatic opening and adding intents for that symbol are rejected or left pending with a durable event.
- Close, cancel, explicit user close commands, stop-loss, trailing take-profit, and account-safety actions remain allowed.
- Exchange state wins over local state.
- System-owned local orders manually cancelled in the client are marked `externally_cancelled`.
- System-owned local positions manually closed or reduced in the client are marked `externally_closed` or reconciled down to the exchange size.
- Manual or unmatched positions are marked `imported` and are included in local position and PnL reporting.
- If a manual position exceeds the system's normal notional cap, that cap breach alone must not trigger automatic reduction.
- Margin danger remains an account-safety rule. If Bitget reports liquidation or margin danger, the executor may still cancel orders and reduce or close positions to avoid account failure.
- The symbol automatically leaves manual override only when both exchange position size and exchange open orders for that symbol are zero.

Manual override state is stored durably in SQLite `executor_state`, so restarts do not forget that automatic new openings are paused.

## Order Rules

Opening orders:

- First attempt is maker, priced off the seeded REST ticker snapshot.
- Maker buy rests at best bid.
- Maker sell rests at best ask.
- If the maker order is not filled within `open_maker_timeout_seconds = 15`, cancel it.
- If signal, risk, and market state are still valid, retry maker once. The market snapshot is refreshed from a new REST ticker before the retry so the limit price is current.
- If the second maker attempt also times out, cancel it.
- If signal, risk, and market state are still valid, use taker. The market snapshot is refreshed again before the taker fallback so the crossing market order uses a current price.

Closing orders:

- First attempt is maker.
- Timeout is shorter than opening timeout.
- On timeout, cancel the maker order.
- If the close is still required, use taker.

Every timeout path must cancel before moving to the next attempt. The executor must not leave stale orders because of a retry, restart, or reconciliation event.

## Risk Gate

The risk gate runs before every open, add, reduce, close, reverse, and forced-risk action.

Rules:

- Leverage is configurable and defaults to `5x`.
- Leverage controls margin usage only. It does not decide order size.
- Intent notional must be clipped by both `target_notional` and `max_order_notional`.
- Total notional cannot exceed configured equity multiple.
- New openings are blocked if unrealized loss over the last 24h reaches `10%` of equity.
- Close, stop-loss, forced reduce, and cancel remain allowed during trading suspension.
- Stale market data blocks new openings.
- Missing private account state blocks new openings.
- Dangerous margin state triggers cancel and reduce/close behavior.
- Duplicate or already-processed intents cannot place duplicate exchange orders.

The executor may reject an unsafe intent with a durable error in SQLite.

## Reconciliation

REST is the source of truth for exchange state in M3. (A long-running WS-driven cache is M4; M3 verifies WS connectivity/auth/subscription and parses WS messages, but execution and reconciliation both run over REST.)

The reconciler queries Bitget for:

- open orders;
- recent order details;
- positions;
- account equity and available margin.

It compares exchange state with SQLite and in-memory state. Missing orders, fills, positions, and equity snapshots are inserted or updated locally. If local state conflicts with exchange state, exchange state wins and an `events` row is written.

Reconciliation must be safe to run repeatedly.

## Telegram

Telegram remains a notification layer, not a control dependency.

Demo mode does not proactively send normal opening or closing trade messages. In M3 the Telegram bot is notification-only; interactive query commands (/positions, /trades, /pnl, equity, available margin) are deferred to M4.

Demo mode still sends active Telegram messages for:

- WebSocket authentication failure;
- REST order/cancel failure after retry;
- margin danger or emergency reduce/close.
- manual override entered or cleared.

Future live mode sends active Telegram messages for:

- every opened position or opening fill;
- every closed position or closing fill, including realized PnL for that trade when available;
- rejected intent caused by safety rules;
- WebSocket authentication failure;
- REST order/cancel failure after retry;
- margin danger or emergency reduce/close;
- manual override entered or cleared.

Future live mode also supports query commands for total account realized PnL, unrealized PnL, equity, available margin, current positions, open orders, and recent trades.

Telegram delivery failure must not block order management. All important messages are still written to SQLite `events`.

## Config And Secrets

Existing `configs/default.toml` remains the source for tunable values.

Third milestone adds only the minimum needed config:

- Bitget demo REST base URL;
- Bitget demo public/private WS URLs;
- demo product type;
- demo symbol mapping if needed;
- maker retry counts and timeouts;
- reconciliation interval;
- test reset flag;
- risk thresholds.

Secrets are loaded from local ignored files or environment variables. The plan must not commit secret values.

## Testing And Verification

Daily local tests are allowed to use Bitget demo credentials and operate on the demo account.

Required verification:

- Rust unit tests for state machine and risk gate.
- Rust integration tests against Bitget demo for:
  - authentication;
  - public WS subscription;
  - private WS subscription;
  - limit maker order placement;
  - timeout cancel;
  - taker fallback;
  - close position;
  - REST reconciliation after state mismatch.
- Python smoke test that writes a SQLite intent and verifies Rust executes or safely rejects it.
- Existing Python research tests remain green.
- `cargo fmt --check`, `cargo clippy --all-targets --all-features -- -D warnings`, and `cargo test` pass.

Test mode may reset the demo account for the target symbol. Strategy run mode must not do startup reset.

## Acceptance Criteria

This milestone is complete when:

- Rust can start in Bitget demo mode and refuses live mode.
- Public and private Bitget demo WS connections authenticate/subscribe successfully.
- A pending SQLite open intent can become a real Bitget demo order.
- Maker timeout cancellation leaves no stale open order.
- Taker fallback can open or close when configured conditions still hold.
- Orders, fills, positions, equity snapshots, and events are persisted.
- REST reconciliation repairs at least one deliberately missing local order/fill/position record.
- Existing demo positions are adopted instead of automatically closed during normal strategy startup.
- Runtime manual client actions put the affected symbol into manual override, pause automatic new openings for that symbol, and automatically clear only after that symbol has no position and no open orders.
- Demo Telegram does not proactively send normal open/close messages; it sends manual-override enter/clear, critical errors, and margin-danger notifications. Interactive query commands (/positions, /trades, /pnl) are deferred to M4.
- Future live Telegram sends open/close messages and includes realized PnL on closes when available.
- Full Python and Rust verification passes.
