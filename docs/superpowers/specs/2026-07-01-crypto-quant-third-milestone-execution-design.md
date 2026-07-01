# Crypto Quant Third Milestone Execution Design

## Goal

Build the Bitget demo execution safety layer.

This milestone makes the Rust executor capable of running real demo futures orders through Bitget. It does not connect the LightGBM model to automatic production trading. Python may write test or manual `trade_intents`; Rust is the only process allowed to place, cancel, reconcile, and risk-check exchange orders.

## Scope

Included:

- Bitget demo USDT-M futures execution for `ETH/USDT:USDT`.
- Single-process Rust executor.
- Public WebSocket cache for best bid, best ask, ticker, and mark price.
- Private WebSocket cache for orders, fills, positions, and account state.
- REST client for place order, cancel order, query orders, query positions, query account, and query open orders.
- SQLite intent consumption and order/fill/position/equity/event persistence.
- Local order state machine with idempotent intent processing.
- REST reconciliation to repair missed WebSocket updates.
- Demo integration tests that are allowed to place, cancel, and close Bitget demo orders.
- Telegram notifications only for fills and major errors.

Excluded:

- Live Bitget trading.
- Full model-to-intent automation.
- Production model publication.
- Multi-exchange execution.
- Redis, Kafka, FastAPI, or multiple execution services.

## Architecture

The executor remains one Rust process:

```text
SQLite trade_intents
        |
        v
Rust executor
  - public WS market cache
  - private WS account/order cache
  - REST action client
  - local state machine
  - risk gate
  - REST reconciler
        |
        v
Bitget demo futures
```

The fast path is:

1. Public WS keeps market data in memory.
2. Private WS keeps account, order, fill, and position state in memory.
3. Rust polls pending SQLite intents.
4. Risk gate checks cached state before any order action.
5. REST sends place/cancel requests.
6. The local state machine immediately records the order lifecycle.
7. WS updates fill in real-time state.
8. REST reconciliation periodically repairs missing local data.

SQLite is the durable command queue and audit log, not the hot decision store.

## Bitget Demo Safety

Demo mode is mandatory in this milestone.

The executor must refuse to start unless:

- config trading mode is `demo`;
- demo API key, secret, and passphrase are present;
- REST requests include Bitget's demo trading header `paptrading: 1`;
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
3. subscribes to public and private WS channels;
4. reconciles local and exchange state;
5. begins processing eligible intents.

If an existing position matches local orders/fills, it is treated as system-owned and its previous context is restored. If no local ownership can be found, it is marked as imported. Imported positions use exchange average entry price and first local adoption time as the baseline for stop-loss, trailing take-profit, and 24h holding review.

## Order Rules

Opening orders:

- First attempt is maker.
- Maker buy uses cached best bid.
- Maker sell uses cached best ask.
- If the maker order is not filled within `open_maker_timeout_seconds = 15`, cancel it.
- If signal, risk, and market state are still valid, retry maker once.
- If the second maker attempt also times out, cancel it.
- If signal, risk, and market state are still valid, use taker.

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

WebSocket is the real-time path. REST is the repair path.

The reconciler periodically queries Bitget for:

- open orders;
- recent order details;
- positions;
- account equity and available margin.

It compares exchange state with SQLite and in-memory state. Missing orders, fills, positions, and equity snapshots are inserted or updated locally. If local state conflicts with exchange state, exchange state wins and an `events` row is written.

Reconciliation must be safe to run repeatedly.

## Telegram

Telegram remains a notification layer, not a control dependency.

The executor sends active Telegram messages only for:

- order filled;
- position closed with realized PnL if available;
- rejected intent caused by safety rules;
- WebSocket authentication failure;
- REST order/cancel failure after retry;
- margin danger or emergency reduce/close.

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
- Telegram sends only fills and major errors.
- Full Python and Rust verification passes.
