# Crypto Quant Fifth Milestone Signal Daemon Design

## Goal

Build a thin Python signal daemon that completes the demo automatic trading loop.

M5 is not an alpha milestone. It proves that:

```text
official free market data
  -> Python prodigy-signal refreshes parquet
  -> closed 15m bar signal evaluation
  -> SQLite trade_intents
  -> Rust prodigy-executor --daemon execution
  -> Rust REST reconcile writes SQLite truth
  -> Telegram read-only queries
```

The default demo signal source is `example-factors`. `dummy-cycle` exists only for deterministic tests and failure isolation.

## Scope

Included:

- Add a Python CLI entry point named `prodigy-signal`.
- Support `prodigy-signal --once` for tests and debugging.
- Support `prodigy-signal --daemon` for long-running demo signal generation.
- Refresh official free market data into the existing parquet store before signal evaluation.
- Evaluate only closed `15m` bars.
- Use the existing example factors and shared signal parameters.
- Write only `open` and `close` `trade_intents`.
- Read position, order, PnL, manual override, and reconcile freshness state from SQLite.
- Use Rust executor state in SQLite as the durable idempotency record.
- Keep Rust as the only component that talks to Bitget account, position, order, and execution APIs.
- Add the minimum Rust executor change needed for `action=close` to close the full current exchange position with a reduce-only order.

Excluded:

- Real alpha discovery.
- Live trading.
- Python account, position, order, or execution API calls to Bitget.
- Python WebSocket intrabar signal generation.
- Telegram remote trading controls.
- `reverse`, `reduce`, or `cancel` intent generation.
- New services such as Redis, Kafka, FastAPI, actors, or an event bus.
- New SQLite tables unless implementation proves existing `executor_state` cannot safely hold the idempotency key.

## Symbols

M5 enables one symbol by default:

```text
research/config symbol: ETH/USDT:USDT
exchange/executor symbol: ETHUSDT
```

The config shape may support a list of symbols, but M5 runs one enabled symbol by default. Multi-symbol activation is deferred.

## Runtime Modes

`prodigy-signal --once`:

- refreshes data once;
- evaluates the latest closed `15m` bar once;
- writes at most one intent for each enabled symbol;
- exits.

`prodigy-signal --daemon`:

- loops every `poll_interval_secs`;
- refreshes data before evaluation;
- only evaluates new closed `15m` bars;
- exits cleanly on SIGINT or SIGTERM after writing a shutdown event when possible.

## Data Rules

Before each signal evaluation, the daemon refreshes official free data through the existing data modules and writes it to parquet.

The signal layer uses closed bars only. If the latest exchange data contains a still-open `15m` candle, that candle is ignored.

Data refresh failure does not write a trading intent. It writes an event or warning and waits for the next run.

## Signal Source

Default source: `example-factors`.

The example source:

- loads the latest closed `15m` OHLCV data;
- computes existing example factors;
- produces an example score in `[-1, 1]`;
- maps the score through the shared signal parameters.

Test source: `dummy-cycle`.

`dummy-cycle` is deterministic and exists to prove the loop:

```text
Python writes intent -> Rust daemon executes/reconciles -> SQLite changes -> Telegram query can read state
```

`dummy-cycle` is not the default demo source.

## State Authority

Python does not query Bitget account, position, order, or execution APIs.

The authority chain is:

```text
Bitget API truth
  -> Rust REST reconcile
  -> SQLite orders/fills/positions/equity/events/executor_state
  -> Python signal daemon reads SQLite
  -> Python writes trade_intents
```

If SQLite state is stale, Python skips the bar and waits for Rust reconcile.

State freshness is checked using the latest Rust reconcile/equity snapshot signal available in SQLite. Default:

```text
max_state_age_secs = 120
```

If the latest authority marker is older than this value, no new intent is written.

## Intent Rules

M5 writes only:

```text
action = open
action = close
```

M5 never writes:

```text
reverse
reduce
cancel
```

Open rule:

```text
abs(score) >= entry_threshold
entry_threshold = 0.6
score > 0 -> open long
score < 0 -> open short
```

Position sizing reuses the shared research/backtest signal parameters:

```text
abs(score) = 0.6 -> min_order_fraction * total_notional_cap
abs(score) = 1.0 -> max_order_fraction * total_notional_cap
between 0.6 and 1.0 -> linear mapping

min_order_fraction = 0.05
max_order_fraction = 0.10
```

These values are config defaults, not hardcoded strategy constants.

Close rule:

```text
long position:
  score <= -exit_threshold -> close

short position:
  score >= +exit_threshold -> close
```

M5 keeps `exit_threshold` configurable.

If a position exists and a reverse signal appears, M5 writes only a `close` intent. It does not open the opposite direction in the same closed bar. Opposite-direction opening can only be considered on a later closed bar.

## Close Intent Semantics

Python does not calculate the precise contract size for `action=close`.

For a close signal, Python writes:

```text
action = close
side = current SQLite position side
target_notional = 0.0
max_order_notional = 0.0
```

The Rust executor must treat `action=close` as a full-position reduce-only close:

- resolve the current exchange position size for the symbol and side through the Rust execution/reconcile authority path;
- ignore `target_notional` and `max_order_notional` for close order sizing;
- submit a reduce-only order for the full current position size;
- never place a zero-size close order;
- fail the close intent with a diagnostic event if no matching exchange position exists.

This is a small M5 Rust executor adjustment. The normal `open` path keeps the existing notional-based sizing.

## Holding Expiry Rule

When a position reaches `max_holding_bars`, M5 does not close unconditionally.

It performs an expiry check:

```text
if holding_bars >= max_holding_bars:
    if unrealized_pnl >= 0:
        close only if abs(score) < 0.2
    else:
        close only if abs(score) < 0.4
```

The PnL branch uses SQLite state written by Rust reconcile. If PnL or position age cannot be read reliably, the daemon skips rather than guessing.

## Skip Rules

For a symbol and closed bar, the daemon writes no trading intent when:

- SQLite authority state is stale;
- `manual_override` is active;
- there is a pending or accepted intent for the symbol;
- there is an unfinished system order for the symbol;
- the closed bar idempotency key was already processed;
- data refresh failed;
- factor computation failed;
- the score does not trigger open or close;
- the current position state is ambiguous.

`manual_override` is conservative in M5: when active for a symbol, Python writes no strategy intent for that symbol.

## Idempotency

Each enabled symbol and closed bar has one idempotency key:

```text
signal_processed:{source}:{symbol}:{timeframe}:{closed_bar_ts}
```

Example:

```text
signal_processed:example-factors:ETHUSDT:15m:2026-07-04T10:15:00Z
```

The key is stored in existing SQLite `executor_state`; M5 does not add a new processed-bar table.

The recorded value should include the decision outcome, such as:

- `open_intent_written`;
- `close_intent_written`;
- `skipped_stale_state`;
- `skipped_pending_intent`;
- `skipped_pending_order`;
- `skipped_manual_override`;
- `no_signal`;
- `error_data_refresh`;
- `error_factor_compute`.

## Transaction Boundary

Writing a `trade_intent` and marking the processed bar must happen in one SQLite transaction whenever an intent is written.

This prevents inconsistent states such as:

- intent written but processed-bar marker missing, causing duplicate writes after restart;
- processed-bar marker written but intent missing, causing a missed trade.

For skip/no-signal outcomes, writing only the processed-bar marker is acceptable. If the marker write fails, the next run may re-evaluate the bar.

The existing Python `write_trade_intent` helper commits immediately. M5 must refactor it or add a no-commit insert helper so the signal daemon can commit:

```text
insert trade_intent
set executor_state signal_processed:...
commit
```

as one SQLite transaction.

## Config

M5 uses unified config values rather than a separate hardcoded strategy config.

Default values:

```text
enabled_symbols = ["ETH/USDT:USDT"]
exchange_symbols = {"ETH/USDT:USDT": "ETHUSDT"}
timeframe = "15m"
signal_source = "example-factors"
max_state_age_secs = 120
poll_interval_secs = 30
entry_threshold = 0.6
exit_threshold = 0.2
min_order_fraction = 0.05
max_order_fraction = 0.10
max_holding_bars = 96
```

`max_holding_bars = 96` means 24 hours on `15m` bars.

The total notional cap, holding horizon, and signal thresholds should reuse the existing research/backtest signal parameter path where possible. `exit_threshold` maps to the existing close-threshold concept in `SignalParams`.

## Error Handling

Errors are isolated to the current run or current bar.

- Data refresh error: write event, skip.
- Parquet load error: write event, skip.
- Factor compute error: write event, skip.
- SQLite busy or transient error: write event if possible, retry next loop.
- State stale: skip without calling Bitget directly.
- SIGINT/SIGTERM: stop the daemon loop and write a shutdown event if possible.

The signal daemon must not crash the Rust executor. The only shared boundary is SQLite.

## Testing Acceptance Criteria

M5 is complete when:

1. `prodigy-signal --once` runs successfully on a test SQLite database.
2. `prodigy-signal --daemon` runs a bounded test loop and exits cleanly.
3. Default enabled symbol is `ETH/USDT:USDT`, mapped to executor symbol `ETHUSDT`.
4. `example-factors` is the default source.
5. `dummy-cycle` is available only as an explicit test source.
6. The daemon refreshes data before evaluating a bar.
7. Only closed `15m` bars can produce decisions.
8. A repeated run on the same closed bar does not write a duplicate intent.
9. Intent write and processed-bar marker write are committed in one SQLite transaction.
10. Python has a no-commit intent insert path for transaction composition.
11. Rust `action=close` resolves the current exchange position size and sends a reduce-only full-position close.
12. Rust close sizing ignores Python `target_notional`.
13. Stale SQLite authority state skips intent generation.
14. `manual_override` skips intent generation for the symbol.
15. Pending or accepted intents skip new intent generation.
16. Unfinished system orders skip new intent generation.
17. Existing position plus reverse signal writes close only, never same-bar reverse/open.
18. Holding expiry uses the profit/loss branch thresholds.
19. M5 writes only `open` and `close` intents.
20. Python does not call Bitget account, position, order, or execution APIs.
21. Existing Rust executor tests still pass.
22. Existing Python tests still pass.

## Deferred

Deferred to later milestones:

- Real factor promotion and alpha validation.
- Published model-driven signal generation.
- Python WebSocket intrabar signal overlay.
- Multi-symbol activation by default.
- Live trading.
- Telegram remote trading controls.
