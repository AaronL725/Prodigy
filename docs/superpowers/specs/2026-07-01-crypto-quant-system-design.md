# Crypto Quant Timing System Design

## Goal

Build a personal crypto timing system that keeps the institutional factor-research discipline from `temp/factor_liang`, but adapts it to Bitget USDT-M futures trading.

The system supports multi-symbol trading, but the first enabled symbol is `ETH/USDT:USDT`. It uses Bitget demo trading for paper trading and Bitget live futures for production trading. BWE Equation is used only as research inspiration; all data comes from free official exchange APIs.

## Non-Goals

- No paid historical data in the first version.
- No BWE scraping or undocumented web API dependency.
- No local paper-trading matching engine; paper trading uses Bitget official demo trading.
- No FastAPI, Redis, Kafka, or service mesh in the first version.
- No production dependence on notebooks.
- No real alpha factor library in the first version; only three example factors exist to test the pipeline.

## Architecture

The system has two long-running processes connected by SQLite:

- Python research and signal process
  - pulls market data through CCXT;
  - stores historical data;
  - computes factors;
  - runs research reports and backtests;
  - trains LightGBM;
  - writes trade intents to SQLite;
  - runs the Telegram bot.

- Rust execution process
  - connects directly to Bitget official REST/WebSocket;
  - supports demo and live mode;
  - keeps latest market, account, order, and position state in memory;
  - reads trade intents and control commands from SQLite;
  - performs all execution-time risk checks;
  - places, cancels, and tracks orders;
  - writes orders, fills, positions, equity snapshots, and events back to SQLite.

SQLite is the message channel and audit log. It stores `trade_intents`, `control_commands`, `orders`, `fills`, `positions`, `equity_snapshots`, `models`, `events`, and task checkpoints.

## Buffer and Failure Isolation

Every important boundary gets a buffer so one failure does not collapse the whole system:

- Data buffer
  - Raw pulled data is written before feature computation.
  - Pull jobs store checkpoint timestamps.
  - Missing ranges are recorded and can be retried.

- Factor buffer
  - Research factors, example factors, and production factor library are isolated.
  - A bad research factor cannot enter model training until promoted.

- Model buffer
  - Training creates versioned model artifacts.
  - Live trading loads only published model versions.
  - Model updates never overwrite the active model in place.

- Message buffer
  - Python writes durable intents to SQLite.
  - Rust marks intents as accepted, rejected, executed, or failed.
  - Intent processing is idempotent by `intent_id`.

- Execution buffer
  - Rust uses cached WebSocket state for speed.
  - If market data is stale, it refuses new opening orders.
  - If SQLite, REST, or WS has a transient error, Rust records the error and retries within configured limits.

- Risk buffer
  - Strategy signals cannot bypass Rust risk checks.
  - Trading suspension blocks new opening orders but still allows close, stop-loss, cancel, and emergency actions.
  - Margin safety rules can override strategy decisions.

- Notification buffer
  - Telegram failure does not block execution.
  - Important events are persisted in SQLite even if Telegram delivery fails.

## Data Sources

Historical and research data use free official APIs through CCXT:

- Bitget futures for the enabled trading symbols.
- Binance, OKX, and Bybit for BTC/ETH funding and open interest.

The first version stores:

- 15m OHLCV;
- funding rate and funding history;
- open interest;
- metadata for exchange, symbol, timeframe, start time, end time, and last successful pull.

Rust execution uses Bitget official REST/WebSocket directly. Python does not handle live order execution.

## Research Methodology

The system keeps the institutional workflow from `factor_liang`:

- A factor is a feature, not a strategy.
- Each factor is evaluated independently before being used in a model.
- Research reports include:
  - distribution;
  - missing rate;
  - extreme values;
  - stability and autocorrelation;
  - forward-return IC/ICIR;
  - bucket returns;
  - net trading performance;
  - turnover or trigger frequency;
  - exposure diagnostics.

The A-share concepts are adapted:

- Cross-sectional RankIC becomes time-series and cross-symbol forward-return evaluation.
- Index-relative excess return becomes net trading return versus flat and buy-and-hold baselines.
- Risk factor exposure becomes crypto exposure checks: volatility, trend beta, funding, OI, BTC/ETH correlation, fee sensitivity, and regime sensitivity.

## Factor Layout

The code separates research from production:

- `notebooks/research/`
  - Jupyter notebooks for exploratory factor research and visual analysis.

- `research_factors/`
  - experimental Python factor files.
  - allowed to be rough.

- `factor_library/`
  - validated production-quality factors.
  - only promoted factors are used for production training.

- `examples/`
  - example factors used only to test the pipeline:
    - 15m momentum;
    - funding z-score;
    - OI change.

All factors expose a common interface and output rows shaped like:

```text
timestamp, symbol, factor_name, value
```

Promotion from research to library records:

- factor name;
- data dependencies;
- parameters;
- research report path;
- validated sample period;
- allowed horizons;
- whether it is allowed for training;
- whether it is allowed for live scoring.

## Backtesting and Factor Evaluation

The backtest design keeps the convenient `Backtester` class style from `factor_liang`, but avoids repeated pasted code.

`Backtester` is a facade with methods such as:

- `plot_factor_distribution()`
- `plot_autocorrelation()`
- `plot_ic_cumsum(horizon)`
- `plot_bucket_returns(horizon)`
- `performance_summary()`
- `exposure_report()`
- `run_full_report()`

Internally, it delegates to smaller evaluator, simulator, plotting, and reporting modules.

The crypto backtester includes:

- maker fee;
- taker fee;
- 59% futures commission rebate;
- funding cost;
- configurable slippage assumptions;
- execution simulation for maker attempts and taker fallback;
- single-order notional cap;
- total notional cap;
- stop-loss;
- trailing take-profit;
- 24h holding review;
- trading suspension rules.

The default VIP0 Bitget futures fee assumptions are:

- maker fee: `0.02%`;
- taker fee: `0.06%`;
- net maker fee after rebate: `0.0082%`;
- net taker fee after rebate: `0.0246%`.

## Machine Learning

LightGBM is the factor aggregator.

It does not place orders and cannot bypass risk checks.

Training uses multi-horizon labels:

- 1h forward return;
- 4h forward return;
- 24h forward return.

The model outputs an `alpha_score`. The trading layer maps this score into long, short, exit, or no-action signals through configurable thresholds.

The ML workflow:

- walk-forward training and validation;
- weekly tuning and retraining;
- explicit model publication step;
- live process loads only published model versions;
- each model version records:
  - training interval;
  - validation interval;
  - factor versions;
  - parameters;
  - validation metrics;
  - model artifact hash.

## Trading and Position Rules

Trading venue:

- Bitget USDT-M futures.

Initial trading symbol:

- `ETH/USDT:USDT`.

The code supports multi-symbol trading from the start.

Account and position mode:

- cross margin;
- one-way net position mode;
- default exchange leverage: `5x`, configurable.

Leverage controls margin efficiency. It does not decide position size.

Position sizing:

- total notional cap: `5x equity`;
- no per-symbol notional cap in the first version;
- per-order notional cap: `10%` of total notional cap, default `0.5x equity`;
- actual order notional is decided by model signal strength and target exposure, then clipped by per-order and total caps.

Signal behavior:

- The system may compute scores every 15 minutes.
- It does not mechanically rebalance every 15 minutes.
- It trades only on signal events.
- Opening signal: score crosses into long or short zone and there is no same-direction position.
- Existing positions are adjusted only by reverse signal, exit signal, stop-loss, trailing take-profit, 24h holding review, or risk controls.

Holding rules:

- Target holding time is within 24h.
- At 24h, a fresh same-direction signal can extend the position.
- Without a fresh same-direction signal, the position exits.

Stop-loss:

- single-position notional loss rate reaches `8%`, configurable.

Trailing take-profit:

- activates when unrealized PnL divided by position notional reaches `10%`, configurable.
- uses price-action structure high/low plus ATR buffer.

Funding:

- funding is included in backtest and PnL.
- first version does not use funding as an entry or exit filter.

## Risk Controls

Trading suspension:

- if current unrealized net loss reaches `10% equity`, stop new opening orders;
- closing, stop-loss, cancellation, and emergency actions remain allowed.

Margin safety:

- if Bitget margin/risk indicators approach configured danger thresholds, Rust cancels open system orders and reduces or closes positions.
- this is an anti-liquidation rule, separate from trading suspension.

Startup safety:

- Rust starts by syncing account, positions, and open orders.
- It cancels stale open orders created by this system.

Order safety:

- every order has a `clientOid`;
- Rust cancels only system-created orders;
- partial fills only act on the remaining quantity;
- signal invalidation, timeout, stop, startup, and risk events cancel open system orders.

## Execution Rules

Opening orders:

1. Submit a `post_only` limit order.
   - Long: `best_bid`.
   - Short: `best_ask`.
   - Wait `15s`.
2. If not fully filled, cancel and recheck signal and risk.
3. Submit a second `post_only` limit order if still valid.
   - Long: `best_bid + 1 tick`, without crossing `best_ask`.
   - Short: `best_ask - 1 tick`, without crossing `best_bid`.
   - Wait `15s`.
4. If still not fully filled, cancel and recheck.
5. If still valid, use taker for the remaining quantity; otherwise abandon the intent.

Normal closing orders:

1. Submit a `post_only` limit order.
   - Close long: `best_ask`.
   - Close short: `best_bid`.
   - Wait `8s`.
2. If not fully filled, cancel and recheck.
3. If still required, use taker for the remaining quantity.

Emergency actions:

- stop-loss;
- margin safety;
- `/close_all`;
- critical failure requiring de-risking.

These bypass maker attempts and use taker orders.

Execution speed:

- Rust executor is always running.
- Rust maintains WebSocket caches for order book, account, orders, and positions.
- SQLite polling interval defaults to `250ms`, configurable.
- Rust does not run full account sync for every intent.
- If cached market data is stale, opening orders are rejected or handled conservatively.

## Paper Trading and Live Trading

Paper trading uses Bitget official demo trading:

- demo product type such as `SUSDT-FUTURES`;
- `PAPTRADING: 1` where required by Bitget;
- demo REST/WebSocket endpoints or headers as documented by Bitget.

The same Rust execution code supports demo and live mode through configuration.

## Telegram Bot

Telegram is implemented in Python.

Active notifications are intentionally sparse.

Send notifications only for:

- opening fill;
- add/reduce fill;
- closing fill;
- final handling of partial fills;
- major order failure;
- cancel failure;
- Rust executor crash;
- long Bitget WS disconnection;
- severe data staleness causing trading suspension;
- trading suspension;
- margin safety action;
- `/close_all` result;
- startup cleanup that cancels stale system orders.

Do not notify for:

- every score calculation;
- normal polling;
- non-trading factor movement;
- ordinary maker timeout that is handled normally;
- normal cancel and retry.

Queries:

- `/status`
- `/positions`
- `/orders`
- `/pnl`
- `/risk`
- `/model`

Controls:

- `/stop`
- `/resume`
- `/close_all`

Control commands require a Telegram user-id whitelist. `/close_all` requires confirmation.

## Configuration

All tunable values live in config, not scattered in code:

- enabled symbols;
- demo/live mode;
- leverage;
- total notional cap;
- per-order notional cap;
- signal thresholds;
- stop-loss;
- trailing take-profit start;
- ATR and swing parameters;
- maker timeouts;
- SQLite poll interval;
- stale data thresholds;
- fee and rebate assumptions;
- weekly training schedule;
- model version to load;
- Telegram whitelist.

API keys are loaded from environment variables or local ignored secret files.

## Testing

Python checks:

- factor interface smoke tests;
- forward-return calculation;
- IC/ICIR calculation;
- bucket return calculation;
- fee, funding, and PnL calculation;
- `Backtester.run_full_report()` smoke test;
- LightGBM training smoke test.

Rust checks:

- Bitget signature test;
- intent idempotency test;
- stale-data opening-order rejection;
- risk check rejection;
- order state machine for partial fills;
- cancellation retry behavior;
- SQLite recovery behavior.

Integration checks:

- Python writes a trade intent and Rust demo executor consumes it.
- Telegram `/stop` blocks new opening orders.
- Telegram `/close_all` writes a command and Rust performs cancel plus taker close.
- Rust startup cancels stale system-created orders.

## Initial Milestone

The first working milestone is:

1. initialize Python and Rust project skeletons;
2. create SQLite schema;
3. pull Bitget ETH 15m OHLCV with CCXT;
4. implement three example factors;
5. implement `Backtester.run_full_report()`;
6. train a LightGBM smoke model;
7. write one demo trade intent;
8. have Rust consume and reject it safely in dry executor mode;
9. implement Telegram `/status`, `/stop`, and `/resume`.

Live orders are not part of the first milestone until the above path is stable.
