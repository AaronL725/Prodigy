# Crypto Quant Second Milestone Research Design

## Goal

Build the first usable offline research loop for ETH Bitget futures: official free market data, parquet research storage, factor notebooks, bar-level backtesting, and LightGBM factor aggregation smoke tests.

This milestone does not place demo or live orders. It produces research data, diagnostics, backtest reports, and model artifacts only.

## Scope

Second milestone includes:

- ETH/USDT:USDT Bitget USDT-M futures only.
- 15m OHLCV from 2024-01-01 to now.
- Historical funding rates from 2024-01-01 to now.
- Parquet gzip as the main research and backtest storage.
- SQLite only for checkpoints, task state, audit events, and model metadata.
- Research notebooks styled after `temp/factor_liang`.
- Three example factors for pipeline and ML aggregation testing.
- 15m bar-level lot backtesting.
- LightGBM smoke aggregation with purged walk-forward validation and final 30D holdout.
- Minimal `argparse` CLI for data backfill and example model training.

Second milestone excludes:

- Historical open interest. Bitget official free API and CCXT do not provide full historical OI; current OI polling is left for a later milestone.
- Bitget demo or live order placement.
- Real Telegram network bot.
- Production factor promotion workflow.
- Automated model tuning.
- Production model publication.
- Trade intent generation from the ML model.

## Project Layout

Keep the top level small:

```text
Prodigy/
  configs/
  crates/
  docs/
  schema/
  src/
  tests/
  research/
  data/
  models/
  var/
```

New directories:

```text
research/
  notebooks/
  reports/
  factor_library/
  scratch/

data/
  raw/
  processed/

models/
  example_lgbm/

var/
```

Directory rules:

- `src/`: production Python library code.
- `crates/`: Rust executor code.
- `research/`: notebooks, reports, scratch research, and future promoted factors.
- `data/`: local parquet data; ignored by git except `.gitkeep`.
- `models/`: local model artifacts; ignored by git except `.gitkeep`.
- `var/`: SQLite databases, logs, and runtime files; ignored by git except `.gitkeep`.
- `docs/`: Superpowers specs and plans.
- `configs/`: tunable config.
- `schema/`: SQLite schema migrations.

## Data Sources

Symbol:

```text
ETH/USDT:USDT
```

Venue:

```text
Bitget USDT-M futures
```

Start date:

```text
2024-01-01
```

Data:

- OHLCV: CCXT Bitget, timeframe `15m`.
- Funding history: Bitget official REST endpoint `/api/v2/mix/market/history-fund-rate`.
- Open interest: excluded from this milestone.

CCXT support verified in local `ccxt 4.5.63`:

```text
fetchOHLCV = True
fetchFundingRateHistory = True
fetchOpenInterest = True
fetchOpenInterestHistory = False
```

Funding is implemented through official REST instead of CCXT so pagination, fields, and failures stay explicit.

Network:

- Try direct connection first.
- If direct connection fails, retry with `http://127.0.0.1:7897`.
- Proxy URL is configurable.
- Network failures must not corrupt existing parquet files or checkpoints.

## Storage

Research and backtest data uses date-partitioned parquet gzip:

```text
data/raw/bitget/ETH-USDT-SWAP/ohlcv/timeframe=15m/date=YYYY-MM-DD.parquet.gzip
data/raw/bitget/ETH-USDT-SWAP/funding_rates/date=YYYY-MM-DD.parquet.gzip
data/processed/example_features.parquet.gzip
```

Each daily partition is written atomically:

1. Write to a temporary path.
2. Read it back and validate minimal schema.
3. Move it into the final partition path.

SQLite stores small operational state only:

- data backfill checkpoints;
- task success and failure state;
- gap and data-quality events;
- model metadata in `models`;
- audit events.

Large market data, features, and predictions are not stored in SQLite.

## Data Quality

The backfill command reports:

- expected 15m bar count per day;
- missing timestamps;
- duplicate timestamps;
- non-monotonic timestamps;
- null OHLCV values;
- negative volume;
- funding rows per day;
- latest successful checkpoint.

Data quality problems are recorded in SQLite `events` and printed in the CLI summary. Existing valid partitions are not deleted automatically.

## CLI

Use standard-library `argparse`; do not add Typer or Click.

Data backfill:

```bash
prodigy-data backfill \
  --symbol ETH/USDT:USDT \
  --start 2024-01-01 \
  --timeframe 15m
```

Optional flags:

```text
--end YYYY-MM-DD
--data-root data
--db var/prodigy.sqlite
--proxy-url http://127.0.0.1:7897
```

Example ML training:

```bash
prodigy-ml train-example \
  --symbol ETH/USDT:USDT \
  --horizon 1h
```

The CLI is for repeatable research jobs, not a final product interface.

## Research Notebooks

Notebook location:

```text
research/notebooks/
```

Create:

```text
research/notebooks/00_data_check.ipynb
research/notebooks/01_example_momentum_factor.ipynb
research/notebooks/02_example_funding_factor.ipynb
research/notebooks/03_example_volatility_factor.ipynb
research/notebooks/99_combine_example_factors.ipynb
```

Notebook style follows `temp/factor_liang`:

- research-first, code-driven notebooks;
- import common research packages up front;
- read parquet from the shared data area;
- define factor classes or compute functions directly in notebook cells;
- call `Backtester` diagnostics from the notebook;
- keep output charts in the notebook;
- do not save each research factor series as a long-lived parquet asset.

The three example factors exist only to test research, backtest, and ML aggregation flow:

- `ExampleMomentumFactor`;
- `ExampleFundingFactor`;
- `ExampleVolatilityFactor`.

`99_combine_example_factors.ipynb` recomputes or imports the three example factor series in memory, builds a feature matrix, and may save:

```text
data/processed/example_features.parquet.gzip
```

That file is a temporary ML smoke artifact, not a promoted factor-library asset.

`research/factor_library/` remains empty except `.gitkeep` in this milestone.

## Factor, Signal, Backtest Separation

The research flow is:

```text
factor value series -> signal layer -> Backtester
```

The factor layer outputs continuous scores:

```text
score in [-1, +1]
```

The signal layer converts scores into lot-level trading events.

Opening rule:

```text
abs(score) >= 0.6
```

Single-order size maps linearly:

```text
abs(score) = 0.6 -> 5% of total_notional_cap
abs(score) = 1.0 -> 10% of total_notional_cap
```

Same-direction add rules:

- Same-direction add is allowed.
- `add_cooldown_bars = 4` by default.
- Total notional cannot exceed `total_notional_cap`.

Opposite close rules:

```text
long lot closes when score <= -0.2
short lot closes when score >= +0.2
```

Each opening creates an independent lot. Closing closes full lots, not partial shares of a net position.

## Holding Rules

Default:

```text
max_holding_hours = 24
extension_hours = 1
```

At 24h:

- Long lot extends 1h if `score >= 0.6`; otherwise it closes.
- Short lot extends 1h if `score <= -0.6`; otherwise it closes.

After extension, the same 1h review repeats until the signal no longer confirms the position.

## Risk and Exit Rules

Stop loss:

```text
stop_loss_position_notional_fraction = 0.08
```

Trailing take profit starts at:

```text
trailing_start_position_notional_fraction = 0.10
```

Trailing stop uses structure high/low plus ATR buffer:

```text
atr_window = 14
swing_lookback_bars = 5
atr_multiplier = 1.5
```

Long lots:

- detect recent swing lows;
- trailing stop is `swing_low - atr * atr_multiplier`;
- trailing stop can only move upward;
- close if bar close is below the trailing stop.

Short lots:

- detect recent swing highs;
- trailing stop is `swing_high + atr * atr_multiplier`;
- trailing stop can only move downward;
- close if bar close is above the trailing stop.

## Backtester

Backtester is a convenient facade inspired by `factor_liang`, but adapted to single-symbol crypto timing.

It supports:

- factor distribution;
- score autocorrelation;
- forward-return IC and rolling IC;
- signal distribution;
- lot trade simulation;
- maker/taker fee;
- 59% commission rebate;
- funding cost;
- configurable slippage;
- total notional cap;
- per-lot lifecycle;
- equity curve;
- drawdown;
- trade summary;
- performance summary.

Notebook-facing method names should preserve the familiar style where practical:

```python
bt.PlotAutocorrelation()
bt.PlotRankIcCumsum()
bt.PlotSignalDistribution()
bt.PlotEquityCurve()
bt.PlotDrawdown()
bt.GetPerformanceSummary()
bt.GetTradeSummary()
```

All trading and risk parameters are configurable.

## ML Aggregation Smoke

Machine learning is used to aggregate researcher-designed features. It is not used to discover opaque factors in this milestone.

Features:

```text
example_momentum
example_funding
example_volatility
```

Labels:

```text
15m forward return
1h forward return
4h forward return
24h forward return
```

Default training horizon:

```text
1h
```

Validation:

```text
Purged Walk-Forward + Final 30D Holdout
```

Defaults:

```text
min_train_days = 365
valid_days = 30
step_days = 30
final_holdout_days = 30
purge_gap = label_horizon_bars
mode = expanding window
```

For 1h labels, default purge gap is 4 bars. For 24h labels, default purge gap is 96 bars.

Model:

- LightGBM fixed-parameter smoke model;
- no automated hyperparameter tuning;
- no production model publication;
- no trade intent generation.

The trainer saves:

```text
models/example_lgbm/<model_version>.txt
```

SQLite `models` metadata records:

- `model_version`;
- `train_start`;
- `train_end`;
- `validation_start`;
- `validation_end`;
- `artifact_path`;
- `artifact_hash`;
- `metrics_json`.

Metrics include:

- fold train rows;
- fold validation rows;
- prediction IC;
- directional accuracy;
- simple long-short validation return;
- feature importance;
- final 30D holdout metrics;
- artifact hash.

## Acceptance Criteria

Engineering success is required; alpha performance is not.

Data:

- `prodigy-data backfill` writes ETH 15m OHLCV parquet partitions.
- `prodigy-data backfill` writes ETH funding-rate parquet partitions.
- backfill updates checkpoint state.
- data-quality summary is printed and recorded.

Research:

- all five notebooks exist under `research/notebooks/`;
- notebooks read the shared parquet data;
- each example factor notebook creates a factor value series and calls `Backtester`;
- notebook outputs are preserved.

Backtest:

- score-to-signal conversion runs with the configured threshold and lot rules;
- bar-level lot simulation runs with fees, rebate, funding, slippage, stop-loss, trailing take-profit, and 24h review;
- performance summary, trade summary, equity curve, and drawdown are produced.

ML:

- feature matrix with three example factors is generated;
- 15m, 1h, 4h, and 24h labels can be generated;
- default 1h LightGBM smoke model trains;
- purged walk-forward folds are generated;
- final 30D holdout is excluded from fold selection and evaluated last;
- model artifact is saved;
- SQLite `models` metadata row is written.

Verification:

- Python tests pass in `quantmamba`;
- Rust tests still pass;
- `cargo fmt --check` passes;
- `cargo clippy --all-targets -- -D warnings` passes.

