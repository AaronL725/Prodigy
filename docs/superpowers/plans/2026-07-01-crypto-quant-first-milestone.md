# Crypto Quant First Milestone Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the first testable milestone for the Prodigy crypto quant system: Python data/factor/backtest/model smoke path, SQLite outbox, Rust dry executor, and minimal Telegram control service.

**Architecture:** Python owns research, factor evaluation, model training smoke tests, and Telegram command handling. Rust owns execution-loop structure and consumes SQLite intents in dry mode only. SQLite is the durable message channel and audit log between the two processes.

**Tech Stack:** Python 3.11+, pandas, numpy, ccxt, lightgbm, pytest, matplotlib; Rust 2021, rusqlite, serde, anyhow; SQLite; Git.

---

## Scope

This plan implements the first milestone from `docs/superpowers/specs/2026-07-01-crypto-quant-system-design.md`.

It does not place live Bitget orders. The Rust executor consumes intents and rejects them safely in dry mode. Bitget REST/WebSocket execution comes in a separate plan after the research, database, and control path are stable.

## File Structure

- Create `pyproject.toml`
  - Python package metadata, dependencies, pytest config.
- Create `Cargo.toml`
  - Rust workspace root.
- Create `crates/executor/Cargo.toml`
  - Rust dry executor crate dependencies.
- Create `crates/executor/src/main.rs`
  - CLI entrypoint for consuming intents in dry mode.
- Create `crates/executor/src/db.rs`
  - SQLite access for the Rust executor.
- Create `crates/executor/src/types.rs`
  - Rust types matching the SQLite outbox fields.
- Create `schema/001_initial.sql`
  - Shared SQLite schema.
- Create `configs/default.toml`
  - Safe first-milestone defaults.
- Create `src/prodigy/__init__.py`
  - Python package marker.
- Create `src/prodigy/config.py`
  - Standard-library config loader.
- Create `src/prodigy/db.py`
  - SQLite schema init and Python DB helpers.
- Create `src/prodigy/data/ccxt_fetcher.py`
  - CCXT OHLCV fetcher with injectable exchange for tests.
- Create `src/prodigy/factors/base.py`
  - Common factor output contract.
- Create `src/prodigy/factors/examples.py`
  - Three example factors: momentum, funding z-score, OI change.
- Create `src/prodigy/research/evaluator.py`
  - Forward returns, IC, bucket returns, fee/PnL helpers.
- Create `src/prodigy/research/backtester.py`
  - `Backtester` facade inspired by `factor_liang`.
- Create `src/prodigy/ml/trainer.py`
  - LightGBM smoke trainer and model metadata hashing.
- Create `src/prodigy/signals/intents.py`
  - Python helper to write trade intents.
- Create `src/prodigy/telegram/service.py`
  - Pure Telegram command service for `/status`, `/stop`, `/resume`.
- Create `tests/` files listed per task.

## Task 1: Python Project Skeleton

**Files:**
- Create: `pyproject.toml`
- Create: `src/prodigy/__init__.py`
- Create: `tests/test_package_import.py`

- [ ] **Step 1: Write the package import test**

Create `tests/test_package_import.py`:

```python
def test_package_imports():
    import prodigy

    assert prodigy.__version__ == "0.1.0"
```

- [ ] **Step 2: Run the test to verify it fails**

Run:

```bash
python -m pytest tests/test_package_import.py -v
```

Expected: FAIL with `ModuleNotFoundError: No module named 'prodigy'`.

- [ ] **Step 3: Create the Python package metadata**

Create `pyproject.toml`:

```toml
[build-system]
requires = ["setuptools>=68", "wheel"]
build-backend = "setuptools.build_meta"

[project]
name = "prodigy-quant"
version = "0.1.0"
requires-python = ">=3.11"
dependencies = [
  "ccxt>=4.4.0",
  "lightgbm>=4.5.0",
  "matplotlib>=3.8.0",
  "numpy>=1.26.0",
  "pandas>=2.2.0",
  "pyarrow>=16.0.0",
  "pytest>=8.0.0",
]

[tool.setuptools.packages.find]
where = ["src"]

[tool.pytest.ini_options]
testpaths = ["tests"]
pythonpath = ["src"]
addopts = "-q"
```

Create `src/prodigy/__init__.py`:

```python
__version__ = "0.1.0"
```

- [ ] **Step 4: Run the test to verify it passes**

Run:

```bash
python -m pytest tests/test_package_import.py -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pyproject.toml src/prodigy/__init__.py tests/test_package_import.py
git commit -m "feat: add python package skeleton"
```

## Task 2: Shared SQLite Schema

**Files:**
- Create: `schema/001_initial.sql`
- Create: `src/prodigy/db.py`
- Create: `tests/test_db_schema.py`

- [ ] **Step 1: Write the failing schema test**

Create `tests/test_db_schema.py`:

```python
import sqlite3

from prodigy.db import connect, init_db


def test_init_db_creates_core_tables(tmp_path):
    db_path = tmp_path / "prodigy.sqlite"

    with connect(db_path) as conn:
        init_db(conn)
        tables = {
            row[0]
            for row in conn.execute(
                "select name from sqlite_master where type = 'table'"
            ).fetchall()
        }

    assert {
        "trade_intents",
        "control_commands",
        "orders",
        "fills",
        "positions",
        "equity_snapshots",
        "models",
        "events",
        "task_checkpoints",
    }.issubset(tables)


def test_trade_intents_are_unique_by_id(tmp_path):
    db_path = tmp_path / "prodigy.sqlite"

    with connect(db_path) as conn:
        init_db(conn)
        conn.execute(
            """
            insert into trade_intents (
              intent_id, created_at, symbol, side, action, target_notional,
              max_order_notional, status, source
            ) values (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "intent-1",
                "2026-07-01T00:00:00Z",
                "ETH/USDT:USDT",
                "long",
                "open",
                1000.0,
                500.0,
                "pending",
                "test",
            ),
        )

        try:
            conn.execute(
                """
                insert into trade_intents (
                  intent_id, created_at, symbol, side, action, target_notional,
                  max_order_notional, status, source
                ) values (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "intent-1",
                    "2026-07-01T00:00:01Z",
                    "ETH/USDT:USDT",
                    "long",
                    "open",
                    1000.0,
                    500.0,
                    "pending",
                    "test",
                ),
            )
        except sqlite3.IntegrityError:
            duplicate_rejected = True
        else:
            duplicate_rejected = False

    assert duplicate_rejected is True
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
python -m pytest tests/test_db_schema.py -v
```

Expected: FAIL with `ModuleNotFoundError` or import error for `prodigy.db`.

- [ ] **Step 3: Create the schema**

Create `schema/001_initial.sql`:

```sql
create table if not exists trade_intents (
  intent_id text primary key,
  created_at text not null,
  symbol text not null,
  side text not null check (side in ('long', 'short', 'flat')),
  action text not null check (action in ('open', 'close', 'reduce', 'reverse')),
  target_notional real not null,
  max_order_notional real not null,
  status text not null check (status in ('pending', 'accepted', 'rejected', 'executed', 'failed')),
  source text not null,
  reason text,
  model_version text,
  processed_at text,
  error text
);

create table if not exists control_commands (
  command_id text primary key,
  created_at text not null,
  command text not null check (command in ('stop', 'resume', 'close_all')),
  status text not null check (status in ('pending', 'accepted', 'rejected', 'executed', 'failed')),
  requested_by text not null,
  processed_at text,
  error text
);

create table if not exists orders (
  order_id text primary key,
  client_oid text not null unique,
  intent_id text,
  symbol text not null,
  side text not null,
  action text not null,
  order_type text not null,
  status text not null,
  price real,
  size real,
  filled_size real not null default 0,
  created_at text not null,
  updated_at text not null,
  foreign key (intent_id) references trade_intents(intent_id)
);

create table if not exists fills (
  fill_id text primary key,
  order_id text not null,
  symbol text not null,
  side text not null,
  price real not null,
  size real not null,
  fee real not null,
  created_at text not null,
  foreign key (order_id) references orders(order_id)
);

create table if not exists positions (
  symbol text primary key,
  side text not null,
  notional real not null,
  entry_price real not null,
  unrealized_pnl real not null,
  updated_at text not null
);

create table if not exists equity_snapshots (
  snapshot_id text primary key,
  created_at text not null,
  equity real not null,
  available_margin real not null,
  unrealized_pnl real not null,
  realized_pnl_24h real not null
);

create table if not exists models (
  model_version text primary key,
  created_at text not null,
  train_start text not null,
  train_end text not null,
  validation_start text not null,
  validation_end text not null,
  artifact_path text not null,
  artifact_hash text not null,
  metrics_json text not null
);

create table if not exists events (
  event_id text primary key,
  created_at text not null,
  severity text not null check (severity in ('info', 'warning', 'error', 'critical')),
  component text not null,
  message text not null,
  payload_json text not null default '{}',
  delivered_to_telegram integer not null default 0
);

create table if not exists task_checkpoints (
  task_name text primary key,
  updated_at text not null,
  checkpoint_value text not null
);

create index if not exists idx_trade_intents_status_created
  on trade_intents(status, created_at);

create index if not exists idx_control_commands_status_created
  on control_commands(status, created_at);

create index if not exists idx_events_delivery
  on events(delivered_to_telegram, severity, created_at);
```

- [ ] **Step 4: Implement DB helpers**

Create `src/prodigy/db.py`:

```python
from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Iterator


SCHEMA_PATH = Path(__file__).resolve().parents[2] / "schema" / "001_initial.sql"


def connect(path: str | Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("pragma foreign_keys = on")
    conn.execute("pragma journal_mode = wal")
    return conn


def init_db(conn: sqlite3.Connection, schema_path: Path = SCHEMA_PATH) -> None:
    conn.executescript(schema_path.read_text())
    conn.commit()


def rows(conn: sqlite3.Connection, query: str, params: tuple = ()) -> Iterator[sqlite3.Row]:
    yield from conn.execute(query, params).fetchall()
```

- [ ] **Step 5: Run tests to verify they pass**

Run:

```bash
python -m pytest tests/test_db_schema.py -v
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add schema/001_initial.sql src/prodigy/db.py tests/test_db_schema.py
git commit -m "feat: add sqlite schema"
```

## Task 3: Config Loader and Safe Defaults

**Files:**
- Create: `configs/default.toml`
- Create: `src/prodigy/config.py`
- Create: `tests/test_config.py`

- [ ] **Step 1: Write the failing config tests**

Create `tests/test_config.py`:

```python
from pathlib import Path

from prodigy.config import load_config


def test_load_default_config():
    cfg = load_config(Path("configs/default.toml"))

    assert cfg["trading"]["enabled_symbols"] == ["ETH/USDT:USDT"]
    assert cfg["trading"]["leverage"] == 5
    assert cfg["risk"]["total_notional_cap_x_equity"] == 5.0
    assert cfg["risk"]["per_order_cap_fraction_of_total"] == 0.10
    assert cfg["execution"]["open_maker_timeout_seconds"] == 15


def test_config_rejects_missing_top_level_section(tmp_path):
    path = tmp_path / "bad.toml"
    path.write_text("[trading]\nleverage = 5\n")

    try:
        load_config(path)
    except ValueError as exc:
        message = str(exc)
    else:
        message = ""

    assert "missing config section: risk" in message
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
python -m pytest tests/test_config.py -v
```

Expected: FAIL with import error for `prodigy.config`.

- [ ] **Step 3: Add safe default config**

Create `configs/default.toml`:

```toml
[trading]
mode = "demo"
enabled_symbols = ["ETH/USDT:USDT"]
leverage = 5

[risk]
total_notional_cap_x_equity = 5.0
per_order_cap_fraction_of_total = 0.10
trading_suspension_unrealized_loss_x_equity = 0.10
stop_loss_position_notional_fraction = 0.08
trailing_start_position_notional_fraction = 0.10

[execution]
sqlite_poll_interval_ms = 250
open_maker_timeout_seconds = 15
close_maker_timeout_seconds = 8
stale_market_data_seconds = 3

[fees]
maker_rate = 0.0002
taker_rate = 0.0006
rebate_fraction = 0.59

[model]
active_model_version = "dry-run"
score_long_threshold = 0.60
score_short_threshold = -0.60
score_exit_threshold = 0.10

[telegram]
enabled = false
allowed_user_ids = []
```

- [ ] **Step 4: Implement config loader**

Create `src/prodigy/config.py`:

```python
from __future__ import annotations

from pathlib import Path
import tomllib
from typing import Any


REQUIRED_SECTIONS = (
    "trading",
    "risk",
    "execution",
    "fees",
    "model",
    "telegram",
)


def load_config(path: str | Path) -> dict[str, Any]:
    data = tomllib.loads(Path(path).read_text())
    for section in REQUIRED_SECTIONS:
        if section not in data:
            raise ValueError(f"missing config section: {section}")
    return data
```

- [ ] **Step 5: Run tests to verify they pass**

Run:

```bash
python -m pytest tests/test_config.py -v
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add configs/default.toml src/prodigy/config.py tests/test_config.py
git commit -m "feat: add config loader"
```

## Task 4: CCXT OHLCV Fetcher

**Files:**
- Create: `src/prodigy/data/__init__.py`
- Create: `src/prodigy/data/ccxt_fetcher.py`
- Create: `tests/test_ccxt_fetcher.py`

- [ ] **Step 1: Write failing fetcher tests**

Create `tests/test_ccxt_fetcher.py`:

```python
from prodigy.data.ccxt_fetcher import fetch_ohlcv_frame


class FakeExchange:
    def __init__(self):
        self.loaded = False
        self.calls = []

    def load_markets(self):
        self.loaded = True

    def fetch_ohlcv(self, symbol, timeframe, since=None, limit=None, params=None):
        self.calls.append((symbol, timeframe, since, limit, params))
        return [
            [1719792000000, 3000.0, 3010.0, 2990.0, 3005.0, 12.0],
            [1719792900000, 3005.0, 3020.0, 3000.0, 3015.0, 15.0],
        ]


def test_fetch_ohlcv_frame_normalizes_columns():
    exchange = FakeExchange()

    frame = fetch_ohlcv_frame(
        exchange=exchange,
        symbol="ETH/USDT:USDT",
        timeframe="15m",
        since_ms=1719792000000,
        limit=2,
    )

    assert exchange.loaded is True
    assert list(frame.columns) == [
        "timestamp",
        "symbol",
        "open",
        "high",
        "low",
        "close",
        "volume",
    ]
    assert frame["symbol"].tolist() == ["ETH/USDT:USDT", "ETH/USDT:USDT"]
    assert frame["close"].tolist() == [3005.0, 3015.0]
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
python -m pytest tests/test_ccxt_fetcher.py -v
```

Expected: FAIL with import error for `prodigy.data`.

- [ ] **Step 3: Implement fetcher**

Create `src/prodigy/data/__init__.py`:

```python
"""Market data loading helpers."""
```

Create `src/prodigy/data/ccxt_fetcher.py`:

```python
from __future__ import annotations

from typing import Any

import pandas as pd


OHLCV_COLUMNS = ["timestamp", "open", "high", "low", "close", "volume"]


def fetch_ohlcv_frame(
    exchange: Any,
    symbol: str,
    timeframe: str,
    since_ms: int | None = None,
    limit: int | None = None,
    params: dict[str, Any] | None = None,
) -> pd.DataFrame:
    exchange.load_markets()
    rows = exchange.fetch_ohlcv(
        symbol,
        timeframe,
        since=since_ms,
        limit=limit,
        params=params or {},
    )
    frame = pd.DataFrame(rows, columns=OHLCV_COLUMNS)
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], unit="ms", utc=True)
    frame.insert(1, "symbol", symbol)
    return frame[["timestamp", "symbol", "open", "high", "low", "close", "volume"]]
```

- [ ] **Step 4: Run tests to verify they pass**

Run:

```bash
python -m pytest tests/test_ccxt_fetcher.py -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/prodigy/data tests/test_ccxt_fetcher.py
git commit -m "feat: add ccxt ohlcv fetcher"
```

## Task 5: Example Factor Interface and Factors

**Files:**
- Create: `src/prodigy/factors/__init__.py`
- Create: `src/prodigy/factors/base.py`
- Create: `src/prodigy/factors/examples.py`
- Create: `tests/test_example_factors.py`

- [ ] **Step 1: Write failing factor tests**

Create `tests/test_example_factors.py`:

```python
import pandas as pd

from prodigy.factors.examples import funding_zscore, momentum_15m, oi_change


def market_frame():
    ts = pd.date_range("2026-07-01", periods=6, freq="15min", tz="UTC")
    return pd.DataFrame(
        {
            "timestamp": ts,
            "symbol": ["ETH/USDT:USDT"] * 6,
            "open": [100, 101, 102, 101, 103, 104],
            "high": [101, 102, 103, 102, 104, 105],
            "low": [99, 100, 101, 100, 102, 103],
            "close": [101, 102, 101, 103, 104, 106],
            "volume": [10, 12, 11, 13, 14, 15],
            "funding_rate": [0.001, 0.002, 0.001, 0.003, 0.004, 0.005],
            "open_interest": [1000, 1010, 1005, 1030, 1040, 1060],
        }
    )


def test_momentum_factor_output_contract():
    result = momentum_15m(market_frame(), periods=2)

    assert list(result.columns) == ["timestamp", "symbol", "factor_name", "value"]
    assert result["factor_name"].unique().tolist() == ["momentum_15m"]
    assert result["value"].notna().sum() == 4


def test_funding_zscore_output_contract():
    result = funding_zscore(market_frame(), window=3)

    assert result["factor_name"].unique().tolist() == ["funding_zscore"]
    assert result["value"].notna().sum() == 4


def test_oi_change_output_contract():
    result = oi_change(market_frame(), periods=2)

    assert result["factor_name"].unique().tolist() == ["oi_change"]
    assert result["value"].notna().sum() == 4
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
python -m pytest tests/test_example_factors.py -v
```

Expected: FAIL with import error for `prodigy.factors`.

- [ ] **Step 3: Implement factor interface and examples**

Create `src/prodigy/factors/__init__.py`:

```python
"""Factor interfaces and example factors."""
```

Create `src/prodigy/factors/base.py`:

```python
from __future__ import annotations

import pandas as pd


FACTOR_COLUMNS = ["timestamp", "symbol", "factor_name", "value"]


def factor_frame(source: pd.DataFrame, factor_name: str, value: pd.Series) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "timestamp": source["timestamp"],
            "symbol": source["symbol"],
            "factor_name": factor_name,
            "value": value.astype("float64"),
        }
    )[FACTOR_COLUMNS]
```

Create `src/prodigy/factors/examples.py`:

```python
from __future__ import annotations

import pandas as pd

from prodigy.factors.base import factor_frame


def momentum_15m(frame: pd.DataFrame, periods: int = 4) -> pd.DataFrame:
    values = frame.groupby("symbol", group_keys=False)["close"].pct_change(periods)
    return factor_frame(frame, "momentum_15m", values)


def funding_zscore(frame: pd.DataFrame, window: int = 20) -> pd.DataFrame:
    grouped = frame.groupby("symbol", group_keys=False)["funding_rate"]
    mean = grouped.transform(lambda s: s.rolling(window, min_periods=2).mean())
    std = grouped.transform(lambda s: s.rolling(window, min_periods=2).std())
    values = (frame["funding_rate"] - mean) / std.replace(0, pd.NA)
    return factor_frame(frame, "funding_zscore", values)


def oi_change(frame: pd.DataFrame, periods: int = 4) -> pd.DataFrame:
    values = frame.groupby("symbol", group_keys=False)["open_interest"].pct_change(periods)
    return factor_frame(frame, "oi_change", values)
```

- [ ] **Step 4: Run tests to verify they pass**

Run:

```bash
python -m pytest tests/test_example_factors.py -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/prodigy/factors tests/test_example_factors.py
git commit -m "feat: add example factor interface"
```

## Task 6: Research Evaluator

**Files:**
- Create: `src/prodigy/research/__init__.py`
- Create: `src/prodigy/research/evaluator.py`
- Create: `tests/test_evaluator.py`

- [ ] **Step 1: Write failing evaluator tests**

Create `tests/test_evaluator.py`:

```python
import pandas as pd

from prodigy.research.evaluator import (
    bucket_returns,
    forward_returns,
    rank_ic_by_timestamp,
)


def price_frame():
    ts = pd.date_range("2026-07-01", periods=5, freq="15min", tz="UTC")
    return pd.DataFrame(
        {
            "timestamp": list(ts) * 2,
            "symbol": ["ETH/USDT:USDT"] * 5 + ["BTC/USDT:USDT"] * 5,
            "close": [100, 101, 103, 102, 104, 200, 198, 202, 204, 208],
        }
    )


def factor_frame():
    ts = pd.date_range("2026-07-01", periods=5, freq="15min", tz="UTC")
    return pd.DataFrame(
        {
            "timestamp": list(ts) * 2,
            "symbol": ["ETH/USDT:USDT"] * 5 + ["BTC/USDT:USDT"] * 5,
            "factor_name": ["example"] * 10,
            "value": [0.1, 0.2, 0.4, 0.3, 0.5, -0.2, -0.1, 0.2, 0.4, 0.6],
        }
    )


def test_forward_returns_by_symbol():
    result = forward_returns(price_frame(), periods=2)

    eth = result[result["symbol"] == "ETH/USDT:USDT"]["forward_return"].tolist()
    assert round(eth[0], 6) == 0.03
    assert pd.isna(eth[-1])


def test_rank_ic_returns_series():
    returns = forward_returns(price_frame(), periods=1)
    result = rank_ic_by_timestamp(factor_frame(), returns)

    assert result.name == "rank_ic"
    assert result.index.name == "timestamp"


def test_bucket_returns_has_bucket_column():
    returns = forward_returns(price_frame(), periods=1)
    result = bucket_returns(factor_frame(), returns, buckets=2)

    assert {"timestamp", "bucket", "mean_forward_return"}.issubset(result.columns)
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
python -m pytest tests/test_evaluator.py -v
```

Expected: FAIL with import error for `prodigy.research`.

- [ ] **Step 3: Implement evaluator functions**

Create `src/prodigy/research/__init__.py`:

```python
"""Research evaluation tools."""
```

Create `src/prodigy/research/evaluator.py`:

```python
from __future__ import annotations

import pandas as pd


def forward_returns(prices: pd.DataFrame, periods: int) -> pd.DataFrame:
    frame = prices.sort_values(["symbol", "timestamp"]).copy()
    frame["forward_return"] = (
        frame.groupby("symbol")["close"].shift(-periods) / frame["close"] - 1.0
    )
    return frame[["timestamp", "symbol", "forward_return"]]


def rank_ic_by_timestamp(factors: pd.DataFrame, returns: pd.DataFrame) -> pd.Series:
    merged = factors.merge(returns, on=["timestamp", "symbol"], how="inner")
    merged = merged.dropna(subset=["value", "forward_return"])
    if merged.empty:
        return pd.Series(dtype="float64", name="rank_ic")
    result = merged.groupby("timestamp").apply(
        lambda g: g["value"].corr(g["forward_return"], method="spearman")
    )
    result.name = "rank_ic"
    result.index.name = "timestamp"
    return result


def bucket_returns(
    factors: pd.DataFrame,
    returns: pd.DataFrame,
    buckets: int = 5,
) -> pd.DataFrame:
    merged = factors.merge(returns, on=["timestamp", "symbol"], how="inner")
    merged = merged.dropna(subset=["value", "forward_return"]).copy()
    if merged.empty:
        return pd.DataFrame(columns=["timestamp", "bucket", "mean_forward_return"])

    def assign_bucket(group: pd.DataFrame) -> pd.Series:
        ranked = group["value"].rank(method="first")
        return pd.qcut(ranked, q=min(buckets, len(group)), labels=False, duplicates="drop")

    merged["bucket"] = merged.groupby("timestamp", group_keys=False).apply(assign_bucket)
    grouped = (
        merged.groupby(["timestamp", "bucket"], as_index=False)["forward_return"]
        .mean()
        .rename(columns={"forward_return": "mean_forward_return"})
    )
    return grouped
```

- [ ] **Step 4: Run tests to verify they pass**

Run:

```bash
python -m pytest tests/test_evaluator.py -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/prodigy/research tests/test_evaluator.py
git commit -m "feat: add factor evaluator"
```

## Task 7: Backtester Facade

**Files:**
- Create: `src/prodigy/research/backtester.py`
- Create: `tests/test_backtester.py`

- [ ] **Step 1: Write failing Backtester tests**

Create `tests/test_backtester.py`:

```python
import pandas as pd

from prodigy.research.backtester import Backtester


def frames():
    ts = pd.date_range("2026-07-01", periods=8, freq="15min", tz="UTC")
    prices = pd.DataFrame(
        {
            "timestamp": ts,
            "symbol": ["ETH/USDT:USDT"] * len(ts),
            "close": [100, 101, 102, 103, 105, 104, 106, 108],
        }
    )
    factors = pd.DataFrame(
        {
            "timestamp": ts,
            "symbol": ["ETH/USDT:USDT"] * len(ts),
            "factor_name": ["momentum_15m"] * len(ts),
            "value": [0.1, 0.2, 0.1, 0.3, 0.5, 0.4, 0.2, 0.6],
        }
    )
    return prices, factors


def test_backtester_full_report_returns_sections():
    prices, factors = frames()
    bt = Backtester(prices=prices, factors=factors)

    report = bt.run_full_report(horizon=2, buckets=3)

    assert set(report) == {
        "distribution",
        "autocorrelation",
        "ic_summary",
        "bucket_returns",
        "performance",
    }
    assert report["distribution"]["count"] == 8
    assert "mean_forward_return" in report["performance"]
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
python -m pytest tests/test_backtester.py -v
```

Expected: FAIL with import error for `prodigy.research.backtester`.

- [ ] **Step 3: Implement Backtester facade**

Create `src/prodigy/research/backtester.py`:

```python
from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from prodigy.research.evaluator import (
    bucket_returns,
    forward_returns,
    rank_ic_by_timestamp,
)


@dataclass
class Backtester:
    prices: pd.DataFrame
    factors: pd.DataFrame

    def factor_distribution(self) -> dict[str, float]:
        values = self.factors["value"].dropna()
        return {
            "count": int(values.count()),
            "mean": float(values.mean()),
            "std": float(values.std()) if len(values) > 1 else 0.0,
            "min": float(values.min()),
            "max": float(values.max()),
        }

    def autocorrelation(self) -> float:
        frame = self.factors.sort_values(["symbol", "timestamp"]).copy()
        corr = frame.groupby("symbol")["value"].apply(lambda s: s.corr(s.shift(1)))
        corr = corr.dropna()
        return float(corr.mean()) if not corr.empty else 0.0

    def ic_summary(self, horizon: int) -> dict[str, float]:
        returns = forward_returns(self.prices, periods=horizon)
        ic = rank_ic_by_timestamp(self.factors, returns).dropna()
        if ic.empty:
            return {"mean": 0.0, "std": 0.0, "icir": 0.0}
        std = float(ic.std()) if len(ic) > 1 else 0.0
        return {
            "mean": float(ic.mean()),
            "std": std,
            "icir": float(ic.mean() / std) if std else 0.0,
        }

    def bucket_returns(self, horizon: int, buckets: int) -> pd.DataFrame:
        returns = forward_returns(self.prices, periods=horizon)
        return bucket_returns(self.factors, returns, buckets=buckets)

    def performance_summary(self, horizon: int) -> dict[str, float]:
        returns = forward_returns(self.prices, periods=horizon)
        merged = self.factors.merge(returns, on=["timestamp", "symbol"], how="inner")
        merged = merged.dropna(subset=["value", "forward_return"])
        if merged.empty:
            return {"mean_forward_return": 0.0, "observations": 0}
        signed = merged["value"].apply(lambda x: 1 if x > 0 else -1 if x < 0 else 0)
        strategy_return = signed * merged["forward_return"]
        return {
            "mean_forward_return": float(strategy_return.mean()),
            "observations": int(strategy_return.count()),
        }

    def run_full_report(self, horizon: int = 4, buckets: int = 5) -> dict[str, object]:
        return {
            "distribution": self.factor_distribution(),
            "autocorrelation": self.autocorrelation(),
            "ic_summary": self.ic_summary(horizon),
            "bucket_returns": self.bucket_returns(horizon, buckets),
            "performance": self.performance_summary(horizon),
        }
```

- [ ] **Step 4: Run tests to verify they pass**

Run:

```bash
python -m pytest tests/test_backtester.py -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/prodigy/research/backtester.py tests/test_backtester.py
git commit -m "feat: add backtester facade"
```

## Task 8: LightGBM Smoke Trainer

**Files:**
- Create: `src/prodigy/ml/__init__.py`
- Create: `src/prodigy/ml/trainer.py`
- Create: `tests/test_trainer.py`

- [ ] **Step 1: Write failing trainer test**

Create `tests/test_trainer.py`:

```python
import pandas as pd

from prodigy.ml.trainer import train_smoke_model


def test_train_smoke_model_returns_metadata():
    frame = pd.DataFrame(
        {
            "momentum_15m": [0.1, 0.2, -0.1, 0.3, -0.2, 0.4, 0.0, 0.5],
            "funding_zscore": [0.0, 1.0, -1.0, 0.5, -0.3, 0.7, 0.1, 0.2],
            "oi_change": [0.01, 0.02, -0.01, 0.03, -0.02, 0.04, 0.00, 0.05],
            "target_1h": [0.01, 0.02, -0.01, 0.03, -0.02, 0.04, 0.0, 0.05],
        }
    )

    result = train_smoke_model(
        frame,
        feature_columns=["momentum_15m", "funding_zscore", "oi_change"],
        target_column="target_1h",
        model_version="smoke-test",
    )

    assert result.model_version == "smoke-test"
    assert len(result.artifact_hash) == 64
    assert "train_rows" in result.metrics
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
python -m pytest tests/test_trainer.py -v
```

Expected: FAIL with import error for `prodigy.ml`.

- [ ] **Step 3: Implement trainer**

Create `src/prodigy/ml/__init__.py`:

```python
"""Model training helpers."""
```

Create `src/prodigy/ml/trainer.py`:

```python
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json

import lightgbm as lgb
import pandas as pd


@dataclass(frozen=True)
class ModelTrainingResult:
    model_version: str
    artifact_hash: str
    metrics: dict[str, float | int]


def train_smoke_model(
    frame: pd.DataFrame,
    feature_columns: list[str],
    target_column: str,
    model_version: str,
) -> ModelTrainingResult:
    clean = frame.dropna(subset=feature_columns + [target_column])
    model = lgb.LGBMRegressor(
        n_estimators=5,
        max_depth=2,
        learning_rate=0.1,
        random_state=7,
        verbosity=-1,
    )
    model.fit(clean[feature_columns], clean[target_column])
    predictions = model.predict(clean[feature_columns])
    payload = {
        "model_version": model_version,
        "features": feature_columns,
        "predictions": [round(float(x), 12) for x in predictions],
    }
    artifact_hash = hashlib.sha256(
        json.dumps(payload, sort_keys=True).encode("utf-8")
    ).hexdigest()
    return ModelTrainingResult(
        model_version=model_version,
        artifact_hash=artifact_hash,
        metrics={"train_rows": int(len(clean))},
    )
```

- [ ] **Step 4: Run tests to verify they pass**

Run:

```bash
python -m pytest tests/test_trainer.py -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/prodigy/ml tests/test_trainer.py
git commit -m "feat: add lightgbm smoke trainer"
```

## Task 9: Trade Intent Writer

**Files:**
- Create: `src/prodigy/signals/__init__.py`
- Create: `src/prodigy/signals/intents.py`
- Create: `tests/test_trade_intents.py`

- [ ] **Step 1: Write failing intent tests**

Create `tests/test_trade_intents.py`:

```python
from prodigy.db import connect, init_db
from prodigy.signals.intents import TradeIntent, write_trade_intent


def test_write_trade_intent_persists_pending_row(tmp_path):
    db_path = tmp_path / "prodigy.sqlite"
    intent = TradeIntent(
        intent_id="intent-eth-open-1",
        created_at="2026-07-01T00:00:00Z",
        symbol="ETH/USDT:USDT",
        side="long",
        action="open",
        target_notional=1000.0,
        max_order_notional=500.0,
        source="test",
        reason="score crossed long threshold",
        model_version="smoke-test",
    )

    with connect(db_path) as conn:
        init_db(conn)
        write_trade_intent(conn, intent)
        row = conn.execute(
            "select intent_id, status, symbol from trade_intents"
        ).fetchone()

    assert dict(row) == {
        "intent_id": "intent-eth-open-1",
        "status": "pending",
        "symbol": "ETH/USDT:USDT",
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
python -m pytest tests/test_trade_intents.py -v
```

Expected: FAIL with import error for `prodigy.signals`.

- [ ] **Step 3: Implement intent writer**

Create `src/prodigy/signals/__init__.py`:

```python
"""Signal and trade-intent helpers."""
```

Create `src/prodigy/signals/intents.py`:

```python
from __future__ import annotations

from dataclasses import dataclass
import sqlite3


@dataclass(frozen=True)
class TradeIntent:
    intent_id: str
    created_at: str
    symbol: str
    side: str
    action: str
    target_notional: float
    max_order_notional: float
    source: str
    reason: str
    model_version: str


def write_trade_intent(conn: sqlite3.Connection, intent: TradeIntent) -> None:
    conn.execute(
        """
        insert into trade_intents (
          intent_id, created_at, symbol, side, action, target_notional,
          max_order_notional, status, source, reason, model_version
        ) values (?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?)
        """,
        (
            intent.intent_id,
            intent.created_at,
            intent.symbol,
            intent.side,
            intent.action,
            intent.target_notional,
            intent.max_order_notional,
            intent.source,
            intent.reason,
            intent.model_version,
        ),
    )
    conn.commit()
```

- [ ] **Step 4: Run tests to verify they pass**

Run:

```bash
python -m pytest tests/test_trade_intents.py -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/prodigy/signals tests/test_trade_intents.py
git commit -m "feat: add trade intent writer"
```

## Task 10: Rust Workspace and Dry Executor

**Files:**
- Create: `Cargo.toml`
- Create: `crates/executor/Cargo.toml`
- Create: `crates/executor/src/types.rs`
- Create: `crates/executor/src/db.rs`
- Create: `crates/executor/src/main.rs`

- [ ] **Step 1: Create Rust workspace files**

Create root `Cargo.toml`:

```toml
[workspace]
members = ["crates/executor"]
resolver = "2"
```

Create `crates/executor/Cargo.toml`:

```toml
[package]
name = "prodigy-executor"
version = "0.1.0"
edition = "2021"

[dependencies]
anyhow = "1.0"
rusqlite = { version = "0.32", features = ["bundled"] }
serde = { version = "1.0", features = ["derive"] }
```

- [ ] **Step 2: Create Rust types**

Create `crates/executor/src/types.rs`:

```rust
#[derive(Debug, Clone, PartialEq)]
pub struct TradeIntent {
    pub intent_id: String,
    pub symbol: String,
    pub side: String,
    pub action: String,
    pub target_notional: f64,
    pub max_order_notional: f64,
}
```

- [ ] **Step 3: Create Rust DB module**

Create `crates/executor/src/db.rs`:

```rust
use anyhow::Result;
use rusqlite::{params, Connection};

use crate::types::TradeIntent;

pub fn pending_intents(conn: &Connection) -> Result<Vec<TradeIntent>> {
    let mut stmt = conn.prepare(
        "select intent_id, symbol, side, action, target_notional, max_order_notional
         from trade_intents
         where status = 'pending'
         order by created_at asc"
    )?;
    let rows = stmt.query_map([], |row| {
        Ok(TradeIntent {
            intent_id: row.get(0)?,
            symbol: row.get(1)?,
            side: row.get(2)?,
            action: row.get(3)?,
            target_notional: row.get(4)?,
            max_order_notional: row.get(5)?,
        })
    })?;

    let mut intents = Vec::new();
    for row in rows {
        intents.push(row?);
    }
    Ok(intents)
}

pub fn reject_intent(conn: &Connection, intent_id: &str, reason: &str) -> Result<()> {
    conn.execute(
        "update trade_intents
         set status = 'rejected',
             processed_at = datetime('now'),
             error = ?
         where intent_id = ? and status = 'pending'",
        params![reason, intent_id],
    )?;
    Ok(())
}
```

- [ ] **Step 4: Create Rust dry executor main**

Create `crates/executor/src/main.rs`:

```rust
mod db;
mod types;

use anyhow::{bail, Result};
use rusqlite::Connection;

fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let db_path = match args.next().as_deref() {
        Some("--db") => args.next().unwrap_or_else(|| "var/prodigy.sqlite".to_string()),
        Some(other) => bail!("unknown argument: {other}"),
        None => "var/prodigy.sqlite".to_string(),
    };

    let conn = Connection::open(db_path)?;
    let intents = db::pending_intents(&conn)?;
    for intent in intents {
        db::reject_intent(
            &conn,
            &intent.intent_id,
            "dry executor rejects intents until Bitget execution is implemented",
        )?;
        println!("rejected {}", intent.intent_id);
    }
    Ok(())
}
```

- [ ] **Step 5: Run Rust checks**

Run:

```bash
cargo test -p prodigy-executor
cargo run -p prodigy-executor -- --db /tmp/missing-prodigy.sqlite
```

Expected:

- `cargo test` passes.
- `cargo run` exits successfully and prints nothing for an empty SQLite file only after the DB has the schema. If the schema is absent, it fails with a SQLite table error. That failure is acceptable before the integration task creates a schema-backed DB.

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml crates/executor
git commit -m "feat: add rust dry executor"
```

## Task 11: Rust Dry Executor Integration

**Files:**
- Create: `tests/test_executor_integration.py`

- [ ] **Step 1: Write integration test**

Create `tests/test_executor_integration.py`:

```python
import subprocess

from prodigy.db import connect, init_db
from prodigy.signals.intents import TradeIntent, write_trade_intent


def test_rust_dry_executor_rejects_pending_intent(tmp_path):
    db_path = tmp_path / "prodigy.sqlite"

    with connect(db_path) as conn:
        init_db(conn)
        write_trade_intent(
            conn,
            TradeIntent(
                intent_id="intent-1",
                created_at="2026-07-01T00:00:00Z",
                symbol="ETH/USDT:USDT",
                side="long",
                action="open",
                target_notional=1000.0,
                max_order_notional=500.0,
                source="test",
                reason="integration",
                model_version="smoke-test",
            ),
        )

    result = subprocess.run(
        ["cargo", "run", "-q", "-p", "prodigy-executor", "--", "--db", str(db_path)],
        check=True,
        text=True,
        capture_output=True,
    )

    with connect(db_path) as conn:
        row = conn.execute(
            "select status, error from trade_intents where intent_id = 'intent-1'"
        ).fetchone()

    assert "rejected intent-1" in result.stdout
    assert row["status"] == "rejected"
    assert "dry executor rejects intents" in row["error"]
```

- [ ] **Step 2: Run integration test**

Run:

```bash
python -m pytest tests/test_executor_integration.py -v
```

Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/test_executor_integration.py
git commit -m "test: add dry executor integration"
```

## Task 12: Telegram Command Service

**Files:**
- Create: `src/prodigy/telegram/__init__.py`
- Create: `src/prodigy/telegram/service.py`
- Create: `tests/test_telegram_service.py`

- [ ] **Step 1: Write failing Telegram service tests**

Create `tests/test_telegram_service.py`:

```python
from prodigy.db import connect, init_db
from prodigy.telegram.service import TelegramCommandService


def test_status_reports_pending_intents(tmp_path):
    db_path = tmp_path / "prodigy.sqlite"
    with connect(db_path) as conn:
        init_db(conn)
        conn.execute(
            """
            insert into trade_intents (
              intent_id, created_at, symbol, side, action, target_notional,
              max_order_notional, status, source
            ) values (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "intent-1",
                "2026-07-01T00:00:00Z",
                "ETH/USDT:USDT",
                "long",
                "open",
                1000.0,
                500.0,
                "pending",
                "test",
            ),
        )
        conn.commit()

        service = TelegramCommandService(conn, allowed_user_ids={"123"})
        message = service.status()

    assert "pending_intents=1" in message


def test_stop_writes_control_command_for_allowed_user(tmp_path):
    db_path = tmp_path / "prodigy.sqlite"
    with connect(db_path) as conn:
        init_db(conn)
        service = TelegramCommandService(conn, allowed_user_ids={"123"})
        message = service.stop(user_id="123", now="2026-07-01T00:00:00Z")
        row = conn.execute("select command, status from control_commands").fetchone()

    assert message == "stop command queued"
    assert dict(row) == {"command": "stop", "status": "pending"}


def test_resume_rejects_unknown_user(tmp_path):
    db_path = tmp_path / "prodigy.sqlite"
    with connect(db_path) as conn:
        init_db(conn)
        service = TelegramCommandService(conn, allowed_user_ids={"123"})
        message = service.resume(user_id="999", now="2026-07-01T00:00:00Z")

    assert message == "unauthorized"
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
python -m pytest tests/test_telegram_service.py -v
```

Expected: FAIL with import error for `prodigy.telegram`.

- [ ] **Step 3: Implement Telegram command service**

Create `src/prodigy/telegram/__init__.py`:

```python
"""Telegram command services."""
```

Create `src/prodigy/telegram/service.py`:

```python
from __future__ import annotations

import sqlite3
import uuid


class TelegramCommandService:
    def __init__(self, conn: sqlite3.Connection, allowed_user_ids: set[str]):
        self.conn = conn
        self.allowed_user_ids = allowed_user_ids

    def status(self) -> str:
        pending_intents = self.conn.execute(
            "select count(*) from trade_intents where status = 'pending'"
        ).fetchone()[0]
        pending_commands = self.conn.execute(
            "select count(*) from control_commands where status = 'pending'"
        ).fetchone()[0]
        return f"pending_intents={pending_intents} pending_commands={pending_commands}"

    def stop(self, user_id: str, now: str) -> str:
        return self._write_command(user_id=user_id, now=now, command="stop")

    def resume(self, user_id: str, now: str) -> str:
        return self._write_command(user_id=user_id, now=now, command="resume")

    def _write_command(self, user_id: str, now: str, command: str) -> str:
        if user_id not in self.allowed_user_ids:
            return "unauthorized"
        self.conn.execute(
            """
            insert into control_commands (
              command_id, created_at, command, status, requested_by
            ) values (?, ?, ?, 'pending', ?)
            """,
            (str(uuid.uuid4()), now, command, user_id),
        )
        self.conn.commit()
        return f"{command} command queued"
```

- [ ] **Step 4: Run tests to verify they pass**

Run:

```bash
python -m pytest tests/test_telegram_service.py -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/prodigy/telegram tests/test_telegram_service.py
git commit -m "feat: add telegram command service"
```

## Task 13: End-to-End Smoke Check

**Files:**
- Create: `tests/test_first_milestone_smoke.py`

- [ ] **Step 1: Write end-to-end smoke test**

Create `tests/test_first_milestone_smoke.py`:

```python
import pandas as pd

from prodigy.db import connect, init_db
from prodigy.factors.examples import funding_zscore, momentum_15m, oi_change
from prodigy.ml.trainer import train_smoke_model
from prodigy.research.backtester import Backtester
from prodigy.signals.intents import TradeIntent, write_trade_intent
from prodigy.telegram.service import TelegramCommandService


def test_first_milestone_python_path(tmp_path):
    timestamps = pd.date_range("2026-07-01", periods=12, freq="15min", tz="UTC")
    market = pd.DataFrame(
        {
            "timestamp": timestamps,
            "symbol": ["ETH/USDT:USDT"] * len(timestamps),
            "open": [100 + i for i in range(len(timestamps))],
            "high": [101 + i for i in range(len(timestamps))],
            "low": [99 + i for i in range(len(timestamps))],
            "close": [100 + i + (i % 3) for i in range(len(timestamps))],
            "volume": [10 + i for i in range(len(timestamps))],
            "funding_rate": [0.001 + i * 0.0001 for i in range(len(timestamps))],
            "open_interest": [1000 + i * 10 for i in range(len(timestamps))],
        }
    )

    factors = pd.concat(
        [
            momentum_15m(market, periods=2),
            funding_zscore(market, window=4),
            oi_change(market, periods=2),
        ],
        ignore_index=True,
    )

    one_factor = factors[factors["factor_name"] == "momentum_15m"]
    report = Backtester(
        prices=market[["timestamp", "symbol", "close"]],
        factors=one_factor,
    ).run_full_report(horizon=2, buckets=3)

    wide = factors.pivot_table(
        index=["timestamp", "symbol"],
        columns="factor_name",
        values="value",
    ).reset_index()
    wide["target_1h"] = market["close"].shift(-4) / market["close"] - 1
    model = train_smoke_model(
        wide,
        feature_columns=["momentum_15m", "funding_zscore", "oi_change"],
        target_column="target_1h",
        model_version="first-milestone-smoke",
    )

    db_path = tmp_path / "prodigy.sqlite"
    with connect(db_path) as conn:
        init_db(conn)
        write_trade_intent(
            conn,
            TradeIntent(
                intent_id="intent-smoke",
                created_at="2026-07-01T00:00:00Z",
                symbol="ETH/USDT:USDT",
                side="long",
                action="open",
                target_notional=1000.0,
                max_order_notional=500.0,
                source="smoke",
                reason="first milestone smoke",
                model_version=model.model_version,
            ),
        )
        telegram = TelegramCommandService(conn, allowed_user_ids={"123"})
        status = telegram.status()

    assert report["distribution"]["count"] == 12
    assert len(model.artifact_hash) == 64
    assert "pending_intents=1" in status
```

- [ ] **Step 2: Run all Python tests**

Run:

```bash
python -m pytest -v
```

Expected: PASS.

- [ ] **Step 3: Run Rust tests**

Run:

```bash
cargo test
```

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add tests/test_first_milestone_smoke.py
git commit -m "test: add first milestone smoke path"
```

## Task 14: Push Milestone Branch

**Files:**
- Modify: none.

- [ ] **Step 1: Verify local status**

Run:

```bash
git status --short --branch
```

Expected: clean working tree on the implementation branch.

- [ ] **Step 2: Push**

Run:

```bash
git push
```

Expected: branch pushed to GitHub.

## Self-Review Notes

Spec coverage:

- Python package skeleton: Tasks 1-9 and 12-13.
- SQLite outbox: Tasks 2, 9, 11.
- CCXT data path: Task 4.
- Three example factors: Task 5.
- `Backtester.run_full_report()`: Task 7.
- LightGBM smoke model: Task 8.
- Rust dry executor: Tasks 10-11.
- Telegram `/status`, `/stop`, `/resume`: Task 12.
- Buffer/failure isolation in first milestone: schema statuses, idempotent intent IDs, dry executor rejection, Telegram service isolated from execution.

Known intentional boundaries:

- No Bitget live orders in this milestone.
- No actual Telegram network bot in this milestone; the command service is implemented and tested before wiring the network adapter.
- No production factor promotion command in this milestone; factor folders and promotion metadata are planned in the next milestone after the core path passes.
