# Crypto Quant Second Milestone Research Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the offline ETH research loop: Bitget data backfill to parquet gzip, notebook research workflow, lot-level bar backtesting, and LightGBM factor aggregation smoke validation.

**Architecture:** Python owns this milestone. Data is pulled from CCXT and Bitget official REST, written to date-partitioned parquet gzip, and tracked by SQLite checkpoints/events. Notebooks use the same parquet loaders and `Backtester` facade as tests; ML smoke trains LightGBM on example factor features and records artifacts plus SQLite metadata.

**Tech Stack:** Python 3.11+ in `quantmamba`, pandas, numpy, pyarrow, ccxt, lightgbm, matplotlib, pytest, SQLite, Rust existing executor checks.

---

## Execution Rules

- Use an isolated git worktree before implementation.
- Do not install new dependencies.
- Use complete `mamba run -n quantmamba python -m pytest <test-path> -v` commands for Python verification.
- Do not read or modify `.env.local`.
- Do not place demo or live orders.
- Do not add open-interest history in this milestone.
- Keep commits small and task-scoped.

## File Structure

Create or modify:

```text
.gitignore
pyproject.toml
configs/default.toml
research/notebooks/*.ipynb
research/reports/.gitkeep
research/factor_library/.gitkeep
research/scratch/.gitkeep
data/raw/.gitkeep
data/processed/.gitkeep
models/example_lgbm/.gitkeep
var/.gitkeep
src/prodigy/data/paths.py
src/prodigy/data/parquet_store.py
src/prodigy/data/quality.py
src/prodigy/data/bitget_rest.py
src/prodigy/data/backfill.py
src/prodigy/cli/__init__.py
src/prodigy/cli/data.py
src/prodigy/cli/ml.py
src/prodigy/factors/examples.py
src/prodigy/research/signals.py
src/prodigy/research/simulator.py
src/prodigy/research/backtester.py
src/prodigy/ml/labels.py
src/prodigy/ml/splits.py
src/prodigy/ml/example_trainer.py
tests/test_project_layout.py
tests/test_parquet_store.py
tests/test_data_quality.py
tests/test_bitget_rest.py
tests/test_backfill.py
tests/test_example_factors.py
tests/test_research_signals.py
tests/test_bar_simulator.py
tests/test_ml_labels_splits.py
tests/test_example_trainer.py
tests/test_research_notebooks.py
tests/test_second_milestone_smoke.py
```

## Task 1: Project Layout and Ignore Rules

**Files:**
- Modify: `.gitignore`
- Create: `research/notebooks/.gitkeep`
- Create: `research/reports/.gitkeep`
- Create: `research/factor_library/.gitkeep`
- Create: `research/scratch/.gitkeep`
- Create: `data/raw/.gitkeep`
- Create: `data/processed/.gitkeep`
- Create: `models/example_lgbm/.gitkeep`
- Create: `var/.gitkeep`
- Test: `tests/test_project_layout.py`

- [ ] **Step 1: Write the failing layout test**

Create `tests/test_project_layout.py`:

```python
from pathlib import Path


def test_second_milestone_directories_exist():
    required = [
        "research/notebooks",
        "research/reports",
        "research/factor_library",
        "research/scratch",
        "data/raw",
        "data/processed",
        "models/example_lgbm",
        "var",
    ]

    for path in required:
        assert Path(path).is_dir(), path
        assert (Path(path) / ".gitkeep").exists() or path == "research/notebooks"


def test_large_local_artifacts_are_ignored():
    ignore_text = Path(".gitignore").read_text()

    for pattern in [
        "data/**/*.parquet",
        "data/**/*.parquet.gzip",
        "models/**/*",
        "var/*",
        "!**/.gitkeep",
    ]:
        assert pattern in ignore_text
```

- [ ] **Step 2: Run the test to verify it fails**

Run:

```bash
mamba run -n quantmamba python -m pytest tests/test_project_layout.py -v
```

Expected: FAIL because the directories and ignore patterns do not exist yet.

- [ ] **Step 3: Create directories and update `.gitignore`**

Create the directories listed above. Put `.gitkeep` in every empty directory. Append these lines to `.gitignore`:

```gitignore
data/**/*.parquet
data/**/*.parquet.gzip
models/**/*
var/*
!**/.gitkeep
```

- [ ] **Step 4: Run the test to verify it passes**

Run:

```bash
mamba run -n quantmamba python -m pytest tests/test_project_layout.py -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add .gitignore research data models var tests/test_project_layout.py
git commit -m "chore: add research data model directories"
```

## Task 2: Parquet Partition Store

**Files:**
- Create: `src/prodigy/data/paths.py`
- Create: `src/prodigy/data/parquet_store.py`
- Test: `tests/test_parquet_store.py`

- [ ] **Step 1: Write failing parquet store tests**

Create `tests/test_parquet_store.py`:

```python
import pandas as pd

from prodigy.data.parquet_store import (
    load_funding_rates,
    load_ohlcv,
    partition_path,
    write_daily_partition,
)
from prodigy.data.paths import symbol_slug


def test_symbol_slug_is_path_safe():
    assert symbol_slug("ETH/USDT:USDT") == "ETH-USDT-SWAP"


def test_write_and_load_ohlcv_daily_partition(tmp_path):
    frame = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                ["2026-07-01T00:00:00Z", "2026-07-01T00:15:00Z"],
                utc=True,
            ),
            "symbol": ["ETH/USDT:USDT", "ETH/USDT:USDT"],
            "open": [100.0, 101.0],
            "high": [102.0, 103.0],
            "low": [99.0, 100.0],
            "close": [101.0, 102.0],
            "volume": [10.0, 11.0],
        }
    )

    write_daily_partition(
        frame,
        data_root=tmp_path,
        exchange="bitget",
        symbol="ETH/USDT:USDT",
        dataset="ohlcv",
        date=pd.Timestamp("2026-07-01"),
        timeframe="15m",
    )

    path = partition_path(
        tmp_path,
        exchange="bitget",
        symbol="ETH/USDT:USDT",
        dataset="ohlcv",
        date=pd.Timestamp("2026-07-01"),
        timeframe="15m",
    )
    assert path.exists()
    assert path.name == "date=2026-07-01.parquet.gzip"

    loaded = load_ohlcv(
        data_root=tmp_path,
        symbol="ETH/USDT:USDT",
        start="2026-07-01",
        end="2026-07-02",
        timeframe="15m",
    )

    assert loaded["timestamp"].tolist() == frame["timestamp"].tolist()
    assert loaded["close"].tolist() == [101.0, 102.0]


def test_write_partition_deduplicates_by_timestamp_symbol(tmp_path):
    frame = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                ["2026-07-01T00:00:00Z", "2026-07-01T00:00:00Z"],
                utc=True,
            ),
            "symbol": ["ETH/USDT:USDT", "ETH/USDT:USDT"],
            "funding_rate": [0.001, 0.002],
            "funding_time": pd.to_datetime(
                ["2026-07-01T00:00:00Z", "2026-07-01T00:00:00Z"],
                utc=True,
            ),
        }
    )

    write_daily_partition(
        frame,
        data_root=tmp_path,
        exchange="bitget",
        symbol="ETH/USDT:USDT",
        dataset="funding_rates",
        date=pd.Timestamp("2026-07-01"),
    )

    loaded = load_funding_rates(
        data_root=tmp_path,
        symbol="ETH/USDT:USDT",
        start="2026-07-01",
        end="2026-07-02",
    )

    assert len(loaded) == 1
    assert loaded.iloc[0]["funding_rate"] == 0.002
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
mamba run -n quantmamba python -m pytest tests/test_parquet_store.py -v
```

Expected: FAIL because `prodigy.data.parquet_store` and `prodigy.data.paths` do not exist.

- [ ] **Step 3: Implement path and parquet helpers**

Create `src/prodigy/data/paths.py` with:

```python
from __future__ import annotations

from pathlib import Path


def symbol_slug(symbol: str) -> str:
    base, settle = symbol.split(":")
    left, right = base.split("/")
    suffix = "SWAP" if settle == right else settle
    return f"{left}-{right}-{suffix}"


def ensure_dir(path: str | Path) -> Path:
    result = Path(path)
    result.mkdir(parents=True, exist_ok=True)
    return result
```

Create `src/prodigy/data/parquet_store.py` with these public functions:

```python
from __future__ import annotations

from pathlib import Path
import tempfile

import pandas as pd

from prodigy.data.paths import ensure_dir, symbol_slug


def partition_path(
    data_root: str | Path,
    exchange: str,
    symbol: str,
    dataset: str,
    date: str | pd.Timestamp,
    timeframe: str | None = None,
) -> Path:
    day = pd.Timestamp(date).strftime("%Y-%m-%d")
    parts = [Path(data_root), "raw", exchange, symbol_slug(symbol), dataset]
    if timeframe is not None:
        parts.append(f"timeframe={timeframe}")
    parts.append(f"date={day}.parquet.gzip")
    return Path(*parts)


def _date_range(start: str | pd.Timestamp, end: str | pd.Timestamp) -> list[pd.Timestamp]:
    start_ts = pd.Timestamp(start).normalize()
    end_ts = pd.Timestamp(end).normalize()
    return list(pd.date_range(start_ts, end_ts, freq="D", inclusive="left"))


def write_daily_partition(
    frame: pd.DataFrame,
    data_root: str | Path,
    exchange: str,
    symbol: str,
    dataset: str,
    date: str | pd.Timestamp,
    timeframe: str | None = None,
) -> Path:
    path = partition_path(data_root, exchange, symbol, dataset, date, timeframe)
    ensure_dir(path.parent)
    clean = frame.sort_values("timestamp").drop_duplicates(
        ["timestamp", "symbol"], keep="last"
    )
    with tempfile.NamedTemporaryFile(
        suffix=".parquet.gzip", dir=path.parent, delete=False
    ) as tmp:
        tmp_path = Path(tmp.name)
    clean.to_parquet(tmp_path, compression="gzip", index=False)
    pd.read_parquet(tmp_path)
    tmp_path.replace(path)
    return path


def _load_range(
    data_root: str | Path,
    symbol: str,
    dataset: str,
    start: str | pd.Timestamp,
    end: str | pd.Timestamp,
    timeframe: str | None = None,
) -> pd.DataFrame:
    frames = []
    for day in _date_range(start, end):
        path = partition_path(data_root, "bitget", symbol, dataset, day, timeframe)
        if path.exists():
            frames.append(pd.read_parquet(path))
    if not frames:
        return pd.DataFrame()
    return (
        pd.concat(frames, ignore_index=True)
        .sort_values(["timestamp", "symbol"])
        .reset_index(drop=True)
    )


def load_ohlcv(
    data_root: str | Path,
    symbol: str,
    start: str | pd.Timestamp,
    end: str | pd.Timestamp,
    timeframe: str = "15m",
) -> pd.DataFrame:
    return _load_range(data_root, symbol, "ohlcv", start, end, timeframe)


def load_funding_rates(
    data_root: str | Path,
    symbol: str,
    start: str | pd.Timestamp,
    end: str | pd.Timestamp,
) -> pd.DataFrame:
    return _load_range(data_root, symbol, "funding_rates", start, end)
```

- [ ] **Step 4: Run tests to verify they pass**

Run:

```bash
mamba run -n quantmamba python -m pytest tests/test_parquet_store.py -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/prodigy/data/paths.py src/prodigy/data/parquet_store.py tests/test_parquet_store.py
git commit -m "feat: add parquet research store"
```

## Task 3: Data Quality Reports

**Files:**
- Create: `src/prodigy/data/quality.py`
- Test: `tests/test_data_quality.py`

- [ ] **Step 1: Write failing data quality tests**

Create `tests/test_data_quality.py`:

```python
import pandas as pd

from prodigy.data.quality import quality_summary


def test_quality_summary_detects_ohlcv_gaps_duplicates_and_bad_volume():
    frame = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                [
                    "2026-07-01T00:00:00Z",
                    "2026-07-01T00:15:00Z",
                    "2026-07-01T00:15:00Z",
                    "2026-07-01T00:45:00Z",
                ],
                utc=True,
            ),
            "symbol": ["ETH/USDT:USDT"] * 4,
            "open": [100.0, 101.0, 101.0, 103.0],
            "high": [101.0, 102.0, 102.0, 104.0],
            "low": [99.0, 100.0, 100.0, 102.0],
            "close": [100.5, 101.5, 101.5, 103.5],
            "volume": [1.0, 2.0, -3.0, 4.0],
        }
    )

    summary = quality_summary(frame, dataset="ohlcv", timeframe="15m")

    assert summary["rows"] == 4
    assert summary["duplicate_timestamp_symbol"] == 1
    assert summary["missing_timestamps"] == 1
    assert summary["negative_volume"] == 1


def test_quality_summary_handles_empty_frame():
    summary = quality_summary(pd.DataFrame(), dataset="funding_rates")

    assert summary["rows"] == 0
    assert summary["missing_timestamps"] == 0
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
mamba run -n quantmamba python -m pytest tests/test_data_quality.py -v
```

Expected: FAIL because `prodigy.data.quality` does not exist.

- [ ] **Step 3: Implement `quality_summary`**

Create `src/prodigy/data/quality.py`:

```python
from __future__ import annotations

import pandas as pd


def quality_summary(
    frame: pd.DataFrame,
    dataset: str,
    timeframe: str | None = None,
) -> dict[str, int | str | None]:
    if frame.empty:
        return {
            "dataset": dataset,
            "rows": 0,
            "duplicate_timestamp_symbol": 0,
            "missing_timestamps": 0,
            "null_values": 0,
            "negative_volume": 0,
            "start": None,
            "end": None,
        }

    clean = frame.copy()
    clean["timestamp"] = pd.to_datetime(clean["timestamp"], utc=True)
    duplicate_count = int(clean.duplicated(["timestamp", "symbol"]).sum())
    null_values = int(clean.isna().sum().sum())
    negative_volume = int((clean.get("volume", pd.Series(dtype=float)) < 0).sum())

    missing = 0
    if dataset == "ohlcv" and timeframe is not None:
        freq = pd.Timedelta(timeframe)
        expected = pd.date_range(
            clean["timestamp"].min(),
            clean["timestamp"].max(),
            freq=freq,
        )
        actual = pd.DatetimeIndex(clean["timestamp"].drop_duplicates().sort_values())
        missing = int(len(expected.difference(actual)))

    return {
        "dataset": dataset,
        "rows": int(len(clean)),
        "duplicate_timestamp_symbol": duplicate_count,
        "missing_timestamps": missing,
        "null_values": null_values,
        "negative_volume": negative_volume,
        "start": clean["timestamp"].min().isoformat(),
        "end": clean["timestamp"].max().isoformat(),
    }
```

- [ ] **Step 4: Run tests**

```bash
mamba run -n quantmamba python -m pytest tests/test_data_quality.py -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/prodigy/data/quality.py tests/test_data_quality.py
git commit -m "feat: add market data quality summary"
```

## Task 4: Bitget Funding REST Client

**Files:**
- Create: `src/prodigy/data/bitget_rest.py`
- Test: `tests/test_bitget_rest.py`

- [ ] **Step 1: Write failing REST parsing and proxy fallback tests**

Create `tests/test_bitget_rest.py`:

```python
import json
from urllib.error import URLError

import pandas as pd

from prodigy.data.bitget_rest import (
    BitgetRestClient,
    parse_funding_rate_rows,
)


def test_parse_funding_rate_rows_normalizes_official_response():
    payload = {
        "code": "00000",
        "data": [
            {
                "symbol": "ETHUSDT",
                "fundingRate": "0.000083",
                "fundingTime": "1782864000000",
            }
        ],
    }

    frame = parse_funding_rate_rows(payload, symbol="ETH/USDT:USDT")

    assert list(frame.columns) == [
        "timestamp",
        "symbol",
        "funding_time",
        "funding_rate",
        "raw_symbol",
    ]
    assert frame.iloc[0]["symbol"] == "ETH/USDT:USDT"
    assert frame.iloc[0]["funding_rate"] == 0.000083
    assert pd.Timestamp(frame.iloc[0]["timestamp"]).tz is not None


def test_client_retries_with_proxy_after_direct_failure():
    calls = []

    def opener(url, proxy_url=None, timeout=10):
        calls.append(proxy_url)
        if proxy_url is None:
            raise URLError("direct failed")
        return json.dumps(
            {
                "code": "00000",
                "data": [
                    {
                        "symbol": "ETHUSDT",
                        "fundingRate": "0.001",
                        "fundingTime": "1782864000000",
                    }
                ],
            }
        ).encode()

    client = BitgetRestClient(proxy_url="http://127.0.0.1:7897", opener=opener)
    frame = client.fetch_funding_rate_page(
        symbol="ETH/USDT:USDT",
        product_type="usdt-futures",
        page_no=1,
        page_size=100,
    )

    assert calls == [None, "http://127.0.0.1:7897"]
    assert frame.iloc[0]["funding_rate"] == 0.001
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
mamba run -n quantmamba python -m pytest tests/test_bitget_rest.py -v
```

Expected: FAIL because `prodigy.data.bitget_rest` does not exist.

- [ ] **Step 3: Implement official REST helper**

Create `src/prodigy/data/bitget_rest.py` with:

```python
from __future__ import annotations

from dataclasses import dataclass
import json
from urllib.error import URLError
from urllib.parse import urlencode
from urllib.request import ProxyHandler, build_opener, urlopen

import pandas as pd


BITGET_BASE_URL = "https://api.bitget.com"


def _market_id(symbol: str) -> str:
    return symbol.split(":")[0].replace("/", "")


def _urlopen(url: str, proxy_url: str | None = None, timeout: int = 10) -> bytes:
    if proxy_url:
        opener = build_opener(ProxyHandler({"http": proxy_url, "https": proxy_url}))
        with opener.open(url, timeout=timeout) as response:
            return response.read()
    with urlopen(url, timeout=timeout) as response:
        return response.read()


def parse_funding_rate_rows(payload: dict, symbol: str) -> pd.DataFrame:
    rows = payload.get("data", [])
    frame = pd.DataFrame(rows)
    if frame.empty:
        return pd.DataFrame(
            columns=["timestamp", "symbol", "funding_time", "funding_rate", "raw_symbol"]
        )
    frame = frame.rename(columns={"symbol": "raw_symbol"})
    frame["timestamp"] = pd.to_datetime(frame["fundingTime"].astype("int64"), unit="ms", utc=True)
    frame["funding_time"] = frame["timestamp"]
    frame["symbol"] = symbol
    frame["funding_rate"] = frame["fundingRate"].astype("float64")
    return frame[["timestamp", "symbol", "funding_time", "funding_rate", "raw_symbol"]]


@dataclass
class BitgetRestClient:
    proxy_url: str | None = None
    timeout: int = 10
    opener: object = _urlopen

    def _get_json(self, path: str, params: dict[str, str | int]) -> dict:
        url = f"{BITGET_BASE_URL}{path}?{urlencode(params)}"
        try:
            raw = self.opener(url, proxy_url=None, timeout=self.timeout)
        except (OSError, URLError):
            if not self.proxy_url:
                raise
            raw = self.opener(url, proxy_url=self.proxy_url, timeout=self.timeout)
        payload = json.loads(raw.decode("utf-8"))
        if payload.get("code") != "00000":
            raise RuntimeError(f"Bitget error: {payload}")
        return payload

    def fetch_funding_rate_page(
        self,
        symbol: str,
        product_type: str,
        page_no: int,
        page_size: int,
    ) -> pd.DataFrame:
        payload = self._get_json(
            "/api/v2/mix/market/history-fund-rate",
            {
                "symbol": _market_id(symbol),
                "productType": product_type,
                "pageNo": page_no,
                "pageSize": page_size,
            },
        )
        return parse_funding_rate_rows(payload, symbol=symbol)
```

- [ ] **Step 4: Run tests**

```bash
mamba run -n quantmamba python -m pytest tests/test_bitget_rest.py -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/prodigy/data/bitget_rest.py tests/test_bitget_rest.py
git commit -m "feat: add bitget funding rest client"
```

## Task 5: Data Backfill Service and `prodigy-data` CLI

**Files:**
- Modify: `pyproject.toml`
- Create: `src/prodigy/data/backfill.py`
- Create: `src/prodigy/cli/__init__.py`
- Create: `src/prodigy/cli/data.py`
- Test: `tests/test_backfill.py`

- [ ] **Step 1: Write failing backfill tests**

Create `tests/test_backfill.py`:

```python
import pandas as pd

from prodigy.data.backfill import BackfillResult, run_backfill
from prodigy.data.parquet_store import load_funding_rates, load_ohlcv
from prodigy.db import connect, init_db


class FakeExchange:
    def load_markets(self):
        return {}

    def fetch_ohlcv(self, symbol, timeframe, since=None, limit=None, params=None):
        return [
            [1782864000000, 100.0, 101.0, 99.0, 100.5, 10.0],
            [1782864900000, 100.5, 102.0, 100.0, 101.0, 11.0],
        ]


class FakeFundingClient:
    def fetch_funding_rate_page(self, symbol, product_type, page_no, page_size):
        if page_no > 1:
            return pd.DataFrame(
                columns=["timestamp", "symbol", "funding_time", "funding_rate", "raw_symbol"]
            )
        return pd.DataFrame(
            {
                "timestamp": pd.to_datetime(["2026-07-01T00:00:00Z"], utc=True),
                "symbol": [symbol],
                "funding_time": pd.to_datetime(["2026-07-01T00:00:00Z"], utc=True),
                "funding_rate": [0.001],
                "raw_symbol": ["ETHUSDT"],
            }
        )


def test_run_backfill_writes_partitions_and_checkpoint(tmp_path):
    db_path = tmp_path / "prodigy.sqlite"
    with connect(db_path) as conn:
        init_db(conn)

    result = run_backfill(
        symbol="ETH/USDT:USDT",
        start="2026-07-01",
        end="2026-07-02",
        timeframe="15m",
        data_root=tmp_path,
        db_path=db_path,
        exchange=FakeExchange(),
        funding_client=FakeFundingClient(),
    )

    assert isinstance(result, BackfillResult)
    assert result.ohlcv_rows == 2
    assert result.funding_rows == 1

    ohlcv = load_ohlcv(tmp_path, "ETH/USDT:USDT", "2026-07-01", "2026-07-02")
    funding = load_funding_rates(tmp_path, "ETH/USDT:USDT", "2026-07-01", "2026-07-02")
    assert len(ohlcv) == 2
    assert len(funding) == 1

    with connect(db_path) as conn:
        row = conn.execute(
            "select checkpoint_value from task_checkpoints where task_name = ?",
            ("backfill:bitget:ETH/USDT:USDT:15m",),
        ).fetchone()
        assert row["checkpoint_value"] == "2026-07-02"
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
mamba run -n quantmamba python -m pytest tests/test_backfill.py -v
```

Expected: FAIL because `prodigy.data.backfill` does not exist.

- [ ] **Step 3: Implement backfill service**

Create `src/prodigy/data/backfill.py` with a `BackfillResult` dataclass and this function:

```python
def run_backfill(
    symbol: str,
    start: str,
    end: str | None,
    timeframe: str,
    data_root: str | Path,
    db_path: str | Path,
    proxy_url: str | None = "http://127.0.0.1:7897",
    exchange: object | None = None,
    funding_client: object | None = None,
) -> BackfillResult:
    # Body requirements are listed below this signature.
```

It must:

- fetch OHLCV once for the requested range through existing `fetch_ohlcv_frame`;
- fetch funding pages through `BitgetRestClient` until an empty page or 100 pages;
- write date partitions using `write_daily_partition`;
- update `task_checkpoints` with `task_name = backfill:bitget:<symbol>:<timeframe>`;
- insert one `events` row with component `data.backfill` and a JSON summary.

Use this checkpoint helper:

```python
def _upsert_checkpoint(conn, task_name: str, value: str) -> None:
    conn.execute(
        """
        insert into task_checkpoints (task_name, updated_at, checkpoint_value)
        values (?, datetime('now'), ?)
        on conflict(task_name) do update set
          updated_at = excluded.updated_at,
          checkpoint_value = excluded.checkpoint_value
        """,
        (task_name, value),
    )
```

Use `uuid.uuid4()` for event IDs and `json.dumps(summary, sort_keys=True)` for payloads.

- [ ] **Step 4: Add `prodigy-data` CLI**

Create `src/prodigy/cli/data.py` with `argparse` subcommand:

```python
from __future__ import annotations

import argparse

from prodigy.data.backfill import run_backfill


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="prodigy-data")
    sub = parser.add_subparsers(dest="command", required=True)
    backfill = sub.add_parser("backfill")
    backfill.add_argument("--symbol", default="ETH/USDT:USDT")
    backfill.add_argument("--start", default="2024-01-01")
    backfill.add_argument("--end")
    backfill.add_argument("--timeframe", default="15m")
    backfill.add_argument("--data-root", default="data")
    backfill.add_argument("--db", default="var/prodigy.sqlite")
    backfill.add_argument("--proxy-url", default="http://127.0.0.1:7897")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "backfill":
        result = run_backfill(
            symbol=args.symbol,
            start=args.start,
            end=args.end,
            timeframe=args.timeframe,
            data_root=args.data_root,
            db_path=args.db,
            proxy_url=args.proxy_url,
        )
        print(result)
    return 0
```

Modify `pyproject.toml`:

```toml
[project.scripts]
prodigy-data = "prodigy.cli.data:main"
```

- [ ] **Step 5: Run tests**

```bash
mamba run -n quantmamba python -m pytest tests/test_backfill.py -v
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add pyproject.toml src/prodigy/data/backfill.py src/prodigy/cli tests/test_backfill.py
git commit -m "feat: add data backfill cli"
```

## Task 6: Example Factor Functions

**Files:**
- Modify: `src/prodigy/factors/examples.py`
- Test: `tests/test_example_factors.py`

- [ ] **Step 1: Extend factor tests**

Replace `tests/test_example_factors.py` with tests for:

```python
import pandas as pd

from prodigy.factors.examples import (
    example_funding_factor,
    example_momentum_factor,
    example_volatility_factor,
)


def market_frame():
    ts = pd.date_range("2026-07-01", periods=30, freq="15min", tz="UTC")
    return pd.DataFrame(
        {
            "timestamp": ts,
            "symbol": ["ETH/USDT:USDT"] * len(ts),
            "open": [100 + i * 0.1 for i in range(len(ts))],
            "high": [101 + i * 0.1 for i in range(len(ts))],
            "low": [99 + i * 0.1 for i in range(len(ts))],
            "close": [100 + i for i in range(len(ts))],
            "volume": [10 + i for i in range(len(ts))],
        }
    )


def funding_frame():
    ts = pd.date_range("2026-07-01", periods=30, freq="15min", tz="UTC")
    return pd.DataFrame(
        {
            "timestamp": ts,
            "symbol": ["ETH/USDT:USDT"] * len(ts),
            "funding_rate": [0.0001 * ((i % 5) - 2) for i in range(len(ts))],
        }
    )


def assert_factor_contract(frame, name):
    assert list(frame.columns) == ["timestamp", "symbol", "factor_name", "value"]
    assert frame["factor_name"].unique().tolist() == [name]
    assert frame["value"].notna().sum() > 0


def test_example_momentum_factor_contract():
    assert_factor_contract(
        example_momentum_factor(market_frame(), lookback_bars=4),
        "example_momentum",
    )


def test_example_funding_factor_contract():
    assert_factor_contract(
        example_funding_factor(funding_frame(), window=5),
        "example_funding",
    )


def test_example_volatility_factor_contract():
    assert_factor_contract(
        example_volatility_factor(market_frame(), atr_window=5),
        "example_volatility",
    )
```

- [ ] **Step 2: Run test to verify it fails**

```bash
mamba run -n quantmamba python -m pytest tests/test_example_factors.py -v
```

Expected: FAIL because the new functions do not exist.

- [ ] **Step 3: Implement the three example factors**

In `src/prodigy/factors/examples.py`, keep old functions for compatibility and add:

- `example_momentum_factor(ohlcv, lookback_bars=4)`: close pct change, clipped and scaled to `[-1, 1]`;
- `example_funding_factor(funding, window=20)`: negative funding z-score so high funding is contrarian bearish;
- `example_volatility_factor(ohlcv, atr_window=14)`: ATR-normalized momentum.

All return `factor_frame(source, factor_name, values)`.

- [ ] **Step 4: Run tests**

```bash
mamba run -n quantmamba python -m pytest tests/test_example_factors.py -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/prodigy/factors/examples.py tests/test_example_factors.py
git commit -m "feat: add crypto example factors"
```

## Task 7: Score-to-Lot Signal Layer

**Files:**
- Create: `src/prodigy/research/signals.py`
- Test: `tests/test_research_signals.py`

- [ ] **Step 1: Write failing signal tests**

Create `tests/test_research_signals.py`:

```python
import pandas as pd

from prodigy.research.signals import SignalParams, score_to_lot_signals


def score_frame(values):
    return pd.DataFrame(
        {
            "timestamp": pd.date_range("2026-07-01", periods=len(values), freq="15min", tz="UTC"),
            "symbol": ["ETH/USDT:USDT"] * len(values),
            "score": values,
        }
    )


def test_open_size_maps_from_five_to_ten_percent():
    params = SignalParams(total_notional_cap=10_000.0)
    signals = score_to_lot_signals(score_frame([0.6, 1.0]), params)

    opens = signals[signals["action"] == "open"]
    assert opens["notional"].round(6).tolist() == [500.0, 1000.0]


def test_add_cooldown_blocks_dense_same_direction_opens():
    params = SignalParams(total_notional_cap=10_000.0, add_cooldown_bars=4)
    signals = score_to_lot_signals(score_frame([0.8, 0.8, 0.8, 0.8, 0.8]), params)

    opens = signals[signals["action"] == "open"]
    assert opens["timestamp"].dt.strftime("%H:%M").tolist() == ["00:00", "01:00"]


def test_opposite_score_closes_lots():
    params = SignalParams(total_notional_cap=10_000.0)
    signals = score_to_lot_signals(score_frame([0.8, 0.1, -0.2]), params)

    assert signals.iloc[-1]["action"] == "close"
    assert signals.iloc[-1]["side"] == "long"
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
mamba run -n quantmamba python -m pytest tests/test_research_signals.py -v
```

Expected: FAIL because `prodigy.research.signals` does not exist.

- [ ] **Step 3: Implement signal layer**

Create `src/prodigy/research/signals.py` with:

- frozen `SignalParams` dataclass;
- `score_to_lot_signals(scores, params)` returning a DataFrame with `timestamp`, `symbol`, `action`, `side`, `score`, `notional`, `lot_id`, `reason`;
- linear notional mapping from 5% to 10% of total cap;
- same-direction cooldown;
- opposite close for open lots.

Use deterministic lot IDs like `lot-000001` for testability.

- [ ] **Step 4: Run tests**

```bash
mamba run -n quantmamba python -m pytest tests/test_research_signals.py -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/prodigy/research/signals.py tests/test_research_signals.py
git commit -m "feat: add score to lot signals"
```

## Task 8: Bar-Level Lot Simulator and Backtester Facade

**Files:**
- Create: `src/prodigy/research/simulator.py`
- Modify: `src/prodigy/research/backtester.py`
- Test: `tests/test_bar_simulator.py`
- Modify: `tests/test_backtester.py`

- [ ] **Step 1: Write failing simulator tests**

Create `tests/test_bar_simulator.py`:

```python
import pandas as pd

from prodigy.research.signals import SignalParams, score_to_lot_signals
from prodigy.research.simulator import BacktestParams, simulate_lots


def prices():
    ts = pd.date_range("2026-07-01", periods=12, freq="15min", tz="UTC")
    return pd.DataFrame(
        {
            "timestamp": ts,
            "symbol": ["ETH/USDT:USDT"] * len(ts),
            "open": [100, 101, 102, 103, 104, 105, 104, 103, 102, 101, 100, 99],
            "high": [101, 102, 103, 104, 105, 106, 105, 104, 103, 102, 101, 100],
            "low": [99, 100, 101, 102, 103, 104, 103, 102, 101, 100, 99, 98],
            "close": [101, 102, 103, 104, 105, 104, 103, 102, 101, 100, 99, 98],
            "volume": [10] * len(ts),
        }
    )


def test_simulator_opens_and_closes_lot_with_fees():
    scores = pd.DataFrame(
        {
            "timestamp": pd.date_range("2026-07-01", periods=3, freq="15min", tz="UTC"),
            "symbol": ["ETH/USDT:USDT"] * 3,
            "score": [0.8, 0.0, -0.2],
        }
    )
    signals = score_to_lot_signals(scores, SignalParams(total_notional_cap=10_000))

    result = simulate_lots(prices(), pd.DataFrame(), signals, BacktestParams(initial_equity=1000))

    assert len(result.trades) == 1
    assert result.trades.iloc[0]["exit_reason"] == "opposite_signal"
    assert result.equity_curve["equity"].iloc[-1] != 1000


def test_simulator_stop_loss_closes_lot():
    scores = pd.DataFrame(
        {
            "timestamp": [pd.Timestamp("2026-07-01T00:00:00Z")],
            "symbol": ["ETH/USDT:USDT"],
            "score": [1.0],
        }
    )
    signals = score_to_lot_signals(scores, SignalParams(total_notional_cap=10_000))
    params = BacktestParams(initial_equity=1000, stop_loss_position_notional_fraction=0.01)

    result = simulate_lots(prices(), pd.DataFrame(), signals, params)

    assert "stop_loss" in set(result.trades["exit_reason"])
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
mamba run -n quantmamba python -m pytest tests/test_bar_simulator.py -v
```

Expected: FAIL because `prodigy.research.simulator` does not exist.

- [ ] **Step 3: Implement simulator**

Create `src/prodigy/research/simulator.py` with:

- `BacktestParams` dataclass for fee, rebate, slippage, holding, stop, trailing, and equity settings;
- `BacktestResult` dataclass with `trades`, `equity_curve`, and `summary`;
- `simulate_lots(prices, funding, signals, params)`.

Simulator rules:

- open at current bar close plus slippage;
- close at current bar close minus slippage;
- net fee = raw fee * `(1 - rebate_fraction)`;
- funding applies when funding timestamp is inside lot holding interval;
- stop-loss checks each lot on each bar;
- 24h holding review extends by 1h only with confirming score if score data is available in signals;
- trailing take-profit uses simple swing high/low and ATR buffer.

- [ ] **Step 4: Extend Backtester facade**

Modify `src/prodigy/research/backtester.py` so it keeps old constructor compatibility and also accepts:

```python
Backtester(
    prices=ohlcv,
    factors=factor,
    funding=funding,
    signals=signals,
    params=BacktestParams(initial_equity=1000.0),
)
```

Add notebook-style methods:

```python
PlotAutocorrelation()
PlotRankIcCumsum()
PlotSignalDistribution()
PlotEquityCurve()
PlotDrawdown()
GetPerformanceSummary()
GetTradeSummary()
```

These methods may call existing lower-case methods internally.

- [ ] **Step 5: Run simulator and existing backtester tests**

```bash
mamba run -n quantmamba python -m pytest tests/test_bar_simulator.py tests/test_backtester.py -v
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/prodigy/research/simulator.py src/prodigy/research/backtester.py tests/test_bar_simulator.py tests/test_backtester.py
git commit -m "feat: add bar level research backtester"
```

## Task 9: Labels and Purged Walk-Forward Splits

**Files:**
- Create: `src/prodigy/ml/labels.py`
- Create: `src/prodigy/ml/splits.py`
- Test: `tests/test_ml_labels_splits.py`

- [ ] **Step 1: Write failing label and split tests**

Create `tests/test_ml_labels_splits.py`:

```python
import pandas as pd

from prodigy.ml.labels import add_forward_return_labels, horizon_to_bars
from prodigy.ml.splits import purged_walk_forward_splits


def frame():
    ts = pd.date_range("2024-01-01", periods=50000, freq="15min", tz="UTC")
    return pd.DataFrame(
        {
            "timestamp": ts,
            "symbol": ["ETH/USDT:USDT"] * len(ts),
            "close": [100 + i * 0.01 for i in range(len(ts))],
        }
    )


def test_horizon_to_bars():
    assert horizon_to_bars("15m") == 1
    assert horizon_to_bars("1h") == 4
    assert horizon_to_bars("4h") == 16
    assert horizon_to_bars("24h") == 96


def test_add_forward_return_labels():
    labeled = add_forward_return_labels(frame().head(10), horizons=["15m", "1h"])

    assert "target_15m" in labeled.columns
    assert "target_1h" in labeled.columns
    assert labeled["target_15m"].notna().sum() == 9
    assert labeled["target_1h"].notna().sum() == 6


def test_purged_walk_forward_excludes_final_holdout_and_gap():
    splits = purged_walk_forward_splits(
        frame(),
        min_train_days=365,
        valid_days=30,
        step_days=30,
        final_holdout_days=30,
        purge_gap_bars=4,
    )

    assert splits.folds
    first = splits.folds[0]
    assert first.train_end < first.valid_start
    assert first.valid_start - first.train_end >= pd.Timedelta(hours=1)
    assert splits.final_holdout_start > splits.folds[-1].valid_end
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
mamba run -n quantmamba python -m pytest tests/test_ml_labels_splits.py -v
```

Expected: FAIL because modules do not exist.

- [ ] **Step 3: Implement labels and split dataclasses**

Create `src/prodigy/ml/labels.py`:

- `horizon_to_bars(horizon: str) -> int`;
- `add_forward_return_labels(frame, horizons)`.

Create `src/prodigy/ml/splits.py`:

- `WalkForwardFold` dataclass;
- `WalkForwardSplits` dataclass;
- `purged_walk_forward_splits(frame, min_train_days, valid_days, step_days, final_holdout_days, purge_gap_bars)`.

Use expanding train windows, 30D validation steps, and final 30D holdout.

- [ ] **Step 4: Run tests**

```bash
mamba run -n quantmamba python -m pytest tests/test_ml_labels_splits.py -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/prodigy/ml/labels.py src/prodigy/ml/splits.py tests/test_ml_labels_splits.py
git commit -m "feat: add purged walk forward labels"
```

## Task 10: LightGBM Example Trainer and `prodigy-ml`

**Files:**
- Create: `src/prodigy/ml/example_trainer.py`
- Create: `src/prodigy/cli/ml.py`
- Modify: `pyproject.toml`
- Test: `tests/test_example_trainer.py`

- [ ] **Step 1: Write failing trainer test**

Create `tests/test_example_trainer.py`:

```python
import pandas as pd

from prodigy.db import connect, init_db
from prodigy.ml.example_trainer import train_example_model


def features():
    ts = pd.date_range("2024-01-01", periods=50000, freq="15min", tz="UTC")
    close = pd.Series([100 + i * 0.01 for i in range(len(ts))])
    return pd.DataFrame(
        {
            "timestamp": ts,
            "symbol": ["ETH/USDT:USDT"] * len(ts),
            "close": close,
            "example_momentum": close.pct_change(4).fillna(0),
            "example_funding": [0.1] * len(ts),
            "example_volatility": close.pct_change().rolling(8).std().fillna(0),
        }
    )


def test_train_example_model_saves_artifact_and_metadata(tmp_path):
    db_path = tmp_path / "prodigy.sqlite"
    with connect(db_path) as conn:
        init_db(conn)

    result = train_example_model(
        frame=features(),
        db_path=db_path,
        model_root=tmp_path / "models",
        horizon="1h",
        model_version="example-test",
    )

    assert result.artifact_path.exists()
    assert len(result.artifact_hash) == 64
    assert result.metrics["fold_count"] > 0
    assert "holdout_prediction_ic" in result.metrics

    with connect(db_path) as conn:
        row = conn.execute(
            "select model_version, artifact_hash from models where model_version = ?",
            ("example-test",),
        ).fetchone()

    assert row["artifact_hash"] == result.artifact_hash
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
mamba run -n quantmamba python -m pytest tests/test_example_trainer.py -v
```

Expected: FAIL because `prodigy.ml.example_trainer` does not exist.

- [ ] **Step 3: Implement example trainer**

Create `src/prodigy/ml/example_trainer.py` with:

- `ExampleTrainingResult` dataclass;
- `train_example_model(frame, db_path, model_root, horizon, model_version)`.

Use:

- features: `example_momentum`, `example_funding`, `example_volatility`;
- labels from `add_forward_return_labels`;
- splits from `purged_walk_forward_splits`;
- `lgb.LGBMRegressor(n_estimators=20, max_depth=3, learning_rate=0.05, random_state=7, verbosity=-1)`;
- save model through `model.booster_.save_model(path)`;
- hash artifact bytes with SHA-256;
- insert or replace SQLite `models` row.

Metrics must include:

```text
fold_count
train_rows
validation_rows
holdout_rows
holdout_prediction_ic
holdout_directional_accuracy
```

- [ ] **Step 4: Add `prodigy-ml` CLI**

Create `src/prodigy/cli/ml.py` with `argparse` and `train-example` command. It loads `data/processed/example_features.parquet.gzip`, calls `train_example_model`, and prints the result.

Modify `pyproject.toml` scripts:

```toml
prodigy-ml = "prodigy.cli.ml:main"
```

- [ ] **Step 5: Run tests**

```bash
mamba run -n quantmamba python -m pytest tests/test_example_trainer.py -v
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add pyproject.toml src/prodigy/ml/example_trainer.py src/prodigy/cli/ml.py tests/test_example_trainer.py
git commit -m "feat: add example lightgbm trainer"
```

## Task 11: Research Notebooks

**Files:**
- Create: `research/notebooks/00_data_check.ipynb`
- Create: `research/notebooks/01_example_momentum_factor.ipynb`
- Create: `research/notebooks/02_example_funding_factor.ipynb`
- Create: `research/notebooks/03_example_volatility_factor.ipynb`
- Create: `research/notebooks/99_combine_example_factors.ipynb`
- Test: `tests/test_research_notebooks.py`

- [ ] **Step 1: Write failing notebook structure test**

Create `tests/test_research_notebooks.py`:

```python
import json
from pathlib import Path


NOTEBOOKS = [
    "00_data_check.ipynb",
    "01_example_momentum_factor.ipynb",
    "02_example_funding_factor.ipynb",
    "03_example_volatility_factor.ipynb",
    "99_combine_example_factors.ipynb",
]


def load_source(path):
    data = json.loads(path.read_text())
    return "\n".join("".join(cell.get("source", [])) for cell in data["cells"])


def test_research_notebooks_exist_and_use_shared_data_backtester():
    root = Path("research/notebooks")
    for name in NOTEBOOKS:
        path = root / name
        assert path.exists(), name
        source = load_source(path)
        assert "load_ohlcv" in source
        assert "Backtester" in source


def test_combine_notebook_builds_example_features():
    source = load_source(Path("research/notebooks/99_combine_example_factors.ipynb"))

    assert "example_momentum" in source
    assert "example_funding" in source
    assert "example_volatility" in source
    assert "example_features.parquet.gzip" in source
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
mamba run -n quantmamba python -m pytest tests/test_research_notebooks.py -v
```

Expected: FAIL because notebooks do not exist.

- [ ] **Step 3: Create notebooks**

Create each notebook as valid JSON with Python 3 kernelspec. Keep the style close to `temp/factor_liang`:

- imports first;
- parameter cell with `DATA_ROOT`, `SYMBOL`, `START`, `END`;
- shared parquet loading cell;
- factor class or compute cell;
- `Backtester` call cell;
- diagnostic plot cells.

Use notebook cells that import:

```python
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from prodigy.data.parquet_store import load_funding_rates, load_ohlcv
from prodigy.factors.examples import (
    example_funding_factor,
    example_momentum_factor,
    example_volatility_factor,
)
from prodigy.research.backtester import Backtester
from prodigy.research.signals import SignalParams, score_to_lot_signals
```

`99_combine_example_factors.ipynb` must build `features` and save:

```python
features.to_parquet(
    "../data/processed/example_features.parquet.gzip",
    compression="gzip",
    index=False,
)
```

Use the correct relative path from `research/notebooks` to repo root:

```python
DATA_ROOT = "../../data"
```

- [ ] **Step 4: Run notebook tests**

```bash
mamba run -n quantmamba python -m pytest tests/test_research_notebooks.py -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add research/notebooks tests/test_research_notebooks.py
git commit -m "docs: add research notebooks"
```

## Task 12: Second Milestone End-to-End Smoke

**Files:**
- Create: `tests/test_second_milestone_smoke.py`

- [ ] **Step 1: Write end-to-end smoke test**

Create `tests/test_second_milestone_smoke.py`:

```python
import pandas as pd

from prodigy.data.parquet_store import write_daily_partition
from prodigy.db import connect, init_db
from prodigy.factors.examples import (
    example_funding_factor,
    example_momentum_factor,
    example_volatility_factor,
)
from prodigy.ml.example_trainer import train_example_model
from prodigy.research.backtester import Backtester
from prodigy.research.signals import SignalParams, score_to_lot_signals
from prodigy.research.simulator import BacktestParams


def market():
    ts = pd.date_range("2024-01-01", periods=50000, freq="15min", tz="UTC")
    close = pd.Series([100 + i * 0.01 + (i % 20) * 0.02 for i in range(len(ts))])
    return pd.DataFrame(
        {
            "timestamp": ts,
            "symbol": ["ETH/USDT:USDT"] * len(ts),
            "open": close,
            "high": close + 1,
            "low": close - 1,
            "close": close,
            "volume": [10 + i % 5 for i in range(len(ts))],
        }
    )


def funding(timestamps):
    return pd.DataFrame(
        {
            "timestamp": timestamps,
            "symbol": ["ETH/USDT:USDT"] * len(timestamps),
            "funding_time": timestamps,
            "funding_rate": [0.0001] * len(timestamps),
            "raw_symbol": ["ETHUSDT"] * len(timestamps),
        }
    )


def test_second_milestone_research_path(tmp_path):
    db_path = tmp_path / "prodigy.sqlite"
    with connect(db_path) as conn:
        init_db(conn)

    ohlcv = market()
    funds = funding(ohlcv["timestamp"].iloc[::32].reset_index(drop=True))
    write_daily_partition(
        ohlcv[ohlcv["timestamp"].dt.date == pd.Timestamp("2024-01-01").date()],
        tmp_path,
        "bitget",
        "ETH/USDT:USDT",
        "ohlcv",
        "2024-01-01",
        "15m",
    )

    momentum = example_momentum_factor(ohlcv).rename(columns={"value": "example_momentum"})
    funding_factor = example_funding_factor(funds).rename(columns={"value": "example_funding"})
    volatility = example_volatility_factor(ohlcv).rename(columns={"value": "example_volatility"})

    features = (
        momentum[["timestamp", "symbol", "example_momentum"]]
        .merge(funding_factor[["timestamp", "symbol", "example_funding"]], on=["timestamp", "symbol"], how="left")
        .merge(volatility[["timestamp", "symbol", "example_volatility"]], on=["timestamp", "symbol"], how="left")
        .fillna(0.0)
        .merge(ohlcv[["timestamp", "symbol", "close"]], on=["timestamp", "symbol"], how="inner")
    )

    scores = features[["timestamp", "symbol"]].copy()
    scores["score"] = features["example_momentum"].clip(-1, 1)
    signals = score_to_lot_signals(scores.head(100), SignalParams(total_notional_cap=10_000))
    bt = Backtester(
        prices=ohlcv.head(200),
        factors=momentum.head(200).rename(columns={"example_momentum": "value"}),
        funding=funds,
        signals=signals,
        params=BacktestParams(initial_equity=1000),
    )

    summary = bt.GetPerformanceSummary()
    model = train_example_model(
        features,
        db_path=db_path,
        model_root=tmp_path / "models",
        horizon="1h",
        model_version="second-milestone-smoke",
    )

    assert "final_equity" in summary
    assert model.artifact_path.exists()
    assert model.metrics["fold_count"] > 0
```

- [ ] **Step 2: Run the smoke test**

```bash
mamba run -n quantmamba python -m pytest tests/test_second_milestone_smoke.py -v
```

Expected: PASS after all earlier tasks are complete.

- [ ] **Step 3: Run full verification**

```bash
mamba run -n quantmamba python -m pytest -v
cargo test
cargo clippy --all-targets -- -D warnings
cargo fmt --check
```

Expected:

- Python tests pass.
- Rust tests pass.
- Clippy passes.
- Rust formatting passes.

- [ ] **Step 4: Commit**

```bash
git add tests/test_second_milestone_smoke.py
git commit -m "test: add second milestone research smoke"
```

## Self-Review Checklist

- Project layout requirements are covered by Task 1.
- Parquet gzip partition storage is covered by Task 2.
- Data quality and checkpoint/audit behavior are covered by Tasks 3 and 5.
- CCXT OHLCV and Bitget official REST funding are covered by Tasks 4 and 5.
- OI history is explicitly excluded.
- Research notebooks and factor_liang-style workflow are covered by Task 11.
- Three example factors are covered by Task 6.
- Factor -> signal -> backtest separation is covered by Tasks 7 and 8.
- Lot-level rules, cooldown, 24h review, stop-loss, and trailing take-profit are covered by Tasks 7 and 8.
- Labels, purged walk-forward, and final 30D holdout are covered by Task 9.
- LightGBM artifact and SQLite model metadata are covered by Task 10.
- End-to-end engineering smoke is covered by Task 12.
