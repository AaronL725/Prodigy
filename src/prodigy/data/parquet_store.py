from __future__ import annotations

from pathlib import Path
import tempfile

import pandas as pd

from prodigy.data.paths import ensure_dir, symbol_slug

DATASET_REQUIRED_COLUMNS = {
    "ohlcv": {"timestamp", "symbol", "open", "high", "low", "close", "volume"},
    "funding_rates": {"timestamp", "symbol", "funding_rate"},
}


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


def _validate_required_columns(frame: pd.DataFrame, dataset: str) -> None:
    required = DATASET_REQUIRED_COLUMNS.get(dataset, {"timestamp", "symbol"})
    missing = sorted(required.difference(frame.columns))
    if missing:
        raise ValueError(
            f"{dataset} partition missing required columns: {', '.join(missing)}"
        )


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
    _validate_required_columns(frame, dataset)
    clean = frame.sort_values("timestamp").drop_duplicates(
        ["timestamp", "symbol"], keep="last"
    )
    with tempfile.NamedTemporaryFile(
        suffix=".parquet.gzip", dir=path.parent, delete=False
    ) as tmp:
        tmp_path = Path(tmp.name)
    try:
        clean.to_parquet(tmp_path, compression="gzip", index=False)
        _validate_required_columns(pd.read_parquet(tmp_path), dataset)
        tmp_path.replace(path)
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise
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
