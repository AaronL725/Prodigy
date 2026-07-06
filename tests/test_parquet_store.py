import pandas as pd
import pytest

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


def test_write_funding_partition_accepts_minimal_schema(tmp_path):
    frame = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2026-07-01T00:00:00Z"], utc=True),
            "symbol": ["ETH/USDT:USDT"],
            "funding_rate": [0.001],
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
    assert loaded["funding_rate"].tolist() == [0.001]


def test_write_daily_partition_rejects_missing_required_columns_and_keeps_existing_partition(
    tmp_path,
):
    good = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2026-07-01T00:00:00Z"], utc=True),
            "symbol": ["ETH/USDT:USDT"],
            "open": [100.0],
            "high": [101.0],
            "low": [99.0],
            "close": [100.5],
            "volume": [10.0],
        }
    )
    write_daily_partition(
        good,
        data_root=tmp_path,
        exchange="bitget",
        symbol="ETH/USDT:USDT",
        dataset="ohlcv",
        date=pd.Timestamp("2026-07-01"),
        timeframe="15m",
    )

    bad = good.drop(columns=["volume"]).assign(close=999.0)
    with pytest.raises(ValueError, match="ohlcv partition missing required columns: volume"):
        write_daily_partition(
            bad,
            data_root=tmp_path,
            exchange="bitget",
            symbol="ETH/USDT:USDT",
            dataset="ohlcv",
            date=pd.Timestamp("2026-07-01"),
            timeframe="15m",
        )

    loaded = load_ohlcv(
        data_root=tmp_path,
        symbol="ETH/USDT:USDT",
        start="2026-07-01",
        end="2026-07-02",
        timeframe="15m",
    )
    assert loaded["close"].tolist() == [100.5]
