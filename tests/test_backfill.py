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
