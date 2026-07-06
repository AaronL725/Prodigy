import json

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


class FakeMixedWindowFundingClient:
    # ponytail: returns funding both inside and outside the requested window —
    # Bitget history-fund-rate is newest-first and unfiltered, so the backfill
    # must window to [start, end) itself.
    def __init__(self):
        self._page = pd.DataFrame(
            {
                "timestamp": pd.to_datetime(
                    ["2026-06-30T00:00:00Z", "2026-07-01T00:00:00Z", "2026-07-02T00:00:00Z"],
                    utc=True,
                ),
                "symbol": ["ETH/USDT:USDT"] * 3,
                "funding_time": pd.to_datetime(
                    ["2026-06-30T00:00:00Z", "2026-07-01T00:00:00Z", "2026-07-02T00:00:00Z"],
                    utc=True,
                ),
                "funding_rate": [0.0001, 0.001, 0.002],
                "raw_symbol": ["ETHUSDT"] * 3,
            }
        )

    def fetch_funding_rate_page(self, symbol, product_type, page_no, page_size):
        return self._page if page_no == 1 else self._page.iloc[0:0]


class FakeEmptyFundingClient:
    def fetch_funding_rate_page(self, symbol, product_type, page_no, page_size):
        return pd.DataFrame(
            columns=["timestamp", "symbol", "funding_time", "funding_rate", "raw_symbol"]
        )


class FakeClosedBarExchange:
    def load_markets(self):
        return {}

    def fetch_ohlcv(self, symbol, timeframe, since=None, limit=None, params=None):
        return [
            [1782907200000, 100.0, 101.0, 99.0, 100.5, 10.0],
            [1782908100000, 100.5, 102.0, 100.0, 101.0, 11.0],
            [1782909000000, 101.0, 103.0, 100.5, 102.0, 12.0],
        ]


class FakeGappyExchange:
    def load_markets(self):
        return {}

    def fetch_ohlcv(self, symbol, timeframe, since=None, limit=None, params=None):
        return [
            [1782864000000, 100.0, 101.0, 99.0, 100.5, 10.0],
            [1782865800000, 101.0, 103.0, 100.5, 102.0, 12.0],
        ]


class FakeBoundaryGapExchange:
    def load_markets(self):
        return {}

    def fetch_ohlcv(self, symbol, timeframe, since=None, limit=None, params=None):
        return [
            [1782864900000, 100.5, 102.0, 100.0, 101.0, 11.0],
            [1782865800000, 101.0, 103.0, 100.5, 102.0, 12.0],
        ]


class FakeFailingFundingClient:
    def fetch_funding_rate_page(self, symbol, product_type, page_no, page_size):
        raise RuntimeError("funding api down")


def test_run_backfill_default_end_uses_latest_closed_bar_boundary(tmp_path):
    db_path = tmp_path / "prodigy.sqlite"
    with connect(db_path) as conn:
        init_db(conn)

    result = run_backfill(
        symbol="ETH/USDT:USDT",
        start="2026-07-01 12:00:00",
        end=None,
        timeframe="15m",
        data_root=tmp_path,
        db_path=db_path,
        exchange=FakeClosedBarExchange(),
        funding_client=FakeEmptyFundingClient(),
        now=pd.Timestamp("2026-07-01 12:33:00", tz="UTC"),
    )

    assert result.end == "2026-07-01T12:30:00+00:00"
    assert result.ohlcv_rows == 2

    ohlcv = load_ohlcv(tmp_path, "ETH/USDT:USDT", "2026-07-01", "2026-07-02")
    assert ohlcv["timestamp"].dt.strftime("%H:%M").tolist() == ["12:00", "12:15"]


def test_run_backfill_records_quality_warning_event_when_quality_problem_exists(tmp_path):
    db_path = tmp_path / "prodigy.sqlite"
    with connect(db_path) as conn:
        init_db(conn)

    run_backfill(
        symbol="ETH/USDT:USDT",
        start="2026-07-01",
        end="2026-07-01 00:45:00",
        timeframe="15m",
        data_root=tmp_path,
        db_path=db_path,
        exchange=FakeGappyExchange(),
        funding_client=FakeEmptyFundingClient(),
    )

    with connect(db_path) as conn:
        rows = conn.execute(
            """
            select severity, message, payload_json
            from events
            where component = 'data.backfill'
              and severity = 'warning'
              and message = 'data quality warning'
            """
        ).fetchall()

    assert len(rows) == 1
    payload = json.loads(rows[0]["payload_json"])
    assert rows[0]["message"] == "data quality warning"
    assert payload["ohlcv_quality"]["missing_timestamps"] == 1
    assert "ohlcv.missing_timestamps" in payload["issues"]


def test_run_backfill_checks_quality_against_requested_window(tmp_path):
    db_path = tmp_path / "prodigy.sqlite"
    with connect(db_path) as conn:
        init_db(conn)

    result = run_backfill(
        symbol="ETH/USDT:USDT",
        start="2026-07-01T00:00:00Z",
        end="2026-07-01T01:00:00Z",
        timeframe="15m",
        data_root=tmp_path,
        db_path=db_path,
        exchange=FakeBoundaryGapExchange(),
        funding_client=FakeEmptyFundingClient(),
    )

    assert result.ohlcv_quality["expected_15m_bars"] == 4
    assert result.ohlcv_quality["missing_timestamps"] == 2


def test_run_backfill_does_not_advance_checkpoint_when_gaps_remain(tmp_path):
    db_path = tmp_path / "prodigy.sqlite"
    with connect(db_path) as conn:
        init_db(conn)
        conn.execute(
            """
            insert into task_checkpoints (task_name, updated_at, checkpoint_value)
            values (?, datetime('now'), ?)
            """,
            ("backfill:bitget:ETH/USDT:USDT:15m", "2026-06-30"),
        )
        conn.commit()

    run_backfill(
        symbol="ETH/USDT:USDT",
        start="2026-07-01",
        end="2026-07-01 00:45:00",
        timeframe="15m",
        data_root=tmp_path,
        db_path=db_path,
        exchange=FakeGappyExchange(),
        funding_client=FakeEmptyFundingClient(),
    )

    with connect(db_path) as conn:
        checkpoint = conn.execute(
            "select checkpoint_value from task_checkpoints where task_name = ?",
            ("backfill:bitget:ETH/USDT:USDT:15m",),
        ).fetchone()
        event = conn.execute(
            """
            select message, payload_json
            from events
            where component = 'data.backfill'
              and severity = 'warning'
              and message = 'checkpoint not advanced'
            """
        ).fetchone()

    assert checkpoint["checkpoint_value"] == "2026-06-30"
    assert json.loads(event["payload_json"])["issues"] == ["ohlcv.missing_timestamps"]


def test_run_backfill_records_error_event_and_reraises_on_failure(tmp_path):
    db_path = tmp_path / "prodigy.sqlite"

    try:
        run_backfill(
            symbol="ETH/USDT:USDT",
            start="2026-07-01",
            end="2026-07-01 00:30:00",
            timeframe="15m",
            data_root=tmp_path,
            db_path=db_path,
            exchange=FakeExchange(),
            funding_client=FakeFailingFundingClient(),
        )
    except RuntimeError as exc:
        assert str(exc) == "funding api down"
    else:
        raise AssertionError("run_backfill should re-raise fetch failures")

    with connect(db_path) as conn:
        row = conn.execute(
            """
            select severity, message, payload_json
            from events
            where component = 'data.backfill'
            """
        ).fetchone()

    assert row["severity"] == "error"
    assert row["message"] == "backfill failed"
    assert json.loads(row["payload_json"])["error"] == "funding api down"


def test_run_backfill_windows_funding_to_range(tmp_path):
    db_path = tmp_path / "prodigy.sqlite"
    with connect(db_path) as conn:
        init_db(conn)

    result = run_backfill(
        symbol="ETH/USDT:USDT",
        start="2026-07-01",
        end="2026-07-01 00:30:00",
        timeframe="15m",
        data_root=tmp_path,
        db_path=db_path,
        exchange=FakeExchange(),
        funding_client=FakeMixedWindowFundingClient(),
    )

    assert result.funding_rows == 1
    funding = load_funding_rates(tmp_path, "ETH/USDT:USDT", "2026-07-01", "2026-07-02")
    assert len(funding) == 1
    assert set(funding["timestamp"].dt.strftime("%Y-%m-%d")) == {"2026-07-01"}


def test_run_backfill_writes_partitions_and_checkpoint(tmp_path):
    db_path = tmp_path / "prodigy.sqlite"
    with connect(db_path) as conn:
        init_db(conn)

    result = run_backfill(
        symbol="ETH/USDT:USDT",
        start="2026-07-01",
        end="2026-07-01 00:30:00",
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
        assert row["checkpoint_value"] == "2026-07-01 00:30:00"
