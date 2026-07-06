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


def test_quality_summary_reports_non_monotonic_and_daily_counts():
    ohlcv = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                [
                    "2026-07-01T00:00:00Z",
                    "2026-07-01T00:45:00Z",
                    "2026-07-01T00:30:00Z",
                ],
                utc=True,
            ),
            "symbol": ["ETH/USDT:USDT"] * 3,
            "open": [100.0, 101.0, 102.0],
            "high": [101.0, 102.0, 103.0],
            "low": [99.0, 100.0, 101.0],
            "close": [100.5, 101.5, 102.5],
            "volume": [1.0, 2.0, 3.0],
        }
    )

    summary = quality_summary(ohlcv, dataset="ohlcv", timeframe="15m")

    assert summary["non_monotonic_timestamps"] == 1
    assert summary["expected_15m_bars"] == 4
    assert summary["expected_15m_bars_per_day"] == {"2026-07-01": 4}
    assert summary["missing_timestamps"] == 1
    assert summary["missing_timestamps_per_day"] == {"2026-07-01": 1}
    assert summary["start"] == "2026-07-01T00:00:00+00:00"
    assert summary["end"] == "2026-07-01T00:45:00+00:00"

    funding = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                [
                    "2026-07-01T00:00:00Z",
                    "2026-07-01T08:00:00Z",
                    "2026-07-02T00:00:00Z",
                ],
                utc=True,
            ),
            "symbol": ["ETH/USDT:USDT"] * 3,
            "funding_time": pd.to_datetime(
                [
                    "2026-07-01T00:00:00Z",
                    "2026-07-01T08:00:00Z",
                    "2026-07-02T00:00:00Z",
                ],
                utc=True,
            ),
            "funding_rate": [0.001, 0.002, 0.003],
        }
    )

    funding_summary = quality_summary(funding, dataset="funding_rates")

    assert funding_summary["funding_rows_per_day"] == {
        "2026-07-01": 2,
        "2026-07-02": 1,
    }


def test_quality_summary_checks_requested_ohlcv_window_boundaries():
    frame = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                ["2026-07-01T00:15:00Z", "2026-07-01T00:30:00Z"],
                utc=True,
            ),
            "symbol": ["ETH/USDT:USDT"] * 2,
            "open": [101.0, 102.0],
            "high": [102.0, 103.0],
            "low": [100.0, 101.0],
            "close": [101.5, 102.5],
            "volume": [2.0, 3.0],
        }
    )

    summary = quality_summary(
        frame,
        dataset="ohlcv",
        timeframe="15m",
        start="2026-07-01T00:00:00Z",
        end="2026-07-01T01:00:00Z",
    )

    assert summary["expected_15m_bars"] == 4
    assert summary["missing_timestamps"] == 2
    assert summary["missing_timestamps_per_day"] == {"2026-07-01": 2}


def test_quality_summary_counts_empty_requested_ohlcv_window_as_missing():
    summary = quality_summary(
        pd.DataFrame(),
        dataset="ohlcv",
        timeframe="15m",
        start="2026-07-01T00:00:00Z",
        end="2026-07-01T01:00:00Z",
    )

    assert summary["expected_15m_bars"] == 4
    assert summary["missing_timestamps"] == 4


def test_quality_summary_handles_empty_frame():
    summary = quality_summary(pd.DataFrame(), dataset="funding_rates")

    assert summary["rows"] == 0
    assert summary["missing_timestamps"] == 0
