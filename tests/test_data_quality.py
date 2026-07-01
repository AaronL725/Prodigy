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
