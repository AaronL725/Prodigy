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
