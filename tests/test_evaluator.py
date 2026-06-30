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
