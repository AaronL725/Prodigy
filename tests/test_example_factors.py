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
