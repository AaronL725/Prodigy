import pandas as pd

from prodigy.factors.examples import funding_zscore, momentum_15m, oi_change


def market_frame():
    ts = pd.date_range("2026-07-01", periods=6, freq="15min", tz="UTC")
    return pd.DataFrame(
        {
            "timestamp": ts,
            "symbol": ["ETH/USDT:USDT"] * 6,
            "open": [100, 101, 102, 101, 103, 104],
            "high": [101, 102, 103, 102, 104, 105],
            "low": [99, 100, 101, 100, 102, 103],
            "close": [101, 102, 101, 103, 104, 106],
            "volume": [10, 12, 11, 13, 14, 15],
            "funding_rate": [0.001, 0.002, 0.001, 0.003, 0.004, 0.005],
            "open_interest": [1000, 1010, 1005, 1030, 1040, 1060],
        }
    )


def test_momentum_factor_output_contract():
    result = momentum_15m(market_frame(), periods=2)

    assert list(result.columns) == ["timestamp", "symbol", "factor_name", "value"]
    assert result["factor_name"].unique().tolist() == ["momentum_15m"]
    assert result["value"].notna().sum() == 4


def test_funding_zscore_output_contract():
    result = funding_zscore(market_frame(), window=3)

    assert result["factor_name"].unique().tolist() == ["funding_zscore"]
    assert result["value"].notna().sum() == 4


def test_oi_change_output_contract():
    result = oi_change(market_frame(), periods=2)

    assert result["factor_name"].unique().tolist() == ["oi_change"]
    assert result["value"].notna().sum() == 4
