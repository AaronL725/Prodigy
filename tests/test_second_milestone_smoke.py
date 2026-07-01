import pandas as pd

from prodigy.data.parquet_store import write_daily_partition
from prodigy.db import connect, init_db
from prodigy.factors.examples import (
    example_funding_factor,
    example_momentum_factor,
    example_volatility_factor,
)
from prodigy.ml.example_trainer import train_example_model
from prodigy.research.backtester import Backtester
from prodigy.research.signals import SignalParams, score_to_lot_signals
from prodigy.research.simulator import BacktestParams


def market():
    ts = pd.date_range("2024-01-01", periods=50000, freq="15min", tz="UTC")
    close = pd.Series([100 + i * 0.01 + (i % 20) * 0.02 for i in range(len(ts))])
    return pd.DataFrame(
        {
            "timestamp": ts,
            "symbol": ["ETH/USDT:USDT"] * len(ts),
            "open": close,
            "high": close + 1,
            "low": close - 1,
            "close": close,
            "volume": [10 + i % 5 for i in range(len(ts))],
        }
    )


def funding(timestamps):
    return pd.DataFrame(
        {
            "timestamp": timestamps,
            "symbol": ["ETH/USDT:USDT"] * len(timestamps),
            "funding_time": timestamps,
            "funding_rate": [0.0001] * len(timestamps),
            "raw_symbol": ["ETHUSDT"] * len(timestamps),
        }
    )


def test_second_milestone_research_path(tmp_path):
    db_path = tmp_path / "prodigy.sqlite"
    with connect(db_path) as conn:
        init_db(conn)

    ohlcv = market()
    funds = funding(ohlcv["timestamp"].iloc[::32].reset_index(drop=True))
    write_daily_partition(
        ohlcv[ohlcv["timestamp"].dt.date == pd.Timestamp("2024-01-01").date()],
        tmp_path,
        "bitget",
        "ETH/USDT:USDT",
        "ohlcv",
        "2024-01-01",
        "15m",
    )

    momentum = example_momentum_factor(ohlcv).rename(columns={"value": "example_momentum"})
    funding_factor = example_funding_factor(funds).rename(columns={"value": "example_funding"})
    volatility = example_volatility_factor(ohlcv).rename(columns={"value": "example_volatility"})

    features = (
        momentum[["timestamp", "symbol", "example_momentum"]]
        .merge(funding_factor[["timestamp", "symbol", "example_funding"]], on=["timestamp", "symbol"], how="left")
        .merge(volatility[["timestamp", "symbol", "example_volatility"]], on=["timestamp", "symbol"], how="left")
        .fillna(0.0)
        .merge(ohlcv[["timestamp", "symbol", "close"]], on=["timestamp", "symbol"], how="inner")
    )

    scores = features[["timestamp", "symbol"]].copy()
    scores["score"] = features["example_momentum"].clip(-1, 1)
    signals = score_to_lot_signals(scores.head(100), SignalParams(total_notional_cap=10_000))
    bt = Backtester(
        prices=ohlcv.head(200),
        factors=momentum.head(200).rename(columns={"example_momentum": "value"}),
        funding=funds,
        signals=signals,
        params=BacktestParams(initial_equity=1000),
    )

    summary = bt.GetPerformanceSummary()
    model = train_example_model(
        features,
        db_path=db_path,
        model_root=tmp_path / "models",
        horizon="1h",
        model_version="second-milestone-smoke",
    )

    assert "final_equity" in summary
    assert model.artifact_path.exists()
    assert model.metrics["fold_count"] > 0
