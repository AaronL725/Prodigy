import pandas as pd

from prodigy.research.signals import SignalParams, score_to_lot_signals
from prodigy.research.simulator import BacktestParams, simulate_lots


def prices():
    ts = pd.date_range("2026-07-01", periods=12, freq="15min", tz="UTC")
    return pd.DataFrame(
        {
            "timestamp": ts,
            "symbol": ["ETH/USDT:USDT"] * len(ts),
            "open": [100, 101, 102, 103, 104, 105, 104, 103, 102, 101, 100, 99],
            "high": [101, 102, 103, 104, 105, 106, 105, 104, 103, 102, 101, 100],
            "low": [99, 100, 101, 102, 103, 104, 103, 102, 101, 100, 99, 98],
            "close": [101, 102, 103, 104, 105, 104, 103, 102, 101, 100, 99, 98],
            "volume": [10] * len(ts),
        }
    )


def test_simulator_opens_and_closes_lot_with_fees():
    scores = pd.DataFrame(
        {
            "timestamp": pd.date_range("2026-07-01", periods=3, freq="15min", tz="UTC"),
            "symbol": ["ETH/USDT:USDT"] * 3,
            "score": [0.8, 0.0, -0.2],
        }
    )
    signals = score_to_lot_signals(scores, SignalParams(total_notional_cap=10_000))

    result = simulate_lots(prices(), pd.DataFrame(), signals, BacktestParams(initial_equity=1000))

    assert len(result.trades) == 1
    assert result.trades.iloc[0]["exit_reason"] == "opposite_signal"
    assert result.equity_curve["equity"].iloc[-1] != 1000


def test_simulator_stop_loss_closes_lot():
    scores = pd.DataFrame(
        {
            "timestamp": [pd.Timestamp("2026-07-01T00:00:00Z")],
            "symbol": ["ETH/USDT:USDT"],
            "score": [1.0],
        }
    )
    signals = score_to_lot_signals(scores, SignalParams(total_notional_cap=10_000))
    params = BacktestParams(initial_equity=1000, stop_loss_position_notional_fraction=0.01)

    result = simulate_lots(prices(), pd.DataFrame(), signals, params)

    assert "stop_loss" in set(result.trades["exit_reason"])
