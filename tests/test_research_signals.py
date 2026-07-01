import pandas as pd

from prodigy.research.signals import SignalParams, score_to_lot_signals


def score_frame(values):
    return pd.DataFrame(
        {
            "timestamp": pd.date_range("2026-07-01", periods=len(values), freq="15min", tz="UTC"),
            "symbol": ["ETH/USDT:USDT"] * len(values),
            "score": values,
        }
    )


def test_open_size_maps_from_five_to_ten_percent():
    params = SignalParams(total_notional_cap=10_000.0)
    signals = score_to_lot_signals(score_frame([0.6, 1.0]), params)

    opens = signals[signals["action"] == "open"]
    assert opens["notional"].round(6).tolist() == [500.0, 1000.0]


def test_add_cooldown_blocks_dense_same_direction_opens():
    params = SignalParams(total_notional_cap=10_000.0, add_cooldown_bars=4)
    signals = score_to_lot_signals(score_frame([0.8, 0.8, 0.8, 0.8, 0.8]), params)

    opens = signals[signals["action"] == "open"]
    assert opens["timestamp"].dt.strftime("%H:%M").tolist() == ["00:00", "01:00"]


def test_opposite_score_closes_lots():
    params = SignalParams(total_notional_cap=10_000.0)
    signals = score_to_lot_signals(score_frame([0.8, 0.1, -0.2]), params)

    assert signals.iloc[-1]["action"] == "close"
    assert signals.iloc[-1]["side"] == "long"
