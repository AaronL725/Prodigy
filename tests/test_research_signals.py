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
    params = SignalParams(total_notional_cap=10_000.0, add_cooldown_bars=1)
    signals = score_to_lot_signals(score_frame([0.6, 1.0]), params)

    opens = signals[signals["action"] == "open"]
    assert opens["notional"].round(6).tolist() == [500.0, 1000.0]


def test_open_size_fractions_are_configurable():
    # The 5%..10% mapping is configurable on SignalParams, not hardcoded.
    params = SignalParams(
        total_notional_cap=10_000.0,
        add_cooldown_bars=1,
        min_size_fraction=0.02,
        max_size_fraction=0.20,
    )
    signals = score_to_lot_signals(score_frame([0.6, 1.0]), params)

    opens = signals[signals["action"] == "open"]
    # 0.6 -> 2% = 200, 1.0 -> 20% = 2000
    assert opens["notional"].round(6).tolist() == [200.0, 2000.0]


def test_add_cooldown_blocks_dense_same_direction_opens():
    params = SignalParams(total_notional_cap=10_000.0, add_cooldown_bars=4)
    signals = score_to_lot_signals(score_frame([0.8, 0.8, 0.8, 0.8, 0.8]), params)

    opens = signals[signals["action"] == "open"]
    assert opens["timestamp"].dt.strftime("%H:%M").tolist() == ["00:00", "01:00"]


def test_add_cooldown_blocks_stronger_same_direction_open_inside_window():
    params = SignalParams(total_notional_cap=10_000.0, add_cooldown_bars=4)
    signals = score_to_lot_signals(score_frame([0.8, 0.9]), params)

    opens = signals[signals["action"] == "open"]
    assert opens["timestamp"].dt.strftime("%H:%M").tolist() == ["00:00"]


def test_opposite_score_closes_lots():
    params = SignalParams(total_notional_cap=10_000.0)
    signals = score_to_lot_signals(score_frame([0.8, 0.1, -0.2]), params)

    assert signals.iloc[-1]["action"] == "close"
    assert signals.iloc[-1]["side"] == "long"


def test_total_open_notional_cannot_exceed_cap():
    # spec: "Total notional cannot exceed total_notional_cap". A long run of
    # max-strength scores must stop opening once cumulative open notional hits
    # the cap, even though cooldown/strength rules would keep permitting adds.
    params = SignalParams(total_notional_cap=10_000.0, add_cooldown_bars=4)
    signals = score_to_lot_signals(score_frame([1.0] * 50), params)

    opens = signals[signals["action"] == "open"]
    assert opens["notional"].sum() <= 10_000.0 + 1e-9
    # each max-strength open is 1000 (10% of 10000), so exactly 10 fit
    assert len(opens) == 10
