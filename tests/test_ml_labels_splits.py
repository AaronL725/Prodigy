import pandas as pd

from prodigy.ml.labels import add_forward_return_labels, horizon_to_bars
from prodigy.ml.splits import purged_walk_forward_splits


def frame():
    ts = pd.date_range("2024-01-01", periods=50000, freq="15min", tz="UTC")
    return pd.DataFrame(
        {
            "timestamp": ts,
            "symbol": ["ETH/USDT:USDT"] * len(ts),
            "close": [100 + i * 0.01 for i in range(len(ts))],
        }
    )


def test_horizon_to_bars():
    assert horizon_to_bars("15m") == 1
    assert horizon_to_bars("1h") == 4
    assert horizon_to_bars("4h") == 16
    assert horizon_to_bars("24h") == 96


def test_add_forward_return_labels():
    labeled = add_forward_return_labels(frame().head(10), horizons=["15m", "1h"])

    assert "target_15m" in labeled.columns
    assert "target_1h" in labeled.columns
    assert labeled["target_15m"].notna().sum() == 9
    assert labeled["target_1h"].notna().sum() == 6


def test_purged_walk_forward_excludes_final_holdout_and_gap():
    splits = purged_walk_forward_splits(
        frame(),
        min_train_days=365,
        valid_days=30,
        step_days=30,
        final_holdout_days=30,
        purge_gap_bars=4,
    )

    assert splits.folds
    first = splits.folds[0]
    assert first.train_end < first.valid_start
    assert first.valid_start - first.train_end >= pd.Timedelta(hours=1)
    assert splits.final_holdout_start > splits.folds[-1].valid_end
