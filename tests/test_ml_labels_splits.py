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


def test_add_forward_return_labels_handles_unsorted_input():
    # A research-grade labeler must not silently produce wrong labels when the
    # input frame is out of timestamp order. Shuffle the rows; labels must match
    # the sorted version (forward return is per-symbol, time-ordered).
    sorted_frame = frame().head(20)
    shuffled = sorted_frame.sample(frac=1, random_state=1).reset_index(drop=True)

    labeled_shuffled = add_forward_return_labels(shuffled, horizons=["1h"])
    labeled_sorted = add_forward_return_labels(sorted_frame, horizons=["1h"])

    # Align by (symbol, timestamp) and compare the label column. NaN==NaN rows
    # (last `horizon` bars) are expected on both sides; compare via fillna.
    merged = labeled_shuffled.merge(
        labeled_sorted, on=["timestamp", "symbol"], suffixes=("_shuf", "_sort")
    )
    assert merged["target_1h_shuf"].fillna(-99).round(10).tolist() == merged[
        "target_1h_sort"
    ].fillna(-99).round(10).tolist()


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


def test_purged_walk_forward_no_label_leak_into_validation():
    # A 1h label uses 4 forward bars: target_i = forward[i+4]. With purge_gap_bars
    # == label_bars == 4, the last training sample's target must land STRICTLY
    # before valid_start, never on or past it — otherwise the validation set's
    # first bar contaminates a training label (optimistic IC).
    label_bars = horizon_to_bars("1h")  # 4
    splits = purged_walk_forward_splits(
        frame(),
        min_train_days=365,
        valid_days=30,
        step_days=30,
        final_holdout_days=30,
        purge_gap_bars=label_bars,
    )

    ts = pd.DatetimeIndex(frame()["timestamp"])
    bar = pd.Timedelta(minutes=15)
    for fold in splits.folds:
        last_train_target_ts = fold.train_end + label_bars * bar
        assert last_train_target_ts < fold.valid_start, (
            f"label leak: train_end {fold.train_end} + {label_bars} bars = "
            f"{last_train_target_ts} >= valid_start {fold.valid_start}"
        )

