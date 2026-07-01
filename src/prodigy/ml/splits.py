from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

# ponytail: 15-min bars for this milestone; 1 day = 96 bars.
_BARS_PER_DAY = 96


@dataclass(frozen=True)
class WalkForwardFold:
    train_start: pd.Timestamp
    train_end: pd.Timestamp
    valid_start: pd.Timestamp
    valid_end: pd.Timestamp


@dataclass(frozen=True)
class WalkForwardSplits:
    folds: list[WalkForwardFold]
    final_holdout_start: pd.Timestamp
    final_holdout_end: pd.Timestamp


def purged_walk_forward_splits(
    frame: pd.DataFrame,
    min_train_days: int = 365,
    valid_days: int = 30,
    step_days: int = 30,
    final_holdout_days: int = 30,
    purge_gap_bars: int = 4,
) -> WalkForwardSplits:
    ts = pd.DatetimeIndex(pd.Series(frame["timestamp"]).reset_index(drop=True))
    n = len(ts)
    bars_per_day = _BARS_PER_DAY

    start_time = ts[0]
    end_time = ts[n - 1]
    final_holdout_start = end_time - pd.Timedelta(days=final_holdout_days) + pd.Timedelta(minutes=15)

    folds: list[WalkForwardFold] = []

    # First validation window starts right after the minimum expanding-train span.
    valid_offset_days = min_train_days
    while True:
        valid_start_idx = valid_offset_days * bars_per_day
        valid_end_idx = valid_offset_days * bars_per_day + valid_days * bars_per_day - 1
        if valid_end_idx >= n:
            break
        valid_start_time = ts[valid_start_idx]
        valid_end_time = ts[valid_end_idx]
        # Stop once the next validation window enters the final holdout.
        if valid_end_time >= final_holdout_start:
            break

        # Train ends purge_gap bars before the validation start. purge_gap must be
        # >= the label horizon so the last training sample's forward label lands
        # STRICTLY before valid_start (label_i references bar i+horizon; a sample
        # at valid_start - purge_gap reaches valid_start - purge_gap + horizon,
        # which is < valid_start only when purge_gap > horizon, i.e. strict).
        # The -1 makes the gap exclusive so target_idx == valid_start can't happen.
        train_end_idx = valid_start_idx - purge_gap_bars - 1
        if train_end_idx < 0:
            break
        train_end_time = ts[train_end_idx]

        folds.append(
            WalkForwardFold(
                train_start=start_time,
                train_end=train_end_time,
                valid_start=valid_start_time,
                valid_end=valid_end_time,
            )
        )
        valid_offset_days += step_days

    return WalkForwardSplits(
        folds=folds,
        final_holdout_start=final_holdout_start,
        final_holdout_end=end_time,
    )
