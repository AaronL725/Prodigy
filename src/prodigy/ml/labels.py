from __future__ import annotations

import pandas as pd

# ponytail: this milestone's horizons only; extend the map for new horizons.
_HORIZON_BARS = {
    "15m": 1,
    "1h": 4,
    "4h": 16,
    "24h": 96,
}


def horizon_to_bars(horizon: str) -> int:
    return _HORIZON_BARS[horizon]


def add_forward_return_labels(
    frame: pd.DataFrame, horizons: list[str]
) -> pd.DataFrame:
    # Sort per symbol by timestamp so shift(-bars) is a true forward return even
    # when the caller passes an unordered frame (research frames aren't always
    # pre-sorted). Reset index so groupby/shift align cleanly.
    labeled = frame.sort_values(["symbol", "timestamp"]).reset_index(drop=True)
    for horizon in horizons:
        bars = horizon_to_bars(horizon)
        forward = labeled.groupby("symbol")["close"].shift(-bars)
        labeled[f"target_{horizon}"] = forward / labeled["close"] - 1
    return labeled
