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
    labeled = frame.copy()
    close_by_symbol = labeled.groupby("symbol")["close"]
    for horizon in horizons:
        bars = horizon_to_bars(horizon)
        labeled[f"target_{horizon}"] = close_by_symbol.shift(-bars) / labeled["close"] - 1
    return labeled
