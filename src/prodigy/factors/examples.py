from __future__ import annotations

import pandas as pd

from prodigy.factors.base import factor_frame


def momentum_15m(frame: pd.DataFrame, periods: int = 4) -> pd.DataFrame:
    values = frame.groupby("symbol", group_keys=False)["close"].pct_change(periods)
    return factor_frame(frame, "momentum_15m", values)


def funding_zscore(frame: pd.DataFrame, window: int = 20) -> pd.DataFrame:
    grouped = frame.groupby("symbol", group_keys=False)["funding_rate"]
    mean = grouped.transform(lambda s: s.rolling(window).mean())
    std = grouped.transform(lambda s: s.rolling(window).std())
    values = (frame["funding_rate"] - mean) / std.replace(0, pd.NA)
    return factor_frame(frame, "funding_zscore", values)


def oi_change(frame: pd.DataFrame, periods: int = 4) -> pd.DataFrame:
    values = frame.groupby("symbol", group_keys=False)["open_interest"].pct_change(periods)
    return factor_frame(frame, "oi_change", values)
