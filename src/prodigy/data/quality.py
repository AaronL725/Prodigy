from __future__ import annotations

import pandas as pd


def quality_summary(
    frame: pd.DataFrame,
    dataset: str,
    timeframe: str | None = None,
) -> dict[str, int | str | None]:
    if frame.empty:
        return {
            "dataset": dataset,
            "rows": 0,
            "duplicate_timestamp_symbol": 0,
            "missing_timestamps": 0,
            "null_values": 0,
            "negative_volume": 0,
            "start": None,
            "end": None,
        }

    clean = frame.copy()
    clean["timestamp"] = pd.to_datetime(clean["timestamp"], utc=True)
    duplicate_count = int(clean.duplicated(["timestamp", "symbol"]).sum())
    null_values = int(clean.isna().sum().sum())
    negative_volume = int((clean.get("volume", pd.Series(dtype=float)) < 0).sum())

    missing = 0
    if dataset == "ohlcv" and timeframe is not None:
        freq = pd.Timedelta(timeframe)
        expected = pd.date_range(
            clean["timestamp"].min(),
            clean["timestamp"].max(),
            freq=freq,
        )
        actual = pd.DatetimeIndex(clean["timestamp"].drop_duplicates().sort_values())
        missing = int(len(expected.difference(actual)))

    return {
        "dataset": dataset,
        "rows": int(len(clean)),
        "duplicate_timestamp_symbol": duplicate_count,
        "missing_timestamps": missing,
        "null_values": null_values,
        "negative_volume": negative_volume,
        "start": clean["timestamp"].min().isoformat(),
        "end": clean["timestamp"].max().isoformat(),
    }
