from __future__ import annotations

import pandas as pd


def _utc_timestamp(value: object) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    return ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")


def quality_summary(
    frame: pd.DataFrame,
    dataset: str,
    timeframe: str | None = None,
    start: object | None = None,
    end: object | None = None,
) -> dict[str, object]:
    expected = None
    if dataset == "ohlcv" and timeframe is not None and start is not None and end is not None:
        freq = pd.Timedelta(timeframe)
        expected = pd.date_range(
            _utc_timestamp(start),
            _utc_timestamp(end),
            freq=freq,
            inclusive="left",
        )

    if frame.empty:
        missing_per_day = {}
        expected_per_day = {}
        expected_count = 0
        if expected is not None:
            expected_count = int(len(expected))
            expected_per_day = {
                str(day): int(count)
                for day, count in pd.Series(expected.strftime("%Y-%m-%d")).value_counts(sort=False).items()
            }
            missing_per_day = expected_per_day
        summary = {
            "dataset": dataset,
            "rows": 0,
            "duplicate_timestamp_symbol": 0,
            "non_monotonic_timestamps": 0,
            "missing_timestamps": expected_count,
            "missing_timestamps_per_day": missing_per_day,
            "null_values": 0,
            "negative_volume": 0,
            "rows_per_day": {},
            "funding_rows_per_day": {},
            "start": None,
            "end": None,
        }
        if expected is not None:
            summary[f"expected_{timeframe}_bars"] = expected_count
            summary[f"expected_{timeframe}_bars_per_day"] = expected_per_day
        return summary

    clean = frame.copy()
    clean["timestamp"] = pd.to_datetime(clean["timestamp"], utc=True)
    duplicate_count = int(clean.duplicated(["timestamp", "symbol"]).sum())
    non_monotonic = int((clean["timestamp"].diff() < pd.Timedelta(0)).sum())
    null_values = int(clean.isna().sum().sum())
    negative_volume = int((clean.get("volume", pd.Series(dtype=float)) < 0).sum())
    rows_per_day = {
        str(day): int(count)
        for day, count in clean.groupby(clean["timestamp"].dt.strftime("%Y-%m-%d")).size().items()
    }

    missing = 0
    missing_per_day = {}
    expected_count = 0
    expected_per_day = {}
    if dataset == "ohlcv" and timeframe is not None:
        freq = pd.Timedelta(timeframe)
        if expected is None:
            expected = pd.date_range(
                clean["timestamp"].min(),
                clean["timestamp"].max(),
                freq=freq,
            )
        actual = pd.DatetimeIndex(clean["timestamp"].drop_duplicates().sort_values())
        missing_index = expected.difference(actual)
        expected_count = int(len(expected))
        expected_per_day = {
            str(day): int(count)
            for day, count in pd.Series(expected.strftime("%Y-%m-%d")).value_counts(sort=False).items()
        }
        missing = int(len(missing_index))
        missing_per_day = {
            str(day): int(count)
            for day, count in pd.Series(missing_index.strftime("%Y-%m-%d")).value_counts(sort=False).items()
        }

    summary = {
        "dataset": dataset,
        "rows": int(len(clean)),
        "duplicate_timestamp_symbol": duplicate_count,
        "non_monotonic_timestamps": non_monotonic,
        "missing_timestamps": missing,
        "missing_timestamps_per_day": missing_per_day,
        "null_values": null_values,
        "negative_volume": negative_volume,
        "rows_per_day": rows_per_day,
        "funding_rows_per_day": rows_per_day if dataset == "funding_rates" else {},
        "start": clean["timestamp"].min().isoformat(),
        "end": clean["timestamp"].max().isoformat(),
    }
    if dataset == "ohlcv" and timeframe is not None:
        summary[f"expected_{timeframe}_bars"] = expected_count
        summary[f"expected_{timeframe}_bars_per_day"] = expected_per_day
    return summary
