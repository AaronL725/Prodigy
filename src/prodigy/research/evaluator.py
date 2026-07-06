from __future__ import annotations

import pandas as pd


def forward_returns(prices: pd.DataFrame, periods: int) -> pd.DataFrame:
    frame = prices.sort_values(["symbol", "timestamp"]).copy()
    frame["forward_return"] = (
        frame.groupby("symbol")["close"].shift(-periods) / frame["close"] - 1.0
    )
    return frame[["timestamp", "symbol", "forward_return"]]


def rank_ic_by_timestamp(factors: pd.DataFrame, returns: pd.DataFrame) -> pd.Series:
    merged = factors.merge(returns, on=["timestamp", "symbol"], how="inner")
    merged = merged.dropna(subset=["value", "forward_return"])
    if merged.empty:
        return pd.Series(dtype="float64", name="rank_ic")
    result = merged.groupby("timestamp").apply(
        lambda g: g["value"].corr(g["forward_return"], method="spearman"),
        include_groups=False,
    )
    result.name = "rank_ic"
    result.index.name = "timestamp"
    return result


def bucket_returns(
    factors: pd.DataFrame,
    returns: pd.DataFrame,
    buckets: int = 5,
) -> pd.DataFrame:
    merged = factors.merge(returns, on=["timestamp", "symbol"], how="inner")
    merged = merged.dropna(subset=["value", "forward_return"]).copy()
    if merged.empty:
        return pd.DataFrame(columns=["timestamp", "bucket", "mean_forward_return"])

    def assign_bucket(group: pd.DataFrame) -> pd.Series:
        ranked = group["value"].rank(method="first")
        return pd.qcut(ranked, q=min(buckets, len(group)), labels=False, duplicates="drop")

    merged["bucket"] = merged.groupby("timestamp", group_keys=False).apply(
        assign_bucket,
        include_groups=False,
    )
    grouped = (
        merged.groupby(["timestamp", "bucket"], as_index=False)["forward_return"]
        .mean()
        .rename(columns={"forward_return": "mean_forward_return"})
    )
    return grouped
