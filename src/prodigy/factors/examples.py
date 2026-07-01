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


def example_momentum_factor(ohlcv: pd.DataFrame, lookback_bars: int = 4) -> pd.DataFrame:
    momentum = ohlcv.groupby("symbol", group_keys=False)["close"].pct_change(lookback_bars)
    values = momentum.clip(-1, 1)
    return factor_frame(ohlcv, "example_momentum", values)


def example_funding_factor(funding: pd.DataFrame, window: int = 20) -> pd.DataFrame:
    grouped = funding.groupby("symbol", group_keys=False)["funding_rate"]
    mean = grouped.transform(lambda s: s.rolling(window).mean())
    std = grouped.transform(lambda s: s.rolling(window).std())
    zscore = (funding["funding_rate"] - mean) / std.replace(0, pd.NA)
    values = (-zscore).clip(-1, 1)
    return factor_frame(funding, "example_funding", values)


def example_volatility_factor(ohlcv: pd.DataFrame, atr_window: int = 14) -> pd.DataFrame:
    prev_close = ohlcv.groupby("symbol", group_keys=False)["close"].shift(1)
    true_range = pd.concat(
        [
            ohlcv["high"] - ohlcv["low"],
            (ohlcv["high"] - prev_close).abs(),
            (ohlcv["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    atr = true_range.groupby(ohlcv["symbol"]).transform(
        lambda s: s.rolling(atr_window).mean()
    )
    momentum = ohlcv.groupby("symbol", group_keys=False)["close"].pct_change()
    values = (momentum / atr.replace(0, pd.NA)).clip(-1, 1)
    return factor_frame(ohlcv, "example_volatility", values)
