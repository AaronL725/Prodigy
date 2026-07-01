from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from prodigy.research.evaluator import (
    bucket_returns,
    forward_returns,
    rank_ic_by_timestamp,
)


@dataclass
class Backtester:
    prices: pd.DataFrame
    factors: pd.DataFrame

    def factor_distribution(self) -> dict[str, float]:
        values = self.factors["value"].dropna()
        return {
            "count": int(values.count()),
            "mean": float(values.mean()),
            "std": float(values.std()) if len(values) > 1 else 0.0,
            "min": float(values.min()),
            "max": float(values.max()),
        }

    def autocorrelation(self) -> float:
        # ponytail: Series.autocorr(1) is the native lag-1 Pearson corr the
        # hand-rolled s.corr(s.shift(1)) computed; NaN (constant/single-point
        # series) drops out uniformly via dropna, matching prior behavior.
        corr = (
            self.factors.sort_values(["symbol", "timestamp"])
            .groupby("symbol")["value"]
            .apply(lambda s: s.autocorr(1))
            .dropna()
        )
        return float(corr.mean()) if not corr.empty else 0.0

    def ic_summary(self, horizon: int, returns: pd.DataFrame | None = None) -> dict[str, float]:
        returns = returns if returns is not None else forward_returns(self.prices, periods=horizon)
        ic = rank_ic_by_timestamp(self.factors, returns).dropna()
        if ic.empty:
            return {"mean": 0.0, "std": 0.0, "icir": 0.0}
        std = float(ic.std()) if len(ic) > 1 else 0.0
        return {
            "mean": float(ic.mean()),
            "std": std,
            "icir": float(ic.mean() / std) if std else 0.0,
        }

    def bucket_returns(
        self, horizon: int, buckets: int, returns: pd.DataFrame | None = None
    ) -> pd.DataFrame:
        returns = returns if returns is not None else forward_returns(self.prices, periods=horizon)
        return bucket_returns(self.factors, returns, buckets=buckets)

    def performance_summary(
        self, horizon: int, returns: pd.DataFrame | None = None
    ) -> dict[str, float]:
        returns = returns if returns is not None else forward_returns(self.prices, periods=horizon)
        merged = self.factors.merge(returns, on=["timestamp", "symbol"], how="inner")
        merged = merged.dropna(subset=["value", "forward_return"])
        if merged.empty:
            return {"mean_forward_return": 0.0, "observations": 0}
        signed = merged["value"].apply(lambda x: 1 if x > 0 else -1 if x < 0 else 0)
        strategy_return = signed * merged["forward_return"]
        return {
            "mean_forward_return": float(strategy_return.mean()),
            "observations": int(strategy_return.count()),
        }

    def run_full_report(self, horizon: int = 4, buckets: int = 5) -> dict[str, object]:
        # ponytail: forward_returns is the same for the three horizon-bound
        # methods; compute once, thread it through so run_full_report doesn't
        # rebuild the same frame three times. Public signatures are unchanged.
        returns = forward_returns(self.prices, periods=horizon)
        return {
            "distribution": self.factor_distribution(),
            "autocorrelation": self.autocorrelation(),
            "ic_summary": self.ic_summary(horizon, returns),
            "bucket_returns": self.bucket_returns(horizon, buckets, returns),
            "performance": self.performance_summary(horizon, returns),
        }
