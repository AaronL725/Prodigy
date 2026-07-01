from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import pandas as pd

from prodigy.research.evaluator import (
    bucket_returns,
    forward_returns,
    rank_ic_by_timestamp,
)
from prodigy.research.simulator import BacktestParams, simulate_lots

if TYPE_CHECKING:
    from matplotlib.figure import Figure


@dataclass
class Backtester:
    prices: pd.DataFrame
    factors: pd.DataFrame
    funding: pd.DataFrame | None = None
    signals: pd.DataFrame | None = None
    params: BacktestParams | None = None

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

    # ------------------------------------------------------------------
    # Notebook-style facade (capitalized, factor_liang-style). These delegate
    # to the lower-case methods above and the simulator. They never call
    # plt.show() so they are safe under headless test/CI. Plot methods return
    # the computed value or a matplotlib Figure (rendered lazily by the
    # caller); non-plot methods return Series/dict/DataFrame.
    # ------------------------------------------------------------------

    def PlotAutocorrelation(self) -> float:
        return self.autocorrelation()

    def PlotRankIcCumsum(self, horizon: int = 4) -> pd.Series:
        # ponytail: cumsum of per-timestamp rank IC over a forward-return horizon.
        returns = forward_returns(self.prices, periods=horizon)
        ic = rank_ic_by_timestamp(self.factors, returns).dropna()
        return ic.cumsum()

    def PlotSignalDistribution(self) -> pd.Series:
        # ponytail: use lot signal scores when present, else fall back to factor values.
        if self.signals is not None and not self.signals.empty and "score" in self.signals.columns:
            values = self.signals["score"]
        else:
            values = self.factors["value"]
        return values.dropna().describe()

    def PlotEquityCurve(self) -> pd.DataFrame:
        return self._simulate().equity_curve

    def PlotDrawdown(self) -> pd.Series:
        equity = self._simulate().equity_curve["equity"]
        running_max = equity.cummax()
        return equity / running_max - 1.0

    def GetPerformanceSummary(self) -> dict[str, object]:
        return self._simulate().summary

    def GetTradeSummary(self) -> pd.DataFrame:
        return self._simulate().trades

    def _simulate(self):
        # ponytail: lazy default params so the simulator is usable without the
        # caller constructing a BacktestParams explicitly.
        params = self.params if self.params is not None else BacktestParams()
        funding = self.funding if self.funding is not None else pd.DataFrame()
        signals = self.signals if self.signals is not None else pd.DataFrame()
        return simulate_lots(self.prices, funding, signals, params)
