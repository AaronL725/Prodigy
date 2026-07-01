from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True)
class SignalParams:
    total_notional_cap: float
    open_threshold: float = 0.6
    close_threshold: float = 0.2
    add_cooldown_bars: int = 4
    # spec defaults: abs(score)=0.6 -> 5% of cap, abs(score)=1.0 -> 10%.
    min_size_fraction: float = 0.05
    max_size_fraction: float = 0.10


def _notional(score: float, params: SignalParams) -> float:
    # Linear map from min_size_fraction (at open_threshold) to max_size_fraction
    # (at |score|=1.0). Configurable so backtests can sweep position sizing.
    mag = min(max(abs(score), params.open_threshold), 1.0)
    span = 1.0 - params.open_threshold
    fraction = params.min_size_fraction + (
        params.max_size_fraction - params.min_size_fraction
    ) * ((mag - params.open_threshold) / span if span else 0.0)
    return params.total_notional_cap * fraction


# ponytail: single-symbol milestone; plain row loop, dict tracking open lots.
def score_to_lot_signals(scores: pd.DataFrame, params: SignalParams) -> pd.DataFrame:
    rows: list[dict] = []
    lot_counter = 0
    open_lots: dict[str, dict] = {}  # lot_id -> lot state (side, notional, ...)
    last_open: dict[str, dict] = {}  # direction -> {"bar": idx, "score": float}
    open_notional = 0.0  # sum of notional across currently-open lots

    for bar, frame in scores.reset_index(drop=True).iterrows():
        symbol = frame["symbol"]
        score = float(frame["score"])
        ts = frame["timestamp"]

        # Opposite close: an open LONG closes when score <= -close_threshold;
        # an open SHORT closes when score >= +close_threshold.
        to_close = [
            lot_id
            for lot_id, lot in open_lots.items()
            if (lot["side"] == "long" and score <= -params.close_threshold)
            or (lot["side"] == "short" and score >= params.close_threshold)
        ]
        for lot_id in to_close:
            lot = open_lots.pop(lot_id)
            open_notional -= lot["notional"]
            rows.append(
                {
                    "timestamp": ts,
                    "symbol": symbol,
                    "action": "close",
                    "side": lot["side"],
                    "score": score,
                    "notional": lot["notional"],
                    "lot_id": lot_id,
                    "reason": "close_opposite",
                }
            )

        # Open when |score| >= open_threshold.
        if abs(score) >= params.open_threshold:
            side = "long" if score > 0 else "short"
            prev = last_open.get(side)
            # Same-direction add allowed after cooldown, or when the signal is
            # strictly stronger than the last opened signal (immediate reinforcement).
            cooldown_ok = (
                prev is None
                or (bar - prev["bar"]) >= params.add_cooldown_bars
                or abs(score) > abs(prev["score"])
            )
            notional = _notional(score, params) if cooldown_ok else 0.0
            # spec: "Total notional cannot exceed total_notional_cap".
            if cooldown_ok and open_notional + notional <= params.total_notional_cap + 1e-9:
                lot_counter += 1
                lot_id = f"lot-{lot_counter:06d}"
                open_lots[lot_id] = {"side": side, "notional": notional}
                open_notional += notional
                last_open[side] = {"bar": bar, "score": score}
                rows.append(
                    {
                        "timestamp": ts,
                        "symbol": symbol,
                        "action": "open",
                        "side": side,
                        "score": score,
                        "notional": notional,
                        "lot_id": lot_id,
                        "reason": "open_threshold",
                    }
                )

    return pd.DataFrame(
        rows,
        columns=[
            "timestamp",
            "symbol",
            "action",
            "side",
            "score",
            "notional",
            "lot_id",
            "reason",
        ],
    )
