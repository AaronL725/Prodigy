"""Bar-level lot simulator for single-symbol research backtests.

The simulator consumes OHLCV prices, funding rates, and the lot-level signal
events produced by :func:`prodigy.research.signals.score_to_lot_signals`, then
walks each price bar, opening/closing lots with fees, slippage, funding cost,
stop-loss, trailing take-profit, and a 24h holding review.

This is deliberately stateful: a plain loop over bars with a list of open lots
is the right shape. No state-machine framework, no speculative abstraction.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field

import numpy as np
import pandas as pd

from prodigy.research.signals import score_confirms_side

BARS_PER_HOUR = 4  # 15m bars


@dataclass
class BacktestParams:
    """Configurable fee, slippage, holding, stop, trailing, and equity settings.

    Defaults match the first-milestone config (``configs/default.toml``):
    maker 0.0002, taker 0.0006, rebate 0.59.
    """

    initial_equity: float = 1000.0
    maker_rate: float = 0.0002
    taker_rate: float = 0.0006
    rebate_fraction: float = 0.59
    slippage: float = 0.0
    stop_loss_position_notional_fraction: float = 0.08
    trailing_start_position_notional_fraction: float = 0.10
    max_holding_hours: int = 24
    extension_hours: int = 1
    atr_window: int = 14
    swing_lookback_bars: int = 5
    atr_multiplier: float = 1.5
    add_cooldown_bars: int = 4


@dataclass
class BacktestResult:
    """Container for simulator output: closed trades, equity curve, summary."""

    trades: pd.DataFrame
    equity_curve: pd.DataFrame
    summary: dict = field(default_factory=dict)


def _atr(prices: pd.DataFrame, window: int) -> pd.Series:
    """Per-bar Average True Range as a rolling mean of true range.

    True range uses high/low/close. Returns a Series aligned to prices rows;
    values are NaN for the first ``window-1`` bars where the window is not full.
    """
    high = prices["high"].to_numpy(dtype=float)
    low = prices["low"].to_numpy(dtype=float)
    close = prices["close"].to_numpy(dtype=float)
    prev_close = np.concatenate(([close[0]], close[:-1]))
    tr = np.maximum.reduce(
        [
            high - low,
            np.abs(high - prev_close),
            np.abs(low - prev_close),
        ]
    )
    tr_series = pd.Series(tr, index=prices.index)
    return tr_series.rolling(window=window, min_periods=window).mean()


def _signal_lookup(signals: pd.DataFrame) -> dict[tuple, list[dict]]:
    """Index signals by (timestamp, symbol) for O(1) per-bar lookup.

    A bar may carry multiple signals (e.g. a close then an open). Returns them
    in original row order so the simulator can process closes before opens.
    """
    lookup: dict[tuple, list[dict]] = {}
    if signals.empty:
        return lookup
    for key, frame in signals.groupby(
        [signals["timestamp"], signals["symbol"]], sort=False
    ):
        lookup[key] = frame.to_dict("records")
    return lookup


def _raw_score_lookup(signals: pd.DataFrame) -> dict[tuple, float]:
    scores = signals.attrs.get("raw_scores")
    if not isinstance(scores, pd.DataFrame) or scores.empty:
        return {}
    if not {"timestamp", "symbol", "score"}.issubset(scores.columns):
        return {}
    return scores.set_index(["timestamp", "symbol"])["score"].astype(float).to_dict()


def _net_fee(params: BacktestParams) -> float:
    # ponytail: taker-only fills; net fee = raw * (1 - rebate_fraction).
    return params.taker_rate * (1.0 - params.rebate_fraction)


def simulate_lots(
    prices: pd.DataFrame,
    funding: pd.DataFrame,
    signals: pd.DataFrame,
    params: BacktestParams,
) -> BacktestResult:
    """Walk price bars, simulating lot lifecycle and accumulating realized PnL.

    Open at current bar close +/- slippage (taker fee on entry); close at
    current bar close -/+ slippage (taker fee on exit). Net fee is
    ``raw_fee * (1 - rebate_fraction)``. See module docstring for the full
    rule set.
    """
    prices = prices.reset_index(drop=True)
    net_fee = _net_fee(params)
    max_holding_bars = params.max_holding_hours * BARS_PER_HOUR

    atr_series = _atr(prices, params.atr_window)
    sig_by_bar = _signal_lookup(signals)
    raw_score_by_bar = _raw_score_lookup(signals)
    review_threshold = float(signals.attrs.get("open_threshold", 0.6))
    has_funding = (
        funding is not None and not funding.empty and "funding_rate" in funding.columns
    )
    funding_df = (
        funding
        if has_funding
        else pd.DataFrame(columns=["timestamp", "symbol", "funding_rate"])
    )

    open_lots: list[dict] = []
    trades: list[dict] = []
    realized_pnl = 0.0
    equity_rows: list[dict] = []

    for i in range(len(prices)):
        bar = prices.iloc[i]
        symbol = bar["symbol"]
        ts = bar["timestamp"]
        close = float(bar["close"])
        high = float(bar["high"])
        low = float(bar["low"])
        key = (ts, symbol)

        # 1. Process signals at this bar: opposite-signal closes first, then opens.
        for signal in sig_by_bar.get(key, ()):
            if signal["action"] == "close":
                lot_id = signal["lot_id"]
                lot = next((lot for lot in open_lots if lot["lot_id"] == lot_id), None)
                if lot is not None:
                    _close_lot(lot, close, params.slippage, net_fee, ts, "opposite_signal", funding_df)
                    realized_pnl += lot["pnl"]
                    trades.append(_trade_row(lot))
                    open_lots.remove(lot)
            elif signal["action"] == "open":
                lot = _open_lot(signal, close, params.slippage, net_fee, ts, i)
                open_lots.append(lot)

        # 2. Evaluate open lots for exits (stop-loss / trailing / 24h holding).
        still_open: list[dict] = []
        for lot in open_lots:
            atr = float(atr_series.iloc[i])
            exited = _evaluate_exits(
                lot,
                prices,
                i,
                ts,
                close,
                high,
                low,
                atr,
                params,
                funding_df,
                net_fee,
                sig_by_bar,
                raw_score_by_bar,
                review_threshold,
                key,
                max_holding_bars,
            )
            if exited is not None:
                realized_pnl += lot["pnl"]
                trades.append(_trade_row(lot))
            else:
                still_open.append(lot)
        open_lots = still_open

        equity = params.initial_equity + realized_pnl
        equity_rows.append({"timestamp": ts, "symbol": symbol, "equity": equity})

    # Force-close any lots still open at the last bar (mark-to-market) so their
    # unrealized PnL is reflected in the final equity / drawdown instead of
    # silently dropped. Uses the last bar's close +/- slippage like any exit.
    unrealized_at_end = 0.0
    if open_lots and len(prices) > 0:
        last = prices.iloc[-1]
        last_close = float(last["close"])
        last_ts = last["timestamp"]
        for lot in open_lots:
            _close_lot(lot, last_close, params.slippage, net_fee, last_ts, "end_of_data", funding_df)
            realized_pnl += lot["pnl"]
            unrealized_at_end += lot["pnl"]
            trades.append(_trade_row(lot))
        open_lots = []
        # Rewrite the final equity row with the marked-to-market equity.
        equity_rows[-1]["equity"] = params.initial_equity + realized_pnl

    trades_df = pd.DataFrame(trades)
    equity_df = pd.DataFrame(equity_rows)
    final_equity = (
        float(equity_df["equity"].iloc[-1]) if not equity_df.empty else params.initial_equity
    )

    summary: dict[str, object] = {
        "final_equity": final_equity,
        "initial_equity": params.initial_equity,
        "num_trades": int(len(trades_df)),
        "realized_pnl": float(realized_pnl),
        "unrealized_pnl_at_end": float(unrealized_at_end),
        "return_pct": float((final_equity - params.initial_equity) / params.initial_equity)
        if params.initial_equity
        else 0.0,
    }

    return BacktestResult(trades=trades_df, equity_curve=equity_df, summary=summary)


def _open_lot(
    signal: Mapping[str, object],
    close: float,
    slippage: float,
    net_fee: float,
    ts: pd.Timestamp,
    bar_index: int,
) -> dict:
    """Open a lot at close +/- slippage with taker entry fee."""
    side = signal["side"]
    notional = float(signal["notional"])
    # ponytail: long pays close*(1+slippage); short receives close*(1-slippage).
    if side == "long":
        fill_price = close * (1.0 + slippage)
        sign = 1.0
    else:
        fill_price = close * (1.0 - slippage)
        sign = -1.0
    qty = notional / fill_price
    entry_fee = qty * fill_price * net_fee
    return {
        "lot_id": signal["lot_id"],
        "symbol": signal["symbol"],
        "side": side,
        "sign": sign,
        "qty": qty,
        "notional": notional,
        "entry_price": fill_price,
        "entry_ts": ts,
        "open_bar": bar_index,
        "entry_fee": entry_fee,
        "trailing_active": False,
        "trailing_stop": None,
        "extended": False,
        "exit_price": None,
        "exit_ts": None,
        "exit_fee": 0.0,
        "funding_cost": 0.0,
        "pnl": 0.0,
        "exit_reason": None,
    }


def _close_lot(
    lot: dict,
    close: float,
    slippage: float,
    net_fee: float,
    exit_ts: pd.Timestamp,
    reason: str,
    funding_df: pd.DataFrame,
) -> None:
    """Close a lot at close -/+ slippage with taker exit fee; compute realized PnL.

    Applies funding cost over the holding interval: long pays positive funding
    and receives negative; short is the mirror.
    """
    if lot["side"] == "long":
        exit_price = close * (1.0 - slippage)
    else:
        exit_price = close * (1.0 + slippage)
    gross = (exit_price - lot["entry_price"]) * lot["qty"] * lot["sign"]
    exit_fee = lot["qty"] * exit_price * net_fee
    funding_pnl = _funding_pnl(funding_df, lot["symbol"], lot["entry_ts"], exit_ts)
    # ponytail: long (sign +1) pays positive funding -> positive cost; short mirrors.
    funding_cost = lot["sign"] * funding_pnl * lot["notional"]
    lot["exit_price"] = exit_price
    lot["exit_ts"] = exit_ts
    lot["exit_fee"] = exit_fee
    lot["funding_cost"] = funding_cost
    lot["exit_reason"] = reason
    # ponytail: pnl = gross move - entry fee - exit fee - funding cost.
    lot["pnl"] = gross - lot["entry_fee"] - exit_fee - funding_cost


def _funding_pnl(
    funding: pd.DataFrame, symbol: str, entry_ts: pd.Timestamp, exit_ts: pd.Timestamp
) -> float:
    """Sum of funding rates whose timestamp falls inside [entry_ts, exit_ts].

    Returned as a raw rate sum; the caller sign-adjusts for long/short and
    scales by notional.
    """
    if funding.empty:
        return 0.0
    mask = (
        (funding["symbol"] == symbol)
        & (funding["timestamp"] >= entry_ts)
        & (funding["timestamp"] <= exit_ts)
    )
    subset = funding.loc[mask]
    if subset.empty:
        return 0.0
    return float(subset["funding_rate"].sum())


def _swing(prices: pd.DataFrame, i: int, lookback: int) -> tuple[float, float]:
    """Swing low / high over the trailing ``lookback`` bars including bar ``i``."""
    start = max(0, i - lookback + 1)
    window = prices.iloc[start : i + 1]
    return float(window["low"].min()), float(window["high"].max())


def _evaluate_exits(
    lot: dict,
    prices: pd.DataFrame,
    i: int,
    ts: pd.Timestamp,
    close: float,
    high: float,
    low: float,
    atr: float,
    params: BacktestParams,
    funding_df: pd.DataFrame,
    net_fee: float,
    sig_by_bar: dict,
    raw_score_by_bar: dict[tuple, float],
    review_threshold: float,
    key: tuple,
    max_holding_bars: int,
) -> dict | None:
    """Check stop-loss, trailing take-profit, and 24h holding review for a lot.

    Returns the lot if it was closed, else None. Stop-loss is checked against
    unrealized loss/notional; trailing activates once profit exceeds the start
    threshold then trails a swing +/- ATR buffer; 24h review extends by
    extension_hours only if a confirming same-direction score is present.
    """
    notional = lot["notional"]
    unrealized = (close - lot["entry_price"]) * lot["qty"] * lot["sign"] - lot["entry_fee"]

    # 1. Stop-loss: unrealized loss / notional >= threshold.
    if -unrealized / notional >= params.stop_loss_position_notional_fraction:
        _close_lot(lot, close, params.slippage, net_fee, ts, "stop_loss", funding_df)
        return lot

    # 2. Trailing take-profit.
    profit_fraction = unrealized / notional
    if not lot["trailing_active"] and profit_fraction >= params.trailing_start_position_notional_fraction:
        lot["trailing_active"] = True
    if lot["trailing_active"] and atr == atr:  # ponytail: guard ATR NaN.
        swing_low, swing_high = _swing(prices, i, params.swing_lookback_bars)
        if lot["side"] == "long":
            trail = swing_low - atr * params.atr_multiplier
            if lot["trailing_stop"] is None or trail > lot["trailing_stop"]:
                lot["trailing_stop"] = trail
            if close < lot["trailing_stop"]:
                _close_lot(lot, close, params.slippage, net_fee, ts, "trailing", funding_df)
                return lot
        else:
            trail = swing_high + atr * params.atr_multiplier
            if lot["trailing_stop"] is None or trail < lot["trailing_stop"]:
                lot["trailing_stop"] = trail
            if close > lot["trailing_stop"]:
                _close_lot(lot, close, params.slippage, net_fee, ts, "trailing", funding_df)
                return lot

    # 3. 24h holding review: extend by extension_hours only if a confirming
    # same-direction score is present at this bar; otherwise close.
    held_bars = i - lot["open_bar"]
    if held_bars >= max_holding_bars:
        raw_score = raw_score_by_bar.get(key)
        confirming = (
            score_confirms_side(raw_score, lot["side"], review_threshold)
            if raw_score is not None
            else False
        )
        if raw_score is None:
            for signal in sig_by_bar.get(key, ()):
                if signal["action"] == "open":
                    score = float(signal["score"])
                    if score_confirms_side(score, lot["side"], review_threshold):
                        confirming = True
                        break
        if confirming:
            lot["extended"] = True
            # ponytail: extend the review deadline by extension_hours (1h = 4 bars
            # @15m), NOT another full max_holding window. Shifting open_bar forward
            # so the next review lands at i + extension_hours*BARS_PER_HOUR.
            lot["open_bar"] = i - max_holding_bars + params.extension_hours * BARS_PER_HOUR
        else:
            _close_lot(lot, close, params.slippage, net_fee, ts, "holding", funding_df)
            return lot

    return None


def _trade_row(lot: dict) -> dict:
    """Flatten a closed lot into a trades-DataFrame row."""
    return {
        "lot_id": lot["lot_id"],
        "symbol": lot["symbol"],
        "side": lot["side"],
        "notional": lot["notional"],
        "qty": lot["qty"],
        "entry_price": lot["entry_price"],
        "entry_ts": lot["entry_ts"],
        "exit_price": lot["exit_price"],
        "exit_ts": lot["exit_ts"],
        "entry_fee": lot["entry_fee"],
        "exit_fee": lot["exit_fee"],
        "funding_cost": lot["funding_cost"],
        "pnl": lot["pnl"],
        "exit_reason": lot["exit_reason"],
    }
