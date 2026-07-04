from __future__ import annotations

import re
import sqlite3
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import pandas as pd

from prodigy.db import connect, init_db
from prodigy.signals.intents import TradeIntent, insert_trade_intent
from prodigy.signals.state import (
    get_executor_state,
    has_unfinished_system_order,
    has_unresolved_intent,
    is_manual_override_active,
    set_executor_state,
    signal_processed_key,
)


def _floor_alias(timeframe: str) -> str:
    # ponytail: pandas 3.x rejects "15m" for .floor() (wants "min"); normalize once here.
    return re.sub(r"(\d+)m$", r"\1min", timeframe)


@dataclass(frozen=True)
class SignalDaemonConfig:
    total_notional_cap: float
    entry_threshold: float = 0.6
    exit_threshold: float = 0.2
    min_order_fraction: float = 0.05
    max_order_fraction: float = 0.10
    max_holding_bars: int = 96
    profit_hold_score_threshold: float = 0.2
    loss_hold_score_threshold: float = 0.4


@dataclass(frozen=True)
class PositionState:
    side: str
    unrealized_pnl: float


@dataclass(frozen=True)
class SignalDecision:
    action: str
    side: str
    target_notional: float
    reason: str


def latest_closed_bar(frame: pd.DataFrame, now: pd.Timestamp, timeframe: str) -> pd.Series:
    if frame.empty:
        raise ValueError("no OHLCV rows available")
    now = pd.Timestamp(now).tz_convert("UTC") if pd.Timestamp(now).tzinfo else pd.Timestamp(now, tz="UTC")
    alias = _floor_alias(timeframe)
    cutoff = now.floor(alias) - pd.Timedelta(timeframe)
    closed = frame[pd.to_datetime(frame["timestamp"], utc=True) <= cutoff]
    if closed.empty:
        raise ValueError("no closed bar available")
    return closed.sort_values("timestamp").iloc[-1]


def combine_example_score(row: pd.Series) -> float:
    cols = ["example_momentum", "example_funding", "example_volatility"]
    values = [float(row[c]) for c in cols if c in row and pd.notna(row[c])]
    if not values:
        return 0.0
    return max(min(sum(values) / len(values), 1.0), -1.0)


def _notional(score: float, cfg: SignalDaemonConfig) -> float:
    mag = min(max(abs(score), cfg.entry_threshold), 1.0)
    span = 1.0 - cfg.entry_threshold
    fraction = cfg.min_order_fraction + (
        cfg.max_order_fraction - cfg.min_order_fraction
    ) * ((mag - cfg.entry_threshold) / span if span else 0.0)
    # ponytail: round to 8dp so a clean money value (e.g. 750.0) survives FP noise like 750.0000000000001.
    return round(cfg.total_notional_cap * fraction, 8)


def decide_intent(
    score: float,
    position: PositionState | None,
    holding_bars: int,
    cfg: SignalDaemonConfig,
) -> SignalDecision | None:
    if position is not None:
        if position.side == "long" and score <= -cfg.exit_threshold:
            return SignalDecision("close", "long", 0.0, "close_opposite")
        if position.side == "short" and score >= cfg.exit_threshold:
            return SignalDecision("close", "short", 0.0, "close_opposite")
        if holding_bars >= cfg.max_holding_bars:
            threshold = (
                cfg.profit_hold_score_threshold
                if position.unrealized_pnl >= 0
                else cfg.loss_hold_score_threshold
            )
            if abs(score) < threshold:
                reason = "holding_expiry_profit" if position.unrealized_pnl >= 0 else "holding_expiry_loss"
                return SignalDecision("close", position.side, 0.0, reason)
        return None

    if abs(score) >= cfg.entry_threshold:
        side = "long" if score > 0 else "short"
        return SignalDecision("open", side, _notional(score, cfg), "open_threshold")

    return None


def process_decision(
    conn: sqlite3.Connection,
    decision: SignalDecision,
    processed_key: str,
    created_at: str,
    symbol: str,
    source: str,
    model_version: str,
) -> None:
    outcome = "open_intent_written" if decision.action == "open" else "close_intent_written"
    intent = TradeIntent(
        intent_id=f"{source}-{symbol}-{uuid.uuid4().hex[:12]}",
        created_at=created_at,
        symbol=symbol,
        side=decision.side,
        action=decision.action,
        target_notional=decision.target_notional,
        max_order_notional=decision.target_notional if decision.action == "open" else 0.0,
        source=source,
        reason=decision.reason,
        model_version=model_version,
    )
    # ponytail: `with conn` commits on success, rolls back on exception — so the
    # intent insert and the signal_processed marker are atomic; neither persists
    # alone. Move either statement outside this block and a crash between them
    # can double-fire orders on the next cycle (idempotency relies on the marker).
    with conn:
        insert_trade_intent(conn, intent)
        set_executor_state(conn, processed_key, outcome, created_at)


@dataclass(frozen=True)
class RunOnceConfig:
    db_path: str | Path
    data_root: str | Path
    research_symbol: str
    exchange_symbol: str
    source: str
    now: pd.Timestamp
    refresh_data: Callable[[], None]
    score_loader: Callable[[], float]
    signal_cfg: SignalDaemonConfig = SignalDaemonConfig(total_notional_cap=10_000)
    max_state_age_secs: int = 120
    timeframe: str = "15m"


def _latest_equity_snapshot_age_secs(conn: sqlite3.Connection, now: pd.Timestamp) -> float | None:
    row = conn.execute(
        "select created_at from equity_snapshots order by created_at desc limit 1"
    ).fetchone()
    if row is None:
        return None
    created = pd.Timestamp(row["created_at"])
    created = created.tz_localize("UTC") if created.tzinfo is None else created.tz_convert("UTC")
    now = pd.Timestamp(now)
    now = now.tz_localize("UTC") if now.tzinfo is None else now.tz_convert("UTC")
    return (now - created).total_seconds()


def _position_state(conn: sqlite3.Connection, symbol: str) -> PositionState | None:
    row = conn.execute(
        "select side, unrealized_pnl from positions where symbol = ?",
        (symbol,),
    ).fetchone()
    if row is None:
        return None
    return PositionState(side=str(row["side"]), unrealized_pnl=float(row["unrealized_pnl"]))


def run_once(cfg: RunOnceConfig) -> str:
    now = pd.Timestamp(cfg.now)
    now = now.tz_localize("UTC") if now.tzinfo is None else now.tz_convert("UTC")
    closed_ts = (
        now.floor(_floor_alias(cfg.timeframe)) - pd.Timedelta(cfg.timeframe)
    ).isoformat().replace("+00:00", "Z")
    key = signal_processed_key(cfg.source, cfg.exchange_symbol, cfg.timeframe, closed_ts)

    with connect(cfg.db_path) as conn:
        init_db(conn)
        if get_executor_state(conn, key) is not None:
            return "already_processed"
        age = _latest_equity_snapshot_age_secs(conn, now)
        if age is None or age > cfg.max_state_age_secs:
            return "skipped_stale_state"
        if is_manual_override_active(conn, cfg.exchange_symbol):
            return "skipped_manual_override"
        if has_unresolved_intent(conn, cfg.exchange_symbol):
            return "skipped_pending_intent"
        if has_unfinished_system_order(conn, cfg.exchange_symbol):
            return "skipped_pending_order"

    cfg.refresh_data()
    score = cfg.score_loader()

    with connect(cfg.db_path) as conn:
        init_db(conn)
        position = _position_state(conn, cfg.exchange_symbol)
        decision = decide_intent(score, position, holding_bars=0, cfg=cfg.signal_cfg)
        if decision is None:
            set_executor_state(conn, key, "no_signal", now.isoformat())
            conn.commit()
            return "no_signal"
        process_decision(
            conn=conn,
            decision=decision,
            processed_key=key,
            created_at=now.isoformat().replace("+00:00", "Z"),
            symbol=cfg.exchange_symbol,
            source=cfg.source,
            model_version=cfg.source,
        )
        return "open_intent_written" if decision.action == "open" else "close_intent_written"
