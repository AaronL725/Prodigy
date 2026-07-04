from __future__ import annotations

import re
import sqlite3
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import pandas as pd

from prodigy.data.parquet_store import load_funding_rates, load_ohlcv
from prodigy.db import connect, init_db
from prodigy.factors.examples import (
    example_funding_factor,
    example_momentum_factor,
    example_volatility_factor,
)
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


def expected_closed_bar_ts(now: pd.Timestamp, timeframe: str) -> str:
    """The fully-closed bar timestamp for `now`, as an ISO-8601 'Z' string.

    Used both by run_once (to key idempotency) and by callers without a data
    layer (dummy-cycle) so they report the same expected bar the daemon will
    compare against.
    """
    now = pd.Timestamp(now)
    now = now.tz_localize("UTC") if now.tzinfo is None else now.tz_convert("UTC")
    closed = now.floor(_floor_alias(timeframe)) - pd.Timedelta(timeframe)
    return closed.isoformat().replace("+00:00", "Z")


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
    opened_at: str | None = None


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
    # score_loader returns (score, actual_closed_bar_ts). The closed-bar ts is
    # the bar the score was actually computed on; run_once compares it to the
    # expected closed bar to reject stale data.
    score_loader: Callable[[], tuple[float, str]]
    signal_cfg: SignalDaemonConfig = SignalDaemonConfig(total_notional_cap=10_000)
    max_state_age_secs: int = 120
    timeframe: str = "15m"


def write_signal_event(conn: sqlite3.Connection, severity: str, message: str) -> None:
    # ponytail: spec error handling requires writing a SQLite event on
    # refresh/factor errors and on shutdown. Inline insert keeps the daemon
    # dependency-free; events is the shared Rust/Python observability table.
    conn.execute(
        """
        insert into events (event_id, created_at, severity, component, message, payload_json)
        values (?, ?, ?, 'signal', ?, '{}')
        """,
        (
            f"signal-{uuid.uuid4().hex[:16]}",
            pd.Timestamp.now(tz="UTC").isoformat().replace("+00:00", "Z"),
            severity,
            message,
        ),
    )


def try_write_signal_event(db_path: str | Path, severity: str, message: str) -> None:
    # Best-effort event write on its OWN connection: opens, writes, commits, and
    # swallows any failure (DB busy/locked/disk-full). The daemon's skip decision
    # must NOT be undone by a logging failure — a transient SQLite lock here can't
    # crash the daemon (which would defeat the spec's "write event, skip" rule).
    # Logged to stderr so a chronic logging failure is still observable.
    try:
        with connect(db_path) as conn:
            init_db(conn)
            write_signal_event(conn, severity, message)
            conn.commit()
    except Exception as exc:  # noqa: BLE001 — best-effort logging must not raise
        import sys

        print(f"prodigy-signal: event write failed ({severity}): {exc}", file=sys.stderr)


def _normalize_closed_ts(value: str, timeframe: str) -> str:
    # Compare apples to apples: run_once builds closed_ts as an ISO "Z" string
    # floored to the timeframe boundary. The score loader reports whatever ts
    # the parquet bar carries (tz-aware). Normalize both to the same floored
    # "Z" form so a comparison isn't defeated by "+00:00" vs "Z" or sub-bar
    # noise. Falls back to the raw string if parsing fails (the comparison then
    # fails and the bar is skipped, the safe outcome).
    parsed = _parse_utc(value)
    if parsed is None:
        return str(value)
    floored = parsed.floor(_floor_alias(timeframe))
    return floored.isoformat().replace("+00:00", "Z")


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
        "select side, unrealized_pnl, opened_at from positions where symbol = ?",
        (symbol,),
    ).fetchone()
    if row is None:
        return None
    return PositionState(
        side=str(row["side"]),
        unrealized_pnl=float(row["unrealized_pnl"]),
        opened_at=row["opened_at"],
    )


def _parse_utc(value: str | pd.Timestamp) -> pd.Timestamp | None:
    try:
        ts = pd.Timestamp(value)
    except (ValueError, TypeError):
        return None
    if ts.tzinfo is None:
        return ts.tz_localize("UTC")
    return ts.tz_convert("UTC")


def _holding_bars(opened_at: str | None, closed_ts: str, timeframe: str) -> int:
    # ponytail: spec says skip (no guess) when opened_at can't be read — return 0
    # so decide_intent's expiry branch never fires for that position.
    if opened_at is None:
        return 0
    opened = _parse_utc(opened_at)
    closed = _parse_utc(closed_ts)
    if opened is None or closed is None:
        return 0
    return max(0, int((closed - opened) / pd.Timedelta(timeframe)))


def run_once(cfg: RunOnceConfig) -> str:
    now = pd.Timestamp(cfg.now)
    now = now.tz_localize("UTC") if now.tzinfo is None else now.tz_convert("UTC")
    closed_ts = expected_closed_bar_ts(now, cfg.timeframe)
    key = signal_processed_key(cfg.source, cfg.exchange_symbol, cfg.timeframe, closed_ts)

def _gate_skip(
    conn: sqlite3.Connection,
    cfg: RunOnceConfig,
    key: str,
    now: pd.Timestamp,
    *,
    check_already_processed: bool,
) -> str | None:
    """Return the first skip reason the current SQLite state triggers, else None.

    Gate order: already_processed → stale_state → manual_override →
    pending_intent → pending_order. Called TWICE in run_once — before refresh
    (cheap pre-check, releases the handle so refresh/score run with no DB lock)
    and again on the FINAL write connection, because refresh/score can take
    seconds and a manual_override, pending intent, unfinished order, or stale
    state appearing during that window must still block the write (TOCTOU guard
    for the M5 skip rules). `check_already_processed` lets the second call reuse
    the same gate without re-asserting idempotency the caller already owns.
    """
    if check_already_processed and get_executor_state(conn, key) is not None:
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
    return None


def run_once(cfg: RunOnceConfig) -> str:
    now = pd.Timestamp(cfg.now)
    now = now.tz_localize("UTC") if now.tzinfo is None else now.tz_convert("UTC")
    closed_ts = expected_closed_bar_ts(now, cfg.timeframe)
    key = signal_processed_key(cfg.source, cfg.exchange_symbol, cfg.timeframe, closed_ts)

    with connect(cfg.db_path) as conn:
        init_db(conn)
        # M5 limitation: the processed-key check below and the marker write in
        # process_decision happen on SEPARATE connections (refresh/score run
        # with no handle held), so two concurrent prodigy-signal instances could
        # both pass this check and double-write an intent for one bar. M5 is a
        # single-instance demo daemon by design; serial calls are idempotent
        # (test_run_once_is_idempotent_per_closed_bar). A multi-instance
        # deployment would need BEGIN IMMEDIATE re-check inside the write
        # transaction — out of M5 scope (and it would contend with the Rust
        # executor's WAL writes on the same DB).
        pre = _gate_skip(conn, cfg, key, now, check_already_processed=True)
        if pre is not None:
            return pre

    # ponytail: spec error handling — refresh/factor errors must write an event
    # and skip this run, NOT crash (the daemon must not crash the Rust executor;
    # SQLite is the only shared boundary). Neither skip writes the processed
    # marker, so the bar is re-evaluated next run once the data layer recovers.
    # The event write is best-effort via try_write_signal_event: a SQLite failure
    # logging the event must not prevent the skip itself.
    try:
        cfg.refresh_data()
    except Exception as exc:  # noqa: BLE001 — daemon isolates any refresh failure
        try_write_signal_event(cfg.db_path, "warning", f"data refresh error: {exc}")
        return "error_data_refresh"

    try:
        score, actual_closed_ts = cfg.score_loader()
    except Exception as exc:  # noqa: BLE001 — daemon isolates any factor/score failure
        try_write_signal_event(cfg.db_path, "warning", f"factor compute error: {exc}")
        return "error_factor_compute"

    # The processed key is derived from `now` (the expected closed bar), but the
    # data layer may lag — e.g. now=10:16 expects the 10:00 bar, but the freshest
    # closed bar in parquet could be 09:45 if refresh fell short. Writing a 10:00
    # marker/intent from 09:45 data would be a stale-data bug, so when the actual
    # closed bar reported by the score loader does not match the expected one,
    # skip without marking the bar and let the next run re-evaluate once data
    # catches up.
    if _normalize_closed_ts(actual_closed_ts, cfg.timeframe) != closed_ts:
        try_write_signal_event(
            cfg.db_path,
            "warning",
            f"stale data: expected closed bar {closed_ts}, latest available {actual_closed_ts}",
        )
        return "skipped_stale_data"

    with connect(cfg.db_path) as conn:
        init_db(conn)
        # TOCTOU re-check: refresh + score ran outside this transaction and can
        # take seconds; a manual_override, pending intent, unfinished order, or
        # stale state that appeared during that window must still block the
        # write. already_processed is owned by the caller (it can't change
        # within a single run_once), so the re-check excludes it. None of these
        # transient skips writes the processed marker.
        reskip = _gate_skip(conn, cfg, key, now, check_already_processed=False)
        if reskip is not None:
            return reskip
        position = _position_state(conn, cfg.exchange_symbol)
        holding_bars = (
            _holding_bars(position.opened_at, closed_ts, cfg.timeframe)
            if position is not None
            else 0
        )
        decision = decide_intent(score, position, holding_bars=holding_bars, cfg=cfg.signal_cfg)
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


def load_example_score(
    data_root: str | Path,
    research_symbol: str,
    now: pd.Timestamp,
    timeframe: str = "15m",
) -> tuple[float, str]:
    """Return (score, actual_closed_bar_ts).

    The closed-bar ts lets run_once verify the data layer's latest bar matches
    the expected closed bar for `now`, so a stale parquet can't drive an intent
    for a fresher bar.
    """
    now = pd.Timestamp(now).tz_convert("UTC") if pd.Timestamp(now).tzinfo else pd.Timestamp(now, tz="UTC")
    start = now - pd.Timedelta(days=7)
    end = now + pd.Timedelta(days=1)
    ohlcv = load_ohlcv(data_root, research_symbol, start, end, timeframe)
    funding = load_funding_rates(data_root, research_symbol, start, end)
    closed = latest_closed_bar(ohlcv, now, timeframe)
    closed_ts = closed["timestamp"]

    momentum = example_momentum_factor(ohlcv).rename(columns={"value": "example_momentum"})
    volatility = example_volatility_factor(ohlcv).rename(columns={"value": "example_volatility"})
    features = momentum[["timestamp", "symbol", "example_momentum"]].merge(
        volatility[["timestamp", "symbol", "example_volatility"]],
        on=["timestamp", "symbol"],
        how="left",
    )
    if not funding.empty:
        funding_factor = example_funding_factor(funding).rename(columns={"value": "example_funding"})
        features = features.merge(
            funding_factor[["timestamp", "symbol", "example_funding"]],
            on=["timestamp", "symbol"],
            how="left",
        )
    # ponytail: normalize both sides of the equality to tz-aware UTC so a
    # naive-vs-aware mismatch (parquet round-trip surprise) can't yield an empty
    # selection and blow up .iloc[-1]. One guard here beats per-call fixes.
    features_ts = pd.to_datetime(features["timestamp"], utc=True)
    closed_ts = pd.Timestamp(closed_ts)
    closed_ts = closed_ts.tz_convert("UTC") if closed_ts.tzinfo else closed_ts.tz_localize("UTC")
    row = features[features_ts == closed_ts].iloc[-1]
    closed_ts_str = closed_ts.isoformat().replace("+00:00", "Z")
    return combine_example_score(row), closed_ts_str
