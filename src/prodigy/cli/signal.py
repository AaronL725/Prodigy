from __future__ import annotations

import argparse
import signal as os_signal
import sqlite3
import sys
import time
from typing import Callable

import pandas as pd

from prodigy.config import load_config
from prodigy.data.backfill import run_backfill
from prodigy.signals.daemon import (
    RunOnceConfig,
    SignalDaemonConfig,
    expected_closed_bar_ts,
    is_transient_sqlite_error,
    load_example_score,
    run_once,
    try_write_signal_event,
)


ALLOWED_SIGNAL_SOURCES = ("example-factors", "dummy-cycle")


def resolve_signal_source(cli_value: str | None, config_value: str) -> str:
    """Pick the signal source: CLI flag wins, else the config value.

    M5 supports only example-factors (default demo source) and dummy-cycle
    (explicit test source). Any other value — including a typo — must NOT
    silently fall back to example-factors scoring while keeping the typo as the
    source/idempotency key, so reject it loudly.
    """
    source = cli_value if cli_value is not None else config_value
    if source not in ALLOWED_SIGNAL_SOURCES:
        raise ValueError(
            f"unsupported signal source {source!r}; expected one of {ALLOWED_SIGNAL_SOURCES}"
        )
    return source


def build_signal_daemon_config(signal_cfg: dict) -> SignalDaemonConfig:
    # total_notional_cap reuses the shared research/backtest signal-param
    # concept (research/signals.py SignalParams.total_notional_cap): one
    # notional cap drives both backtest lot sizing and live signal sizing, so
    # an operator tunes position sizing in one place. Configurable here, not
    # hardcoded.
    return SignalDaemonConfig(
        total_notional_cap=signal_cfg["total_notional_cap"],
        entry_threshold=signal_cfg["entry_threshold"],
        exit_threshold=signal_cfg["exit_threshold"],
        min_order_fraction=signal_cfg["min_order_fraction"],
        max_order_fraction=signal_cfg["max_order_fraction"],
        max_holding_bars=signal_cfg["max_holding_bars"],
        profit_hold_score_threshold=signal_cfg["profit_hold_score_threshold"],
        loss_hold_score_threshold=signal_cfg["loss_hold_score_threshold"],
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="prodigy-signal")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--once", action="store_true")
    mode.add_argument("--daemon", action="store_true")
    parser.add_argument("--config", default="configs/default.toml")
    parser.add_argument("--db", default="var/prodigy.sqlite")
    parser.add_argument("--data-root", default="data")
    parser.add_argument("--signal-source", default=None)
    parser.add_argument("--max-loops", type=int)
    return parser


def run_daemon_loop(
    args,
    signal_cfg: dict,
    db_path,
    source: str,
    refresh_data: Callable[[], None],
    score_loader: Callable[[], tuple[float, str]],
    stop_flag: Callable[[], bool],
    now_factory: Callable[[], pd.Timestamp] = lambda: pd.Timestamp.now(tz="UTC"),
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], pd.Timestamp] = lambda: pd.Timestamp.now(tz="UTC"),
) -> int:
    """Drive run_once on the daemon cadence, exiting cleanly on stop.

    `stop_flag` is consulted between iterations; when true (SIGINT/SIGTERM in
    production, injected in tests) the loop writes a shutdown event and exits 0.
    Best-effort event write: a SQLite failure logging it must not mask the exit.
    `source` is the already-validated signal source (see resolve_signal_source).
    """
    research_symbol = signal_cfg["enabled_symbols"][0]
    exchange_symbol = signal_cfg["exchange_symbols"][research_symbol]
    signal_daemon_cfg = build_signal_daemon_config(signal_cfg)
    loops = args.max_loops if args.max_loops is not None else (1 if args.once else None)
    count = 0
    try:
        while loops is None or count < loops:
            # Check stop BEFORE starting a new iteration: a SIGINT/SIGTERM that
            # fires during the previous sleep must not let the loop start another
            # run_once (spec: stop accepting new intents / opening new orders).
            # Skipped for --once, which is a one-shot run with no stop source.
            if not args.once and stop_flag():
                break
            try:
                result = run_once(
                    RunOnceConfig(
                        db_path=db_path,
                        data_root=args.data_root,
                        research_symbol=research_symbol,
                        exchange_symbol=exchange_symbol,
                        source=source,
                        now=now_factory(),
                        refresh_data=refresh_data,
                        score_loader=score_loader,
                        signal_cfg=signal_daemon_cfg,
                        max_state_age_secs=signal_cfg["max_state_age_secs"],
                        clock=clock,
                    )
                )
            except sqlite3.OperationalError as exc:
                if not is_transient_sqlite_error(exc):
                    raise
                try_write_signal_event(
                    db_path, "warning", f"sqlite transient error: {exc}"
                )
                result = "error_sqlite_busy"
            print(result)
            count += 1
            if args.once:
                break
            if stop_flag():
                break
            sleep(int(signal_cfg["poll_interval_secs"]))
            # Re-check immediately after sleep so a signal that fired during the
            # sleep exits here rather than top-of-loop on the next pass (same
            # guarantee, defensive belt-and-suspenders).
            if stop_flag():
                break
    finally:
        # --once is a one-shot run, not a shutdown; only log shutdown for the
        # long-running daemon path. Best-effort: a SQLite failure here must not
        # mask the clean exit.
        if not args.once:
            _write_shutdown_event(db_path)
    return 0


def _write_shutdown_event(db_path) -> None:
    # try_write_signal_event opens its own connection and swallows any failure,
    # so a SQLite lock here can't mask the clean exit.
    try_write_signal_event(db_path, "info", "shutdown: signal daemon stopping")


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if not args.once and not args.daemon:
        args.once = True
    cfg = load_config(args.config)
    signal_cfg = cfg["signal"]
    research_symbol = signal_cfg["enabled_symbols"][0]
    timeframe = signal_cfg["timeframe"]
    # Resolve + validate the signal source: CLI flag wins, else the config
    # value. An unknown source must not silently fall back to example-factors.
    try:
        source = resolve_signal_source(args.signal_source, signal_cfg["signal_source"])
    except ValueError as exc:
        print(f"prodigy-signal: {exc}", file=sys.stderr)
        return 2

    def refresh_data() -> None:
        now = pd.Timestamp.now(tz="UTC")
        run_backfill(
            symbol=research_symbol,
            start=(now - pd.Timedelta(days=7)).strftime("%Y-%m-%d"),
            # ponytail: run_backfill treats end as an exclusive boundary; using
            # tomorrow includes today's intraday closed bars without changing the
            # data layer in M5.
            end=(now + pd.Timedelta(days=1)).strftime("%Y-%m-%d"),
            timeframe=timeframe,
            data_root=args.data_root,
            db_path=args.db,
        )

    def score_loader() -> tuple[float, str]:
        now = pd.Timestamp.now(tz="UTC")
        if source == "dummy-cycle":
            # dummy-cycle has no data layer; report the expected closed bar so
            # run_once's stale-data check passes deterministically.
            return 1.0, expected_closed_bar_ts(now, timeframe)
        return load_example_score(args.data_root, research_symbol, now, timeframe)

    # SIGINT/SIGTERM -> set the stop flag so the loop exits after the current
    # iteration and writes a shutdown event. A simple flag (not a hard interrupt)
    # so an in-flight run_once always finishes and the processed-bar marker
    # transaction can't be torn.
    stopping = {"v": False}

    def _on_signal(_signum, _frame):
        stopping["v"] = True

    for sig in (os_signal.SIGINT, os_signal.SIGTERM):
        os_signal.signal(sig, _on_signal)

    return run_daemon_loop(
        args,
        signal_cfg=signal_cfg,
        db_path=args.db,
        source=source,
        refresh_data=refresh_data,
        score_loader=score_loader,
        stop_flag=lambda: stopping["v"],
    )


if __name__ == "__main__":
    raise SystemExit(main())
