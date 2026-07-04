from __future__ import annotations

import argparse
import time

import pandas as pd

from prodigy.config import load_config
from prodigy.data.backfill import run_backfill
from prodigy.signals.daemon import (
    RunOnceConfig,
    SignalDaemonConfig,
    expected_closed_bar_ts,
    load_example_score,
    run_once,
)


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
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="prodigy-signal")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--once", action="store_true")
    mode.add_argument("--daemon", action="store_true")
    parser.add_argument("--config", default="configs/default.toml")
    parser.add_argument("--db", default="var/prodigy.sqlite")
    parser.add_argument("--data-root", default="data")
    parser.add_argument("--signal-source", default="example-factors")
    parser.add_argument("--max-loops", type=int)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if not args.once and not args.daemon:
        args.once = True
    cfg = load_config(args.config)
    signal_cfg = cfg["signal"]
    research_symbol = signal_cfg["enabled_symbols"][0]
    exchange_symbol = signal_cfg["exchange_symbols"][research_symbol]
    timeframe = signal_cfg["timeframe"]
    signal_daemon_cfg = build_signal_daemon_config(signal_cfg)

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
        if args.signal_source == "dummy-cycle":
            # dummy-cycle has no data layer; report the expected closed bar so
            # run_once's stale-data check passes deterministically.
            return 1.0, expected_closed_bar_ts(now, timeframe)
        return load_example_score(args.data_root, research_symbol, now, timeframe)

    loops = args.max_loops if args.max_loops is not None else (1 if args.once else None)
    count = 0
    while loops is None or count < loops:
        result = run_once(
            RunOnceConfig(
                db_path=args.db,
                data_root=args.data_root,
                research_symbol=research_symbol,
                exchange_symbol=exchange_symbol,
                source=args.signal_source,
                now=pd.Timestamp.now(tz="UTC"),
                refresh_data=refresh_data,
                score_loader=score_loader,
                signal_cfg=signal_daemon_cfg,
                max_state_age_secs=signal_cfg["max_state_age_secs"],
            )
        )
        print(result)
        count += 1
        if args.once:
            break
        time.sleep(int(signal_cfg["poll_interval_secs"]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
