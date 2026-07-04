from __future__ import annotations

import argparse
import time

import pandas as pd

from prodigy.config import load_config
from prodigy.data.backfill import run_backfill
from prodigy.signals.daemon import RunOnceConfig, load_example_score, run_once


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

    def score_loader() -> float:
        if args.signal_source == "dummy-cycle":
            return 1.0
        return load_example_score(args.data_root, research_symbol, pd.Timestamp.now(tz="UTC"), timeframe)

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
