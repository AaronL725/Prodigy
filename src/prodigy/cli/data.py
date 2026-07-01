from __future__ import annotations

import argparse
import json

from prodigy.data.backfill import run_backfill


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="prodigy-data")
    sub = parser.add_subparsers(dest="command", required=True)
    backfill = sub.add_parser("backfill")
    backfill.add_argument("--symbol", default="ETH/USDT:USDT")
    backfill.add_argument("--start", default="2024-01-01")
    backfill.add_argument("--end")
    backfill.add_argument("--timeframe", default="15m")
    backfill.add_argument("--data-root", default="data")
    backfill.add_argument("--db", default="var/prodigy.sqlite")
    backfill.add_argument("--proxy-url", default="http://127.0.0.1:7897")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "backfill":
        result = run_backfill(
            symbol=args.symbol,
            start=args.start,
            end=args.end,
            timeframe=args.timeframe,
            data_root=args.data_root,
            db_path=args.db,
            proxy_url=args.proxy_url,
        )
        print(result)
        print("OHLCV quality:")
        print(json.dumps(result.ohlcv_quality, indent=2, sort_keys=True))
        print("Funding quality:")
        print(json.dumps(result.funding_quality, indent=2, sort_keys=True))
    return 0
