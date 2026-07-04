from __future__ import annotations

import argparse


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
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
