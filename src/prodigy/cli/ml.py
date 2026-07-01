from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from prodigy.ml.example_trainer import train_example_model

# ponytail: fixed example-features layout produced by the data CLI.
_EXAMPLE_FEATURES_REL = Path("processed") / "example_features.parquet.gzip"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="prodigy-ml")
    sub = parser.add_subparsers(dest="command", required=True)
    train = sub.add_parser("train-example")
    train.add_argument("--symbol", default="ETH/USDT:USDT")
    train.add_argument("--horizon", default="1h")
    train.add_argument("--data-root", default="data")
    train.add_argument("--db", default="var/prodigy.sqlite")
    train.add_argument("--model-version", default="example-1h")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "train-example":
        parquet_path = Path(args.data_root) / _EXAMPLE_FEATURES_REL
        if not parquet_path.exists():
            print(f"error: example features not found at {parquet_path}")
            print("run prodigy-data backfill and the example feature builder first")
            return 1
        frame = pd.read_parquet(parquet_path)
        result = train_example_model(
            frame=frame,
            db_path=args.db,
            model_root=parquet_path.parent.parent / "models",
            horizon=args.horizon,
            model_version=args.model_version,
        )
        print(f"trained {result.model_version}")
        print(f"artifact: {result.artifact_path}")
        print(f"hash: {result.artifact_hash}")
        print("metrics:")
        print(json.dumps(result.metrics, indent=2, sort_keys=True, default=str))
    return 0
