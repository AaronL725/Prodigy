from __future__ import annotations

from pathlib import Path


def symbol_slug(symbol: str) -> str:
    base, settle = symbol.split(":")
    left, right = base.split("/")
    suffix = "SWAP" if settle == right else settle
    return f"{left}-{right}-{suffix}"


def ensure_dir(path: str | Path) -> Path:
    result = Path(path)
    result.mkdir(parents=True, exist_ok=True)
    return result
