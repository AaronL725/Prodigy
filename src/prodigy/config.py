from __future__ import annotations

from pathlib import Path
import tomllib
from typing import Any


REQUIRED_SECTIONS = (
    "trading",
    "risk",
    "execution",
    "fees",
    "model",
    "telegram",
)


def load_config(path: str | Path) -> dict[str, Any]:
    data = tomllib.loads(Path(path).read_text())
    for section in REQUIRED_SECTIONS:
        if section not in data:
            raise ValueError(f"missing config section: {section}")
    return data
