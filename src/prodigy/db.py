from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Iterator


SCHEMA_PATH = Path(__file__).resolve().parents[2] / "schema" / "001_initial.sql"


def connect(path: str | Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("pragma foreign_keys = on")
    conn.execute("pragma journal_mode = wal")
    return conn


def init_db(conn: sqlite3.Connection, schema_path: Path = SCHEMA_PATH) -> None:
    conn.executescript(schema_path.read_text())
    conn.commit()


def rows(conn: sqlite3.Connection, query: str, params: tuple = ()) -> Iterator[sqlite3.Row]:
    yield from conn.execute(query, params).fetchall()
