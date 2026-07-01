from __future__ import annotations

import sqlite3
from pathlib import Path


SCHEMA_PATH = Path(__file__).resolve().parents[2] / "schema" / "001_initial.sql"


def connect(path: str | Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("pragma foreign_keys = on")
    conn.execute("pragma journal_mode = wal")
    # ponytail: 5s busy_timeout so Python and the Rust executor can both touch
    # this WAL file; SQLite waits and retries on SQLITE_BUSY instead of erroring
    # on the first lock contention. Raise if the poll loop proves contended.
    conn.execute("pragma busy_timeout = 5000")
    return conn


def init_db(conn: sqlite3.Connection, schema_path: Path = SCHEMA_PATH) -> None:
    conn.executescript(schema_path.read_text())
    conn.commit()
