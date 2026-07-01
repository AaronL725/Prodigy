from __future__ import annotations

import sqlite3
from pathlib import Path


SCHEMA_DIR = Path(__file__).resolve().parents[2] / "schema"
SCHEMA_PATH = SCHEMA_DIR / "001_initial.sql"


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
    if schema_path == SCHEMA_PATH:
        conn.executescript(SCHEMA_PATH.read_text())
        _ensure_execution_schema(conn)
    else:
        conn.executescript(schema_path.read_text())
    conn.commit()


def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {row["name"] for row in conn.execute(f"pragma table_info({table})")}


def _add_column_if_missing(
    conn: sqlite3.Connection, table: str, column: str, definition: str
) -> None:
    if column not in _columns(conn, table):
        conn.execute(f"alter table {table} add column {definition}")


def _ensure_execution_schema(conn: sqlite3.Connection) -> None:
    _add_column_if_missing(conn, "orders", "exchange_order_id", "exchange_order_id text")
    _add_column_if_missing(conn, "orders", "attempt", "attempt integer not null default 1")
    _add_column_if_missing(conn, "orders", "raw_json", "raw_json text not null default '{}'")
    _add_column_if_missing(conn, "orders", "last_error", "last_error text")
    _add_column_if_missing(conn, "fills", "trade_id", "trade_id text")
    _add_column_if_missing(conn, "fills", "client_oid", "client_oid text")
    _add_column_if_missing(conn, "fills", "raw_json", "raw_json text not null default '{}'")
    _add_column_if_missing(
        conn,
        "positions",
        "ownership",
        "ownership text not null default 'system' check (ownership in ('system', 'imported'))",
    )
    _add_column_if_missing(conn, "positions", "opened_at", "opened_at text")
    _add_column_if_missing(conn, "positions", "adopted_at", "adopted_at text")
    _add_column_if_missing(conn, "positions", "source_intent_id", "source_intent_id text")
    _add_column_if_missing(conn, "positions", "raw_json", "raw_json text not null default '{}'")
    conn.executescript(
        """
        create table if not exists executor_state (
          key text primary key,
          value text not null,
          updated_at text not null
        );
        create index if not exists idx_orders_intent_status
          on orders(intent_id, status, updated_at);
        create index if not exists idx_fills_order_symbol
          on fills(order_id, symbol, created_at);
        """
    )
