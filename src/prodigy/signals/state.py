from __future__ import annotations

import sqlite3


def signal_processed_key(source: str, symbol: str, timeframe: str, closed_bar_ts: str) -> str:
    return f"signal_processed:{source}:{symbol}:{timeframe}:{closed_bar_ts}"


def get_executor_state(conn: sqlite3.Connection, key: str) -> str | None:
    row = conn.execute("select value from executor_state where key = ?", (key,)).fetchone()
    return None if row is None else str(row["value"])


def set_executor_state(
    conn: sqlite3.Connection,
    key: str,
    value: str,
    updated_at: str,
) -> None:
    conn.execute(
        """
        insert into executor_state (key, value, updated_at)
        values (?, ?, ?)
        on conflict(key) do update set
          value = excluded.value,
          updated_at = excluded.updated_at
        """,
        (key, value, updated_at),
    )


def has_unresolved_intent(conn: sqlite3.Connection, symbol: str) -> bool:
    row = conn.execute(
        """
        select 1 from trade_intents
        where symbol = ? and status in ('pending', 'accepted')
        limit 1
        """,
        (symbol,),
    ).fetchone()
    return row is not None


def has_unfinished_system_order(conn: sqlite3.Connection, symbol: str) -> bool:
    row = conn.execute(
        """
        select 1 from orders
        where symbol = ?
          and intent_id is not null
          and status not in ('filled', 'cancelled', 'canceled', 'rejected',
                             'failed', 'externally_cancelled', 'externally_closed')
        limit 1
        """,
        (symbol,),
    ).fetchone()
    return row is not None


def is_manual_override_active(conn: sqlite3.Connection, symbol: str) -> bool:
    return get_executor_state(conn, f"manual_override:{symbol}") == "active"
