import sqlite3

from prodigy.db import connect, init_db


def test_init_db_creates_core_tables(tmp_path):
    db_path = tmp_path / "prodigy.sqlite"

    with connect(db_path) as conn:
        init_db(conn)
        tables = {
            row[0]
            for row in conn.execute(
                "select name from sqlite_master where type = 'table'"
            ).fetchall()
        }

    assert {
        "trade_intents",
        "control_commands",
        "orders",
        "fills",
        "positions",
        "equity_snapshots",
        "models",
        "events",
        "task_checkpoints",
    }.issubset(tables)


def test_trade_intents_are_unique_by_id(tmp_path):
    db_path = tmp_path / "prodigy.sqlite"

    with connect(db_path) as conn:
        init_db(conn)
        conn.execute(
            """
            insert into trade_intents (
              intent_id, created_at, symbol, side, action, target_notional,
              max_order_notional, status, source
            ) values (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "intent-1",
                "2026-07-01T00:00:00Z",
                "ETH/USDT:USDT",
                "long",
                "open",
                1000.0,
                500.0,
                "pending",
                "test",
            ),
        )

        try:
            conn.execute(
                """
                insert into trade_intents (
                  intent_id, created_at, symbol, side, action, target_notional,
                  max_order_notional, status, source
                ) values (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "intent-1",
                    "2026-07-01T00:00:01Z",
                    "ETH/USDT:USDT",
                    "long",
                    "open",
                    1000.0,
                    500.0,
                    "pending",
                    "test",
                ),
            )
        except sqlite3.IntegrityError:
            duplicate_rejected = True
        else:
            duplicate_rejected = False

    assert duplicate_rejected is True
