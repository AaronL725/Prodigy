import sqlite3

import pytest

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


def test_control_commands_accept_cancel_all_on_new_db(tmp_path):
    db_path = tmp_path / "prodigy.sqlite"
    with connect(db_path) as conn:
        init_db(conn)
        conn.execute(
            """
            insert into control_commands (
              command_id, created_at, command, status, requested_by
            ) values ('c1', '2026-07-06T00:00:00Z', 'cancel_all', 'pending', '123')
            """
        )
        row = conn.execute("select command from control_commands").fetchone()

    assert row["command"] == "cancel_all"


def test_init_db_migrates_old_control_commands_check_constraint(tmp_path):
    db_path = tmp_path / "prodigy.sqlite"
    raw = sqlite3.connect(db_path)
    raw.executescript(
        """
        create table control_commands (
          command_id text primary key,
          created_at text not null,
          command text not null check (command in ('stop', 'resume', 'close_all')),
          status text not null check (status in ('pending', 'accepted', 'rejected', 'executed', 'failed')),
          requested_by text not null,
          processed_at text,
          error text
        );
        insert into control_commands (
          command_id, created_at, command, status, requested_by
        ) values ('old-stop', '2026-07-06T00:00:00Z', 'stop', 'pending', '123');
        """
    )
    raw.commit()
    raw.close()

    with connect(db_path) as conn:
        init_db(conn)
        conn.execute(
            """
            insert into control_commands (
              command_id, created_at, command, status, requested_by
            ) values ('new-cancel', '2026-07-06T00:01:00Z', 'cancel_all', 'pending', '123')
            """
        )
        commands = [
            row["command"]
            for row in conn.execute("select command from control_commands order by command_id")
        ]
        init_db(conn)
        index_exists = (
            conn.execute(
                """
                select 1 from sqlite_master
                where type = 'index' and name = 'idx_control_commands_status_created'
                """
            ).fetchone()
            is not None
        )

    assert commands == ["cancel_all", "stop"]
    assert index_exists


def test_control_commands_migration_rolls_back_on_copy_failure(tmp_path):
    db_path = tmp_path / "prodigy.sqlite"
    raw = sqlite3.connect(db_path)
    raw.executescript(
        """
        create table control_commands (
          command_id text primary key,
          created_at text not null,
          command text not null check (command in ('stop', 'resume', 'close_all', 'bad')),
          status text not null check (status in ('pending', 'accepted', 'rejected', 'executed', 'failed')),
          requested_by text not null,
          processed_at text,
          error text
        );
        insert into control_commands (
          command_id, created_at, command, status, requested_by
        ) values ('bad-command', '2026-07-06T00:00:00Z', 'bad', 'pending', '123');
        """
    )
    raw.commit()
    raw.close()

    with connect(db_path) as conn:
        with pytest.raises(sqlite3.IntegrityError):
            init_db(conn)

    raw = sqlite3.connect(db_path)
    try:
        tables = {
            row[0]
            for row in raw.execute(
                "select name from sqlite_master where type = 'table'"
            ).fetchall()
        }
        row = raw.execute("select command from control_commands").fetchone()
    finally:
        raw.close()

    assert "control_commands" in tables
    assert "control_commands_old" not in tables
    assert row[0] == "bad"


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

        with pytest.raises(sqlite3.IntegrityError):
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


def test_execution_schema_adds_order_and_position_context(tmp_path):
    db_path = tmp_path / "prodigy.sqlite"

    with connect(db_path) as conn:
        init_db(conn)
        order_columns = {
            row["name"]
            for row in conn.execute("pragma table_info(orders)").fetchall()
        }
        position_columns = {
            row["name"]
            for row in conn.execute("pragma table_info(positions)").fetchall()
        }

    assert {
        "exchange_order_id",
        "attempt",
        "raw_json",
        "last_error",
    }.issubset(order_columns)
    assert {
        "ownership",
        "opened_at",
        "adopted_at",
        "source_intent_id",
        "raw_json",
    }.issubset(position_columns)
