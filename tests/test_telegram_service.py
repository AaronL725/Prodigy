import time

from prodigy.db import connect, init_db
from prodigy.signals.state import set_executor_state
from prodigy.telegram.service import TelegramCommandService


def set_active_executor(conn, heartbeat_ms=None):
    heartbeat_ms = heartbeat_ms or str(int(time.time() * 1000))
    set_executor_state(conn, "active_mode", "live", "2026-07-01T00:00:00Z")
    set_executor_state(conn, "active_instance_id", "inst-live", "2026-07-01T00:00:00Z")
    set_executor_state(conn, "active_started_at", heartbeat_ms, "2026-07-01T00:00:00Z")
    set_executor_state(conn, "active_heartbeat_at", heartbeat_ms, "2026-07-01T00:00:00Z")


def test_status_reports_pending_intents(tmp_path):
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
        conn.commit()

        service = TelegramCommandService(conn, allowed_user_ids={"123"})
        message = service.status()

    assert "pending_intents=1" in message


def test_stop_writes_control_command_for_allowed_user(tmp_path):
    db_path = tmp_path / "prodigy.sqlite"
    with connect(db_path) as conn:
        init_db(conn)
        set_active_executor(conn)
        service = TelegramCommandService(conn, allowed_user_ids={"123"})
        message = service.stop(user_id="123", now="2026-07-01T00:00:00Z")
        row = conn.execute("select command, status from control_commands").fetchone()

    assert message == "stop command queued"
    assert dict(row) == {"command": "stop", "status": "pending"}


def test_stop_without_active_executor_does_not_queue(tmp_path):
    db_path = tmp_path / "prodigy.sqlite"
    with connect(db_path) as conn:
        init_db(conn)
        service = TelegramCommandService(conn, allowed_user_ids={"123"})
        message = service.stop(user_id="123", now="2026-07-01T00:00:00Z")
        queued = conn.execute("select count(*) from control_commands").fetchone()[0]
        rejected = conn.execute(
            """
            select count(*) from events
            where component = 'telegram'
              and message = 'telegram control command rejected'
            """
        ).fetchone()[0]

    assert message == "no active executor"
    assert queued == 0
    assert rejected == 1


def test_stop_with_stale_active_executor_does_not_queue(tmp_path):
    db_path = tmp_path / "prodigy.sqlite"
    with connect(db_path) as conn:
        init_db(conn)
        set_active_executor(conn, "0")
        service = TelegramCommandService(conn, allowed_user_ids={"123"})
        message = service.stop(user_id="123", now="2026-07-01T00:00:00Z")
        queued = conn.execute("select count(*) from control_commands").fetchone()[0]
        rejected = conn.execute(
            """
            select count(*) from events
            where component = 'telegram'
              and message = 'telegram control command rejected'
            """
        ).fetchone()[0]

    assert message == "no active executor"
    assert queued == 0
    assert rejected == 1


def test_stop_with_corrupt_active_started_at_does_not_queue(tmp_path):
    db_path = tmp_path / "prodigy.sqlite"
    with connect(db_path) as conn:
        init_db(conn)
        heartbeat_ms = str(int(time.time() * 1000))
        set_executor_state(conn, "active_mode", "live", "2026-07-01T00:00:00Z")
        set_executor_state(conn, "active_instance_id", "inst-live", "2026-07-01T00:00:00Z")
        set_executor_state(conn, "active_started_at", "not-ms", "2026-07-01T00:00:00Z")
        set_executor_state(conn, "active_heartbeat_at", heartbeat_ms, "2026-07-01T00:00:00Z")
        service = TelegramCommandService(conn, allowed_user_ids={"123"})
        message = service.stop(user_id="123", now="2026-07-01T00:00:00Z")
        queued = conn.execute("select count(*) from control_commands").fetchone()[0]

    assert message == "no active executor"
    assert queued == 0


def test_stop_with_blank_active_instance_does_not_queue(tmp_path):
    db_path = tmp_path / "prodigy.sqlite"
    with connect(db_path) as conn:
        init_db(conn)
        heartbeat_ms = str(int(time.time() * 1000))
        set_executor_state(conn, "active_mode", "live", "2026-07-01T00:00:00Z")
        set_executor_state(conn, "active_instance_id", "  ", "2026-07-01T00:00:00Z")
        set_executor_state(conn, "active_started_at", heartbeat_ms, "2026-07-01T00:00:00Z")
        set_executor_state(conn, "active_heartbeat_at", heartbeat_ms, "2026-07-01T00:00:00Z")
        service = TelegramCommandService(conn, allowed_user_ids={"123"})
        message = service.stop(user_id="123", now="2026-07-01T00:00:00Z")
        queued = conn.execute("select count(*) from control_commands").fetchone()[0]

    assert message == "no active executor"
    assert queued == 0


def test_stop_writes_active_mode_and_instance(tmp_path):
    db_path = tmp_path / "prodigy.sqlite"
    with connect(db_path) as conn:
        init_db(conn)
        set_active_executor(conn)
        service = TelegramCommandService(conn, allowed_user_ids={"123"})
        message = service.stop(user_id="123", now="2026-07-01T00:00:00Z")
        row = conn.execute(
            "select command, mode, instance_id from control_commands"
        ).fetchone()

    assert message == "stop command queued"
    assert dict(row) == {
        "command": "stop",
        "mode": "live",
        "instance_id": "inst-live",
    }


def test_resume_rejects_unknown_user(tmp_path):
    db_path = tmp_path / "prodigy.sqlite"
    with connect(db_path) as conn:
        init_db(conn)
        service = TelegramCommandService(conn, allowed_user_ids={"123"})
        message = service.resume(user_id="999", now="2026-07-01T00:00:00Z")

    assert message == "unauthorized"


def test_int_user_ids_match_int_or_str_whitelist(tmp_path):
    # Real Telegram user IDs arrive as ints; config may carry ints or strings.
    db_path = tmp_path / "prodigy.sqlite"
    with connect(db_path) as conn:
        init_db(conn)
        set_active_executor(conn)
        service = TelegramCommandService(conn, allowed_user_ids={123})
        message = service.stop(user_id=123, now="2026-07-01T00:00:00Z")

    assert message == "stop command queued"
