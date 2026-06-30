from prodigy.db import connect, init_db
from prodigy.telegram.service import TelegramCommandService


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
        service = TelegramCommandService(conn, allowed_user_ids={"123"})
        message = service.stop(user_id="123", now="2026-07-01T00:00:00Z")
        row = conn.execute("select command, status from control_commands").fetchone()

    assert message == "stop command queued"
    assert dict(row) == {"command": "stop", "status": "pending"}


def test_resume_rejects_unknown_user(tmp_path):
    db_path = tmp_path / "prodigy.sqlite"
    with connect(db_path) as conn:
        init_db(conn)
        service = TelegramCommandService(conn, allowed_user_ids={"123"})
        message = service.resume(user_id="999", now="2026-07-01T00:00:00Z")

    assert message == "unauthorized"
