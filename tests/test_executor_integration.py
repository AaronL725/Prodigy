import subprocess

from prodigy.db import connect, init_db
from prodigy.signals.intents import TradeIntent, write_trade_intent


def test_rust_demo_executor_processes_pending_intent(tmp_path):
    db_path = tmp_path / "prodigy.sqlite"

    with connect(db_path) as conn:
        init_db(conn)
        write_trade_intent(
            conn,
            TradeIntent(
                intent_id="intent-1",
                created_at="2026-07-01T00:00:00Z",
                symbol="ETH/USDT:USDT",
                side="long",
                action="open",
                target_notional=100.0,
                max_order_notional=100.0,
                source="test",
                reason="integration",
                model_version="smoke-test",
            ),
        )

    result = subprocess.run(
        [
            "cargo",
            "run",
            "-q",
            "-p",
            "prodigy-executor",
            "--",
            "--db",
            str(db_path),
            "--once",
            "--test-reset-demo-state",
        ],
        check=True,
        text=True,
        capture_output=True,
    )

    with connect(db_path) as conn:
        intent = conn.execute(
            "select status, error from trade_intents where intent_id = 'intent-1'"
        ).fetchone()
        order_count = conn.execute("select count(*) from orders").fetchone()[0]
        event_count = conn.execute("select count(*) from events").fetchone()[0]

    assert "processed intent-1" in result.stdout
    assert intent["status"] in {"accepted", "executed"}
    assert intent["error"] is None
    assert order_count >= 1
    assert event_count >= 1
