import subprocess

from prodigy.db import connect, init_db
from prodigy.signals.intents import TradeIntent, write_trade_intent


def test_rust_dry_executor_rejects_pending_intent(tmp_path):
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
                target_notional=1000.0,
                max_order_notional=500.0,
                source="test",
                reason="integration",
                model_version="smoke-test",
            ),
        )

    result = subprocess.run(
        ["cargo", "run", "-q", "-p", "prodigy-executor", "--", "--db", str(db_path)],
        check=True,
        text=True,
        capture_output=True,
    )

    with connect(db_path) as conn:
        row = conn.execute(
            "select status, error from trade_intents where intent_id = 'intent-1'"
        ).fetchone()

    assert "rejected intent-1" in result.stdout
    assert row["status"] == "rejected"
    assert "dry executor rejects intents" in row["error"]
