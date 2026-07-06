from prodigy.db import connect, init_db
from prodigy.signals.intents import TradeIntent, insert_trade_intent, write_trade_intent


def test_write_trade_intent_persists_pending_row(tmp_path):
    db_path = tmp_path / "prodigy.sqlite"
    intent = TradeIntent(
        intent_id="intent-eth-open-1",
        created_at="2026-07-01T00:00:00Z",
        symbol="ETH/USDT:USDT",
        side="long",
        action="open",
        target_notional=1000.0,
        max_order_notional=500.0,
        source="test",
        reason="score crossed long threshold",
        model_version="smoke-test",
    )

    with connect(db_path) as conn:
        init_db(conn)
        write_trade_intent(conn, intent)
        row = conn.execute(
            "select intent_id, status, symbol from trade_intents"
        ).fetchone()

    assert dict(row) == {
        "intent_id": "intent-eth-open-1",
        "status": "pending",
        "symbol": "ETH/USDT:USDT",
    }


def test_insert_trade_intent_does_not_commit(tmp_path):
    db_path = tmp_path / "prodigy.sqlite"
    intent = TradeIntent(
        intent_id="intent-no-commit",
        created_at="2026-07-04T00:00:00Z",
        symbol="ETHUSDT",
        side="long",
        action="open",
        target_notional=100.0,
        max_order_notional=100.0,
        source="test",
        reason="transaction test",
        model_version="m5-test",
    )

    with connect(db_path) as conn:
        init_db(conn)
        conn.execute("begin")
        insert_trade_intent(conn, intent)
        conn.rollback()
        row = conn.execute(
            "select intent_id from trade_intents where intent_id = ?",
            (intent.intent_id,),
        ).fetchone()

    assert row is None
