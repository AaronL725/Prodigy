from prodigy.db import connect, init_db
from prodigy.signals.intents import TradeIntent, write_trade_intent


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
