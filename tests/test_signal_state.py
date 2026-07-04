from prodigy.db import connect, init_db
from prodigy.signals.state import (
    get_executor_state,
    has_unfinished_system_order,
    has_unresolved_intent,
    is_manual_override_active,
    set_executor_state,
    signal_processed_key,
)


def test_signal_processed_key_uses_exchange_symbol():
    assert (
        signal_processed_key("example-factors", "ETHUSDT", "15m", "2026-07-04T10:15:00Z")
        == "signal_processed:example-factors:ETHUSDT:15m:2026-07-04T10:15:00Z"
    )


def test_executor_state_round_trip(tmp_path):
    db_path = tmp_path / "prodigy.sqlite"
    with connect(db_path) as conn:
        init_db(conn)
        set_executor_state(conn, "signal_processed:test", "no_signal", "2026-07-04T00:00:00Z")
        row = get_executor_state(conn, "signal_processed:test")

    assert row == "no_signal"


def test_pending_intent_blocks_symbol(tmp_path):
    db_path = tmp_path / "prodigy.sqlite"
    with connect(db_path) as conn:
        init_db(conn)
        conn.execute(
            """
            insert into trade_intents (
              intent_id, created_at, symbol, side, action, target_notional,
              max_order_notional, status, source, reason, model_version
            ) values ('i1', '2026-07-04T00:00:00Z', 'ETHUSDT', 'long', 'open',
                      100, 100, 'pending', 'test', 'x', 'm')
            """
        )
        conn.commit()

        assert has_unresolved_intent(conn, "ETHUSDT") is True
        assert has_unresolved_intent(conn, "BTCUSDT") is False


def test_manual_override_blocks_symbol(tmp_path):
    db_path = tmp_path / "prodigy.sqlite"
    with connect(db_path) as conn:
        init_db(conn)
        set_executor_state(conn, "manual_override:ETHUSDT", "active", "2026-07-04T00:00:00Z")
        assert is_manual_override_active(conn, "ETHUSDT") is True
        assert is_manual_override_active(conn, "BTCUSDT") is False


def test_unfinished_system_order_blocks_symbol(tmp_path):
    db_path = tmp_path / "prodigy.sqlite"
    with connect(db_path) as conn:
        init_db(conn)
        conn.execute(
            """
            insert into trade_intents (
              intent_id, created_at, symbol, side, action, target_notional,
              max_order_notional, status, source, reason, model_version
            ) values ('i1', '2026-07-04T00:00:00Z', 'ETHUSDT', 'long', 'open',
                      100, 100, 'accepted', 'test', 'x', 'm')
            """
        )
        conn.execute(
            """
            insert into orders (
              order_id, client_oid, intent_id, symbol, side, action, order_type,
              status, price, size, filled_size, created_at, updated_at
            ) values ('o1', 'c1', 'i1', 'ETHUSDT', 'buy', 'open', 'limit',
                      'submitted', 100, 0.1, 0, '2026-07-04T00:00:00Z', '2026-07-04T00:00:00Z')
            """
        )
        conn.commit()

        assert has_unfinished_system_order(conn, "ETHUSDT") is True
        assert has_unfinished_system_order(conn, "BTCUSDT") is False
