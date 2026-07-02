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
        # A row must NEVER claim status='filled' without a real fill: a confirmed
        # zero-fill is needs_reconcile/failed, not filled. This is the invariant
        # the executor enforces (it used to mark zero-fill taker orders "filled").
        false_fills = conn.execute(
            "select count(*) from orders where status = 'filled' and filled_size <= 0"
        ).fetchone()[0]
        # Every 'filled' order must have a matching fills row (real price/size/fee).
        filled_orders = conn.execute(
            "select count(*) from orders where status = 'filled'"
        ).fetchone()[0]
        fills_for_filled = conn.execute(
            "select count(*) from fills where client_oid in "
            "(select client_oid from orders where status = 'filled')"
        ).fetchone()[0]
        event_count = conn.execute("select count(*) from events").fetchone()[0]

    assert "processed intent-1" in result.stdout
    # The intent must reach a terminal state. On this Bitget demo book the only
    # ask (1977) sits beyond the exchange price-limit band (~1886), so a buy
    # cannot genuinely fill; the executor must then FAIL the intent with a clear
    # diagnostic rather than falsely mark it executed. When the book does allow a
    # fill, status is 'executed'. Either terminal state is honest; 'pending'/
    # 'accepted' (stuck) is not.
    assert intent["status"] in ("executed", "failed"), (
        f"expected a terminal state (executed|failed), got {intent['status']}"
    )
    if intent["status"] == "failed":
        assert intent["error"], "a failed intent must record a diagnostic error"
    assert order_count >= 1, "expected at least one demo order to be attempted"
    assert false_fills == 0, "an order must not be marked filled with no fill"
    assert fills_for_filled == filled_orders, (
        "every filled order must have a matching real fills row"
    )
    assert event_count >= 1
