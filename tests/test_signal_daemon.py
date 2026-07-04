import pandas as pd

from prodigy.signals.daemon import (
    PositionState,
    SignalDaemonConfig,
    SignalDecision,
    combine_example_score,
    decide_intent,
    latest_closed_bar,
)


def test_latest_closed_bar_ignores_open_bar():
    frame = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                ["2026-07-04T10:00:00Z", "2026-07-04T10:15:00Z"],
                utc=True,
            ),
            "symbol": ["ETH/USDT:USDT", "ETH/USDT:USDT"],
            "close": [100.0, 101.0],
        }
    )

    row = latest_closed_bar(frame, now=pd.Timestamp("2026-07-04T10:29:59Z"), timeframe="15m")

    assert row["timestamp"] == pd.Timestamp("2026-07-04T10:00:00Z")


def test_combine_example_score_clips_to_range():
    features = pd.DataFrame(
        {
            "example_momentum": [2.0],
            "example_funding": [-0.5],
            "example_volatility": [0.5],
        }
    )

    assert combine_example_score(features.iloc[-1]) == 0.6666666666666666


def test_decide_opens_when_score_crosses_threshold():
    cfg = SignalDaemonConfig(total_notional_cap=10_000)

    decision = decide_intent(score=0.8, position=None, holding_bars=0, cfg=cfg)

    assert decision == SignalDecision(action="open", side="long", target_notional=750.0, reason="open_threshold")


def test_decide_reverse_signal_closes_existing_position_only():
    cfg = SignalDaemonConfig(total_notional_cap=10_000)
    position = PositionState(side="long", unrealized_pnl=10.0)

    decision = decide_intent(score=-0.8, position=position, holding_bars=3, cfg=cfg)

    assert decision == SignalDecision(action="close", side="long", target_notional=0.0, reason="close_opposite")


def test_decide_holding_expiry_profit_and_loss_thresholds():
    cfg = SignalDaemonConfig(total_notional_cap=10_000, max_holding_bars=96)

    profit_hold = decide_intent(
        score=0.3,
        position=PositionState(side="long", unrealized_pnl=1.0),
        holding_bars=96,
        cfg=cfg,
    )
    profit_close = decide_intent(
        score=0.1,
        position=PositionState(side="long", unrealized_pnl=1.0),
        holding_bars=96,
        cfg=cfg,
    )
    loss_close = decide_intent(
        score=0.3,
        position=PositionState(side="long", unrealized_pnl=-1.0),
        holding_bars=96,
        cfg=cfg,
    )

    assert profit_hold is None
    assert profit_close == SignalDecision(action="close", side="long", target_notional=0.0, reason="holding_expiry_profit")
    assert loss_close == SignalDecision(action="close", side="long", target_notional=0.0, reason="holding_expiry_loss")


from prodigy.db import connect, init_db
from prodigy.signals.daemon import process_decision
from prodigy.signals.state import get_executor_state, signal_processed_key


def test_process_decision_writes_intent_and_marker_in_one_transaction(tmp_path):
    db_path = tmp_path / "prodigy.sqlite"
    key = signal_processed_key("dummy-cycle", "ETHUSDT", "15m", "2026-07-04T10:15:00Z")

    with connect(db_path) as conn:
        init_db(conn)
        decision = SignalDecision("open", "long", 100.0, "open_threshold")
        process_decision(
            conn=conn,
            decision=decision,
            processed_key=key,
            created_at="2026-07-04T10:15:01Z",
            symbol="ETHUSDT",
            source="dummy-cycle",
            model_version="dummy-cycle",
        )

        intent = conn.execute("select action, side, target_notional from trade_intents").fetchone()
        marker = get_executor_state(conn, key)

    assert dict(intent) == {"action": "open", "side": "long", "target_notional": 100.0}
    assert marker == "open_intent_written"


def test_process_decision_close_uses_zero_notional(tmp_path):
    db_path = tmp_path / "prodigy.sqlite"
    key = signal_processed_key("dummy-cycle", "ETHUSDT", "15m", "2026-07-04T10:15:00Z")

    with connect(db_path) as conn:
        init_db(conn)
        process_decision(
            conn=conn,
            decision=SignalDecision("close", "long", 0.0, "close_opposite"),
            processed_key=key,
            created_at="2026-07-04T10:15:01Z",
            symbol="ETHUSDT",
            source="dummy-cycle",
            model_version="dummy-cycle",
        )
        intent = conn.execute("select action, side, target_notional, max_order_notional from trade_intents").fetchone()

    assert dict(intent) == {
        "action": "close",
        "side": "long",
        "target_notional": 0.0,
        "max_order_notional": 0.0,
    }
