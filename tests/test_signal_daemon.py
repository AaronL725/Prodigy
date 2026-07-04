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


from prodigy.signals.daemon import RunOnceConfig, run_once
from prodigy.signals.state import set_executor_state


def test_run_once_skips_when_state_is_stale(tmp_path):
    db_path = tmp_path / "prodigy.sqlite"
    with connect(db_path) as conn:
        init_db(conn)

    result = run_once(
        RunOnceConfig(
            db_path=db_path,
            data_root=tmp_path / "data",
            research_symbol="ETH/USDT:USDT",
            exchange_symbol="ETHUSDT",
            source="dummy-cycle",
            now=pd.Timestamp("2026-07-04T10:16:00Z"),
            refresh_data=lambda: None,
            score_loader=lambda: 1.0,
        )
    )

    assert result == "skipped_stale_state"
    with connect(db_path) as conn:
        assert conn.execute("select count(*) from trade_intents").fetchone()[0] == 0
        key = signal_processed_key("dummy-cycle", "ETHUSDT", "15m", "2026-07-04T10:00:00Z")
        assert get_executor_state(conn, key) is None


def test_run_once_is_idempotent_per_closed_bar(tmp_path):
    db_path = tmp_path / "prodigy.sqlite"
    with connect(db_path) as conn:
        init_db(conn)
        conn.execute(
            """
            insert into equity_snapshots (
              snapshot_id, created_at, equity, available_margin, unrealized_pnl, realized_pnl_24h
            ) values ('s1', '2026-07-04T10:15:30Z', 1000, 1000, 0, 0)
            """
        )
        conn.commit()

    cfg = RunOnceConfig(
        db_path=db_path,
        data_root=tmp_path / "data",
        research_symbol="ETH/USDT:USDT",
        exchange_symbol="ETHUSDT",
        source="dummy-cycle",
        now=pd.Timestamp("2026-07-04T10:16:00Z"),
        refresh_data=lambda: None,
        score_loader=lambda: 1.0,
    )

    assert run_once(cfg) == "open_intent_written"
    assert run_once(cfg) == "already_processed"

    with connect(db_path) as conn:
        assert conn.execute("select count(*) from trade_intents").fetchone()[0] == 1


def test_run_once_skips_manual_override(tmp_path):
    db_path = tmp_path / "prodigy.sqlite"
    with connect(db_path) as conn:
        init_db(conn)
        conn.execute(
            """
            insert into equity_snapshots (
              snapshot_id, created_at, equity, available_margin, unrealized_pnl, realized_pnl_24h
            ) values ('s1', '2026-07-04T10:15:30Z', 1000, 1000, 0, 0)
            """
        )
        set_executor_state(conn, "manual_override:ETHUSDT", "active", "2026-07-04T10:15:30Z")
        conn.commit()

    result = run_once(
        RunOnceConfig(
            db_path=db_path,
            data_root=tmp_path / "data",
            research_symbol="ETH/USDT:USDT",
            exchange_symbol="ETHUSDT",
            source="dummy-cycle",
            now=pd.Timestamp("2026-07-04T10:16:00Z"),
            refresh_data=lambda: None,
            score_loader=lambda: 1.0,
        )
    )

    assert result == "skipped_manual_override"
    with connect(db_path) as conn:
        key = signal_processed_key("dummy-cycle", "ETHUSDT", "15m", "2026-07-04T10:00:00Z")
        assert get_executor_state(conn, key) is None


from prodigy.data.parquet_store import write_daily_partition
from prodigy.signals.daemon import load_example_score


def test_load_example_score_reads_parquet_and_uses_closed_bar(tmp_path):
    ts = pd.date_range("2026-07-04T09:00:00Z", periods=8, freq="15min")
    ohlcv = pd.DataFrame(
        {
            "timestamp": ts,
            "symbol": ["ETH/USDT:USDT"] * len(ts),
            "open": [100, 101, 102, 103, 104, 105, 106, 107],
            "high": [101, 102, 103, 104, 105, 106, 107, 108],
            "low": [99, 100, 101, 102, 103, 104, 105, 106],
            "close": [100, 102, 104, 106, 108, 110, 112, 114],
            "volume": [10] * len(ts),
        }
    )
    funding = pd.DataFrame(
        {
            "timestamp": ts,
            "symbol": ["ETH/USDT:USDT"] * len(ts),
            "funding_rate": [0.0001] * len(ts),
        }
    )
    write_daily_partition(
        ohlcv,
        data_root=tmp_path,
        exchange="bitget",
        symbol="ETH/USDT:USDT",
        dataset="ohlcv",
        date="2026-07-04",
        timeframe="15m",
    )
    write_daily_partition(
        funding,
        data_root=tmp_path,
        exchange="bitget",
        symbol="ETH/USDT:USDT",
        dataset="funding_rates",
        date="2026-07-04",
    )

    score = load_example_score(
        data_root=tmp_path,
        research_symbol="ETH/USDT:USDT",
        now=pd.Timestamp("2026-07-04T11:00:00Z"),
        timeframe="15m",
    )

    assert -1.0 <= score <= 1.0


def test_run_once_closes_position_via_holding_expiry_from_opened_at(tmp_path):
    # opened_at far enough back that holding_bars >= max_holding_bars (96); score
    # below profit_hold threshold so decide_intent's expiry-profit branch fires.
    db_path = tmp_path / "prodigy.sqlite"
    with connect(db_path) as conn:
        init_db(conn)
        conn.execute(
            """
            insert into equity_snapshots (
              snapshot_id, created_at, equity, available_margin, unrealized_pnl, realized_pnl_24h
            ) values ('s1', '2026-07-04T10:15:30Z', 1000, 1000, 0, 0)
            """
        )
        conn.execute(
            """
            insert into positions (symbol, side, notional, entry_price, unrealized_pnl, updated_at, opened_at)
            values ('ETHUSDT', 'long', 100.0, 100.0, 1.0, '2026-07-04T10:15:30Z', '2026-07-01T00:00:00Z')
            """
        )
        conn.commit()

    result = run_once(
        RunOnceConfig(
            db_path=db_path,
            data_root=tmp_path / "data",
            research_symbol="ETH/USDT:USDT",
            exchange_symbol="ETHUSDT",
            source="dummy-cycle",
            now=pd.Timestamp("2026-07-04T10:16:00Z"),
            refresh_data=lambda: None,
            score_loader=lambda: 0.1,
        )
    )

    assert result == "close_intent_written"
    with connect(db_path) as conn:
        row = conn.execute(
            "select action, side, target_notional, reason from trade_intents"
        ).fetchone()
    assert dict(row) == {
        "action": "close",
        "side": "long",
        "target_notional": 0.0,
        "reason": "holding_expiry_profit",
    }


def test_run_once_does_not_guess_holding_bars_when_opened_at_missing(tmp_path):
    # Spec: when opened_at can't be read, daemon skips rather than guessing age.
    db_path = tmp_path / "prodigy.sqlite"
    with connect(db_path) as conn:
        init_db(conn)
        conn.execute(
            """
            insert into equity_snapshots (
              snapshot_id, created_at, equity, available_margin, unrealized_pnl, realized_pnl_24h
            ) values ('s1', '2026-07-04T10:15:30Z', 1000, 1000, 0, 0)
            """
        )
        conn.execute(
            """
            insert into positions (symbol, side, notional, entry_price, unrealized_pnl, updated_at)
            values ('ETHUSDT', 'long', 100.0, 100.0, 1.0, '2026-07-04T10:15:30Z')
            """
        )
        conn.commit()

    result = run_once(
        RunOnceConfig(
            db_path=db_path,
            data_root=tmp_path / "data",
            research_symbol="ETH/USDT:USDT",
            exchange_symbol="ETHUSDT",
            source="dummy-cycle",
            now=pd.Timestamp("2026-07-04T10:16:00Z"),
            refresh_data=lambda: None,
            score_loader=lambda: 0.1,
        )
    )

    assert result == "no_signal"
    with connect(db_path) as conn:
        assert conn.execute("select count(*) from trade_intents").fetchone()[0] == 0


def _seed_fresh_equity(db_path):
    """Shared fixture: an equity snapshot fresh enough to pass the stale gate."""
    with connect(db_path) as conn:
        init_db(conn)
        conn.execute(
            """
            insert into equity_snapshots (
              snapshot_id, created_at, equity, available_margin, unrealized_pnl, realized_pnl_24h
            ) values ('s1', '2026-07-04T10:15:30Z', 1000, 1000, 0, 0)
            """
        )
        conn.commit()


def test_run_once_skips_on_data_refresh_error_and_writes_event(tmp_path):
    # Spec error handling: "Data refresh error: write event, skip." Must NOT
    # crash, must NOT write a trade_intent, must NOT mark the bar processed.
    db_path = tmp_path / "prodigy.sqlite"
    _seed_fresh_equity(db_path)

    def boom() -> None:
        raise RuntimeError("refresh failed")

    result = run_once(
        RunOnceConfig(
            db_path=db_path,
            data_root=tmp_path / "data",
            research_symbol="ETH/USDT:USDT",
            exchange_symbol="ETHUSDT",
            source="dummy-cycle",
            now=pd.Timestamp("2026-07-04T10:16:00Z"),
            refresh_data=boom,
            score_loader=lambda: 1.0,
        )
    )

    assert result == "error_data_refresh"
    with connect(db_path) as conn:
        assert conn.execute("select count(*) from trade_intents").fetchone()[0] == 0
        key = signal_processed_key("dummy-cycle", "ETHUSDT", "15m", "2026-07-04T10:00:00Z")
        assert get_executor_state(conn, key) is None
        evt = conn.execute(
            "select severity, component, message from events where component = 'signal'"
        ).fetchone()
    assert evt is not None
    assert evt["severity"] == "warning"
    assert "refresh failed" in evt["message"]


def test_run_once_skips_on_factor_compute_error_and_writes_event(tmp_path):
    # Spec error handling: "Factor compute error: write event, skip."
    db_path = tmp_path / "prodigy.sqlite"
    _seed_fresh_equity(db_path)

    def boom() -> float:
        raise RuntimeError("factor boom")

    result = run_once(
        RunOnceConfig(
            db_path=db_path,
            data_root=tmp_path / "data",
            research_symbol="ETH/USDT:USDT",
            exchange_symbol="ETHUSDT",
            source="dummy-cycle",
            now=pd.Timestamp("2026-07-04T10:16:00Z"),
            refresh_data=lambda: None,
            score_loader=boom,
        )
    )

    assert result == "error_factor_compute"
    with connect(db_path) as conn:
        assert conn.execute("select count(*) from trade_intents").fetchone()[0] == 0
        key = signal_processed_key("dummy-cycle", "ETHUSDT", "15m", "2026-07-04T10:00:00Z")
        assert get_executor_state(conn, key) is None
        evt = conn.execute(
            "select severity, component, message from events where component = 'signal'"
        ).fetchone()
    assert evt is not None
    assert evt["severity"] == "warning"
    assert "factor boom" in evt["message"]
