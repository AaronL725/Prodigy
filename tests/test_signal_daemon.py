import pandas as pd
import sqlite3

from prodigy.signals.daemon import (
    PositionState,
    SignalDaemonConfig,
    SignalDecision,
    combine_example_score,
    decide_intent,
    latest_closed_bar,
)


def _clock(ts: str = "2026-07-04T10:16:00Z"):
    """Deterministic clock for run_once tests: freshness judged at this fixed
    time so a seeded snapshot's age is stable (production uses real wall-clock).
    """
    return lambda: pd.Timestamp(ts)


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
            clock=_clock(),
            now=pd.Timestamp("2026-07-04T10:16:00Z"),
            refresh_data=lambda: None,
            score_loader=lambda: (1.0, "2026-07-04T10:00:00Z"),
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
        clock=_clock(),
        now=pd.Timestamp("2026-07-04T10:16:00Z"),
        refresh_data=lambda: None,
        score_loader=lambda: (1.0, "2026-07-04T10:00:00Z"),
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
            clock=_clock(),
            now=pd.Timestamp("2026-07-04T10:16:00Z"),
            refresh_data=lambda: None,
            score_loader=lambda: (1.0, "2026-07-04T10:00:00Z"),
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

    score, closed_ts = load_example_score(
        data_root=tmp_path,
        research_symbol="ETH/USDT:USDT",
        now=pd.Timestamp("2026-07-04T11:00:00Z"),
        timeframe="15m",
    )

    assert -1.0 <= score <= 1.0
    # The latest data bar is 10:45 (8 bars from 09:00, 15m each); at now=11:00
    # the expected closed bar is also 10:45, so the loader reports that bar.
    assert closed_ts == "2026-07-04T10:45:00Z"


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
            clock=_clock(),
            now=pd.Timestamp("2026-07-04T10:16:00Z"),
            refresh_data=lambda: None,
            score_loader=lambda: (0.1, "2026-07-04T10:00:00Z"),
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


def test_run_once_skips_ambiguous_position_when_opened_at_missing(tmp_path):
    # Spec: when a position exists but its age (opened_at) can't be read
    # reliably, the daemon SKIPS rather than guessing — a transient skip that
    # must NOT write the processed marker (the bar is re-evaluated next run).
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
            clock=_clock(),
            now=pd.Timestamp("2026-07-04T10:16:00Z"),
            refresh_data=lambda: None,
            score_loader=lambda: (0.1, "2026-07-04T10:00:00Z"),
        )
    )

    assert result == "skipped_ambiguous_position"
    with connect(db_path) as conn:
        assert conn.execute("select count(*) from trade_intents").fetchone()[0] == 0
        key = signal_processed_key("dummy-cycle", "ETHUSDT", "15m", "2026-07-04T10:00:00Z")
        assert get_executor_state(conn, key) is None


def test_run_once_skips_ambiguous_position_when_opened_at_unparseable(tmp_path):
    # Same skip when opened_at is present but can't be parsed — age is still
    # unreadable, so the daemon must not guess.
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
            insert into positions (symbol, side, notional, entry_price, unrealized_pnl, opened_at, updated_at)
            values ('ETHUSDT', 'long', 100.0, 100.0, 1.0, 'not-a-timestamp', '2026-07-04T10:15:30Z')
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
            clock=_clock(),
            now=pd.Timestamp("2026-07-04T10:16:00Z"),
            refresh_data=lambda: None,
            score_loader=lambda: (0.1, "2026-07-04T10:00:00Z"),
        )
    )

    assert result == "skipped_ambiguous_position"
    with connect(db_path) as conn:
        assert conn.execute("select count(*) from trade_intents").fetchone()[0] == 0
        key = signal_processed_key("dummy-cycle", "ETHUSDT", "15m", "2026-07-04T10:00:00Z")
        assert get_executor_state(conn, key) is None


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
            clock=_clock(),
            now=pd.Timestamp("2026-07-04T10:16:00Z"),
            refresh_data=boom,
            score_loader=lambda: (1.0, "2026-07-04T10:00:00Z"),
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
            clock=_clock(),
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


def test_run_once_skips_when_data_bar_is_stale(tmp_path):
    # The processed key is derived from `now` (expected closed bar 10:00), but
    # the data layer may only have an older closed bar (09:45). The daemon must
    # NOT write a 10:00 marker/intent from 09:45 data — it skips as stale data.
    # score_loader returns (score, actual_closed_bar_ts).
    db_path = tmp_path / "prodigy.sqlite"
    _seed_fresh_equity(db_path)

    result = run_once(
        RunOnceConfig(
            db_path=db_path,
            data_root=tmp_path / "data",
            research_symbol="ETH/USDT:USDT",
            exchange_symbol="ETHUSDT",
            source="dummy-cycle",
            clock=_clock(),
            now=pd.Timestamp("2026-07-04T10:16:00Z"),
            refresh_data=lambda: None,
            score_loader=lambda: (1.0, "2026-07-04T09:45:00Z"),
        )
    )

    assert result == "skipped_stale_data"
    with connect(db_path) as conn:
        assert conn.execute("select count(*) from trade_intents").fetchone()[0] == 0
        key = signal_processed_key("dummy-cycle", "ETHUSDT", "15m", "2026-07-04T10:00:00Z")
        assert get_executor_state(conn, key) is None
        evt = conn.execute(
            "select severity, component, message from events where component = 'signal'"
        ).fetchone()
    assert evt is not None
    assert "09:45" in evt["message"]


def test_run_once_writes_intent_when_data_bar_matches_expected(tmp_path):
    # When score_loader reports the actual closed bar equals the expected one
    # (10:00 at now=10:16), the daemon proceeds normally.
    db_path = tmp_path / "prodigy.sqlite"
    _seed_fresh_equity(db_path)

    result = run_once(
        RunOnceConfig(
            db_path=db_path,
            data_root=tmp_path / "data",
            research_symbol="ETH/USDT:USDT",
            exchange_symbol="ETHUSDT",
            source="dummy-cycle",
            clock=_clock(),
            now=pd.Timestamp("2026-07-04T10:16:00Z"),
            refresh_data=lambda: None,
            score_loader=lambda: (1.0, "2026-07-04T10:00:00Z"),
        )
    )

    assert result == "open_intent_written"


def test_run_once_rechecks_manual_override_after_refresh(tmp_path):
    # TOCTOU guard: gates run on conn1, then refresh+score (which can take
    # seconds), then the write happens on conn2. If a human flips
    # manual_override to active DURING refresh, the write path must re-check and
    # skip — not write an open intent. The processed marker must NOT be written
    # (transient skip), so the bar is re-evaluated next run.
    db_path = tmp_path / "prodigy.sqlite"
    _seed_fresh_equity(db_path)

    def refresh_data() -> None:
        # Simulate a human touching the symbol mid-refresh.
        with connect(db_path) as conn:
            init_db(conn)
            set_executor_state(conn, "manual_override:ETHUSDT", "active", "2026-07-04T10:15:40Z")
            conn.commit()

    result = run_once(
        RunOnceConfig(
            db_path=db_path,
            data_root=tmp_path / "data",
            research_symbol="ETH/USDT:USDT",
            exchange_symbol="ETHUSDT",
            source="dummy-cycle",
            clock=_clock(),
            now=pd.Timestamp("2026-07-04T10:16:00Z"),
            refresh_data=refresh_data,
            score_loader=lambda: (1.0, "2026-07-04T10:00:00Z"),
        )
    )

    assert result == "skipped_manual_override"
    with connect(db_path) as conn:
        assert conn.execute("select count(*) from trade_intents").fetchone()[0] == 0
        key = signal_processed_key("dummy-cycle", "ETHUSDT", "15m", "2026-07-04T10:00:00Z")
        assert get_executor_state(conn, key) is None


def test_run_once_rechecks_pending_intent_after_refresh(tmp_path):
    # Same TOCTOU guard for the pending-intent gate: an intent that lands during
    # refresh must block a second open intent for the same symbol.
    db_path = tmp_path / "prodigy.sqlite"
    _seed_fresh_equity(db_path)

    def refresh_data() -> None:
        with connect(db_path) as conn:
            init_db(conn)
            conn.execute(
                """
                insert into trade_intents (
                  intent_id, created_at, symbol, side, action, target_notional,
                  max_order_notional, status, source, reason, model_version
                ) values ('pre', '2026-07-04T10:15:40Z', 'ETHUSDT', 'long', 'open',
                          100, 100, 'pending', 'other', 'x', 'm')
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
            clock=_clock(),
            now=pd.Timestamp("2026-07-04T10:16:00Z"),
            refresh_data=refresh_data,
            score_loader=lambda: (1.0, "2026-07-04T10:00:00Z"),
        )
    )

    assert result == "skipped_pending_intent"
    with connect(db_path) as conn:
        # Only the pre-existing pending intent; no new intent from this run.
        assert conn.execute("select count(*) from trade_intents").fetchone()[0] == 1
        key = signal_processed_key("dummy-cycle", "ETHUSDT", "15m", "2026-07-04T10:00:00Z")
        assert get_executor_state(conn, key) is None


def test_run_once_does_not_crash_when_event_write_fails(tmp_path, monkeypatch):
    # Spec error handling: the event write on a refresh/factor error is
    # best-effort. If SQLite is busy/locked and the event write fails, the
    # daemon must STILL return the skip string — not crash. Probe by making the
    # event write raise.
    import prodigy.signals.daemon as daemon_mod

    db_path = tmp_path / "prodigy.sqlite"
    _seed_fresh_equity(db_path)

    def boom(_conn, _severity, _message):
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(daemon_mod, "write_signal_event", boom)

    def refresh_data() -> None:
        raise RuntimeError("refresh failed")

    result = run_once(
        RunOnceConfig(
            db_path=db_path,
            data_root=tmp_path / "data",
            research_symbol="ETH/USDT:USDT",
            exchange_symbol="ETHUSDT",
            source="dummy-cycle",
            clock=_clock(),
            now=pd.Timestamp("2026-07-04T10:16:00Z"),
            refresh_data=refresh_data,
            score_loader=lambda: (1.0, "2026-07-04T10:00:00Z"),
        )
    )

    # Must return the skip, not propagate the OperationalError.
    assert result == "error_data_refresh"
    with connect(db_path) as conn:
        assert conn.execute("select count(*) from trade_intents").fetchone()[0] == 0
        key = signal_processed_key("dummy-cycle", "ETHUSDT", "15m", "2026-07-04T10:00:00Z")
        assert get_executor_state(conn, key) is None


def test_run_once_skips_when_state_goes_stale_during_refresh(tmp_path):
    # TOCTOU for the FRESHNESS gate: the snapshot is fresh at run-start, but
    # refresh/score take real seconds and the snapshot can age past
    # max_state_age_secs by the time the write connection re-checks. The
    # re-check must use the LIVE clock (after refresh), not the run-start now —
    # otherwise a snapshot that went stale mid-run still passes and an intent is
    # written. Inject a clock that advances past the window across the run.
    db_path = tmp_path / "prodigy.sqlite"
    t0 = pd.Timestamp("2026-07-04T10:15:30Z")
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

    # Freshness window is 2s. Pre-check at t0 (0s old -> fresh). refresh sleeps,
    # simulating real elapsed time; the re-check clock returns t0+10s (snapshot
    # now 10s old -> stale).
    clock_calls = {"n": 0}

    def clock() -> pd.Timestamp:
        clock_calls["n"] += 1
        # First call (pre-check) -> run-start time; later calls (re-check) -> aged.
        return t0 if clock_calls["n"] == 1 else t0 + pd.Timedelta(seconds=10)

    def refresh_data() -> None:
        # Simulate the seconds refresh/score take in real operation.
        import time as _time

        _time.sleep(0.05)

    result = run_once(
        RunOnceConfig(
            db_path=db_path,
            data_root=tmp_path / "data",
            research_symbol="ETH/USDT:USDT",
            exchange_symbol="ETHUSDT",
            source="dummy-cycle",
            now=pd.Timestamp("2026-07-04T10:16:00Z"),
            refresh_data=refresh_data,
            score_loader=lambda: (1.0, "2026-07-04T10:00:00Z"),
            max_state_age_secs=2,
            clock=clock,
        )
    )

    assert result == "skipped_stale_state"
    with connect(db_path) as conn:
        assert conn.execute("select count(*) from trade_intents").fetchone()[0] == 0
        key = signal_processed_key("dummy-cycle", "ETHUSDT", "15m", "2026-07-04T10:00:00Z")
        assert get_executor_state(conn, key) is None
