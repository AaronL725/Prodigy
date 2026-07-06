import subprocess
from pathlib import Path

import pandas as pd

from prodigy.db import connect, init_db
from prodigy.signals.daemon import RunOnceConfig, run_once
from prodigy.signals.intents import TradeIntent, write_trade_intent


def _demo_depth_diagnostic():
    """Fetch the DEMO merge-depth (paptrading:1) and print best bid/ask + spread.

    The demo ETHUSDT book is frequently phantom-liquid (best ask/bid far apart,
    beyond the exchange price-limit band); a wide spread explains why a market
    buy is accepted then cancelled with no fill. Printed only as a diagnostic so
    a non-fill run isn't a mystery. Best-effort: never fails the test on its own.
    """
    try:
        out = subprocess.run(
            [
                "curl",
                "-s",
                "-H",
                "paptrading: 1",
                "https://api.bitget.com/api/v2/mix/market/merge-depth"
                "?productType=usdt-futures&symbol=ETHUSDT&limit=5&precision=scale0",
            ],
            check=False,
            text=True,
            capture_output=True,
            timeout=10,
        ).stdout
        import json

        data = json.loads(out).get("data", {})
        asks = data.get("asks") or [[]]
        bids = data.get("bids") or [[]]
        ba, sa = (asks[0][0] if asks[0] else "?"), (bids[0][0] if bids[0] else "?")
        print(f"demo merge-depth: best_ask={ba} best_bid={sa}")
    except Exception as exc:  # noqa: BLE001 - diagnostic only
        print(f"demo merge-depth diagnostic skipped: {exc}")


def test_rust_demo_executor_processes_pending_intent(tmp_path):
    _demo_depth_diagnostic()
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
        # Anti-double-count: the per-trade fills ledger (sourced from the exchange
        # fillList by reconcile) must never SUM to more than the orders' filled_size
        # total. The execution path no longer writes fills from order-detail
        # cumulative baseVolume, so a later fillList repair can't inflate the base.
        fills_size_total = conn.execute(
            "select coalesce(sum(size), 0) from fills"
        ).fetchone()[0]
        orders_filled_total = conn.execute(
            "select coalesce(sum(filled_size), 0) from orders"
        ).fetchone()[0]
        event_count = conn.execute("select count(*) from events").fetchone()[0]

    assert "processed intent-1" in result.stdout
    # The intent must reach a terminal state. When the DEMO book is phantom-
    # liquid (see the demo merge-depth diagnostic above — best ask/bid far apart,
    # beyond the exchange price-limit band) a buy cannot genuinely fill and the
    # executor must FAIL the intent with a clear diagnostic rather than falsely
    # mark it executed. When the book is tradable, status is 'executed'. Either
    # terminal state is honest; 'pending'/'accepted' (stuck) is not.
    assert intent["status"] in ("executed", "failed"), (
        f"expected a terminal state (executed|failed), got {intent['status']}"
    )
    if intent["status"] == "failed":
        assert intent["error"], "a failed intent must record a diagnostic error"
    assert order_count >= 1, "expected at least one demo order to be attempted"
    assert false_fills == 0, "an order must not be marked filled with no fill"
    # fills are populated per-trade from fillList by reconcile, which may run
    # after an in-processing fill — so a filled order may not yet have its fills
    # row on a single run, and one filled order can legitimately have SEVERAL
    # fill rows (multiple trades). The robust anti-double-count invariant is the
    # size one below: the fills ledger never sums above the orders' filled_size.
    assert fills_size_total <= orders_filled_total + 1e-9, (
        f"fills ledger ({fills_size_total}) must not exceed orders filled_size "
        f"total ({orders_filled_total}) — would indicate a double-count"
    )
    assert event_count >= 1


def test_rust_demo_daemon_processes_pending_intent_once(tmp_path):
    _demo_depth_diagnostic()
    db_path = tmp_path / "prodigy.sqlite"

    with connect(db_path) as conn:
        init_db(conn)
        write_trade_intent(
            conn,
            TradeIntent(
                intent_id="daemon-intent-1",
                created_at="2026-07-03T00:00:00Z",
                symbol="ETH/USDT:USDT",
                side="long",
                action="open",
                target_notional=100.0,
                max_order_notional=100.0,
                source="test",
                reason="daemon integration",
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
            "--daemon",
            "--max-runtime-ms",
            "30000",
            "--test-reset-demo-state",
        ],
        check=True,
        text=True,
        capture_output=True,
        # Startup reconcile + set-leverage against the real demo API take ~7s
        # before the loop starts, and a single intent-processing tick can run
        # ~30-40s: the maker open path waits open_maker_timeout_seconds (15s)
        # per attempt, refreshes, then falls back to a taker that the phantom
        # demo book often rejects, finally failing "left to reconcile". The
        # bounded-runtime check fires between ticks, so the first tick runs to
        # the intent's terminal state regardless of max-runtime-ms; the bound
        # only governs when the *next* tick exits. 90s leaves room for compile
        # + startup + one full processing tick.
        timeout=90,
    )

    with connect(db_path) as conn:
        intent = conn.execute(
            "select status, error from trade_intents where intent_id = 'daemon-intent-1'"
        ).fetchone()
        order_count = conn.execute("select count(*) from orders").fetchone()[0]
        event_count = conn.execute("select count(*) from events").fetchone()[0]
        daemon_events = {
            row["message"]
            for row in conn.execute(
                "select message from events where component = 'daemon'"
            ).fetchall()
        }
        # No zero-fill order may be marked filled — the M4 anti-false-fill
        # invariant, mirrored from the --once test.
        false_fills = conn.execute(
            "select count(*) from orders where status = 'filled' and filled_size <= 0"
        ).fetchone()[0]

    # Honest terminal state: executed when the demo book is tradable, failed
    # with a diagnostic when it is phantom-liquid (see _demo_depth_diagnostic).
    # The bounded daemon runtime must actually start, run until the bound, and
    # exit after writing its daemon events; it must not leave the intent accepted
    # or pending as a weakened "runtime only" smoke.
    assert intent["status"] in ("executed", "failed"), (
        f"expected executed|failed, got {intent['status']}"
    )
    if intent["status"] == "failed":
        assert intent["error"], "a failed intent must record a diagnostic error"
    assert order_count >= 1, "expected at least one demo order to be attempted"
    assert event_count >= 1, "daemon must record startup + reconcile + intent events"
    assert {"daemon started", "bounded daemon runtime elapsed", "daemon stopped"} <= daemon_events
    assert false_fills == 0, "an order must not be marked filled with no fill"
    assert "daemon" in result.stdout or result.stderr == ""


def test_rust_daemon_rejects_live_mode(tmp_path):
    db_path = tmp_path / "prodigy.sqlite"
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
            "--daemon",
            "--mode",
            "live",
        ],
        check=False,
        text=True,
        capture_output=True,
    )

    assert result.returncode != 0
    assert "only supports --mode demo" in result.stderr


def test_signal_run_once_writes_intent_for_executor(tmp_path):
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

    result = run_once(
        RunOnceConfig(
            db_path=db_path,
            data_root=tmp_path / "data",
            research_symbol="ETH/USDT:USDT",
            exchange_symbol="ETHUSDT",
            source="dummy-cycle",
            clock=lambda: pd.Timestamp("2026-07-04T10:16:00Z"),
            now=pd.Timestamp("2026-07-04T10:16:00Z"),
            refresh_data=lambda: None,
            score_loader=lambda: (1.0, "2026-07-04T10:00:00Z"),
        )
    )

    with connect(db_path) as conn:
        row = conn.execute(
            "select status, symbol, action, side from trade_intents"
        ).fetchone()

    assert result == "open_intent_written"
    assert dict(row) == {
        "status": "pending",
        "symbol": "ETHUSDT",
        "action": "open",
        "side": "long",
    }


def test_m6_scope_scan_has_no_remote_open_or_live_enablement():
    repo_root = Path(__file__).resolve().parents[1]
    dangerous = subprocess.run(
        [
            "rg",
            "-n",
            (
                "remote_open|open_from_telegram|TELEGRAM_LIVE|BITGET_LIVE|"
                "TELEGRAM_PARAM|remote_shell|shell_from_telegram|model_debug_from_telegram"
            ),
            "src",
            "crates",
        ],
        check=False,
        text=True,
        capture_output=True,
        cwd=repo_root,
    )
    assert dangerous.returncode == 1, dangerous.stdout + dangerous.stderr

    config_rs = (repo_root / "crates/executor/src/config.rs").read_text()
    production_config, test_config = config_rs.split("#[cfg(test)]", 1)
    assert "ws.bitget.com" not in production_config
    assert "wss://ws.bitget.com/v2/ws/public" in test_config
    assert "wss://ws.bitget.com/v2/ws/private" in test_config

    telegram_query = (repo_root / "crates/executor/src/telegram_query.rs").read_text()
    for forbidden in ("BitgetRestClient", "/api/v2", "place-order", "cancel-order"):
        assert forbidden not in telegram_query
