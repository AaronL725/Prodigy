import os
import subprocess
from pathlib import Path

import pandas as pd

from prodigy.db import connect, init_db
from prodigy.signals.daemon import RunOnceConfig, run_once
from prodigy.signals.intents import TradeIntent, write_trade_intent


def _cargo_env_without_exchange_keys():
    keep = (
        "PATH",
        "HOME",
        "CARGO_HOME",
        "RUSTUP_HOME",
        "CARGO_TARGET_DIR",
        "TMPDIR",
        "TEMP",
        "TMP",
        "RUSTC_WRAPPER",
        "RUSTFLAGS",
        "SSL_CERT_FILE",
        "SSL_CERT_DIR",
    )
    return {key: os.environ[key] for key in keep if key in os.environ}


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
    # The intent must reach a terminal state. When the DEMO book is phantom-liquid,
    # a buy cannot genuinely fill and the executor must FAIL the intent with a
    # clear diagnostic rather than falsely mark it executed. When the book is
    # tradable, status is 'executed'. Either terminal state is honest;
    # 'pending'/'accepted' (stuck) is not.
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
    # with a diagnostic when it is phantom-liquid.
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


def test_live_daemon_without_keys_fails_before_private_calls(tmp_path):
    db_path = tmp_path / "prodigy.sqlite"
    live_env = {
        **_cargo_env_without_exchange_keys(),
        "BITGET_LIVE_API_KEY": "",
        "BITGET_LIVE_API_SECRET": "",
        "BITGET_LIVE_API_PASSPHRASE": "",
        "PRODIGY_LIVE_TRADING_ENABLED": "",
        "PRODIGY_LIVE_CONFIRM": "",
    }

    missing_creds = subprocess.run(
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
        env=live_env,
    )

    assert missing_creds.returncode != 0
    assert "BITGET_LIVE_API_KEY" in missing_creds.stderr
    assert not db_path.exists()


def test_live_daemon_with_fake_keys_missing_enable_fails_before_private_calls(tmp_path):
    db_path = tmp_path / "prodigy.sqlite"
    live_env = {
        **_cargo_env_without_exchange_keys(),
        "BITGET_LIVE_API_KEY": "live-key",
        "BITGET_LIVE_API_SECRET": "live-secret",
        "BITGET_LIVE_API_PASSPHRASE": "live-pass",
        "PRODIGY_LIVE_TRADING_ENABLED": "",
        "PRODIGY_LIVE_CONFIRM": "",
    }

    missing_enable = subprocess.run(
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
        env=live_env,
    )

    assert missing_enable.returncode != 0
    assert "live trading enable flag is required" in missing_enable.stderr
    assert not db_path.exists()


def test_live_daemon_with_pending_intent_fails_clean_state_before_private_calls(tmp_path):
    db_path = tmp_path / "prodigy.sqlite"
    with connect(db_path) as conn:
        init_db(conn)
        write_trade_intent(
            conn,
            TradeIntent(
                intent_id="live-pending-1",
                created_at="2026-07-05T00:00:00Z",
                symbol="ETH/USDT:USDT",
                side="long",
                action="open",
                target_notional=100.0,
                max_order_notional=100.0,
                source="test",
                reason="live startup gate",
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
            "--mode",
            "live",
        ],
        check=False,
        text=True,
        capture_output=True,
        env={
            **_cargo_env_without_exchange_keys(),
            "BITGET_LIVE_API_KEY": "live-key",
            "BITGET_LIVE_API_SECRET": "live-secret",
            "BITGET_LIVE_API_PASSPHRASE": "live-pass",
            "PRODIGY_LIVE_TRADING_ENABLED": "1",
            "PRODIGY_LIVE_CONFIRM": "I_UNDERSTAND_THIS_CAN_TRADE_REAL_MONEY",
        },
    )

    assert result.returncode != 0
    assert "live startup blocked: 1 pending trade intents" in result.stderr
    assert "prodigy executor only supports Bitget demo mode" not in result.stderr
    with connect(db_path) as conn:
        active_keys = {
            row["key"]
            for row in conn.execute(
                """
                select key from executor_state
                where key in (
                  'active_mode',
                  'active_instance_id',
                  'active_started_at',
                  'active_heartbeat_at'
                )
                """
            ).fetchall()
        }
    assert active_keys == set()


def test_live_dry_validate_needs_no_keys_and_leaves_no_active_executor(tmp_path):
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
            "--mode",
            "live",
            "--dry-validate",
        ],
        check=True,
        text=True,
        capture_output=True,
        env=_cargo_env_without_exchange_keys(),
    )

    assert "live dry validation passed" in result.stdout
    with connect(db_path) as conn:
        init_db(conn)
        assert (
            conn.execute(
                "select value from executor_state where key = 'active_mode'"
            ).fetchone()
            is None
        )
        assert (
            conn.execute(
                "select value from executor_state where key = 'active_instance_id'"
            ).fetchone()
            is None
        )


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


def test_m7_live_readiness_scope_scan_targets_dangerous_patterns_only():
    repo_root = Path(__file__).resolve().parents[1]
    dangerous_patterns = (
        "remote_open|open_from_telegram|"
        "TELEGRAM_LIVE|BITGET_LIVE|ENABLE_LIVE_TRADING|LIVE_TRADING_ENABLED|"
        "allow_live_trading|live_trading_enabled|live_order_execution_enabled|"
        "TELEGRAM_PARAM|remote_param|set_param_from_telegram|"
        "remote_shell|shell_from_telegram|"
        "model_debug_from_telegram|remote_model_debug"
    )
    dangerous = subprocess.run(
        ["rg", "-n", dangerous_patterns, "src", "crates"],
        check=False,
        text=True,
        capture_output=True,
        cwd=repo_root,
    )
    assert dangerous.returncode in (0, 1), dangerous.stdout + dangerous.stderr

    def rust_cfg_test_ranges(path):
        lines = path.read_text().splitlines()
        ranges = []
        line_index = 0
        while line_index < len(lines):
            if "#[cfg(test)]" not in lines[line_index]:
                line_index += 1
                continue

            start = line_index + 1
            item_index = line_index + 1
            while item_index < len(lines) and (
                not lines[item_index].strip()
                or lines[item_index].lstrip().startswith("#[")
            ):
                item_index += 1

            if item_index == len(lines):
                ranges.append((start, len(lines)))
                break

            depth = 0
            opened = False
            end = item_index + 1
            for end_index in range(item_index, len(lines)):
                line = lines[end_index]
                depth += line.count("{") - line.count("}")
                opened = opened or "{" in line
                if opened and depth <= 0:
                    end = end_index + 1
                    break
                if not opened and line.rstrip().endswith(";"):
                    end = end_index + 1
                    break
            else:
                end = len(lines)

            ranges.append((start, end))
            line_index = end

        return ranges

    cfg_test_ranges = {}
    m8_live_env_keys = (
        "BITGET_LIVE_API_KEY",
        "BITGET_LIVE_API_SECRET",
        "BITGET_LIVE_API_PASSPHRASE",
        "PRODIGY_LIVE_TRADING_ENABLED",
    )
    production_hits = []
    for hit in dangerous.stdout.splitlines():
        path_text, line_text, _ = hit.split(":", 2)
        path = repo_root / path_text
        line_no = int(line_text)
        if path.suffix == ".rs":
            cfg_test_ranges.setdefault(path, rust_cfg_test_ranges(path))
            if any(start <= line_no <= end for start, end in cfg_test_ranges[path]):
                continue
        if path_text == "crates/executor/src/main.rs" and any(
            key in hit for key in m8_live_env_keys
        ):
            continue
        production_hits.append(hit)
    assert not production_hits, "\n".join(production_hits)

    config_rs = (repo_root / "crates/executor/src/config.rs").read_text()

    # M8 Task 1 allows live websocket URLs for exact profile validation. The
    # dangerous scan above is targeted at enablement patterns.
    assert "TradingMode::Live" in config_rs
    assert "wss://ws.bitget.com/v2/ws/public" in config_rs
    assert "wss://ws.bitget.com/v2/ws/private" in config_rs

    telegram_query = (repo_root / "crates/executor/src/telegram_query.rs").read_text()
    for forbidden in ("BitgetRestClient", "/api/v2", "place-order", "cancel-order"):
        assert forbidden not in telegram_query

    daemon_rs = (repo_root / "crates/executor/src/daemon.rs").read_text()
    telegram_loop = daemon_rs.split("pub async fn run_telegram_query_loop", 1)[1]
    telegram_loop = telegram_loop.split("/// Record a `websocket_auth_failed`", 1)[0]
    for forbidden in ("BitgetRestClient", "/api/v2", "place-order", "cancel-order"):
        assert forbidden not in telegram_loop

    m75_forbidden = subprocess.run(
        [
            "rg",
            "-n",
            "tgux:open|tgux:set_param|tgux:model_debug|tgux:shell|tgux:live|remote_shell|model_debug_from_telegram",
            "src",
            "crates",
        ],
        check=False,
        text=True,
        capture_output=True,
        cwd=repo_root,
    )
    assert m75_forbidden.returncode in (0, 1), (
        m75_forbidden.stdout + m75_forbidden.stderr
    )
    m75_production_hits = []
    for hit in m75_forbidden.stdout.splitlines():
        path_text, line_text, _ = hit.split(":", 2)
        path = repo_root / path_text
        line_no = int(line_text)
        if path.suffix == ".rs":
            cfg_test_ranges.setdefault(path, rust_cfg_test_ranges(path))
            if any(start <= line_no <= end for start, end in cfg_test_ranges[path]):
                continue
        m75_production_hits.append(hit)
    assert not m75_production_hits, "\n".join(m75_production_hits)
