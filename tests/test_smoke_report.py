from __future__ import annotations

import subprocess
import urllib.error
import urllib.request
from datetime import UTC, datetime

import pytest

from prodigy.db import connect, init_db
from prodigy.signals.state import get_executor_state, set_executor_state
from prodigy.smoke.report import build_smoke_report, write_smoke_report


def _seed_smoke_db(db_path):
    with connect(db_path) as conn:
        init_db(conn)
        conn.execute(
            """
            insert into trade_intents (
              intent_id, created_at, symbol, side, action, target_notional,
              max_order_notional, status, source, reason, model_version
            ) values ('intent-1', '2026-07-06T00:05:00Z', 'ETHUSDT', 'long', 'open',
                      100, 100, 'pending', 'test', 'demo', 'm6')
            """
        )
        conn.execute(
            """
            insert into control_commands (
              command_id, created_at, command, status, requested_by
            ) values ('cmd-1', '2026-07-06T00:10:00Z', 'stop', 'executed', '123')
            """
        )
        conn.execute(
            """
            insert into events (event_id, created_at, severity, component, message, payload_json)
            values ('evt-1', '2026-07-06T00:20:00Z', 'warning', 'executor',
                    'demo warning recorded', '{}')
            """
        )
        conn.execute(
            """
            insert into positions (
              symbol, side, notional, entry_price, unrealized_pnl, updated_at,
              ownership, opened_at, raw_json
            ) values ('ETHUSDT', 'long', 100, 2000, 1, '2026-07-06T00:25:00Z',
                      'imported', '2026-07-06T00:25:00Z', '{}')
            """
        )
        conn.execute(
            """
            insert into orders (
              order_id, client_oid, intent_id, symbol, side, action, order_type,
              status, price, size, filled_size, created_at, updated_at
            ) values ('order-1', 'client-1', 'intent-1', 'ETHUSDT', 'buy', 'open',
                      'limit', 'submitted', 2000, 0.1, 0,
                      '2026-07-06T00:26:00Z', '2026-07-06T00:26:00Z')
            """
        )
        conn.commit()


def _seed_m6_detail_db(db_path):
    with connect(db_path) as conn:
        init_db(conn)
        conn.execute(
            """
            insert into trade_intents (
              intent_id, created_at, symbol, side, action, target_notional,
              max_order_notional, status, source, reason, model_version,
              processed_at, error
            ) values
              ('intent-pending', '2026-07-06T00:05:00Z', 'ETHUSDT', 'long', 'open',
               100, 100, 'pending', 'test', 'demo', 'm6', null, null),
              ('intent-terminal', '2026-07-06T00:06:00Z', 'ETHUSDT', 'long', 'open',
               100, 100, 'executed', 'test', 'demo', 'm6',
               '2026-07-06T00:07:00Z', null)
            """
        )
        conn.execute(
            """
            insert into control_commands (
              command_id, created_at, command, status, requested_by, processed_at, error
            ) values
              ('cmd-pending', '2026-07-06T00:10:00Z', 'stop', 'pending', '123', null, null),
              ('cmd-ok', '2026-07-06T00:11:00Z', 'resume', 'executed', '123',
               '2026-07-06T00:12:00Z', null),
              ('cmd-failed', '2026-07-06T00:13:00Z', 'close_all', 'failed', '123',
               '2026-07-06T00:14:00Z', 'demo reject')
            """
        )
        conn.execute(
            """
            insert into orders (
              order_id, client_oid, intent_id, symbol, side, action, order_type,
              status, price, size, filled_size, created_at, updated_at
            ) values ('order-open', 'client-open', 'intent-pending', 'ETHUSDT', 'buy', 'open',
                      'limit', 'submitted', 2000, 0.1, 0.04,
                      '2026-07-06T00:26:00Z', '2026-07-06T00:26:00Z')
            """
        )
        conn.execute(
            """
            insert into fills (
              fill_id, order_id, symbol, side, price, size, fee, created_at, client_oid
            ) values ('fill-1', 'order-open', 'ETHUSDT', 'buy', 2010, 0.04, 0.02,
                      '2026-07-06T00:27:00Z', 'client-open')
            """
        )
        conn.execute(
            """
            insert into positions (
              symbol, side, notional, entry_price, unrealized_pnl, updated_at,
              ownership, opened_at, raw_json
            ) values ('ETHUSDT', 'long', 100, 2000, 1.5, '2026-07-06T00:25:00Z',
                      'system', '2026-07-06T00:25:00Z', '{}')
            """
        )
        conn.execute(
            """
            insert into equity_snapshots (
              snapshot_id, created_at, equity, available_margin, unrealized_pnl,
              realized_pnl_24h
            ) values ('eq-1', '2026-07-06T00:28:00Z', 1002, 900, 1.5, 0.5)
            """
        )
        conn.execute(
            """
            insert into events (event_id, created_at, severity, component, message, payload_json)
            values
              ('evt-ws', '2026-07-06T00:20:00Z', 'warning', 'ws',
               'websocket reconnect', '{}'),
              ('evt-rest', '2026-07-06T00:21:00Z', 'error', 'rest',
               'rest timeout', '{}'),
              ('evt-telegram', '2026-07-06T00:22:00Z', 'critical', 'telegram',
               'telegram send failed', '{}')
            """
        )
        set_executor_state(conn, "smoke:component:prodigy-executor", "started pid=101", "2026-07-06T00:00:01Z")
        set_executor_state(conn, "smoke:component:prodigy-signal", "early_exit code=1", "2026-07-06T00:00:02Z")
        set_executor_state(conn, "smoke:telegram:queries", "skipped missing telegram credentials", "2026-07-06T00:00:03Z")
        set_executor_state(conn, "smoke:telegram:controls", "skipped missing telegram credentials", "2026-07-06T00:00:04Z")
        conn.commit()


def test_build_smoke_report_summarizes_sqlite_state(tmp_path):
    db_path = tmp_path / "prodigy.sqlite"
    _seed_smoke_db(db_path)

    report = build_smoke_report(
        db_path,
        started_at="2026-07-06T00:00:00Z",
        ended_at="2026-07-06T00:45:00Z",
        duration_minutes=45,
        issues=["executor exited early with code 1"],
    )

    assert "# M6 Demo Smoke Report" in report
    assert "duration_minutes: 45" in report
    assert "- trade_intents_total: 1" in report
    assert "- control_commands_total: 1" in report
    assert "- executed: 1" in report
    assert "- executor exited early with code 1" in report
    assert "- residual positions: 1" in report
    assert "- residual working orders: 1" in report
    assert "2026-07-06T00:20:00Z | warning | executor | demo warning recorded" in report


def test_build_smoke_report_includes_required_m6_sections_and_details(tmp_path):
    db_path = tmp_path / "prodigy.sqlite"
    _seed_m6_detail_db(db_path)

    report = build_smoke_report(
        db_path,
        started_at="2026-07-06T00:00:00Z",
        ended_at="2026-07-06T00:45:00Z",
        duration_minutes=45,
        issues=["sqlite busy", "manual residual review needed"],
    )

    for heading in [
        "## Run",
        "## Component Startup Status",
        "## Telegram Query/Control Checks",
        "## Trade Intents",
        "## Control Commands",
        "## Open Orders",
        "## Fills / Trade Flow",
        "## Positions",
        "## PnL Snapshot",
        "## Warning/Error/Critical Events",
        "## WS/REST/SQLite/Telegram Issues",
        "## Residual Positions / Orders",
        "## Recommended Fixes",
    ]:
        assert heading in report

    assert "- prodigy-executor: started pid=101" in report
    assert "- prodigy-signal: early_exit code=1" in report
    assert "- queries: skipped missing telegram credentials" in report
    assert "- controls: skipped missing telegram credentials" in report
    assert "- pending: 1" in report
    assert "- executed: 1" in report
    assert "- close_all cmd-failed: failed requested_by=123 error=demo reject" in report
    assert "- ETHUSDT buy open submitted size=0.1 filled=0.04 price=2000.0" in report
    assert "- ETHUSDT buy size=0.04 price=2010.0 fee=0.02" in report
    assert "- ETHUSDT long notional=100.0 entry=2000.0 unrealized_pnl=1.5 ownership=system" in report
    assert "- realized_pnl_24h: n/a" in report
    assert "- unrealized_pnl: 1.5" in report
    assert "- total_pnl: n/a" in report
    assert "- sqlite busy" in report
    assert "- ws: websocket reconnect" in report
    assert "- rest: rest timeout" in report
    assert "- telegram: telegram send failed" in report
    assert "- residual positions: 1" in report
    assert "- residual working orders: 1" in report
    assert "- Review listed issues and residual exposure before the next smoke run." in report


def test_write_smoke_report_writes_markdown_and_executor_state(tmp_path):
    db_path = tmp_path / "prodigy.sqlite"
    report_dir = tmp_path / "reports"
    _seed_smoke_db(db_path)

    path = write_smoke_report(
        db_path,
        report_dir,
        started_at="2026-07-06T00:00:00Z",
        ended_at="2026-07-06T00:45:00Z",
        duration_minutes=45,
        issues=[],
    )

    assert path.parent == report_dir
    assert path.suffix == ".md"
    assert path.read_text().startswith("# M6 Demo Smoke Report")
    with connect(db_path) as conn:
        init_db(conn)
        assert get_executor_state(conn, "smoke:last_report") == str(path)
        assert get_executor_state(conn, "smoke:status") == "completed"


def test_build_smoke_report_does_not_invent_total_pnl_from_zero_snapshot(tmp_path):
    db_path = tmp_path / "prodigy.sqlite"
    with connect(db_path) as conn:
        init_db(conn)
        conn.execute(
            """
            insert into equity_snapshots (
              snapshot_id, created_at, equity, available_margin, unrealized_pnl,
              realized_pnl_24h
            ) values ('eq-zero', '2026-07-06T00:28:00Z', 1000, 900, 0, 0)
            """
        )
        conn.commit()

    report = build_smoke_report(
        db_path,
        started_at="2026-07-06T00:00:00Z",
        ended_at="2026-07-06T00:45:00Z",
        duration_minutes=45,
        issues=[],
    )

    assert "- realized_pnl_24h: n/a" in report
    assert "- total_pnl: n/a" in report
    assert "- realized_pnl_24h: 0.0" not in report
    assert "- total_pnl: 0.0" not in report


def test_write_smoke_report_uses_yyyymmdd_hhmm_filename(tmp_path):
    db_path = tmp_path / "prodigy.sqlite"
    report_dir = tmp_path / "reports"
    _seed_smoke_db(db_path)

    path = write_smoke_report(
        db_path,
        report_dir,
        started_at="2026-07-06T00:00:00Z",
        ended_at="2026-07-06T00:45:09Z",
        duration_minutes=45,
        issues=[],
    )

    assert path.name == "m6-demo-smoke-20260706-0045.md"


def test_write_smoke_report_initializes_empty_database(tmp_path):
    db_path = tmp_path / "empty.sqlite"
    report_dir = tmp_path / "reports"

    path = write_smoke_report(
        db_path,
        report_dir,
        started_at="2026-07-06T00:00:00Z",
        ended_at="2026-07-06T00:30:00Z",
        duration_minutes=30,
        issues=[],
    )

    report = path.read_text()
    assert "- trade_intents_total: 0" in report
    with connect(db_path) as conn:
        assert get_executor_state(conn, "smoke:last_report") == str(path)


def test_smoke_cli_validates_duration_and_can_write_skip_start_report(
    tmp_path, capsys, monkeypatch
):
    from prodigy.cli import smoke

    monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
    monkeypatch.delenv("TELEGRAM_ALLOWED_USER_IDS", raising=False)
    monkeypatch.delenv("TELEGRAM_CHAT_ID", raising=False)

    db_path = tmp_path / "prodigy.sqlite"
    report_dir = tmp_path / "reports"
    ticks = [
        datetime(2026, 7, 6, 0, 0, tzinfo=UTC),
        datetime(2026, 7, 6, 0, 30, tzinfo=UTC),
    ]
    slept = []

    with pytest.raises(SystemExit):
        smoke.build_parser().parse_args(["--duration-minutes", "29"])

    code = smoke.main(
        [
            "--db",
            str(db_path),
            "--duration-minutes",
            "30",
            "--report-dir",
            str(report_dir),
            "--skip-start",
        ],
        sleep=lambda seconds: slept.append(seconds),
        clock=lambda: ticks.pop(0),
    )

    out = capsys.readouterr().out.strip()

    assert code == 0
    assert len(slept) == 60
    assert set(slept) == {30}
    assert out.endswith(".md")
    assert (report_dir / out.split("/")[-1]).exists()
    with connect(db_path) as conn:
        assert get_executor_state(conn, "smoke:status") == "completed"


def test_smoke_cli_records_process_kill_after_terminate_timeout(tmp_path, monkeypatch):
    from prodigy.cli import smoke

    monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
    monkeypatch.delenv("TELEGRAM_ALLOWED_USER_IDS", raising=False)
    monkeypatch.delenv("TELEGRAM_CHAT_ID", raising=False)

    class HangingProcess:
        pid = 999999
        returncode = None

        def __init__(self):
            self.killed = False

        def poll(self):
            return None

        def terminate(self):
            pass

        def wait(self, timeout=None):
            if self.killed:
                self.returncode = -9
                return self.returncode
            raise subprocess.TimeoutExpired("daemon", timeout)

        def kill(self):
            self.killed = True

    started = []
    ticks = [
        datetime(2026, 7, 6, 0, 0, tzinfo=UTC),
        datetime(2026, 7, 6, 0, 30, tzinfo=UTC),
    ]

    path = smoke.run_smoke(
        smoke.build_parser().parse_args(
            [
                "--db",
                str(tmp_path / "prodigy.sqlite"),
                "--duration-minutes",
                "30",
                "--report-dir",
                str(tmp_path / "reports"),
            ]
        ),
        sleep=lambda _seconds: None,
        clock=lambda: ticks.pop(0),
        popen=lambda cmd, **_kwargs: started.append(cmd) or HangingProcess(),
        process_group=False,
    )

    assert len(started) == 2
    report = path.read_text()
    assert "prodigy-executor killed after terminate timeout" in report
    assert "prodigy-signal killed after terminate timeout" in report


def test_smoke_cli_records_component_startup_status_and_telegram_checks(
    tmp_path, monkeypatch
):
    from prodigy.cli import smoke

    monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
    monkeypatch.delenv("TELEGRAM_ALLOWED_USER_IDS", raising=False)
    monkeypatch.delenv("TELEGRAM_CHAT_ID", raising=False)

    class EarlyExitProcess:
        pid = 101
        returncode = 2

        def poll(self):
            return self.returncode

    started = []
    ticks = [
        datetime(2026, 7, 6, 0, 0, tzinfo=UTC),
        datetime(2026, 7, 6, 0, 30, tzinfo=UTC),
    ]

    def fake_popen(cmd, **_kwargs):
        started.append(cmd)
        if len(started) == 1:
            return EarlyExitProcess()
        raise OSError("missing signal")

    path = smoke.run_smoke(
        smoke.build_parser().parse_args(
            [
                "--db",
                str(tmp_path / "prodigy.sqlite"),
                "--duration-minutes",
                "30",
                "--report-dir",
                str(tmp_path / "reports"),
            ]
        ),
        sleep=lambda _seconds: None,
        clock=lambda: ticks.pop(0),
        popen=fake_popen,
        process_group=False,
    )

    assert len(started) == 2
    report = path.read_text()
    assert "- prodigy-executor: early_exit code=2" in report
    assert "- prodigy-signal: failed_to_start missing signal" in report
    assert "- queries: skipped missing telegram credentials" in report
    assert "- controls: skipped missing telegram credentials" in report
    with connect(tmp_path / "prodigy.sqlite") as conn:
        assert (
            get_executor_state(conn, "smoke:component:prodigy-executor")
            == "early_exit code=2"
        )
        assert (
            get_executor_state(conn, "smoke:component:prodigy-signal")
            == "failed_to_start missing signal"
        )


def test_smoke_cli_records_successful_telegram_bot_api_checks(tmp_path, monkeypatch):
    from prodigy.cli import smoke

    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "123:secret-token")
    monkeypatch.setenv("TELEGRAM_ALLOWED_USER_IDS", "123")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "999")

    calls = []

    class FakeResponse:
        def __init__(self, body):
            self.body = body

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def read(self):
            return self.body

    def fake_urlopen(request, timeout):
        calls.append((request.full_url, request.data, timeout))
        if request.full_url.endswith("/getMe"):
            return FakeResponse(b'{"ok": true, "result": {"username": "smoke_bot"}}')
        if request.full_url.endswith("/sendMessage"):
            return FakeResponse(b'{"ok": true, "result": {"message_id": 42}}')
        raise AssertionError(request.full_url)

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

    ticks = [
        datetime(2026, 7, 6, 0, 0, tzinfo=UTC),
        datetime(2026, 7, 6, 0, 30, tzinfo=UTC),
    ]
    path = smoke.run_smoke(
        smoke.build_parser().parse_args(
            [
                "--db",
                str(tmp_path / "prodigy.sqlite"),
                "--duration-minutes",
                "30",
                "--report-dir",
                str(tmp_path / "reports"),
                "--skip-start",
            ]
        ),
        sleep=lambda _seconds: None,
        clock=lambda: ticks.pop(0),
    )

    report = path.read_text()
    assert len(calls) == 2
    assert "- queries: pass getMe username=smoke_bot" in report
    assert "- controls: pass sendMessage message_id=42" in report
    assert "secret-token" not in report


def test_smoke_cli_records_failed_telegram_bot_api_checks_without_token(
    tmp_path, monkeypatch
):
    from prodigy.cli import smoke

    token = "123:secret-token"
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", token)
    monkeypatch.setenv("TELEGRAM_ALLOWED_USER_IDS", "123")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "999")

    def fake_urlopen(request, timeout):
        raise urllib.error.URLError(f"network failed for {request.full_url}")

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

    ticks = [
        datetime(2026, 7, 6, 0, 0, tzinfo=UTC),
        datetime(2026, 7, 6, 0, 30, tzinfo=UTC),
    ]
    path = smoke.run_smoke(
        smoke.build_parser().parse_args(
            [
                "--db",
                str(tmp_path / "prodigy.sqlite"),
                "--duration-minutes",
                "30",
                "--report-dir",
                str(tmp_path / "reports"),
                "--skip-start",
            ]
        ),
        sleep=lambda _seconds: None,
        clock=lambda: ticks.pop(0),
    )

    report = path.read_text()
    assert "- queries: fail getMe error=" in report
    assert "- controls: fail sendMessage error=" in report
    assert token not in report


def test_smoke_start_process_uses_process_group_on_posix():
    from prodigy.cli import smoke

    calls = []

    def fake_popen(cmd, **kwargs):
        calls.append((cmd, kwargs))
        return object()

    smoke._start_process(["demo"], popen=fake_popen, process_group=True)

    assert calls[0][1]["stdout"] is subprocess.DEVNULL
    assert calls[0][1]["stderr"] is subprocess.DEVNULL
    if smoke.os.name == "posix":
        assert calls[0][1]["start_new_session"] is True
    else:
        assert "start_new_session" not in calls[0][1]


def test_smoke_commands_run_signal_with_current_python_module(tmp_path):
    from prodigy.cli import smoke

    commands = dict(smoke._commands(tmp_path / "prodigy.sqlite"))

    assert commands["prodigy-signal"][:3] == [
        smoke.sys.executable,
        "-m",
        "prodigy.cli.signal",
    ]
