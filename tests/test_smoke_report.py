from __future__ import annotations

import subprocess
from datetime import UTC, datetime

import pytest

from prodigy.db import connect, init_db
from prodigy.signals.state import get_executor_state
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


def test_smoke_cli_validates_duration_and_can_write_skip_start_report(tmp_path, capsys):
    from prodigy.cli import smoke

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


def test_smoke_cli_records_process_kill_after_terminate_timeout(tmp_path):
    from prodigy.cli import smoke

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
