from __future__ import annotations

import argparse
import os
import signal
import subprocess
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Callable

from prodigy.db import connect, init_db
from prodigy.signals.state import set_executor_state
from prodigy.smoke.report import write_smoke_report


POLL_SECONDS = 30


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="prodigy-smoke")
    parser.add_argument("--db", default="var/prodigy.sqlite")
    parser.add_argument("--duration-minutes", type=_duration_minutes, default=60)
    parser.add_argument("--report-dir", default="reports")
    parser.add_argument("--skip-start", action="store_true")
    return parser


def run_smoke(
    args: argparse.Namespace,
    *,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], datetime] = lambda: datetime.now(tz=UTC),
    popen: Callable[..., subprocess.Popen] = subprocess.Popen,
    process_group: bool = True,
) -> Path:
    db_path = Path(args.db)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    started_at = _iso(clock())
    issues: list[str] = []
    processes: list[tuple[str, subprocess.Popen]] = []

    with connect(db_path) as conn:
        init_db(conn)
        set_executor_state(conn, "smoke:status", "running", started_at)
        conn.commit()

    try:
        if not args.skip_start:
            for name, cmd in _commands(db_path):
                try:
                    processes.append(
                        (
                            name,
                            _start_process(cmd, popen=popen, process_group=process_group),
                        )
                    )
                except OSError as exc:
                    issues.append(f"{name} failed to start: {exc}")
        _wait_for_duration(
            args.duration_minutes * 60,
            processes=processes,
            issues=issues,
            sleep=sleep,
        )
    finally:
        _stop_processes(processes, issues)

    ended_at = _iso(clock())
    return write_smoke_report(
        db_path,
        args.report_dir,
        started_at=started_at,
        ended_at=ended_at,
        duration_minutes=args.duration_minutes,
        issues=issues,
    )


def main(
    argv: list[str] | None = None,
    *,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], datetime] = lambda: datetime.now(tz=UTC),
    popen: Callable[..., subprocess.Popen] = subprocess.Popen,
) -> int:
    args = build_parser().parse_args(argv)
    report_path = run_smoke(args, sleep=sleep, clock=clock, popen=popen)
    print(report_path)
    return 0


def _duration_minutes(value: str) -> int:
    try:
        minutes = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("duration must be an integer") from exc
    if not 30 <= minutes <= 120:
        raise argparse.ArgumentTypeError("duration must be between 30 and 120 minutes")
    return minutes


def _commands(db_path: Path) -> list[tuple[str, list[str]]]:
    db = str(db_path)
    return [
        (
            "prodigy-executor",
            ["cargo", "run", "-q", "-p", "prodigy-executor", "--", "--daemon", "--db", db],
        ),
        ("prodigy-signal", ["prodigy-signal", "--daemon", "--db", db]),
    ]


def _start_process(
    cmd: list[str],
    *,
    popen: Callable[..., subprocess.Popen],
    process_group: bool,
) -> subprocess.Popen:
    kwargs = {"stdout": subprocess.DEVNULL, "stderr": subprocess.DEVNULL}
    if process_group and os.name == "posix":
        kwargs["start_new_session"] = True
    return popen(cmd, **kwargs)


def _collect_early_exits(
    processes: list[tuple[str, subprocess.Popen]], issues: list[str], seen: set[str]
) -> None:
    for name, proc in processes:
        if name in seen:
            continue
        code = proc.poll()
        if code is not None:
            issues.append(f"{name} exited early with code {code}")
            seen.add(name)


def _wait_for_duration(
    seconds: int,
    *,
    processes: list[tuple[str, subprocess.Popen]],
    issues: list[str],
    sleep: Callable[[float], None],
) -> None:
    remaining = seconds
    seen_exits: set[str] = set()
    while remaining > 0:
        _collect_early_exits(processes, issues, seen_exits)
        step = min(POLL_SECONDS, remaining)
        sleep(step)
        remaining -= step
    _collect_early_exits(processes, issues, seen_exits)


def _stop_processes(
    processes: list[tuple[str, subprocess.Popen]], issues: list[str]
) -> None:
    for name, proc in processes:
        if proc.poll() is not None:
            continue
        _terminate_process(proc)
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            issues.append(f"{name} killed after terminate timeout")
            _kill_process(proc)
            proc.wait()


def _terminate_process(proc: subprocess.Popen) -> None:
    if os.name == "posix":
        try:
            os.killpg(proc.pid, signal.SIGTERM)
            return
        except ProcessLookupError:
            pass
        except OSError:
            pass
    proc.terminate()


def _kill_process(proc: subprocess.Popen) -> None:
    if os.name == "posix":
        try:
            os.killpg(proc.pid, signal.SIGKILL)
            return
        except ProcessLookupError:
            pass
        except OSError:
            pass
    proc.kill()


def _iso(moment: datetime) -> str:
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=UTC)
    return moment.astimezone(UTC).isoformat().replace("+00:00", "Z")


if __name__ == "__main__":
    raise SystemExit(main())
