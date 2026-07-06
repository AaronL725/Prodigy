from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from datetime import UTC, datetime
from pathlib import Path
from typing import Callable

from prodigy.db import connect, init_db
from prodigy.signals.state import set_executor_state
from prodigy.smoke.report import write_smoke_report


POLL_SECONDS = 30
TELEGRAM_TIMEOUT_SECONDS = 5


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
    component_statuses: dict[str, str] = {}
    telegram_checks = _telegram_checks()

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
                    component_statuses[name] = _started_status(processes[-1][1])
                except OSError as exc:
                    issues.append(f"{name} failed to start: {exc}")
                    component_statuses[name] = f"failed_to_start {exc}"
        else:
            for name, _cmd in _commands(db_path):
                component_statuses[name] = "skipped by --skip-start"
        _wait_for_duration(
            args.duration_minutes * 60,
            processes=processes,
            issues=issues,
            component_statuses=component_statuses,
            sleep=sleep,
        )
    finally:
        _stop_processes(processes, issues, component_statuses)

    ended_at = _iso(clock())
    _record_observations(db_path, component_statuses, telegram_checks, ended_at)
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
        ("prodigy-signal", [sys.executable, "-m", "prodigy.cli.signal", "--daemon", "--db", db]),
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
    processes: list[tuple[str, subprocess.Popen]],
    issues: list[str],
    seen: set[str],
    component_statuses: dict[str, str],
) -> None:
    for name, proc in processes:
        if name in seen:
            continue
        code = proc.poll()
        if code is not None:
            issues.append(f"{name} exited early with code {code}")
            component_statuses[name] = f"early_exit code={code}"
            seen.add(name)


def _wait_for_duration(
    seconds: int,
    *,
    processes: list[tuple[str, subprocess.Popen]],
    issues: list[str],
    component_statuses: dict[str, str],
    sleep: Callable[[float], None],
) -> None:
    remaining = seconds
    seen_exits: set[str] = set()
    while remaining > 0:
        _collect_early_exits(processes, issues, seen_exits, component_statuses)
        step = min(POLL_SECONDS, remaining)
        sleep(step)
        remaining -= step
    _collect_early_exits(processes, issues, seen_exits, component_statuses)


def _stop_processes(
    processes: list[tuple[str, subprocess.Popen]],
    issues: list[str],
    component_statuses: dict[str, str],
) -> None:
    for name, proc in processes:
        code = proc.poll()
        if code is not None:
            if name not in component_statuses or component_statuses[name].startswith("started"):
                component_statuses[name] = f"early_exit code={code}"
            continue
        _terminate_process(proc)
        started = component_statuses.get(name, _started_status(proc))
        try:
            proc.wait(timeout=10)
            component_statuses[name] = f"{started}; stopped after duration"
        except subprocess.TimeoutExpired:
            issues.append(f"{name} killed after terminate timeout")
            component_statuses[name] = f"{started}; killed after terminate timeout"
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


def _started_status(proc: subprocess.Popen) -> str:
    return f"started pid={getattr(proc, 'pid', 'unknown')}"


def _telegram_checks() -> dict[str, str]:
    token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    allowed = os.getenv("TELEGRAM_ALLOWED_USER_IDS", "")
    if not token or not allowed:
        status = "skipped missing telegram credentials"
        return {"queries": status, "controls": status}

    chat_id = os.getenv("TELEGRAM_SMOKE_CHAT_ID") or os.getenv("TELEGRAM_CHAT_ID")
    return {
        "queries": _telegram_get_me(token),
        "controls": (
            _telegram_send_message(token, chat_id)
            if chat_id
            else "skipped missing telegram chat"
        ),
    }


def _telegram_get_me(token: str) -> str:
    try:
        payload = _telegram_api(token, "getMe")
        if not payload.get("ok"):
            return f"fail getMe error={_telegram_payload_error(payload, token)}"
        username = payload.get("result", {}).get("username", "unknown")
        return f"pass getMe username={_summary(username, token)}"
    except Exception as exc:
        return f"fail getMe error={_summary(exc, token)}"


def _telegram_send_message(token: str, chat_id: str) -> str:
    try:
        payload = _telegram_api(
            token,
            "sendMessage",
            {"chat_id": chat_id, "text": "M6 smoke check"},
        )
        if not payload.get("ok"):
            return f"fail sendMessage error={_telegram_payload_error(payload, token)}"
        message_id = payload.get("result", {}).get("message_id", "unknown")
        return f"pass sendMessage message_id={_summary(message_id, token)}"
    except Exception as exc:
        return f"fail sendMessage error={_summary(exc, token)}"


def _telegram_api(
    token: str,
    method: str,
    data: dict[str, str] | None = None,
) -> dict[str, object]:
    body = urllib.parse.urlencode(data).encode() if data else None
    request = urllib.request.Request(
        f"https://api.telegram.org/bot{token}/{method}",
        data=body,
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=TELEGRAM_TIMEOUT_SECONDS) as response:
        return json.loads(response.read().decode("utf-8"))


def _telegram_payload_error(payload: dict[str, object], token: str) -> str:
    error = payload.get("description") or payload.get("error_code") or "telegram api error"
    return _summary(error, token)


def _summary(value: object, token: str) -> str:
    text = str(value).replace(token, "<redacted>").replace("\n", " ")
    return text[:160]


def _record_observations(
    db_path: Path,
    component_statuses: dict[str, str],
    telegram_checks: dict[str, str],
    updated_at: str,
) -> None:
    with connect(db_path) as conn:
        init_db(conn)
        for name, status in component_statuses.items():
            set_executor_state(conn, f"smoke:component:{name}", status, updated_at)
        for name, status in telegram_checks.items():
            set_executor_state(conn, f"smoke:telegram:{name}", status, updated_at)
        conn.commit()


if __name__ == "__main__":
    raise SystemExit(main())
