from __future__ import annotations

import re
import sqlite3
from pathlib import Path
from typing import Iterable

from prodigy.db import connect, init_db
from prodigy.signals.state import set_executor_state


COUNT_TABLES = (
    "trade_intents",
    "control_commands",
    "orders",
    "fills",
    "positions",
    "events",
)
STATUS_TABLES = ("trade_intents", "control_commands", "orders")
SUMMARY_LABELS = {
    "trade_intents": "trade_intents_total",
    "control_commands": "control_commands_total",
    "orders": "orders_total",
    "fills": "fills_total",
    "positions": "positions_total",
    "events": "events_total",
}


def build_smoke_report(
    db_path: str | Path,
    started_at: str,
    ended_at: str,
    duration_minutes: int,
    issues: Iterable[str],
) -> str:
    with connect(db_path) as conn:
        init_db(conn)
        counts = {table: _count(conn, table) for table in COUNT_TABLES}
        statuses = {
            table: _status_counts(conn, table)
            for table in STATUS_TABLES
        }
        events = conn.execute(
            """
            select created_at, severity, component, message
            from events
            where severity in ('warning', 'error', 'critical')
            order by created_at desc
            limit 10
            """
        ).fetchall()

    report_issues = list(issues)
    if counts["positions"] > 0:
        report_issues.append(f"residual positions: {counts['positions']}")
    working_orders = sum(
        count for status, count in statuses["orders"] if status in {"submitted", "live"}
    )
    if working_orders > 0:
        report_issues.append(f"residual working orders: {working_orders}")
    issue_lines = [f"- {issue}" for issue in report_issues] or ["- none"]
    event_lines = [
        f"- {row['created_at']} | {row['severity']} | {row['component']} | {row['message']}"
        for row in events
    ] or ["- none"]
    lines = [
        "# M6 Demo Smoke Report",
        "",
        "## Metadata",
        f"started_at: {started_at}",
        f"ended_at: {ended_at}",
        f"duration_minutes: {duration_minutes}",
        f"database: {Path(db_path)}",
        "",
        "## SQLite Summary",
        *[
            f"- {SUMMARY_LABELS[table]}: {counts[table]}"
            for table in COUNT_TABLES
        ],
        "",
        "## Status Counts",
    ]
    for table in STATUS_TABLES:
        lines.append(f"### {table}")
        table_statuses = statuses[table]
        if table_statuses:
            lines.extend(f"- {status}: {count}" for status, count in table_statuses)
        else:
            lines.append("- none")
    lines.extend(
        [
            "",
            "## Issues",
            *issue_lines,
            "",
            "## Recent Warning/Error/Critical Events",
            *event_lines,
            "",
        ]
    )
    return "\n".join(lines)


def write_smoke_report(
    db_path: str | Path,
    report_dir: str | Path,
    started_at: str,
    ended_at: str,
    duration_minutes: int,
    issues: Iterable[str],
) -> Path:
    report_dir = Path(report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / f"m6-demo-smoke-{_filename_time(ended_at)}.md"
    report = build_smoke_report(db_path, started_at, ended_at, duration_minutes, list(issues))
    report_path.write_text(report, encoding="utf-8")

    with connect(db_path) as conn:
        init_db(conn)
        set_executor_state(conn, "smoke:last_report", str(report_path), ended_at)
        set_executor_state(conn, "smoke:status", "completed", ended_at)
        conn.commit()

    return report_path


def _count(conn: sqlite3.Connection, table: str) -> int:
    return int(conn.execute(f"select count(*) from {table}").fetchone()[0])


def _status_counts(conn: sqlite3.Connection, table: str) -> list[tuple[str, int]]:
    rows = conn.execute(
        f"select status, count(*) as count from {table} group by status order by status"
    ).fetchall()
    return [(str(row["status"]), int(row["count"])) for row in rows]


def _filename_time(value: str) -> str:
    return re.sub(r"[^0-9A-Za-z]+", "", value)
