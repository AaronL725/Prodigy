from __future__ import annotations

import re
import sqlite3
from datetime import datetime
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
        components = _state_entries(conn, "smoke:component:")
        telegram_checks = _state_entries(conn, "smoke:telegram:")
        control_commands = conn.execute(
            """
            select command_id, command, status, requested_by, error
            from control_commands
            order by created_at desc
            limit 20
            """
        ).fetchall()
        open_orders = conn.execute(
            """
            select symbol, side, action, status, size, filled_size, price
            from orders
            where status in ('submitted', 'live')
            order by created_at desc
            limit 20
            """
        ).fetchall()
        fills = conn.execute(
            """
            select symbol, side, size, price, fee
            from fills
            order by created_at desc
            limit 20
            """
        ).fetchall()
        positions = conn.execute(
            """
            select symbol, side, notional, entry_price, unrealized_pnl, ownership
            from positions
            order by updated_at desc
            """
        ).fetchall()
        pnl = conn.execute(
            """
            select equity, available_margin, unrealized_pnl, realized_pnl_24h
            from equity_snapshots
            order by created_at desc
            limit 1
            """
        ).fetchone()
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
    issue_lines = [f"- {issue}" for issue in report_issues]
    issue_lines.extend(_event_issue_lines(events))
    if not issue_lines:
        issue_lines.append("- none")
    event_lines = [
        f"- {row['created_at']} | {row['severity']} | {row['component']} | {row['message']}"
        for row in events
    ] or ["- none"]
    residual_lines = []
    if counts["positions"] > 0:
        residual_lines.append(f"- residual positions: {counts['positions']}")
    if working_orders > 0:
        residual_lines.append(f"- residual working orders: {working_orders}")
    if not residual_lines:
        residual_lines.append("- none")
    has_risk = report_issues or events or counts["positions"] > 0 or working_orders > 0
    lines = [
        "# M6 Demo Smoke Report",
        "",
        "## Run",
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
        "## Component Startup Status",
        *_state_lines(components),
        "",
        "## Telegram Query/Control Checks",
        *_state_lines(telegram_checks),
        "",
        "## Trade Intents",
        *_status_lines(statuses["trade_intents"]),
        "",
        "## Control Commands",
        *_status_lines(statuses["control_commands"]),
        *_control_command_lines(control_commands),
        "",
        "## Open Orders",
        *_order_lines(open_orders),
        "",
        "## Fills / Trade Flow",
        *_fill_lines(fills),
        "",
        "## Positions",
        *_position_lines(positions),
        "",
        "## PnL Snapshot",
        *_pnl_lines(pnl),
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
            "## WS/REST/SQLite/Telegram Issues",
            *issue_lines,
            "",
            "## Residual Positions / Orders",
            *residual_lines,
            "",
            "## Warning/Error/Critical Events",
            *event_lines,
            "",
            "## Recommended Fixes",
            *(
                ["- Review listed issues and residual exposure before the next smoke run."]
                if has_risk
                else ["- none"]
            ),
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
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).strftime("%Y%m%d-%H%M")
    except ValueError:
        digits = re.sub(r"\D+", "", value)
        return f"{digits[:8]}-{digits[8:12]}" if len(digits) >= 12 else digits


def _state_entries(conn: sqlite3.Connection, prefix: str) -> list[tuple[str, str]]:
    rows = conn.execute(
        """
        select key, value
        from executor_state
        where key like ?
        order by key
        """,
        (f"{prefix}%",),
    ).fetchall()
    return [(str(row["key"])[len(prefix):], str(row["value"])) for row in rows]


def _state_lines(entries: list[tuple[str, str]]) -> list[str]:
    return [f"- {key}: {value}" for key, value in entries] or ["- none recorded"]


def _status_lines(statuses: list[tuple[str, int]]) -> list[str]:
    return [f"- {status}: {count}" for status, count in statuses] or ["- none"]


def _control_command_lines(rows: list[sqlite3.Row]) -> list[str]:
    lines = []
    for row in rows:
        error = f" error={row['error']}" if row["error"] else ""
        lines.append(
            f"- {row['command']} {row['command_id']}: {row['status']} "
            f"requested_by={row['requested_by']}{error}"
        )
    return lines or ["- none"]


def _order_lines(rows: list[sqlite3.Row]) -> list[str]:
    return [
        f"- {row['symbol']} {row['side']} {row['action']} {row['status']} "
        f"size={_num(row['size'])} filled={_num(row['filled_size'])} "
        f"price={_num(row['price'])}"
        for row in rows
    ] or ["- none"]


def _fill_lines(rows: list[sqlite3.Row]) -> list[str]:
    return [
        f"- {row['symbol']} {row['side']} size={_num(row['size'])} "
        f"price={_num(row['price'])} fee={_num(row['fee'])}"
        for row in rows
    ] or ["- none"]


def _position_lines(rows: list[sqlite3.Row]) -> list[str]:
    return [
        f"- {row['symbol']} {row['side']} notional={_num(row['notional'])} "
        f"entry={_num(row['entry_price'])} unrealized_pnl={_num(row['unrealized_pnl'])} "
        f"ownership={row['ownership']}"
        for row in rows
    ] or ["- none"]


def _pnl_lines(row: sqlite3.Row | None) -> list[str]:
    if row is None:
        return ["- realized_pnl_24h: n/a", "- unrealized_pnl: n/a", "- total_pnl: n/a"]
    # ponytail: no realized-PnL ledger exists yet; total stays unknown until one does.
    return [
        f"- equity: {_num(row['equity'])}",
        f"- available_margin: {_num(row['available_margin'])}",
        "- realized_pnl_24h: n/a",
        f"- unrealized_pnl: {_num(row['unrealized_pnl'])}",
        "- total_pnl: n/a",
    ]


def _event_issue_lines(rows: list[sqlite3.Row]) -> list[str]:
    return [
        f"- {row['component']}: {row['message']}"
        for row in rows
        if str(row["component"]).lower() in {"ws", "websocket", "rest", "sqlite", "telegram"}
    ]


def _num(value: object) -> str:
    return "n/a" if value is None else str(float(value))
