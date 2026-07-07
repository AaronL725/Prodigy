from __future__ import annotations

import json
import sqlite3
import time
import uuid

ACTIVE_LOCK_STALE_TIMEOUT_MS = 30_000


class TelegramCommandService:
    def __init__(self, conn: sqlite3.Connection, allowed_user_ids: set[str] | set[int]):
        # ponytail: real Telegram user IDs arrive as ints; config may carry ints
        # or strings. Normalize to strings once so the whitelist check can't
        # silently mismatch (int 123 != str "123").
        self.conn = conn
        self.allowed_user_ids = {str(uid) for uid in allowed_user_ids}

    def status(self) -> str:
        pending_intents = self.conn.execute(
            "select count(*) from trade_intents where status = 'pending'"
        ).fetchone()[0]
        pending_commands = self.conn.execute(
            "select count(*) from control_commands where status = 'pending'"
        ).fetchone()[0]
        return f"pending_intents={pending_intents} pending_commands={pending_commands}"

    def stop(self, user_id: str | int, now: str) -> str:
        return self._write_command(user_id=user_id, now=now, command="stop")

    def resume(self, user_id: str | int, now: str) -> str:
        return self._write_command(user_id=user_id, now=now, command="resume")

    def _write_command(self, user_id: str | int, now: str, command: str) -> str:
        if str(user_id) not in self.allowed_user_ids:
            return "unauthorized"
        target = self._active_executor_target()
        if target is None:
            self.conn.execute(
                """
                insert into events (
                  event_id, created_at, severity, component, message, payload_json
                ) values (?, ?, 'warning', 'telegram', ?, ?)
                """,
                (
                    str(uuid.uuid4()),
                    now,
                    "telegram control command rejected",
                    json.dumps(
                        {
                            "command": command,
                            "requested_by": user_id,
                            "error": "no_active_executor",
                        }
                    ),
                ),
            )
            self.conn.commit()
            return "no active executor"
        mode, instance_id = target
        self.conn.execute(
            """
            insert into control_commands (
              command_id, created_at, command, status, requested_by, mode, instance_id
            ) values (?, ?, ?, 'pending', ?, ?, ?)
            """,
            (str(uuid.uuid4()), now, command, user_id, mode, instance_id),
        )
        self.conn.commit()
        return f"{command} command queued"

    def _active_executor_target(self) -> tuple[str, str] | None:
        target = self.conn.execute(
            """
            select
              (select value from executor_state where key = 'active_mode'),
              (select value from executor_state where key = 'active_instance_id'),
              (select value from executor_state where key = 'active_started_at'),
              (select value from executor_state where key = 'active_heartbeat_at')
            """
        ).fetchone()
        mode, instance_id, started_at, heartbeat_at = target
        mode = str(mode).strip() if mode is not None else ""
        instance_id = str(instance_id).strip() if instance_id is not None else ""
        if not mode or not instance_id or not started_at or not heartbeat_at:
            return None
        if mode not in {"demo", "live"}:
            return None
        try:
            int(started_at)
            heartbeat_ms = int(heartbeat_at)
        except ValueError:
            return None
        if int(time.time() * 1000) - heartbeat_ms > ACTIVE_LOCK_STALE_TIMEOUT_MS:
            return None
        return mode, instance_id
