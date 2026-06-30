from __future__ import annotations

import sqlite3
import uuid


class TelegramCommandService:
    def __init__(self, conn: sqlite3.Connection, allowed_user_ids: set[str]):
        self.conn = conn
        self.allowed_user_ids = allowed_user_ids

    def status(self) -> str:
        pending_intents = self.conn.execute(
            "select count(*) from trade_intents where status = 'pending'"
        ).fetchone()[0]
        pending_commands = self.conn.execute(
            "select count(*) from control_commands where status = 'pending'"
        ).fetchone()[0]
        return f"pending_intents={pending_intents} pending_commands={pending_commands}"

    def stop(self, user_id: str, now: str) -> str:
        return self._write_command(user_id=user_id, now=now, command="stop")

    def resume(self, user_id: str, now: str) -> str:
        return self._write_command(user_id=user_id, now=now, command="resume")

    def _write_command(self, user_id: str, now: str, command: str) -> str:
        if user_id not in self.allowed_user_ids:
            return "unauthorized"
        self.conn.execute(
            """
            insert into control_commands (
              command_id, created_at, command, status, requested_by
            ) values (?, ?, ?, 'pending', ?)
            """,
            (str(uuid.uuid4()), now, command, user_id),
        )
        self.conn.commit()
        return f"{command} command queued"
