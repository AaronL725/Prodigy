from __future__ import annotations

from dataclasses import dataclass
import sqlite3


@dataclass(frozen=True)
class TradeIntent:
    intent_id: str
    created_at: str
    symbol: str
    side: str
    action: str
    target_notional: float
    max_order_notional: float
    source: str
    reason: str
    model_version: str


def write_trade_intent(conn: sqlite3.Connection, intent: TradeIntent) -> None:
    conn.execute(
        """
        insert into trade_intents (
          intent_id, created_at, symbol, side, action, target_notional,
          max_order_notional, status, source, reason, model_version
        ) values (?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?)
        """,
        (
            intent.intent_id,
            intent.created_at,
            intent.symbol,
            intent.side,
            intent.action,
            intent.target_notional,
            intent.max_order_notional,
            intent.source,
            intent.reason,
            intent.model_version,
        ),
    )
    conn.commit()
