from __future__ import annotations

from typing import Any

import pandas as pd


OHLCV_COLUMNS = ["timestamp", "open", "high", "low", "close", "volume"]


def fetch_ohlcv_frame(
    exchange: Any,
    symbol: str,
    timeframe: str,
    since_ms: int | None = None,
    limit: int | None = None,
    params: dict[str, Any] | None = None,
) -> pd.DataFrame:
    exchange.load_markets()
    rows = exchange.fetch_ohlcv(
        symbol,
        timeframe,
        since=since_ms,
        limit=limit,
        params=params or {},
    )
    frame = pd.DataFrame(rows, columns=OHLCV_COLUMNS)
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], unit="ms", utc=True)
    frame.insert(1, "symbol", symbol)
    return frame[["timestamp", "symbol", "open", "high", "low", "close", "volume"]]
