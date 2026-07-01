from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import uuid

import pandas as pd

from prodigy.data.bitget_rest import BitgetRestClient
from prodigy.data.ccxt_fetcher import fetch_ohlcv_frame
from prodigy.data.parquet_store import write_daily_partition
from prodigy.data.quality import quality_summary
from prodigy.db import connect, init_db

EXCHANGE_NAME = "bitget"
PRODUCT_TYPE = "usdt-futures"
MAX_FUNDING_PAGES = 100
FUNDING_PAGE_SIZE = 100
PAGE_LIMIT = 1000  # Bitget futures OHLCV page cap


def _timeframe_ms(timeframe: str) -> int:
    # ponytail: reuse pd.Timedelta (same unit handling as quality.py) so any
    # timeframe string parses — no hardcoded map needed.
    return int(pd.Timedelta(timeframe) / pd.Timedelta(1, "ms"))


@dataclass
class BackfillResult:
    symbol: str
    start: str
    end: str
    timeframe: str
    ohlcv_rows: int
    funding_rows: int
    ohlcv_quality: dict
    funding_quality: dict


def _upsert_checkpoint(conn, task_name: str, value: str) -> None:
    conn.execute(
        """
        insert into task_checkpoints (task_name, updated_at, checkpoint_value)
        values (?, datetime('now'), ?)
        on conflict(task_name) do update set
          updated_at = excluded.updated_at,
          checkpoint_value = excluded.checkpoint_value
        """,
        (task_name, value),
    )


def run_backfill(
    symbol: str,
    start: str,
    end: str | None,
    timeframe: str,
    data_root: str | Path,
    db_path: str | Path,
    proxy_url: str | None = "http://127.0.0.1:7897",
    exchange: object | None = None,
    funding_client: object | None = None,
) -> BackfillResult:
    # ponytail: build real Bitget/CCXT clients only when not injected so tests
    # never touch the network or import ccxt. No factory, just a branch.
    if exchange is None:
        import ccxt

        exchange = ccxt.bitget({"proxies": {"http": proxy_url, "https": proxy_url}})
    if funding_client is None:
        funding_client = BitgetRestClient(proxy_url=proxy_url)

    effective_end = end if end is not None else start

    # ponytail: forward-paginate OHLCV by since_ms from start to end. Bitget
    # returns only the most recent page per call, so a single fetch would miss
    # historical data — page until we reach end_ms.
    start_ms = int(pd.Timestamp(start, tz="UTC").value // 1_000_000)
    end_ms = int(pd.Timestamp(effective_end, tz="UTC").value // 1_000_000)
    bar_ms = _timeframe_ms(timeframe)
    pages = []
    cursor = start_ms
    while cursor < end_ms:
        page = fetch_ohlcv_frame(
            exchange, symbol, timeframe, since_ms=cursor, limit=PAGE_LIMIT
        )
        if page.empty:
            break
        pages.append(page)
        last_ts_ms = int(page["timestamp"].iloc[-1].value // 1_000_000)
        if last_ts_ms >= end_ms or last_ts_ms <= cursor:
            # stop: reached end, or exchange returned no progress (e.g. a fake
            # that ignores since/limit and re-emits identical rows every page).
            break
        cursor = last_ts_ms + bar_ms
    ohlcv = (
        pd.concat(pages, ignore_index=True)
        if pages
        else pd.DataFrame(columns=["timestamp", "symbol", "open", "high", "low", "close", "volume"])
    )
    if not ohlcv.empty:
        ohlcv["timestamp"] = pd.to_datetime(ohlcv["timestamp"], utc=True)
        ohlcv = ohlcv.drop_duplicates(subset=["timestamp", "symbol"]).reset_index(drop=True)
        ohlcv = ohlcv[(ohlcv["timestamp"] >= pd.Timestamp(start_ms, unit="ms", tz="UTC")) & (ohlcv["timestamp"] < pd.Timestamp(end_ms, unit="ms", tz="UTC"))]
    for day, day_frame in ohlcv.groupby(ohlcv["timestamp"].dt.floor("D")):
        write_daily_partition(
            day_frame,
            data_root=data_root,
            exchange=EXCHANGE_NAME,
            symbol=symbol,
            dataset="ohlcv",
            date=day,
            timeframe=timeframe,
        )

    funding_pages = []
    for page_no in range(1, MAX_FUNDING_PAGES + 1):
        page = funding_client.fetch_funding_rate_page(
            symbol=symbol,
            product_type=PRODUCT_TYPE,
            page_no=page_no,
            page_size=FUNDING_PAGE_SIZE,
        )
        if page.empty:
            break
        funding_pages.append(page)
    funding = pd.concat(funding_pages, ignore_index=True) if funding_pages else pd.DataFrame()
    if not funding.empty:
        for day, day_frame in funding.groupby(funding["timestamp"].dt.floor("D")):
            write_daily_partition(
                day_frame,
                data_root=data_root,
                exchange=EXCHANGE_NAME,
                symbol=symbol,
                dataset="funding_rates",
                date=day,
            )

    ohlcv_quality = quality_summary(ohlcv, "ohlcv", timeframe)
    funding_quality = quality_summary(funding, "funding_rates")

    task_name = f"backfill:{EXCHANGE_NAME}:{symbol}:{timeframe}"
    summary = {
        "symbol": symbol,
        "start": start,
        "end": effective_end,
        "timeframe": timeframe,
        "ohlcv_rows": int(len(ohlcv)),
        "funding_rows": int(len(funding)),
        "ohlcv_quality": ohlcv_quality,
        "funding_quality": funding_quality,
    }

    with connect(db_path) as conn:
        init_db(conn)
        _upsert_checkpoint(conn, task_name, effective_end)
        conn.execute(
            """
            insert into events (event_id, created_at, severity, component, message, payload_json)
            values (?, datetime('now'), ?, ?, ?, ?)
            """,
            (
                str(uuid.uuid4()),
                "info",
                "data.backfill",
                f"backfill {symbol} {start} to {effective_end}",
                json.dumps(summary, sort_keys=True),
            ),
        )
        conn.commit()

    return BackfillResult(
        symbol=symbol,
        start=start,
        end=effective_end,
        timeframe=timeframe,
        ohlcv_rows=int(len(ohlcv)),
        funding_rows=int(len(funding)),
        ohlcv_quality=ohlcv_quality,
        funding_quality=funding_quality,
    )
