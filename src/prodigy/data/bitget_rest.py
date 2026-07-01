from __future__ import annotations

from dataclasses import dataclass
import json
from urllib.error import URLError
from urllib.parse import urlencode
from urllib.request import ProxyHandler, build_opener, urlopen

import pandas as pd


BITGET_BASE_URL = "https://api.bitget.com"


def _market_id(symbol: str) -> str:
    return symbol.split(":")[0].replace("/", "")


def _urlopen(url: str, proxy_url: str | None = None, timeout: int = 10) -> bytes:
    if proxy_url:
        opener = build_opener(ProxyHandler({"http": proxy_url, "https": proxy_url}))
        with opener.open(url, timeout=timeout) as response:
            return response.read()
    with urlopen(url, timeout=timeout) as response:
        return response.read()


def parse_funding_rate_rows(payload: dict, symbol: str) -> pd.DataFrame:
    rows = payload.get("data", [])
    frame = pd.DataFrame(rows)
    if frame.empty:
        return pd.DataFrame(
            columns=["timestamp", "symbol", "funding_time", "funding_rate", "raw_symbol"]
        )
    frame = frame.rename(columns={"symbol": "raw_symbol"})
    frame["timestamp"] = pd.to_datetime(frame["fundingTime"].astype("int64"), unit="ms", utc=True)
    frame["funding_time"] = frame["timestamp"]
    frame["symbol"] = symbol
    frame["funding_rate"] = frame["fundingRate"].astype("float64")
    return frame[["timestamp", "symbol", "funding_time", "funding_rate", "raw_symbol"]]


@dataclass
class BitgetRestClient:
    proxy_url: str | None = None
    timeout: int = 10
    opener: object = _urlopen

    def _get_json(self, path: str, params: dict[str, str | int]) -> dict:
        url = f"{BITGET_BASE_URL}{path}?{urlencode(params)}"
        try:
            raw = self.opener(url, proxy_url=None, timeout=self.timeout)
        except (OSError, URLError):
            if not self.proxy_url:
                raise
            raw = self.opener(url, proxy_url=self.proxy_url, timeout=self.timeout)
        payload = json.loads(raw.decode("utf-8"))
        if payload.get("code") != "00000":
            raise RuntimeError(f"Bitget error: {payload}")
        return payload

    def fetch_funding_rate_page(
        self,
        symbol: str,
        product_type: str,
        page_no: int,
        page_size: int,
    ) -> pd.DataFrame:
        payload = self._get_json(
            "/api/v2/mix/market/history-fund-rate",
            {
                "symbol": _market_id(symbol),
                "productType": product_type,
                "pageNo": page_no,
                "pageSize": page_size,
            },
        )
        return parse_funding_rate_rows(payload, symbol=symbol)
