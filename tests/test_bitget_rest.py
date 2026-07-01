import json
from urllib.error import URLError

import pandas as pd

from prodigy.data.bitget_rest import (
    BitgetRestClient,
    parse_funding_rate_rows,
)


def test_parse_funding_rate_rows_normalizes_official_response():
    payload = {
        "code": "00000",
        "data": [
            {
                "symbol": "ETHUSDT",
                "fundingRate": "0.000083",
                "fundingTime": "1782864000000",
            }
        ],
    }

    frame = parse_funding_rate_rows(payload, symbol="ETH/USDT:USDT")

    assert list(frame.columns) == [
        "timestamp",
        "symbol",
        "funding_time",
        "funding_rate",
        "raw_symbol",
    ]
    assert frame.iloc[0]["symbol"] == "ETH/USDT:USDT"
    assert frame.iloc[0]["funding_rate"] == 0.000083
    assert pd.Timestamp(frame.iloc[0]["timestamp"]).tz is not None


def test_client_retries_with_proxy_after_direct_failure():
    calls = []

    def opener(url, proxy_url=None, timeout=10):
        calls.append(proxy_url)
        if proxy_url is None:
            raise URLError("direct failed")
        return json.dumps(
            {
                "code": "00000",
                "data": [
                    {
                        "symbol": "ETHUSDT",
                        "fundingRate": "0.001",
                        "fundingTime": "1782864000000",
                    }
                ],
            }
        ).encode()

    client = BitgetRestClient(proxy_url="http://127.0.0.1:7897", opener=opener)
    frame = client.fetch_funding_rate_page(
        symbol="ETH/USDT:USDT",
        product_type="usdt-futures",
        page_no=1,
        page_size=100,
    )

    assert calls == [None, "http://127.0.0.1:7897"]
    assert frame.iloc[0]["funding_rate"] == 0.001
