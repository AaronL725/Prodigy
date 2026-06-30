from prodigy.data.ccxt_fetcher import fetch_ohlcv_frame


class FakeExchange:
    def __init__(self):
        self.loaded = False
        self.calls = []

    def load_markets(self):
        self.loaded = True

    def fetch_ohlcv(self, symbol, timeframe, since=None, limit=None, params=None):
        self.calls.append((symbol, timeframe, since, limit, params))
        return [
            [1719792000000, 3000.0, 3010.0, 2990.0, 3005.0, 12.0],
            [1719792900000, 3005.0, 3020.0, 3000.0, 3015.0, 15.0],
        ]


def test_fetch_ohlcv_frame_normalizes_columns():
    exchange = FakeExchange()

    frame = fetch_ohlcv_frame(
        exchange=exchange,
        symbol="ETH/USDT:USDT",
        timeframe="15m",
        since_ms=1719792000000,
        limit=2,
    )

    assert exchange.loaded is True
    assert list(frame.columns) == [
        "timestamp",
        "symbol",
        "open",
        "high",
        "low",
        "close",
        "volume",
    ]
    assert frame["symbol"].tolist() == ["ETH/USDT:USDT", "ETH/USDT:USDT"]
    assert frame["close"].tolist() == [3005.0, 3015.0]
