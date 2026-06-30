import pandas as pd

from prodigy.db import connect, init_db
from prodigy.factors.examples import funding_zscore, momentum_15m, oi_change
from prodigy.ml.trainer import train_smoke_model
from prodigy.research.backtester import Backtester
from prodigy.signals.intents import TradeIntent, write_trade_intent
from prodigy.telegram.service import TelegramCommandService


def test_first_milestone_python_path(tmp_path):
    timestamps = pd.date_range("2026-07-01", periods=12, freq="15min", tz="UTC")
    market = pd.DataFrame(
        {
            "timestamp": timestamps,
            "symbol": ["ETH/USDT:USDT"] * len(timestamps),
            "open": [100 + i for i in range(len(timestamps))],
            "high": [101 + i for i in range(len(timestamps))],
            "low": [99 + i for i in range(len(timestamps))],
            "close": [100 + i + (i % 3) for i in range(len(timestamps))],
            "volume": [10 + i for i in range(len(timestamps))],
            "funding_rate": [0.001 + i * 0.0001 for i in range(len(timestamps))],
            "open_interest": [1000 + i * 10 for i in range(len(timestamps))],
        }
    )

    factors = pd.concat(
        [
            momentum_15m(market, periods=2),
            funding_zscore(market, window=4),
            oi_change(market, periods=2),
        ],
        ignore_index=True,
    )

    one_factor = factors[factors["factor_name"] == "momentum_15m"]
    report = Backtester(
        prices=market[["timestamp", "symbol", "close"]],
        factors=one_factor,
    ).run_full_report(horizon=2, buckets=3)

    wide = factors.pivot_table(
        index=["timestamp", "symbol"],
        columns="factor_name",
        values="value",
    ).reset_index()
    wide["target_1h"] = market["close"].shift(-4) / market["close"] - 1
    model = train_smoke_model(
        wide,
        feature_columns=["momentum_15m", "funding_zscore", "oi_change"],
        target_column="target_1h",
        model_version="first-milestone-smoke",
    )

    db_path = tmp_path / "prodigy.sqlite"
    with connect(db_path) as conn:
        init_db(conn)
        write_trade_intent(
            conn,
            TradeIntent(
                intent_id="intent-smoke",
                created_at="2026-07-01T00:00:00Z",
                symbol="ETH/USDT:USDT",
                side="long",
                action="open",
                target_notional=1000.0,
                max_order_notional=500.0,
                source="smoke",
                reason="first milestone smoke",
                model_version=model.model_version,
            ),
        )
        telegram = TelegramCommandService(conn, allowed_user_ids={"123"})
        status = telegram.status()

    assert report["distribution"]["count"] == 10
    assert len(model.artifact_hash) == 64
    assert "pending_intents=1" in status
