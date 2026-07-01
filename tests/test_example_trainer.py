import pandas as pd

from prodigy.db import connect, init_db
from prodigy.ml.example_trainer import train_example_model


def features():
    ts = pd.date_range("2024-01-01", periods=50000, freq="15min", tz="UTC")
    close = pd.Series([100 + i * 0.01 for i in range(len(ts))])
    return pd.DataFrame(
        {
            "timestamp": ts,
            "symbol": ["ETH/USDT:USDT"] * len(ts),
            "close": close,
            "example_momentum": close.pct_change(4).fillna(0),
            "example_funding": [0.1] * len(ts),
            "example_volatility": close.pct_change().rolling(8).std().fillna(0),
        }
    )


def test_train_example_model_saves_artifact_and_metadata(tmp_path):
    db_path = tmp_path / "prodigy.sqlite"
    with connect(db_path) as conn:
        init_db(conn)

    result = train_example_model(
        frame=features(),
        db_path=db_path,
        model_root=tmp_path / "models",
        horizon="1h",
        model_version="example-test",
    )

    assert result.artifact_path.exists()
    assert len(result.artifact_hash) == 64
    assert result.metrics["fold_count"] > 0
    assert "holdout_prediction_ic" in result.metrics

    with connect(db_path) as conn:
        row = conn.execute(
            "select model_version, artifact_hash from models where model_version = ?",
            ("example-test",),
        ).fetchone()

    assert row["artifact_hash"] == result.artifact_hash
