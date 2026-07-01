import pandas as pd
import pytest

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
    # spec: metrics must include simple long-short validation return and
    # feature importance (not just IC + directional accuracy).
    assert "validation_long_short_return" in result.metrics
    assert "feature_importance" in result.metrics

    with connect(db_path) as conn:
        row = conn.execute(
            "select model_version, artifact_hash from models where model_version = ?",
            ("example-test",),
        ).fetchone()

    assert row["artifact_hash"] == result.artifact_hash


def test_train_example_model_raises_on_insufficient_data(tmp_path):
    # Too few rows to form any walk-forward fold / holdout training set.
    # Must raise ValueError instead of writing a half-baked artifact + metadata.
    db_path = tmp_path / "prodigy.sqlite"
    with connect(db_path) as conn:
        init_db(conn)

    ts = pd.date_range("2024-01-01", periods=10, freq="15min", tz="UTC")
    tiny = pd.DataFrame(
        {
            "timestamp": ts,
            "symbol": ["ETH/USDT:USDT"] * 10,
            "close": [100.0] * 10,
            "example_momentum": [0.0] * 10,
            "example_funding": [0.0] * 10,
            "example_volatility": [0.0] * 10,
        }
    )

    with pytest.raises(ValueError):
        train_example_model(
            frame=tiny,
            db_path=db_path,
            model_root=tmp_path / "models",
            horizon="1h",
            model_version="tiny",
        )

    # nothing should have been written
    assert not (tmp_path / "models").exists() or not any(
        (tmp_path / "models").rglob("*.txt")
    )
    with connect(db_path) as conn:
        row = conn.execute(
            "select model_version from models where model_version = ?", ("tiny",)
        ).fetchone()
    assert row is None

