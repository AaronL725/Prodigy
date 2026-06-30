import pandas as pd

from prodigy.ml.trainer import train_smoke_model


def test_train_smoke_model_returns_metadata():
    frame = pd.DataFrame(
        {
            "momentum_15m": [0.1, 0.2, -0.1, 0.3, -0.2, 0.4, 0.0, 0.5],
            "funding_zscore": [0.0, 1.0, -1.0, 0.5, -0.3, 0.7, 0.1, 0.2],
            "oi_change": [0.01, 0.02, -0.01, 0.03, -0.02, 0.04, 0.00, 0.05],
            "target_1h": [0.01, 0.02, -0.01, 0.03, -0.02, 0.04, 0.0, 0.05],
        }
    )

    result = train_smoke_model(
        frame,
        feature_columns=["momentum_15m", "funding_zscore", "oi_change"],
        target_column="target_1h",
        model_version="smoke-test",
    )

    assert result.model_version == "smoke-test"
    assert len(result.artifact_hash) == 64
    assert "train_rows" in result.metrics
