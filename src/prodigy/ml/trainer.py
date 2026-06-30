from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json

import lightgbm as lgb
import pandas as pd


@dataclass(frozen=True)
class ModelTrainingResult:
    model_version: str
    artifact_hash: str
    metrics: dict[str, float | int]


def train_smoke_model(
    frame: pd.DataFrame,
    feature_columns: list[str],
    target_column: str,
    model_version: str,
) -> ModelTrainingResult:
    clean = frame.dropna(subset=feature_columns + [target_column])
    model = lgb.LGBMRegressor(
        n_estimators=5,
        max_depth=2,
        learning_rate=0.1,
        random_state=7,
        verbosity=-1,
    )
    model.fit(clean[feature_columns], clean[target_column])
    predictions = model.predict(clean[feature_columns])
    payload = {
        "model_version": model_version,
        "features": feature_columns,
        "predictions": [round(float(x), 12) for x in predictions],
    }
    artifact_hash = hashlib.sha256(
        json.dumps(payload, sort_keys=True).encode("utf-8")
    ).hexdigest()
    return ModelTrainingResult(
        model_version=model_version,
        artifact_hash=artifact_hash,
        metrics={"train_rows": int(len(clean))},
    )
