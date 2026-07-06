from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import lightgbm as lgb
import pandas as pd

from prodigy.db import connect, init_db
from prodigy.ml.labels import add_forward_return_labels, horizon_to_bars
from prodigy.ml.splits import purged_walk_forward_splits

# ponytail: fixed LGBM params; no tuning in the example trainer.
_FEATURES = ["example_momentum", "example_funding", "example_volatility"]
_TARGET_TEMPLATE = "target_{}"


@dataclass(frozen=True)
class ExampleTrainingResult:
    model_version: str
    artifact_path: Path
    artifact_hash: str
    metrics: dict


def _prediction_ic(pred: pd.Series, actual: pd.Series) -> float:
    # ponytail: spearman IC; return 0.0 on constant/NaN/short input rather than
    # letting pandas emit a ConstantInputWarning for degenerate arrays.
    aligned = pd.concat([pred, actual], axis=1).dropna()
    if len(aligned) < 2:
        return 0.0
    preds = aligned.iloc[:, 0]
    targets = aligned.iloc[:, 1]
    if preds.nunique() < 2 or targets.nunique() < 2:
        return 0.0
    corr = preds.corr(targets, method="spearman")
    if pd.isna(corr):
        return 0.0
    return float(corr)


def _directional_accuracy(pred: pd.Series, actual: pd.Series) -> float:
    # ponytail: mean(sign(pred)==sign(actual)); 0.0 on empty input.
    aligned = pd.concat([pred, actual], axis=1).dropna()
    if aligned.empty:
        return 0.0
    return float((aligned.iloc[:, 0].transform("sign") == aligned.iloc[:, 1].transform("sign")).mean())


def _long_short_return(pred: pd.Series, actual: pd.Series) -> float:
    # ponytail: simple long-short validation return — go long when pred>0,
    # short when pred<0, earn pred_sign * forward_return; 0.0 on empty.
    aligned = pd.concat([pred, actual], axis=1).dropna()
    if aligned.empty:
        return 0.0
    return float((aligned.iloc[:, 0].transform("sign") * aligned.iloc[:, 1]).mean())


def _new_model() -> lgb.LGBMRegressor:
    return lgb.LGBMRegressor(
        n_estimators=20,
        max_depth=3,
        learning_rate=0.05,
        random_state=7,
        verbosity=-1,
    )


def train_example_model(
    frame: pd.DataFrame,
    db_path: str | Path,
    model_root: str | Path,
    horizon: str,
    model_version: str,
) -> ExampleTrainingResult:
    target = _TARGET_TEMPLATE.format(horizon)
    purge_gap_bars = horizon_to_bars(horizon)
    labeled = add_forward_return_labels(frame, [horizon])

    splits = purged_walk_forward_splits(
        labeled,
        min_train_days=365,
        valid_days=30,
        step_days=30,
        final_holdout_days=30,
        purge_gap_bars=purge_gap_bars,
    )

    timestamp = labeled["timestamp"]
    fold_details = []
    total_train_rows = 0
    total_valid_rows = 0

    for fold in splits.folds:
        train_mask = (timestamp >= fold.train_start) & (timestamp <= fold.train_end)
        valid_mask = (timestamp >= fold.valid_start) & (timestamp <= fold.valid_end)
        train_rows = labeled.loc[train_mask, _FEATURES + [target]].dropna()
        valid_rows = labeled.loc[valid_mask, _FEATURES + [target]].dropna()
        if train_rows.empty or valid_rows.empty:
            continue

        model = _new_model()
        model.fit(train_rows[_FEATURES], train_rows[target])
        predictions = pd.Series(
            model.predict(valid_rows[_FEATURES]),
            index=valid_rows.index,
            name="pred",
        )
        actual = valid_rows[target].rename("actual")

        fold_details.append(
            {
                "train_rows": int(len(train_rows)),
                "validation_rows": int(len(valid_rows)),
                "validation_prediction_ic": _prediction_ic(predictions, actual),
                "validation_directional_accuracy": _directional_accuracy(predictions, actual),
                "validation_long_short_return": _long_short_return(predictions, actual),
            }
        )
        total_train_rows += len(train_rows)
        total_valid_rows += len(valid_rows)

    # ponytail: holdout model is the saved artifact. Train on rows BEFORE the
    # holdout, but also drop the last `label_bars` rows whose forward label
    # would reach into the holdout — otherwise the final-model training labels
    # reference holdout prices (label leakage).
    bar = pd.Timedelta(minutes=15)
    label_bars = horizon_to_bars(horizon)
    holdout_train_cutoff = splits.final_holdout_start - label_bars * bar
    holdout_mask = timestamp >= splits.final_holdout_start
    train_before_holdout_mask = timestamp < holdout_train_cutoff
    train_rows = labeled.loc[train_before_holdout_mask, _FEATURES + [target]].dropna()
    holdout_rows = labeled.loc[holdout_mask, _FEATURES + [target]].dropna()

    if train_rows.empty:
        # No usable training data: refuse to emit a half-baked artifact/metadata
        # rather than crash on an unfitted booster.
        raise ValueError(
            "not enough pre-holdout training rows to fit the example model; "
            "need more history before the final holdout window"
        )

    holdout_model = _new_model()
    holdout_model.fit(train_rows[_FEATURES], train_rows[target])

    holdout_prediction_ic = 0.0
    holdout_directional_accuracy = 0.0
    holdout_long_short_return = 0.0
    holdout_row_count = int(len(holdout_rows))
    feature_importance: dict[str, float] = {}

    if not holdout_rows.empty:
        predictions = pd.Series(
            holdout_model.predict(holdout_rows[_FEATURES]),
            index=holdout_rows.index,
            name="pred",
        )
        actual = holdout_rows[target].rename("actual")
        holdout_prediction_ic = _prediction_ic(predictions, actual)
        holdout_directional_accuracy = _directional_accuracy(predictions, actual)
        holdout_long_short_return = _long_short_return(predictions, actual)

    importances = holdout_model.booster_.feature_importance(importance_type="gain")
    feature_importance = {
        name: float(imp) for name, imp in zip(_FEATURES, importances)
    }

    artifact_dir = Path(model_root) / "example_lgbm"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    artifact_path = artifact_dir / f"{model_version}.txt"
    holdout_model.booster_.save_model(artifact_path)
    artifact_hash = hashlib.sha256(artifact_path.read_bytes()).hexdigest()

    validation_long_short_return = (
        float(sum(f["validation_long_short_return"] for f in fold_details) / len(fold_details))
        if fold_details
        else 0.0
    )

    metrics = {
        "fold_count": len(fold_details),
        "train_rows": int(total_train_rows),
        "validation_rows": int(total_valid_rows),
        "holdout_rows": holdout_row_count,
        "holdout_prediction_ic": holdout_prediction_ic,
        "holdout_directional_accuracy": holdout_directional_accuracy,
        "holdout_long_short_return": holdout_long_short_return,
        "validation_long_short_return": validation_long_short_return,
        "feature_importance": feature_importance,
        "folds": fold_details,
        "train_start": str(splits.folds[0].train_start) if splits.folds else None,
        "validation_end": str(splits.folds[-1].valid_end) if splits.folds else None,
        "final_holdout_start": str(splits.final_holdout_start),
        "final_holdout_end": str(splits.final_holdout_end),
    }

    _store_model_row(
        db_path=db_path,
        model_version=model_version,
        splits=splits,
        artifact_path=artifact_path,
        artifact_hash=artifact_hash,
        metrics=metrics,
    )

    return ExampleTrainingResult(
        model_version=model_version,
        artifact_path=artifact_path,
        artifact_hash=artifact_hash,
        metrics=metrics,
    )


def _store_model_row(
    db_path: str | Path,
    model_version: str,
    splits,
    artifact_path: Path,
    artifact_hash: str,
    metrics: dict,
) -> None:
    created_at = datetime.now(timezone.utc).isoformat()
    train_start = str(splits.folds[0].train_start) if splits.folds else created_at
    train_end = str(splits.folds[-1].train_end) if splits.folds else created_at
    validation_start = str(splits.folds[0].valid_start) if splits.folds else created_at
    validation_end = str(splits.folds[-1].valid_end) if splits.folds else created_at

    with connect(db_path) as conn:
        init_db(conn)
        conn.execute(
            """
            insert or replace into models (
              model_version, created_at, train_start, train_end,
              validation_start, validation_end, artifact_path, artifact_hash, metrics_json
            ) values (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                model_version,
                created_at,
                train_start,
                train_end,
                validation_start,
                validation_end,
                str(artifact_path),
                artifact_hash,
                json.dumps(metrics, sort_keys=True),
            ),
        )
        conn.commit()
