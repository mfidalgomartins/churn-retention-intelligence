"""Time-split churn probability model with independent probability calibration."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
import sklearn
import yaml
from sklearn.compose import ColumnTransformer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    average_precision_score,
    brier_score_loss,
    log_loss,
    roc_auc_score,
)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

from churn.common import REFERENCE_DATE, outputs_tables_dir, processed_dir, project_root


def load_config(root: Path | None = None) -> dict[str, Any]:
    root = root or project_root()
    return yaml.safe_load((root / "config" / "modeling.yml").read_text(encoding="utf-8"))


def load_snapshots(root: Path | None = None) -> pd.DataFrame:
    root = root or project_root()
    path = root / "data" / "processed" / "customer_monthly_snapshots.csv"
    return pd.read_csv(
        path,
        parse_dates=["observation_date", "label_end_date"],
    )


def split_snapshots(
    snapshots: pd.DataFrame,
    config: dict[str, Any],
) -> dict[str, pd.DataFrame]:
    labelled = snapshots[snapshots["label_available"].eq(1)].copy()
    labelled["churn_within_horizon"] = labelled["churn_within_horizon"].astype(int)
    split_config = config["splits"]
    splits: dict[str, pd.DataFrame] = {}
    for name in ("train", "calibration", "test"):
        start = pd.Timestamp(split_config[f"{name}_start"])
        end = pd.Timestamp(split_config[f"{name}_end"])
        frame = labelled[labelled["observation_date"].between(start, end)].copy()
        if len(frame) < int(config["minimum_split_rows"]):
            raise ValueError(
                f"{name} split has {len(frame)} rows; minimum is {config['minimum_split_rows']}"
            )
        if frame["churn_within_horizon"].nunique() != 2:
            raise ValueError(f"{name} split must contain both outcome classes")
        splits[name] = frame
    return splits


def build_base_model(config: dict[str, Any]) -> Pipeline:
    categorical = list(config["categorical_features"])
    numeric = list(config["numeric_features"])
    preprocessor = ColumnTransformer(
        transformers=[
            (
                "categorical",
                OneHotEncoder(handle_unknown="ignore", drop="first"),
                categorical,
            ),
            ("numeric", StandardScaler(), numeric),
        ],
        remainder="drop",
        verbose_feature_names_out=True,
    )
    classifier = LogisticRegression(
        C=float(config["regularization_c"]),
        class_weight="balanced",
        max_iter=2000,
        random_state=42,
        solver="lbfgs",
    )
    return Pipeline([("preprocessor", preprocessor), ("classifier", classifier)])


def fit_model(
    splits: dict[str, pd.DataFrame],
    config: dict[str, Any],
) -> tuple[Pipeline, LogisticRegression]:
    features = [*config["categorical_features"], *config["numeric_features"]]
    target = "churn_within_horizon"
    base_model = build_base_model(config)
    base_model.fit(splits["train"][features], splits["train"][target])

    calibration_scores = base_model.decision_function(splits["calibration"][features])
    calibrator = LogisticRegression(C=1_000_000.0, max_iter=1000, solver="lbfgs")
    calibrator.fit(calibration_scores.reshape(-1, 1), splits["calibration"][target])
    return base_model, calibrator


def calibrated_probability(
    base_model: Pipeline,
    calibrator: LogisticRegression,
    frame: pd.DataFrame,
    features: list[str],
) -> np.ndarray:
    scores = base_model.decision_function(frame[features])
    return calibrator.predict_proba(scores.reshape(-1, 1))[:, 1]


def _calibration_regression(y_true: np.ndarray, probability: np.ndarray) -> tuple[float, float]:
    clipped = np.clip(probability, 1e-6, 1 - 1e-6)
    logits = np.log(clipped / (1 - clipped)).reshape(-1, 1)
    regression = LogisticRegression(C=1_000_000.0, max_iter=1000, solver="lbfgs")
    regression.fit(logits, y_true)
    return float(regression.intercept_[0]), float(regression.coef_[0][0])


def _top_decile_lift(y_true: np.ndarray, probability: np.ndarray) -> float:
    count = max(int(np.ceil(len(probability) * 0.10)), 1)
    top = y_true[np.argsort(probability)[-count:]].mean()
    base = y_true.mean()
    return float(top / base) if base > 0 else 0.0


def performance_table(
    splits: dict[str, pd.DataFrame],
    base_model: Pipeline,
    calibrator: LogisticRegression,
    config: dict[str, Any],
) -> pd.DataFrame:
    features = [*config["categorical_features"], *config["numeric_features"]]
    rows: list[dict[str, Any]] = []
    for name, frame in splits.items():
        target = frame["churn_within_horizon"].to_numpy(dtype=int)
        probability = calibrated_probability(base_model, calibrator, frame, features)
        intercept, slope = _calibration_regression(target, probability)
        rows.append(
            {
                "split": name,
                "start_date": frame["observation_date"].min().date().isoformat(),
                "end_date": frame["observation_date"].max().date().isoformat(),
                "rows": len(frame),
                "customers": frame["customer_id"].nunique(),
                "event_rate": target.mean(),
                "roc_auc": roc_auc_score(target, probability),
                "average_precision": average_precision_score(target, probability),
                "brier_score": brier_score_loss(target, probability),
                "log_loss": log_loss(target, probability, labels=[0, 1]),
                "calibration_intercept": intercept,
                "calibration_slope": slope,
                "top_decile_lift": _top_decile_lift(target, probability),
            }
        )
    result = pd.DataFrame(rows)
    metric_columns = [
        "event_rate",
        "roc_auc",
        "average_precision",
        "brier_score",
        "log_loss",
        "calibration_intercept",
        "calibration_slope",
        "top_decile_lift",
    ]
    result[metric_columns] = result[metric_columns].round(6)
    return result


def calibration_table(y_true: pd.Series, probability: np.ndarray) -> pd.DataFrame:
    frame = pd.DataFrame({"outcome": y_true.to_numpy(dtype=int), "probability": probability})
    frame["calibration_bin"] = pd.qcut(
        frame["probability"],
        q=10,
        labels=False,
        duplicates="drop",
    )
    result = frame.groupby("calibration_bin", as_index=False).agg(
        rows=("outcome", "size"),
        mean_predicted_probability=("probability", "mean"),
        observed_event_rate=("outcome", "mean"),
        minimum_probability=("probability", "min"),
        maximum_probability=("probability", "max"),
    )
    result["calibration_gap"] = result["observed_event_rate"] - result["mean_predicted_probability"]
    numeric = [column for column in result if column not in {"calibration_bin", "rows"}]
    result[numeric] = result[numeric].round(6)
    return result


def coefficient_table(base_model: Pipeline) -> pd.DataFrame:
    preprocessor = base_model.named_steps["preprocessor"]
    classifier = base_model.named_steps["classifier"]
    names = preprocessor.get_feature_names_out()
    coefficients = classifier.coef_[0]
    result = pd.DataFrame({"feature": names, "coefficient": coefficients})
    result["odds_ratio"] = np.exp(result["coefficient"])
    result["absolute_coefficient"] = result["coefficient"].abs()
    result = result.sort_values("absolute_coefficient", ascending=False).reset_index(drop=True)
    result[["coefficient", "odds_ratio", "absolute_coefficient"]] = result[
        ["coefficient", "odds_ratio", "absolute_coefficient"]
    ].round(6)
    return result


def assign_probability_tier(probability: np.ndarray, config: dict[str, Any]) -> np.ndarray:
    tiers = config["probability_tiers"]
    return np.select(
        [
            probability >= float(tiers["critical"]),
            probability >= float(tiers["high"]),
            probability >= float(tiers["medium"]),
        ],
        ["critical", "high", "medium"],
        default="low",
    )


def _score_frame(
    frame: pd.DataFrame,
    base_model: Pipeline,
    calibrator: LogisticRegression,
    config: dict[str, Any],
) -> pd.DataFrame:
    features = [*config["categorical_features"], *config["numeric_features"]]
    probability = calibrated_probability(base_model, calibrator, frame, features)
    output = frame.copy()
    output["churn_probability_90d"] = probability.round(6)
    output["probability_risk_tier"] = assign_probability_tier(probability, config)
    return output


def build_prediction_outputs(
    snapshots: pd.DataFrame,
    current_features: pd.DataFrame,
    base_model: Pipeline,
    calibrator: LogisticRegression,
    config: dict[str, Any],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    test_start = pd.Timestamp(config["splits"]["test_start"])
    history = snapshots[snapshots["observation_date"] >= test_start].copy()
    history = _score_frame(history, base_model, calibrator, config)
    test_end = pd.Timestamp(config["splits"]["test_end"])
    history["prediction_scope"] = np.where(
        history["observation_date"] <= test_end,
        "out_of_time_test",
        "forward_monitoring",
    )
    history_columns = [
        "customer_id",
        "observation_date",
        "label_end_date",
        "label_available",
        "churn_within_horizon",
        "current_mrr",
        "churn_probability_90d",
        "probability_risk_tier",
        "prediction_scope",
    ]

    current = current_features[current_features["churn_flag"].eq(0)].copy()
    current = _score_frame(current, base_model, calibrator, config)
    current["scored_at"] = REFERENCE_DATE.date().isoformat()
    current_columns = [
        "customer_id",
        "scored_at",
        "segment",
        "region",
        "acquisition_channel",
        "plan_type",
        "current_mrr",
        "churn_probability_90d",
        "probability_risk_tier",
    ]
    return history[history_columns], current[current_columns]


def monitoring_table(history: pd.DataFrame) -> pd.DataFrame:
    labelled = history[history["label_available"].eq(1)].copy()
    rows: list[dict[str, Any]] = []
    for observation_date, frame in labelled.groupby("observation_date", sort=True):
        outcome = pd.to_numeric(frame["churn_within_horizon"]).to_numpy(dtype=int)
        probability = frame["churn_probability_90d"].to_numpy(dtype=float)
        auc = roc_auc_score(outcome, probability) if np.unique(outcome).size == 2 else np.nan
        rows.append(
            {
                "observation_date": pd.Timestamp(observation_date).date().isoformat(),
                "rows": len(frame),
                "observed_event_rate": outcome.mean(),
                "mean_predicted_probability": probability.mean(),
                "calibration_gap": outcome.mean() - probability.mean(),
                "brier_score": brier_score_loss(outcome, probability),
                "roc_auc": auc,
            }
        )
    result = pd.DataFrame(rows)
    numeric = [
        "observed_event_rate",
        "mean_predicted_probability",
        "calibration_gap",
        "brier_score",
        "roc_auc",
    ]
    result[numeric] = result[numeric].round(6)
    return result


def validate_model_performance(performance: pd.DataFrame, config: dict[str, Any]) -> None:
    test = performance.loc[performance["split"].eq("test")].iloc[0]
    thresholds = config["quality_thresholds"]
    failures: list[str] = []
    comparisons = (
        ("roc_auc", ">=", thresholds["minimum_test_roc_auc"]),
        ("average_precision", ">=", thresholds["minimum_test_average_precision"]),
        ("brier_score", "<=", thresholds["maximum_test_brier_score"]),
        (
            "calibration_slope",
            ">=",
            thresholds["minimum_calibration_slope"],
        ),
        (
            "calibration_slope",
            "<=",
            thresholds["maximum_calibration_slope"],
        ),
    )
    for metric, operator, threshold in comparisons:
        value = float(test[metric])
        passed = value >= float(threshold) if operator == ">=" else value <= float(threshold)
        if not passed:
            failures.append(f"{metric}={value:.6f} must be {operator} {float(threshold):.6f}")
    intercept = abs(float(test["calibration_intercept"]))
    maximum_intercept = float(thresholds["maximum_absolute_calibration_intercept"])
    if intercept > maximum_intercept:
        failures.append(
            f"absolute calibration_intercept={intercept:.6f} must be <= {maximum_intercept:.6f}"
        )
    if failures:
        raise ValueError("Model quality gate failed:\n- " + "\n- ".join(failures))


def _psi_from_proportions(expected: np.ndarray, actual: np.ndarray) -> float:
    expected = np.clip(expected.astype(float), 1e-6, None)
    actual = np.clip(actual.astype(float), 1e-6, None)
    return float(np.sum((actual - expected) * np.log(actual / expected)))


def numeric_psi(expected: pd.Series, actual: pd.Series, bins: int = 10) -> float:
    clean_expected = pd.to_numeric(expected, errors="coerce").dropna()
    clean_actual = pd.to_numeric(actual, errors="coerce").dropna()
    edges = np.unique(clean_expected.quantile(np.linspace(0, 1, bins + 1)).to_numpy())
    if len(edges) < 3:
        return 0.0
    edges[0], edges[-1] = -np.inf, np.inf
    expected_counts = np.histogram(clean_expected, bins=edges)[0]
    actual_counts = np.histogram(clean_actual, bins=edges)[0]
    return _psi_from_proportions(
        expected_counts / max(expected_counts.sum(), 1),
        actual_counts / max(actual_counts.sum(), 1),
    )


def categorical_psi(expected: pd.Series, actual: pd.Series) -> float:
    categories = sorted(set(expected.astype(str)) | set(actual.astype(str)))
    expected_share = (
        expected.astype(str).value_counts(normalize=True).reindex(categories, fill_value=0)
    )
    actual_share = actual.astype(str).value_counts(normalize=True).reindex(categories, fill_value=0)
    return _psi_from_proportions(expected_share.to_numpy(), actual_share.to_numpy())


def feature_drift_table(
    train: pd.DataFrame,
    test: pd.DataFrame,
    current: pd.DataFrame,
    config: dict[str, Any],
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for scope, frame in (("out_of_time_test", test), ("current_snapshot", current)):
        for feature in config["numeric_features"]:
            psi = numeric_psi(train[feature], frame[feature])
            rows.append({"scope": scope, "feature": feature, "feature_type": "numeric", "psi": psi})
        for feature in config["categorical_features"]:
            psi = categorical_psi(train[feature], frame[feature])
            rows.append(
                {"scope": scope, "feature": feature, "feature_type": "categorical", "psi": psi}
            )
    result = pd.DataFrame(rows)
    result["status"] = np.select(
        [result["psi"] >= 0.25, result["psi"] >= 0.10],
        ["critical", "warning"],
        default="stable",
    )
    result["psi"] = result["psi"].round(6)
    return result.sort_values(["scope", "psi"], ascending=[True, False]).reset_index(drop=True)


def _write_model_artifact(
    base_model: Pipeline,
    calibrator: LogisticRegression,
    config: dict[str, Any],
    performance: pd.DataFrame,
    root: Path,
) -> tuple[Path, dict[str, Any]]:
    model_dir = root / "outputs" / "models"
    model_dir.mkdir(parents=True, exist_ok=True)
    model_path = model_dir / "churn_probability_model.joblib"
    bundle = {
        "base_model": base_model,
        "calibrator": calibrator,
        "categorical_features": list(config["categorical_features"]),
        "numeric_features": list(config["numeric_features"]),
        "probability_tiers": dict(config["probability_tiers"]),
        "outcome_horizon_days": int(config["outcome_horizon_days"]),
    }
    joblib.dump(bundle, model_path, compress=3)
    model_sha256 = hashlib.sha256(model_path.read_bytes()).hexdigest()
    metadata = {
        "model_version": "2.0.0",
        "trained_as_of": REFERENCE_DATE.date().isoformat(),
        "algorithm": "regularized_logistic_regression",
        "calibration": "platt_scaling_on_separate_time_period",
        "outcome": f"churn_within_{int(config['outcome_horizon_days'])}_days",
        "training_data": "deterministic_synthetic_demonstration",
        "intended_use": "Retention prioritization and controlled intervention testing.",
        "split_dates": {
            key: pd.Timestamp(value).date().isoformat() for key, value in config["splits"].items()
        },
        "scikit_learn_version": sklearn.__version__,
        "model_sha256": model_sha256,
        "test_metrics": performance.loc[performance["split"].eq("test")].iloc[0].to_dict(),
        "limitations": [
            "Synthetic outcomes do not establish external validity.",
            "Repeated customer-month rows make performance descriptive rather than an independent-sample confidence interval.",
            "Probability calibration must be re-estimated when live outcomes replace synthetic data.",
        ],
    }
    (model_dir / "model_metadata.json").write_text(
        json.dumps(metadata, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )
    return model_path, metadata


def main() -> None:
    root = project_root()
    config = load_config(root)
    snapshots = load_snapshots(root)
    splits = split_snapshots(snapshots, config)
    base_model, calibrator = fit_model(splits, config)
    performance = performance_table(splits, base_model, calibrator, config)
    validate_model_performance(performance, config)

    current_features = pd.read_csv(processed_dir() / "customer_retention_features.csv")
    history, current = build_prediction_outputs(
        snapshots,
        current_features,
        base_model,
        calibrator,
        config,
    )
    test_probability = calibrated_probability(
        base_model,
        calibrator,
        splits["test"],
        [*config["categorical_features"], *config["numeric_features"]],
    )
    calibration = calibration_table(splits["test"]["churn_within_horizon"], test_probability)
    coefficients = coefficient_table(base_model)
    monitoring = monitoring_table(history)
    current_model_frame = current_features[current_features["churn_flag"].eq(0)]
    drift = feature_drift_table(splits["train"], splits["test"], current_model_frame, config)

    processed = processed_dir()
    tables = outputs_tables_dir()
    processed.mkdir(parents=True, exist_ok=True)
    tables.mkdir(parents=True, exist_ok=True)
    history.to_csv(
        processed / "customer_probability_history.csv", index=False, date_format="%Y-%m-%d"
    )
    current.to_csv(processed / "customer_churn_probabilities.csv", index=False)
    performance.to_csv(tables / "model_performance.csv", index=False)
    calibration.to_csv(tables / "model_calibration.csv", index=False)
    coefficients.to_csv(tables / "model_coefficients.csv", index=False)
    monitoring.to_csv(tables / "model_outcome_monitoring.csv", index=False)
    drift.to_csv(tables / "model_feature_drift.csv", index=False)
    model_path, _metadata = _write_model_artifact(base_model, calibrator, config, performance, root)

    test_metrics = performance.loc[performance["split"].eq("test")].iloc[0]
    print(
        f"Model trained: test_roc_auc={test_metrics['roc_auc']:.3f}, "
        f"test_brier={test_metrics['brier_score']:.4f}, "
        f"current_scores={len(current):,}, artifact={model_path.relative_to(root)}."
    )


if __name__ == "__main__":
    main()
