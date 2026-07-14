"""Randomized retention holdout design and incremental saved-MRR measurement."""

from __future__ import annotations

import json
import math
import os
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import yaml

from churn.common import outputs_tables_dir, processed_dir, project_root


def load_config(root: Path | None = None) -> dict[str, Any]:
    root = root or project_root()
    return yaml.safe_load((root / "config" / "experiments.yml").read_text(encoding="utf-8"))


def load_ingestion_manifest(root: Path | None = None) -> dict[str, Any]:
    root = root or project_root()
    path = root / "data" / "raw" / "_ingestion_manifest.json"
    if not path.is_file():
        raise FileNotFoundError(f"Ingestion manifest is missing: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def resolve_assignment_date(config: dict[str, Any], manifest: dict[str, Any]) -> str:
    if str(manifest.get("adapter")) == "synthetic":
        raw = config["assignment_date"]
    else:
        env_name = str(config["assignment_date_env"])
        raw = os.environ.get(env_name, manifest.get("reference_date"))
    try:
        parsed = pd.Timestamp(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError("Experiment assignment date is missing or invalid") from exc
    if pd.isna(parsed):
        raise ValueError("Experiment assignment date is missing or invalid")
    return parsed.date().isoformat()


def load_population(root: Path | None = None) -> pd.DataFrame:
    root = root or project_root()
    probabilities = pd.read_csv(root / "data" / "processed" / "customer_churn_probabilities.csv")
    features = pd.read_csv(root / "data" / "processed" / "customer_retention_features.csv")
    columns = [
        "customer_id",
        "tenure_days",
        "recent_sessions_30d",
        "nps_score_recent",
        "failed_payments_90d",
    ]
    return probabilities.merge(features[columns], on="customer_id", validate="one_to_one")


def assign_intervention(
    population: pd.DataFrame,
    config: dict[str, Any],
    *,
    assignment_date: str | None = None,
    source_adapter: str = "synthetic",
) -> pd.DataFrame:
    eligible_tiers = set(config["eligible_probability_tiers"])
    eligible = population[population["probability_risk_tier"].isin(eligible_tiers)].copy()
    if eligible.empty:
        raise ValueError("No customers satisfy the experiment eligibility criteria")
    strata = list(config["allocation"]["stratification_columns"])
    eligible["stratum"] = eligible[strata].astype(str).agg("|".join, axis=1)
    eligible = eligible.sort_values(["stratum", "customer_id"]).reset_index(drop=True)
    sparse_strata = eligible["stratum"].value_counts().loc[lambda values: values < 2]
    if not sparse_strata.empty:
        raise ValueError(
            "Every assignment stratum requires at least two eligible customers; "
            f"sparse strata={sparse_strata.to_dict()}"
        )

    rng = np.random.default_rng(int(config["simulation"]["random_seed"]))
    assignments = pd.Series("control", index=eligible.index, dtype="object")
    treatment_share = float(config["allocation"]["treatment_share"])
    for _stratum, group in eligible.groupby("stratum", sort=True):
        indexes = group.index.to_numpy()
        treatment_count = round(len(indexes) * treatment_share)
        treatment_count = min(max(treatment_count, 1), len(indexes) - 1)
        treatment_indexes = rng.permutation(indexes)[:treatment_count]
        assignments.loc[treatment_indexes] = "treatment"

    eligible["experiment_id"] = str(config["experiment_id"])
    eligible["assignment_date"] = assignment_date or str(config["assignment_date"])
    eligible["source_adapter"] = source_adapter
    eligible["assignment"] = assignments
    eligible["holdout_flag"] = eligible["assignment"].eq("control").astype(int)
    eligible["assignment_probability"] = treatment_share
    return eligible[
        [
            "experiment_id",
            "customer_id",
            "assignment_date",
            "source_adapter",
            "assignment",
            "holdout_flag",
            "assignment_probability",
            "stratum",
            "probability_risk_tier",
            "segment",
            "region",
            "acquisition_channel",
            "plan_type",
            "churn_probability_90d",
            "current_mrr",
            "tenure_days",
            "recent_sessions_30d",
            "nps_score_recent",
            "failed_payments_90d",
        ]
    ]


def simulate_outcomes(assignments: pd.DataFrame, config: dict[str, Any]) -> pd.DataFrame:
    settings = config["simulation"]
    if not settings.get("enabled", False):
        raise ValueError("No observed outcome adapter is configured; simulation is disabled")
    frame = assignments.sort_values("customer_id").copy()
    probability = frame["churn_probability_90d"].to_numpy(dtype=float)
    odds = probability / np.clip(1.0 - probability, 1e-9, None)
    treated = frame["assignment"].eq("treatment").to_numpy()
    odds_multiplier = np.where(treated, float(settings["treatment_odds_ratio"]), 1.0)
    potential_probability = (odds * odds_multiplier) / (1.0 + odds * odds_multiplier)
    rng = np.random.default_rng(int(settings["random_seed"]) + 1)
    churned = rng.random(len(frame)) < potential_probability

    frame["outcome_due_date"] = (
        (
            pd.Timestamp(config["assignment_date"])
            + pd.Timedelta(days=int(config["outcome_horizon_days"]))
        )
        .date()
        .isoformat()
    )
    frame["outcome_status"] = "simulated"
    frame["outcome_source"] = str(settings["outcome_source"])
    frame["assigned_action_delivered"] = frame["assignment"]
    frame["simulated_churn_probability_90d"] = potential_probability.round(6)
    frame["churned_90d"] = churned.astype(int)
    frame["retained_90d"] = (~churned).astype(int)
    frame["lost_mrr_90d"] = np.where(churned, frame["current_mrr"], 0.0).round(2)
    frame["ending_mrr_90d"] = np.where(churned, 0.0, frame["current_mrr"]).round(2)
    return frame[
        [
            "experiment_id",
            "customer_id",
            "assignment",
            "stratum",
            "source_adapter",
            "outcome_due_date",
            "outcome_status",
            "outcome_source",
            "assigned_action_delivered",
            "simulated_churn_probability_90d",
            "churned_90d",
            "retained_90d",
            "current_mrr",
            "lost_mrr_90d",
            "ending_mrr_90d",
        ]
    ]


def build_pending_outcomes(assignments: pd.DataFrame, config: dict[str, Any]) -> pd.DataFrame:
    frame = assignments.sort_values("customer_id").copy()
    due_date = (
        (
            pd.Timestamp(frame["assignment_date"].iloc[0])
            + pd.Timedelta(days=int(config["outcome_horizon_days"]))
        )
        .date()
        .isoformat()
    )
    frame["outcome_due_date"] = due_date
    frame["outcome_status"] = "pending"
    frame["outcome_source"] = "awaiting_observed_outcomes"
    frame["assigned_action_delivered"] = pd.NA
    frame["simulated_churn_probability_90d"] = np.nan
    frame["churned_90d"] = pd.NA
    frame["retained_90d"] = pd.NA
    frame["lost_mrr_90d"] = np.nan
    frame["ending_mrr_90d"] = np.nan
    return frame[
        [
            "experiment_id",
            "customer_id",
            "assignment",
            "stratum",
            "source_adapter",
            "outcome_due_date",
            "outcome_status",
            "outcome_source",
            "assigned_action_delivered",
            "simulated_churn_probability_90d",
            "churned_90d",
            "retained_90d",
            "current_mrr",
            "lost_mrr_90d",
            "ending_mrr_90d",
        ]
    ]


def merge_observed_outcomes(
    assignments: pd.DataFrame,
    observed: pd.DataFrame,
    config: dict[str, Any],
) -> pd.DataFrame:
    required = {
        "customer_id",
        "assigned_action_delivered",
        "churned_90d",
        "lost_mrr_90d",
    }
    missing = sorted(required - set(observed.columns))
    if missing:
        raise ValueError(f"Observed outcome file is missing columns: {missing}")
    observed = observed[list(required)].copy()
    if observed["customer_id"].duplicated().any():
        raise ValueError("Observed outcome file contains duplicate customer_id values")
    unknown = set(observed["customer_id"]) - set(assignments["customer_id"])
    if unknown:
        raise ValueError(f"Observed outcomes contain unassigned customers: {sorted(unknown)[:5]}")

    delivered = observed["assigned_action_delivered"].astype(str)
    invalid_delivery = ~delivered.isin({"treatment", "control"})
    churned = pd.to_numeric(observed["churned_90d"], errors="coerce")
    lost_mrr = pd.to_numeric(observed["lost_mrr_90d"], errors="coerce")
    invalid_numeric = ~churned.isin({0, 1}) | lost_mrr.isna() | lost_mrr.lt(0)
    if invalid_delivery.any() or invalid_numeric.any():
        raise ValueError("Observed outcomes contain invalid delivery, churn, or lost-MRR values")

    pending = build_pending_outcomes(assignments, config).set_index("customer_id")
    supplied = observed.set_index("customer_id")
    current_mrr = pending.loc[supplied.index, "current_mrr"].astype(float)
    if (lost_mrr.to_numpy(dtype=float) > current_mrr.to_numpy(dtype=float) + 0.01).any():
        raise ValueError("Observed lost_mrr_90d cannot exceed assignment-date current MRR")
    if ((churned.eq(0)) & lost_mrr.gt(0)).any():
        raise ValueError("Non-churned observed outcomes must have zero lost MRR")

    source_name = str(config["observed_outcomes"]["source_name"])
    pending.loc[supplied.index, "outcome_status"] = "observed"
    pending.loc[supplied.index, "outcome_source"] = source_name
    pending.loc[supplied.index, "assigned_action_delivered"] = delivered.to_numpy()
    pending.loc[supplied.index, "churned_90d"] = churned.astype(int).to_numpy()
    pending.loc[supplied.index, "retained_90d"] = (1 - churned.astype(int)).to_numpy()
    pending.loc[supplied.index, "lost_mrr_90d"] = lost_mrr.round(2).to_numpy()
    pending.loc[supplied.index, "ending_mrr_90d"] = (
        current_mrr.to_numpy(dtype=float) - lost_mrr.to_numpy(dtype=float)
    ).round(2)
    return pending.reset_index()


def resolve_outcomes(
    assignments: pd.DataFrame,
    config: dict[str, Any],
    source_adapter: str,
    existing_outcomes: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if source_adapter == "synthetic":
        return simulate_outcomes(assignments, config)
    env_name = str(config["observed_outcomes"]["file_env"])
    outcome_path = os.environ.get(env_name)
    if not outcome_path:
        return (
            existing_outcomes.copy()
            if existing_outcomes is not None
            else build_pending_outcomes(assignments, config)
        )
    path = Path(outcome_path).expanduser().resolve()
    if not path.is_file():
        raise FileNotFoundError(f"Observed outcome file is missing: {path}")
    observed = pd.read_csv(path)
    if existing_outcomes is not None:
        previous = existing_outcomes.loc[
            existing_outcomes["outcome_status"].eq("observed"),
            [
                "customer_id",
                "assigned_action_delivered",
                "churned_90d",
                "lost_mrr_90d",
            ],
        ]
        observed = (
            pd.concat([previous, observed], ignore_index=True)
            .drop_duplicates("customer_id", keep="last")
            .reset_index(drop=True)
        )
    return merge_observed_outcomes(assignments, observed, config)


def _standardized_mean_difference(treatment: pd.Series, control: pd.Series) -> float:
    variance = (treatment.var(ddof=1) + control.var(ddof=1)) / 2.0
    if not np.isfinite(variance) or variance <= 0:
        return 0.0
    return float((treatment.mean() - control.mean()) / math.sqrt(variance))


def build_balance_table(assignments: pd.DataFrame) -> pd.DataFrame:
    treatment = assignments[assignments["assignment"].eq("treatment")]
    control = assignments[assignments["assignment"].eq("control")]
    rows: list[dict[str, Any]] = []
    numeric = [
        "churn_probability_90d",
        "current_mrr",
        "tenure_days",
        "recent_sessions_30d",
        "nps_score_recent",
        "failed_payments_90d",
    ]
    for feature in numeric:
        treated = pd.to_numeric(treatment[feature])
        held_out = pd.to_numeric(control[feature])
        rows.append(
            {
                "feature": feature,
                "feature_type": "numeric",
                "treatment_value": treated.mean(),
                "control_value": held_out.mean(),
                "balance_statistic": _standardized_mean_difference(treated, held_out),
                "statistic_name": "standardized_mean_difference",
            }
        )
    for feature in ["probability_risk_tier", "segment", "region", "plan_type"]:
        treated_share = treatment[feature].value_counts(normalize=True)
        control_share = control[feature].value_counts(normalize=True)
        difference = treated_share.subtract(control_share, fill_value=0).abs()
        rows.append(
            {
                "feature": feature,
                "feature_type": "categorical",
                "treatment_value": treated_share.max(),
                "control_value": control_share.max(),
                "balance_statistic": difference.max(),
                "statistic_name": "maximum_absolute_share_difference",
            }
        )
    result = pd.DataFrame(rows)
    numeric_columns = ["treatment_value", "control_value", "balance_statistic"]
    result[numeric_columns] = result[numeric_columns].round(6)
    return result


def _stratified_effect(
    analysis: pd.DataFrame,
    outcome: str,
) -> tuple[float, float, float, float]:
    total = len(analysis)
    effect = 0.0
    variance = 0.0
    used = 0
    for _stratum, group in analysis.groupby("stratum", sort=True):
        treated = group.loc[group["assignment"].eq("treatment"), outcome].astype(float)
        control = group.loc[group["assignment"].eq("control"), outcome].astype(float)
        if treated.empty or control.empty:
            continue
        weight = len(group) / total
        effect += weight * (treated.mean() - control.mean())
        treated_variance = treated.var(ddof=1) if len(treated) > 1 else 0.0
        control_variance = control.var(ddof=1) if len(control) > 1 else 0.0
        variance += weight**2 * (treated_variance / len(treated) + control_variance / len(control))
        used += len(group)
    if used != total:
        raise ValueError("Every experiment stratum must contain treatment and control customers")
    standard_error = math.sqrt(max(variance, 0.0))
    return effect, standard_error, effect - 1.96 * standard_error, effect + 1.96 * standard_error


def build_incrementality_table(
    outcomes: pd.DataFrame,
    minimum_complete_share: float = 1.0,
) -> pd.DataFrame:
    complete = outcomes[
        outcomes["outcome_status"].isin({"simulated", "observed"})
        & outcomes["churned_90d"].notna()
        & outcomes["lost_mrr_90d"].notna()
    ].copy()
    completion_share = len(complete) / len(outcomes) if len(outcomes) else 0.0
    strata_have_both_arms = (
        not complete.empty and complete.groupby("stratum")["assignment"].nunique().eq(2).all()
    )
    estimable = completion_share >= minimum_complete_share and strata_have_both_arms
    treatment = outcomes[outcomes["assignment"].eq("treatment")]
    control = outcomes[outcomes["assignment"].eq("control")]
    treatment_count = len(treatment)
    rows: list[dict[str, Any]] = []
    for metric, unit in (("churned_90d", "rate"), ("lost_mrr_90d", "USD_per_customer")):
        if estimable:
            effect, standard_error, lower, upper = _stratified_effect(complete, metric)
            z_score = effect / standard_error if standard_error > 0 else 0.0
            p_value = math.erfc(abs(z_score) / math.sqrt(2.0)) if standard_error > 0 else 1.0
            treatment_mean = complete.loc[complete["assignment"].eq("treatment"), metric].mean()
            control_mean = complete.loc[complete["assignment"].eq("control"), metric].mean()
        else:
            effect = standard_error = lower = upper = p_value = np.nan
            treatment_mean = control_mean = np.nan
        rows.append(
            {
                "experiment_id": outcomes["experiment_id"].iloc[0],
                "estimand": "intention_to_treat",
                "estimation_status": "estimable" if estimable else "not_estimable",
                "metric": metric,
                "unit": unit,
                "treatment_n": treatment_count,
                "control_n": len(control),
                "analysis_n": len(complete),
                "outcome_completion_share": completion_share,
                "treatment_mean": treatment_mean,
                "control_mean": control_mean,
                "treatment_minus_control": effect,
                "standard_error": standard_error,
                "ci_95_lower": lower,
                "ci_95_upper": upper,
                "p_value": p_value,
                "incremental_saved_mrr": -effect * treatment_count
                if metric == "lost_mrr_90d"
                else np.nan,
            }
        )
    result = pd.DataFrame(rows)
    numeric = [
        "outcome_completion_share",
        "treatment_mean",
        "control_mean",
        "treatment_minus_control",
        "standard_error",
        "ci_95_lower",
        "ci_95_upper",
        "p_value",
        "incremental_saved_mrr",
    ]
    result[numeric] = result[numeric].round(6)
    return result


def build_outcome_monitoring(outcomes: pd.DataFrame) -> pd.DataFrame:
    available = outcomes["outcome_status"].isin({"simulated", "observed"})
    delivered_value = outcomes["assigned_action_delivered"].astype("string").fillna("")
    delivered = delivered_value.ne("")
    monitored = outcomes.assign(
        outcome_available=available.astype(int),
        delivery_contamination=(
            delivered & delivered_value.ne(outcomes["assignment"].astype(str))
        ).astype(int),
        churned_90d=pd.to_numeric(outcomes["churned_90d"], errors="coerce"),
        lost_mrr_90d=pd.to_numeric(outcomes["lost_mrr_90d"], errors="coerce"),
        ending_mrr_90d=pd.to_numeric(outcomes["ending_mrr_90d"], errors="coerce"),
    )
    result = (
        monitored.groupby("assignment", as_index=False)
        .agg(
            assigned_customers=("customer_id", "nunique"),
            outcomes_available=("outcome_available", "sum"),
            delivery_contamination=("delivery_contamination", "sum"),
            churn_rate_90d=("churned_90d", "mean"),
            lost_mrr_90d=("lost_mrr_90d", "sum"),
            ending_mrr_90d=("ending_mrr_90d", "sum"),
        )
        .sort_values("assignment")
    )
    result["outcome_completeness"] = result["outcomes_available"] / result["assigned_customers"]
    result[["churn_rate_90d", "outcome_completeness"]] = result[
        ["churn_rate_90d", "outcome_completeness"]
    ].round(6)
    return result


def load_existing_production_assignment(
    path: Path,
    source_adapter: str,
    experiment_id: str,
) -> pd.DataFrame | None:
    if source_adapter == "synthetic" or not path.is_file():
        return None
    existing = pd.read_csv(path)
    required = {
        "source_adapter",
        "experiment_id",
        "customer_id",
        "assignment_date",
        "assignment",
        "assignment_probability",
        "holdout_flag",
        "stratum",
        "probability_risk_tier",
        "segment",
        "region",
        "acquisition_channel",
        "plan_type",
        "churn_probability_90d",
        "current_mrr",
        "tenure_days",
        "recent_sessions_30d",
        "nps_score_recent",
        "failed_payments_90d",
    }
    if not required.issubset(existing.columns):
        return None
    if set(existing["source_adapter"].astype(str)) != {source_adapter}:
        return None
    if set(existing["experiment_id"].astype(str)) != {experiment_id}:
        return None
    if existing["customer_id"].duplicated().any():
        raise ValueError("Persisted experiment assignment contains duplicate customers")
    return existing


def load_existing_production_outcomes(
    path: Path,
    source_adapter: str,
    assignments: pd.DataFrame,
) -> pd.DataFrame | None:
    if source_adapter == "synthetic" or not path.is_file():
        return None
    existing = pd.read_csv(path, keep_default_na=False)
    required = {
        "experiment_id",
        "customer_id",
        "assignment",
        "stratum",
        "source_adapter",
        "outcome_status",
        "outcome_source",
    }
    if not required.issubset(existing.columns):
        return None
    if set(existing["source_adapter"].astype(str)) != {source_adapter}:
        return None
    if set(existing["experiment_id"].astype(str)) != set(assignments["experiment_id"].astype(str)):
        return None
    if set(existing["customer_id"]) != set(assignments["customer_id"]):
        return None
    if "simulated" in set(existing["outcome_status"]):
        raise ValueError("A production outcome ledger cannot contain simulated outcomes")
    return existing


def validate_experiment(
    assignments: pd.DataFrame,
    balance: pd.DataFrame,
    config: dict[str, Any],
) -> None:
    thresholds = config["quality_thresholds"]
    arm_counts = assignments["assignment"].value_counts()
    if arm_counts.min() < int(thresholds["minimum_arm_size"]):
        raise ValueError(f"Experiment arm below minimum size: {arm_counts.to_dict()}")
    treatment_share = assignments["assignment"].eq("treatment").mean()
    target = float(config["allocation"]["treatment_share"])
    if abs(treatment_share - target) > float(thresholds["maximum_allocation_imbalance"]):
        raise ValueError("Experiment allocation exceeds the configured imbalance threshold")
    numeric_balance = balance[balance["statistic_name"].eq("standardized_mean_difference")]
    maximum_smd = numeric_balance["balance_statistic"].abs().max()
    if maximum_smd > float(thresholds["maximum_absolute_standardized_mean_difference"]):
        raise ValueError(f"Randomization balance failed: maximum absolute SMD={maximum_smd:.3f}")


def main() -> None:
    root = project_root()
    config = load_config(root)
    manifest = load_ingestion_manifest(root)
    source_adapter = str(manifest["adapter"])
    assignment_path = processed_dir() / "intervention_assignments.csv"
    assignment_store_env = str(config["assignment_store"]["file_env"])
    external_assignment_path = os.environ.get(assignment_store_env)
    persisted_assignment_path = assignment_path
    if external_assignment_path:
        persisted_assignment_path = Path(external_assignment_path).expanduser().resolve()
        if not persisted_assignment_path.is_file():
            raise FileNotFoundError(
                f"Persisted assignment file is missing: {persisted_assignment_path}"
            )
    assignments = load_existing_production_assignment(
        persisted_assignment_path,
        source_adapter,
        str(config["experiment_id"]),
    )
    if assignments is None:
        assignments = assign_intervention(
            load_population(root),
            config,
            assignment_date=resolve_assignment_date(config, manifest),
            source_adapter=source_adapter,
        )
    existing_outcomes = load_existing_production_outcomes(
        processed_dir() / "intervention_outcome_ledger.csv",
        source_adapter,
        assignments,
    )
    outcomes = resolve_outcomes(assignments, config, source_adapter, existing_outcomes)
    balance = build_balance_table(assignments)
    incrementality = build_incrementality_table(
        outcomes,
        float(config["observed_outcomes"]["minimum_complete_share_for_estimation"]),
    )
    monitoring = build_outcome_monitoring(outcomes)
    validate_experiment(assignments, balance, config)

    processed = processed_dir()
    tables = outputs_tables_dir()
    processed.mkdir(parents=True, exist_ok=True)
    tables.mkdir(parents=True, exist_ok=True)
    assignments.to_csv(processed / "intervention_assignments.csv", index=False)
    outcomes.to_csv(processed / "intervention_outcome_ledger.csv", index=False)
    balance.to_csv(tables / "intervention_balance.csv", index=False)
    incrementality.to_csv(tables / "intervention_incrementality.csv", index=False)
    monitoring.to_csv(tables / "intervention_outcome_monitoring.csv", index=False)

    saved_row = incrementality.loc[incrementality["metric"].eq("lost_mrr_90d")].iloc[0]
    saved_mrr = saved_row["incremental_saved_mrr"]
    effect_text = (
        f"saved_mrr=${float(saved_mrr):,.2f}"
        if saved_row["estimation_status"] == "estimable"
        else f"effect={saved_row['estimation_status']}"
    )
    print(
        f"Experiment measured: eligible={len(assignments):,}, "
        f"treatment={assignments['assignment'].eq('treatment').sum():,}, "
        f"holdout={assignments['holdout_flag'].sum():,}, {effect_text}."
    )


if __name__ == "__main__":
    main()
