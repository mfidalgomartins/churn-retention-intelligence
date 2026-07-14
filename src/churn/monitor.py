"""Risk-tier transition, calibration, and feature-drift monitoring."""

from __future__ import annotations

from itertools import pairwise
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from churn.common import outputs_tables_dir, project_root


def load_config(root: Path | None = None) -> dict[str, Any]:
    root = root or project_root()
    return yaml.safe_load((root / "config" / "monitoring.yml").read_text(encoding="utf-8"))


def load_inputs(root: Path | None = None) -> dict[str, pd.DataFrame]:
    root = root or project_root()
    return {
        "history": pd.read_csv(
            root / "data" / "processed" / "customer_probability_history.csv",
            parse_dates=["observation_date"],
        ),
        "outcomes": pd.read_csv(root / "outputs" / "tables" / "model_outcome_monitoring.csv"),
        "drift": pd.read_csv(root / "outputs" / "tables" / "model_feature_drift.csv"),
    }


def build_portfolio_trend(history: pd.DataFrame) -> pd.DataFrame:
    frame = history.copy()
    frame["is_critical"] = frame["probability_risk_tier"].eq("critical").astype(int)
    frame["is_high_or_critical"] = (
        frame["probability_risk_tier"].isin(["high", "critical"]).astype(int)
    )
    result = (
        frame.groupby("observation_date", as_index=False)
        .agg(
            scored_customers=("customer_id", "nunique"),
            portfolio_mrr=("current_mrr", "sum"),
            mean_churn_probability=("churn_probability_90d", "mean"),
            critical_customers=("is_critical", "sum"),
            high_or_critical_customers=("is_high_or_critical", "sum"),
        )
        .sort_values("observation_date")
    )
    result["critical_portfolio_share"] = result["critical_customers"] / result["scored_customers"]
    result["high_or_critical_share"] = (
        result["high_or_critical_customers"] / result["scored_customers"]
    )
    result["observation_date"] = result["observation_date"].dt.date.astype(str)
    numeric = [
        "portfolio_mrr",
        "mean_churn_probability",
        "critical_portfolio_share",
        "high_or_critical_share",
    ]
    result[numeric] = result[numeric].round(6)
    return result


def build_transition_history(
    history: pd.DataFrame,
    tier_order: list[str],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    dates = sorted(history["observation_date"].drop_duplicates())
    rank = {tier: index for index, tier in enumerate(tier_order)}
    rows: list[dict[str, Any]] = []
    latest_pairs = pd.DataFrame()
    for from_date, to_date in pairwise(dates):
        interval_days = (pd.Timestamp(to_date) - pd.Timestamp(from_date)).days
        previous = history.loc[
            history["observation_date"].eq(from_date),
            ["customer_id", "probability_risk_tier", "churn_probability_90d"],
        ].rename(
            columns={
                "probability_risk_tier": "from_tier",
                "churn_probability_90d": "from_probability",
            }
        )
        current = history.loc[
            history["observation_date"].eq(to_date),
            ["customer_id", "probability_risk_tier", "churn_probability_90d"],
        ].rename(
            columns={
                "probability_risk_tier": "to_tier",
                "churn_probability_90d": "to_probability",
            }
        )
        pairs = previous.merge(current, on="customer_id", how="inner", validate="one_to_one")
        if pairs.empty:
            continue
        pairs["from_rank"] = pairs["from_tier"].map(rank)
        pairs["to_rank"] = pairs["to_tier"].map(rank)
        critical_entries = (~pairs["from_tier"].eq("critical")) & pairs["to_tier"].eq("critical")
        rows.append(
            {
                "from_date": pd.Timestamp(from_date).date().isoformat(),
                "to_date": pd.Timestamp(to_date).date().isoformat(),
                "interval_days": interval_days,
                "complete_monthly_interval": int(interval_days >= 27),
                "matched_customers": len(pairs),
                "newly_scored_customers": len(current) - len(pairs),
                "exited_scored_population": len(previous) - len(pairs),
                "deteriorated_customers": pairs["to_rank"].gt(pairs["from_rank"]).sum(),
                "improved_customers": pairs["to_rank"].lt(pairs["from_rank"]).sum(),
                "critical_entries": critical_entries.sum(),
                "deteriorated_share": pairs["to_rank"].gt(pairs["from_rank"]).mean(),
                "critical_entry_share": critical_entries.mean(),
                "mean_probability_change": (
                    pairs["to_probability"] - pairs["from_probability"]
                ).mean(),
            }
        )
        if interval_days >= 27:
            latest_pairs = pairs

    transitions = pd.DataFrame(rows)
    numeric = ["deteriorated_share", "critical_entry_share", "mean_probability_change"]
    transitions[numeric] = transitions[numeric].round(6)
    matrix = (
        latest_pairs.groupby(["from_tier", "to_tier"], observed=False)
        .size()
        .rename("customers")
        .reset_index()
    )
    matrix["from_tier_total"] = matrix.groupby("from_tier")["customers"].transform("sum")
    matrix["transition_share"] = (matrix["customers"] / matrix["from_tier_total"]).round(6)
    matrix["from_tier"] = pd.Categorical(matrix["from_tier"], tier_order, ordered=True)
    matrix["to_tier"] = pd.Categorical(matrix["to_tier"], tier_order, ordered=True)
    matrix = matrix.sort_values(["from_tier", "to_tier"]).reset_index(drop=True)
    return transitions, matrix


def _alert_row(
    category: str,
    metric: str,
    value: float,
    threshold: float,
    breached: bool,
    severity: str,
    action: str,
) -> dict[str, Any]:
    return {
        "category": category,
        "metric": metric,
        "status": "alert" if breached else "ok",
        "severity": severity if breached else "info",
        "value": round(float(value), 6),
        "threshold": round(float(threshold), 6),
        "recommended_action": action if breached else "No action required.",
    }


def build_alerts(
    portfolio: pd.DataFrame,
    transitions: pd.DataFrame,
    outcomes: pd.DataFrame,
    drift: pd.DataFrame,
    config: dict[str, Any],
) -> pd.DataFrame:
    thresholds = config["alerts"]
    latest_portfolio = portfolio.iloc[-1]
    monthly_transitions = transitions[transitions["complete_monthly_interval"].eq(1)]
    latest_transition = monthly_transitions.iloc[-1]
    latest_outcome = outcomes.iloc[-1]
    rows = [
        _alert_row(
            "portfolio_risk",
            "critical_portfolio_share",
            latest_portfolio["critical_portfolio_share"],
            thresholds["critical_portfolio_share"],
            latest_portfolio["critical_portfolio_share"] > thresholds["critical_portfolio_share"],
            "high",
            "Review critical accounts and intervention capacity.",
        ),
        _alert_row(
            "risk_migration",
            "critical_entry_share",
            latest_transition["critical_entry_share"],
            thresholds["monthly_critical_entry_share"],
            latest_transition["critical_entry_share"] > thresholds["monthly_critical_entry_share"],
            "high",
            "Inspect newly critical accounts and common leading signals.",
        ),
        _alert_row(
            "risk_migration",
            "mean_probability_change",
            latest_transition["mean_probability_change"],
            thresholds["monthly_probability_increase"],
            latest_transition["mean_probability_change"]
            > thresholds["monthly_probability_increase"],
            "medium",
            "Check whether the portfolio shift is concentrated by segment or channel.",
        ),
        _alert_row(
            "model_outcomes",
            "absolute_calibration_gap",
            abs(float(latest_outcome["calibration_gap"])),
            thresholds["absolute_calibration_gap"],
            abs(float(latest_outcome["calibration_gap"])) > thresholds["absolute_calibration_gap"],
            "high",
            "Recalibrate probabilities after confirming outcome completeness.",
        ),
        _alert_row(
            "model_outcomes",
            "brier_score",
            latest_outcome["brier_score"],
            thresholds["brier_score"],
            latest_outcome["brier_score"] > thresholds["brier_score"],
            "high",
            "Investigate calibration and feature-distribution changes.",
        ),
    ]

    expected = set(config.get("expected_drift_features", []))
    actionable = drift[
        drift["status"].isin(["warning", "critical"]) & ~drift["feature"].isin(expected)
    ]
    maximum_psi = actionable["psi"].max() if not actionable.empty else 0.0
    rows.append(
        _alert_row(
            "feature_drift",
            "maximum_actionable_psi",
            maximum_psi,
            thresholds["feature_psi_warning"],
            maximum_psi >= thresholds["feature_psi_warning"],
            "high" if maximum_psi >= thresholds["feature_psi_critical"] else "medium",
            "Review drifting inputs before retraining or changing decision thresholds.",
        )
    )
    return pd.DataFrame(rows)


def build_monitoring_summary(
    portfolio: pd.DataFrame,
    transitions: pd.DataFrame,
    alerts: pd.DataFrame,
) -> pd.DataFrame:
    latest_portfolio = portfolio.iloc[-1]
    monthly_transitions = transitions[transitions["complete_monthly_interval"].eq(1)]
    latest_transition = monthly_transitions.iloc[-1]
    return pd.DataFrame(
        [
            {
                "as_of_date": latest_portfolio["observation_date"],
                "scored_customers": int(latest_portfolio["scored_customers"]),
                "critical_customers": int(latest_portfolio["critical_customers"]),
                "critical_portfolio_share": latest_portfolio["critical_portfolio_share"],
                "critical_entries": int(latest_transition["critical_entries"]),
                "deteriorated_customers": int(latest_transition["deteriorated_customers"]),
                "open_alerts": int(alerts["status"].eq("alert").sum()),
            }
        ]
    )


def main() -> None:
    root = project_root()
    config = load_config(root)
    inputs = load_inputs(root)
    portfolio = build_portfolio_trend(inputs["history"])
    transitions, matrix = build_transition_history(
        inputs["history"], list(config["risk_tier_order"])
    )
    alerts = build_alerts(portfolio, transitions, inputs["outcomes"], inputs["drift"], config)
    summary = build_monitoring_summary(portfolio, transitions, alerts)

    tables = outputs_tables_dir()
    tables.mkdir(parents=True, exist_ok=True)
    portfolio.to_csv(tables / "risk_tier_portfolio_trend.csv", index=False)
    transitions.to_csv(tables / "risk_tier_transition_history.csv", index=False)
    matrix.to_csv(tables / "risk_tier_transition_matrix.csv", index=False)
    alerts.to_csv(tables / "monitoring_alerts.csv", index=False)
    summary.to_csv(tables / "monitoring_summary.csv", index=False)
    print(
        f"Monitoring complete: transitions={len(transitions):,}, "
        f"open_alerts={int(alerts['status'].eq('alert').sum())}."
    )


if __name__ == "__main__":
    main()
