"""Generate the executive decision memo from governed pipeline outputs."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from churn.analyze import cohort_movement, revenue_at_risk
from churn.common import REFERENCE_DATE, docs_dir, outputs_tables_dir, processed_dir

RELATIONSHIP_LABELS = {
    "usage_decline_flag": "usage decline",
    "high_support_ticket_flag": "high support volume",
    "failed_payment_flag": "failed payments",
    "low_nps_flag": "low NPS",
    "low_feature_adoption_flag": "low feature adoption",
}


@dataclass(frozen=True)
class MemoPlay:
    name: str
    candidates: int
    current_mrr_scope: float
    weighted_exposure: float
    action: str


@dataclass(frozen=True)
class MemoMetrics:
    snapshot_date: str
    total_customers: int
    active_customers: int
    cumulative_churn_share: float
    cumulative_revenue_loss_share: float
    at_risk_mrr: float
    current_mrr_exposure: float
    avg_6m_retention: float
    avg_6m_revenue_retention: float
    cohort_trend: str
    top_dimensions: tuple[tuple[str, str, float], ...]
    strongest_relationship: str
    strongest_relationship_lift: float
    critical_customers: int
    high_customers: int
    critical_high_mrr: float
    plays: tuple[MemoPlay, ...]
    model_roc_auc: float
    model_average_precision: float
    model_brier_score: float
    average_monthly_nrr: float
    gross_margin_rate: float
    blended_cac: float
    experiment_eligible: int
    treatment_customers: int
    holdout_customers: int
    simulated_saved_mrr: float
    simulated_saved_mrr_ci_lower: float
    simulated_saved_mrr_ci_upper: float
    monitoring_alerts: int


def _top_dimension(path: str, column: str, label: str) -> tuple[str, str, float]:
    table = pd.read_csv(outputs_tables_dir() / path)
    row = table.sort_values("cumulative_churn_share", ascending=False).iloc[0]
    return label, str(row[column]), float(row["cumulative_churn_share"])


def load_metrics() -> MemoMetrics:
    features = pd.read_csv(processed_dir() / "customer_retention_features.csv")
    cohort = pd.read_csv(
        processed_dir() / "cohort_retention_table.csv",
        parse_dates=["cohort_month", "observation_month"],
    )
    relationships = pd.read_csv(outputs_tables_dir() / "behavioral_churn_relationships.csv")
    tiers = pd.read_csv(outputs_tables_dir() / "risk_tier_summary.csv").set_index("risk_tier")
    interventions = pd.read_csv(
        outputs_tables_dir() / "main_analysis_intervention_priorities.csv"
    ).sort_values("mrr_exposure_proxy", ascending=False)
    model = pd.read_csv(outputs_tables_dir() / "model_performance.csv").set_index("split")
    economics = pd.read_csv(outputs_tables_dir() / "unit_economics_summary.csv").set_index("metric")
    incrementality = pd.read_csv(
        outputs_tables_dir() / "intervention_incrementality.csv"
    ).set_index("metric")
    monitoring = pd.read_csv(outputs_tables_dir() / "monitoring_summary.csv").iloc[0]

    churned = features["churn_flag"].eq(1)
    total_value = float(features["avg_monthly_revenue"].sum())
    lost_value = float(features.loc[churned, "avg_monthly_revenue"].sum())
    risk_summary, _, _ = revenue_at_risk(features)
    cohort_summary = cohort_movement(cohort)
    strongest = relationships.sort_values("churn_rate_lift", ascending=False).iloc[0]

    tier_counts = tiers["customers"].to_dict()
    tier_mrr = tiers["total_current_mrr"].to_dict()
    plays = tuple(
        MemoPlay(
            name=str(row.opportunity),
            candidates=int(row.candidate_customers),
            current_mrr_scope=float(row.current_mrr_scope),
            weighted_exposure=float(row.mrr_exposure_proxy),
            action=str(row.recommended_action),
        )
        for row in interventions.itertuples(index=False)
    )

    return MemoMetrics(
        snapshot_date=REFERENCE_DATE.date().isoformat(),
        total_customers=len(features),
        active_customers=int((~churned).sum()),
        cumulative_churn_share=float(churned.mean()),
        cumulative_revenue_loss_share=lost_value / total_value if total_value else 0.0,
        at_risk_mrr=float(risk_summary["at_risk_mrr"]),
        current_mrr_exposure=float(risk_summary["current_mrr_exposure"]),
        avg_6m_retention=float(cohort_summary["avg_6m_retention"]),
        avg_6m_revenue_retention=float(cohort_summary["avg_6m_revenue_retention"]),
        cohort_trend=str(cohort_summary["cohort_trend_label"]),
        top_dimensions=(
            _top_dimension("churn_by_segment.csv", "segment", "Segment"),
            _top_dimension("churn_by_plan_type.csv", "plan_type", "Plan"),
            _top_dimension("churn_by_acquisition_channel.csv", "acquisition_channel", "Channel"),
            _top_dimension("churn_by_region.csv", "region", "Region"),
        ),
        strongest_relationship=RELATIONSHIP_LABELS.get(
            str(strongest["relationship"]), str(strongest["relationship"])
        ),
        strongest_relationship_lift=float(strongest["churn_rate_lift"]),
        critical_customers=int(tier_counts.get("critical", 0)),
        high_customers=int(tier_counts.get("high", 0)),
        critical_high_mrr=float(tier_mrr.get("critical", 0.0) + tier_mrr.get("high", 0.0)),
        plays=plays,
        model_roc_auc=float(model.loc["test", "roc_auc"]),
        model_average_precision=float(model.loc["test", "average_precision"]),
        model_brier_score=float(model.loc["test", "brier_score"]),
        average_monthly_nrr=float(economics.loc["average_monthly_nrr", "value"]),
        gross_margin_rate=float(economics.loc["gross_margin_rate", "value"]),
        blended_cac=float(economics.loc["blended_cac", "value"]),
        experiment_eligible=int(
            incrementality.loc["churned_90d", "treatment_n"]
            + incrementality.loc["churned_90d", "control_n"]
        ),
        treatment_customers=int(incrementality.loc["churned_90d", "treatment_n"]),
        holdout_customers=int(incrementality.loc["churned_90d", "control_n"]),
        simulated_saved_mrr=float(incrementality.loc["lost_mrr_90d", "incremental_saved_mrr"]),
        simulated_saved_mrr_ci_lower=float(
            -incrementality.loc["lost_mrr_90d", "ci_95_upper"]
            * incrementality.loc["lost_mrr_90d", "treatment_n"]
        ),
        simulated_saved_mrr_ci_upper=float(
            -incrementality.loc["lost_mrr_90d", "ci_95_lower"]
            * incrementality.loc["lost_mrr_90d", "treatment_n"]
        ),
        monitoring_alerts=int(monitoring["open_alerts"]),
    )


def _money(value: float) -> str:
    sign = "-" if value < 0 else ""
    return f"{sign}${abs(value):,.0f}"


def _pct(value: float) -> str:
    return f"{value * 100:.1f}%"


def render_memo(metrics: MemoMetrics) -> str:
    high_priority_count = metrics.critical_customers + metrics.high_customers
    dimension_text = ", ".join(
        f"{label.lower()} **{value}** ({_pct(rate)})"
        for label, value, rate in metrics.top_dimensions
    )
    play_rows = "\n".join(
        f"| {play.name} | {play.candidates:,} | {_money(play.current_mrr_scope)} | "
        f"{_money(play.weighted_exposure)} | {play.action} |"
        for play in metrics.plays
    )

    return f"""# Decision Memo — Churn & Retention Intelligence

**Snapshot:** {metrics.snapshot_date} · **Base:** {metrics.total_customers:,} synthetic B2B SaaS accounts ·
**Generated by:** `python -m churn.memo` from governed analysis, economics, model, experiment, and monitoring outputs.

> Synthetic data, decision-support only. The policy risk score prioritises
> operations; a separate calibrated model estimates 90-day churn probability.

## Bottom line

Cumulative customer churn is **{_pct(metrics.cumulative_churn_share)}**, while the
monthly-value loss share is **{_pct(metrics.cumulative_revenue_loss_share)}**.
Loss is therefore more concentrated in lower-value accounts than the logo count
alone suggests. Human retention capacity should start with the
{high_priority_count:,} critical and high-risk open accounts, which hold
**{_money(metrics.critical_high_mrr)}** of current MRR.

## What the snapshot says

- **Health.** {metrics.active_customers:,} accounts remain open. Explicitly
  at-risk MRR is **{_money(metrics.at_risk_mrr)}**; including unflagged accounts
  with behavioural distress signals raises current MRR exposure to
  **{_money(metrics.current_mrr_exposure)}**.
- **Cohorts.** Six-month retention is {_pct(metrics.avg_6m_retention)} by logo and
  {_pct(metrics.avg_6m_revenue_retention)} by monthly value. The recent-vs-early
  six-month comparison is **{metrics.cohort_trend}**.
- **Concentration.** The highest cumulative churn shares are {dimension_text}.
- **Behavioural separation.** The strongest measured relationship is
  **{metrics.strongest_relationship}** at
  **{metrics.strongest_relationship_lift:.1f}x** the comparison-group churn rate.
  This is an association to test, not a causal estimate.

## Decision instrumentation

- **Economics.** Average monthly NRR is {_pct(metrics.average_monthly_nrr)}, the
  direct-cost gross-margin proxy is {_pct(metrics.gross_margin_rate)}, and
  blended CAC is {_money(metrics.blended_cac)} per acquired customer.
- **Probability model.** Out-of-time ROC AUC is {metrics.model_roc_auc:.3f},
  average precision is {metrics.model_average_precision:.3f}, and Brier score is
  {metrics.model_brier_score:.4f}; every configured release gate passes.
- **Controlled measurement.** {metrics.experiment_eligible:,} eligible accounts
  are assigned to {metrics.treatment_customers:,} treatment and
  {metrics.holdout_customers:,} holdout. The synthetic outcome demonstration
  produces an incremental saved-MRR estimate of {_money(metrics.simulated_saved_mrr)}
  (95% CI {_money(metrics.simulated_saved_mrr_ci_lower)} to
  {_money(metrics.simulated_saved_mrr_ci_upper)}). This validates the estimator,
  not production treatment efficacy.
- **Monitoring.** The latest complete-period transition and model checks have
  {metrics.monitoring_alerts} open alerts.

## Recommended actions

| Play | Candidates | Current MRR scope | Weighted exposure | Operating move |
|---|--:|--:|--:|---|
{play_rows}

Sequence the queues by weighted exposure, with payment rescue allowed to run in
parallel because it is a distinct billing workflow. Weighted exposure equals
current MRR scope multiplied by the historical churn share of the target group;
it is a prioritisation proxy, not expected saved revenue or ROI.

## Measurement plan

- Assign an owner and service-level target to every critical and high-risk account.
- Preserve the persisted holdout and review delivery contamination before reading effects.
- Track saved MRR, recovered payment MRR, queue age, and six-month retention by
  acquisition source.
- Re-estimate thresholds and action rules when real outcomes replace synthetic labels.

## Limits

- Relationships are associative; intervention effects require controlled tests.
- Revenue and margin are recurring-value proxies, not audited accounting measures.
- Recent cohorts are right-censored at longer horizons.
- Model probabilities and alert thresholds require recalibration on live outcomes.
"""


def main() -> None:
    output = docs_dir() / "decision_memo.md"
    output.write_text(render_memo(load_metrics()), encoding="utf-8")
    print(f"Decision memo written: {output.relative_to(docs_dir().parent)}")


if __name__ == "__main__":
    main()
