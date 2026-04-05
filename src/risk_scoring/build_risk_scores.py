from __future__ import annotations

from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd


def clip01(series: pd.Series) -> pd.Series:
    return series.clip(lower=0.0, upper=1.0)


def load_features(processed_dir: Path) -> pd.DataFrame:
    df = pd.read_csv(processed_dir / "customer_retention_features.csv")
    return df


def compute_scores(df: pd.DataFrame) -> pd.DataFrame:
    # Risk scoring targets recoverable customers only.
    scored = df[df["churn_flag"] == 0].copy()

    # 1) Churn risk inputs scaled to 0-1.
    scored["usage_decline_signal"] = clip01((-scored["usage_trend"]) / 4.0)
    scored["failed_payment_signal"] = clip01(scored["failed_payments_90d"] / 2.0)
    scored["support_burden_signal"] = clip01(scored["support_tickets_90d"] / 6.0)
    scored["low_nps_signal"] = clip01((25.0 - scored["nps_score_recent"]) / 35.0)
    scored["low_adoption_signal"] = clip01((55.0 - scored["feature_adoption_score_recent"]) / 35.0)

    dominant_health_signal = np.maximum.reduce(
        [
            scored["usage_decline_signal"].to_numpy(),
            scored["failed_payment_signal"].to_numpy(),
            scored["low_nps_signal"].to_numpy(),
            scored["low_adoption_signal"].to_numpy(),
        ]
    )
    scored["contract_renewal_risk_signal"] = scored["renewal_near_flag"].astype(float) * (
        0.60 + 0.40 * dominant_health_signal
    )

    # Weighted base score (interpretable additive model).
    weights = {
        "usage_decline_signal": 0.24,
        "failed_payment_signal": 0.22,
        "support_burden_signal": 0.16,
        "low_nps_signal": 0.18,
        "low_adoption_signal": 0.14,
        "contract_renewal_risk_signal": 0.06,
    }

    weighted_base = sum(scored[col] * w for col, w in weights.items())

    # Transparent adjustments for strong operational risk flags.
    scored["at_risk_adjustment"] = scored["at_risk_flag"] * 12.0
    scored["contraction_adjustment"] = scored["contraction_flag"] * 8.0
    scored["dormancy_adjustment"] = (scored["recent_sessions_30d"] == 0).astype(int) * 6.0

    scored["churn_risk_score"] = (100.0 * weighted_base) + scored[
        ["at_risk_adjustment", "contraction_adjustment", "dormancy_adjustment"]
    ].sum(axis=1)
    scored["churn_risk_score"] = scored["churn_risk_score"].clip(lower=0.0, upper=100.0).round(2)

    # 2) Revenue risk score using percentile ranks of value variables.
    current_mrr_rank = scored["current_mrr"].rank(method="average", pct=True)
    avg_monthly_rank = scored["avg_monthly_revenue"].rank(method="average", pct=True)
    lifetime_rank = scored["lifetime_revenue"].rank(method="average", pct=True)

    scored["revenue_risk_score"] = (
        100.0 * (0.60 * current_mrr_rank + 0.30 * avg_monthly_rank + 0.10 * lifetime_rank)
    ).round(2)

    # 3) Retention priority score (risk + economic importance).
    scored["retention_priority_score"] = (
        0.65 * scored["churn_risk_score"] + 0.35 * scored["revenue_risk_score"]
    ).round(2)

    # 4) Risk tier.
    def assign_tier(row: pd.Series) -> str:
        if row["retention_priority_score"] >= 75 or (
            row["churn_risk_score"] >= 85 and row["revenue_risk_score"] >= 70
        ):
            return "critical"
        if row["retention_priority_score"] >= 60:
            return "high"
        if row["retention_priority_score"] >= 40:
            return "medium"
        return "low"

    scored["risk_tier"] = scored.apply(assign_tier, axis=1)

    # 5) Main risk driver from largest weighted contribution.
    contribution_map = {
        "usage decline": scored["usage_decline_signal"] * weights["usage_decline_signal"] * 100.0,
        "failed payments": scored["failed_payment_signal"] * weights["failed_payment_signal"] * 100.0,
        "support burden": scored["support_burden_signal"] * weights["support_burden_signal"] * 100.0,
        "low NPS": scored["low_nps_signal"] * weights["low_nps_signal"] * 100.0,
        "low adoption": scored["low_adoption_signal"] * weights["low_adoption_signal"] * 100.0,
        "contract renewal risk": scored["contract_renewal_risk_signal"] * weights["contract_renewal_risk_signal"] * 100.0,
    }
    contribution_df = pd.DataFrame(contribution_map)
    scored["main_risk_driver"] = contribution_df.idxmax(axis=1)

    # 6) Recommended action.
    def recommend_action(row: pd.Series) -> str:
        if row["risk_tier"] == "critical" and row["revenue_risk_score"] >= 70:
            return "executive save motion"
        if row["main_risk_driver"] == "failed payments" and row["churn_risk_score"] >= 45:
            return "billing intervention"
        if row["main_risk_driver"] == "contract renewal risk" and (
            row["risk_tier"] in {"medium", "high", "critical"} or row["revenue_risk_score"] >= 85
        ):
            return "renewal conversation"
        if row["main_risk_driver"] in {"usage decline", "low adoption"} and row["churn_risk_score"] >= 35:
            return "product adoption campaign"
        if row["risk_tier"] in {"high", "critical"}:
            return "customer success outreach"
        if row["risk_tier"] == "medium" and row["main_risk_driver"] in {"support burden", "low NPS"}:
            return "customer success outreach"
        return "monitor only"

    scored["recommended_action"] = scored.apply(recommend_action, axis=1)

    def action_context(row: pd.Series) -> str:
        return (
            f"Tier={row['risk_tier']}; driver={row['main_risk_driver']}; "
            f"churn_risk={row['churn_risk_score']:.1f}; revenue_risk={row['revenue_risk_score']:.1f}; "
            f"priority={row['retention_priority_score']:.1f}; current_mrr=${row['current_mrr']:.2f}."
        )

    scored["recommendation_context"] = scored.apply(action_context, axis=1)

    scored = scored.sort_values(
        ["retention_priority_score", "current_mrr"], ascending=[False, False]
    ).reset_index(drop=True)

    output_cols = [
        "customer_id",
        "segment",
        "region",
        "acquisition_channel",
        "plan_type",
        "tenure_days",
        "current_mrr",
        "avg_monthly_revenue",
        "lifetime_revenue",
        "churn_risk_score",
        "revenue_risk_score",
        "retention_priority_score",
        "risk_tier",
        "main_risk_driver",
        "recommended_action",
        "recommendation_context",
        "at_risk_flag",
        "payment_failure_flag",
        "renewal_near_flag",
        "contraction_flag",
        "usage_trend",
        "support_tickets_90d",
        "nps_score_recent",
        "feature_adoption_score_recent",
        "failed_payments_90d",
    ]

    return scored[output_cols]


def risk_tier_summary(scored: pd.DataFrame) -> pd.DataFrame:
    summary = scored.groupby("risk_tier", as_index=False).agg(
        customers=("customer_id", "count"),
        share_of_scored_base=("customer_id", lambda s: len(s) / len(scored)),
        total_current_mrr=("current_mrr", "sum"),
        avg_churn_risk_score=("churn_risk_score", "mean"),
        avg_revenue_risk_score=("revenue_risk_score", "mean"),
        avg_retention_priority_score=("retention_priority_score", "mean"),
    )

    tier_order = pd.Categorical(summary["risk_tier"], categories=["critical", "high", "medium", "low"], ordered=True)
    summary = summary.assign(_order=tier_order).sort_values("_order").drop(columns=["_order"]) 
    summary["share_of_scored_base"] = summary["share_of_scored_base"].round(6)
    summary["total_current_mrr"] = summary["total_current_mrr"].round(2)
    summary["avg_churn_risk_score"] = summary["avg_churn_risk_score"].round(2)
    summary["avg_revenue_risk_score"] = summary["avg_revenue_risk_score"].round(2)
    summary["avg_retention_priority_score"] = summary["avg_retention_priority_score"].round(2)

    return summary


def write_methodology_note(docs_dir: Path) -> None:
    methodology_dir = docs_dir / "methodology"
    methodology_dir.mkdir(parents=True, exist_ok=True)
    created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    text = f"""# Risk Scoring Methodology Note

Generated at: `{created_at}`

## Scope
- This scoring layer targets **recoverable customers only** (`churn_flag = 0`).
- Goal: prioritize intervention effort by combining behavioral churn risk and economic importance.

## Score 1: `churn_risk_score` (0-100)
`churn_risk_score = 100 * weighted_signal_sum + adjustments`

Where weighted signal sum is:
- `0.24 * usage_decline_signal`
- `0.22 * failed_payment_signal`
- `0.16 * support_burden_signal`
- `0.18 * low_nps_signal`
- `0.14 * low_adoption_signal`
- `0.06 * contract_renewal_risk_signal`

Signal definitions (all clipped to `[0,1]`):
- `usage_decline_signal = clip((-usage_trend) / 4, 0, 1)`
- `failed_payment_signal = clip(failed_payments_90d / 2, 0, 1)`
- `support_burden_signal = clip(support_tickets_90d / 6, 0, 1)`
- `low_nps_signal = clip((25 - nps_score_recent) / 35, 0, 1)`
- `low_adoption_signal = clip((55 - feature_adoption_score_recent) / 35, 0, 1)`
- `contract_renewal_risk_signal = renewal_near_flag * (0.60 + 0.40 * max(usage_decline_signal, failed_payment_signal, low_nps_signal, low_adoption_signal))`

Adjustments:
- `+12` if `at_risk_flag = 1`
- `+8` if `contraction_flag = 1`
- `+6` if `recent_sessions_30d = 0`

Final score is capped to `[0,100]`.

## Score 2: `revenue_risk_score` (0-100)
Percentile-based value importance:
- `revenue_risk_score = 100 * (0.60 * rank_pct(current_mrr) + 0.30 * rank_pct(avg_monthly_revenue) + 0.10 * rank_pct(lifetime_revenue))`

## Score 3: `retention_priority_score` (0-100)
Combined intervention priority:
- `retention_priority_score = 0.65 * churn_risk_score + 0.35 * revenue_risk_score`

## Score 4: `risk_tier`
- `critical`: priority >= 75 OR (`churn_risk_score >= 85` AND `revenue_risk_score >= 70`)
- `high`: priority >= 60 and not critical
- `medium`: priority >= 40 and not high/critical
- `low`: priority < 40

## Score 5: `main_risk_driver`
Assigned as the largest weighted component among:
- usage decline
- failed payments
- support burden
- low NPS
- low adoption
- contract renewal risk

## Score 6: `recommended_action`
Rules:
- `executive save motion`: critical tier and high revenue importance
- `billing intervention`: failed payments is main driver with meaningful churn risk
- `renewal conversation`: contract renewal risk is main driver for medium+ risk accounts, or high-value renewal-near accounts
- `product adoption campaign`: usage decline or low adoption main driver in elevated risk
- `customer success outreach`: all high/critical accounts, plus medium-tier support/NPS-led risk
- `monitor only`: low-priority low-risk accounts

## Output Tables
- `data/processed/customer_risk_scores.csv`
- `data/processed/customer_risk_priority_ranked.csv`
- `outputs/tables/risk_tier_summary.csv`
"""

    (methodology_dir / "risk_scoring_methodology.md").write_text(text, encoding="utf-8")


def main() -> None:
    project_root = Path(__file__).resolve().parents[2]
    processed_dir = project_root / "data" / "processed"
    outputs_dir = project_root / "outputs" / "tables"
    docs_dir = project_root / "docs"

    outputs_dir.mkdir(parents=True, exist_ok=True)
    processed_dir.mkdir(parents=True, exist_ok=True)

    features = load_features(processed_dir)
    scored = compute_scores(features)
    tier_summary = risk_tier_summary(scored)

    scored.to_csv(processed_dir / "customer_risk_scores.csv", index=False)
    scored.sort_values(["retention_priority_score", "current_mrr"], ascending=[False, False]).to_csv(
        processed_dir / "customer_risk_priority_ranked.csv", index=False
    )
    tier_summary.to_csv(outputs_dir / "risk_tier_summary.csv", index=False)

    write_methodology_note(docs_dir)

    print("Risk scoring completed.")
    print("Scored customers:", len(scored))
    print("Top customer by priority:", scored.iloc[0]["customer_id"], "score", scored.iloc[0]["retention_priority_score"])


if __name__ == "__main__":
    main()
