"""Risk scoring for open customer accounts.

Three interpretable scores feed a four-tier action queue:
    churn_risk_score    — behavioural & operational health (0-100)
    customer_value_score — economic importance via revenue percentiles (0-100)
    retention_priority  — churn risk adjusted by customer value, used to rank
    risk_tier           — critical / high / medium / low
    main_risk_driver    — the signal contributing most to churn_risk_score
    recommended_action  — rule-based play assigned per tier and driver
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from churn.common import docs_dir, outputs_tables_dir, processed_dir

# Policy thresholds are intentionally interpretable. This is a prioritisation
# index, not a calibrated probability model.
SIGNAL_WEIGHTS: dict[str, float] = {
    "usage_decline":   0.26,
    "failed_payments": 0.22,
    "low_nps":         0.20,
    "low_adoption":    0.16,
    "support_burden":  0.16,
}

DRIVER_LABEL: dict[str, str] = {
    "usage_decline":   "usage decline",
    "failed_payments": "failed payments",
    "low_nps":         "low NPS",
    "low_adoption":    "low adoption",
    "support_burden":  "support burden",
}

# Tier cut-offs on retention_priority_score for the canonical synthetic run.
TIER_CUTOFFS: dict[str, float] = {
    "critical": 35.0,
    "high":     25.0,
    "medium":   15.0,
}

REQUIRED_FEATURE_COLUMNS: tuple[str, ...] = (
    "customer_id",
    "segment",
    "region",
    "acquisition_channel",
    "plan_type",
    "tenure_days",
    "current_mrr",
    "avg_monthly_revenue",
    "lifetime_revenue",
    "recent_sessions_30d",
    "usage_trend",
    "support_tickets_90d",
    "nps_score_recent",
    "feature_adoption_score_recent",
    "failed_payments_90d",
    "payment_failure_flag",
    "renewal_near_flag",
    "churn_flag",
    "at_risk_flag",
)

SCORE_OUTPUT_COLUMNS: list[str] = [
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
    "customer_value_score",
    "retention_priority_score",
    "risk_tier",
    "main_risk_driver",
    "recommended_action",
    "recommendation_context",
    "at_risk_flag",
    "payment_failure_flag",
    "renewal_near_flag",
    "usage_trend",
    "support_tickets_90d",
    "nps_score_recent",
    "feature_adoption_score_recent",
    "failed_payments_90d",
]

TIER_SUMMARY_COLUMNS: list[str] = [
    "risk_tier",
    "customers",
    "share_of_scored_base",
    "total_current_mrr",
    "avg_churn_risk_score",
    "avg_customer_value_score",
    "avg_retention_priority_score",
]


def _validate_feature_input(features: pd.DataFrame) -> None:
    missing = sorted(set(REQUIRED_FEATURE_COLUMNS) - set(features.columns))
    if missing:
        raise ValueError(f"features missing required columns: {missing}")


def normalize_signals(df: pd.DataFrame) -> pd.DataFrame:
    """Map raw features to 0-1 signals. Higher = more risk."""
    return pd.DataFrame({
        "usage_decline":   ((-df["usage_trend"]) / 5.0).clip(0, 1),
        "failed_payments": (df["failed_payments_90d"] / 1.5).clip(0, 1),
        "support_burden":  ((df["support_tickets_90d"] - 3.0) / 6.0).clip(0, 1),
        "low_nps":         ((20.0 - df["nps_score_recent"]) / 25.0).clip(0, 1),
        "low_adoption":    ((50.0 - df["feature_adoption_score_recent"]) / 25.0).clip(0, 1),
    })


def churn_risk_score(df: pd.DataFrame, signals: pd.DataFrame) -> pd.Series:
    """Behavioural risk on a 0-100 scale.

    Base = weighted sum of normalised signals (interpretable additive model).
    Concentration bonus = policy assumption that co-firing signals warrant
    more attention than isolated ones.
    Operational adjustments = renewal proximity and dormancy.
    """
    base = sum(signals[k] * w for k, w in SIGNAL_WEIGHTS.items()) * 100.0

    n_active = (signals >= 0.5).sum(axis=1)
    concentration_bonus = (n_active - 2).clip(lower=0) * 4.0

    renewal_amplifier = df["renewal_near_flag"] * base.clip(upper=60.0) * 0.10
    dormancy_adj = (df["recent_sessions_30d"] == 0).astype(int) * 5.0

    score = base + concentration_bonus + renewal_amplifier + dormancy_adj
    return score.clip(0, 100).round(2)


def customer_value_score(df: pd.DataFrame) -> pd.Series:
    """Economic importance via percentile ranks of revenue signals."""
    current = df["current_mrr"].rank(method="average", pct=True)
    avg = df["avg_monthly_revenue"].rank(method="average", pct=True)
    lifetime = df["lifetime_revenue"].rank(method="average", pct=True)
    return (100.0 * (0.60 * current + 0.30 * avg + 0.10 * lifetime)).round(2)


def assign_tiers(priority: pd.Series, churn: pd.Series, value: pd.Series) -> pd.Series:
    """Vectorised tier assignment.

    The double-condition override (very high behavioural risk + high
    revenue stake) catches accounts that don't quite cross the priority bar
    but are individually high-impact saves.
    """
    critical = (priority >= TIER_CUTOFFS["critical"]) | ((churn >= 45.0) & (value >= 70.0))
    high = priority >= TIER_CUTOFFS["high"]
    medium = priority >= TIER_CUTOFFS["medium"]
    return pd.Series(
        np.select([critical, high, medium], ["critical", "high", "medium"], default="low"),
        index=priority.index,
    )


def main_risk_driver(signals: pd.DataFrame) -> pd.Series:
    """Per-customer driver = signal with the largest weighted contribution."""
    contributions = pd.DataFrame({k: signals[k] * w for k, w in SIGNAL_WEIGHTS.items()})
    top_key = contributions.idxmax(axis=1)
    driver = top_key.map(DRIVER_LABEL)
    return driver.mask(contributions.max(axis=1) <= 0, "no material signal")


def recommend_actions(scored: pd.DataFrame) -> pd.Series:
    """Rule-based play assignment. Order matters: first match wins."""
    tier = scored["risk_tier"]
    driver = scored["main_risk_driver"]
    churn = scored["churn_risk_score"]
    value = scored["customer_value_score"]

    conditions = [
        (tier == "critical") & (value >= 70.0),
        (driver == "failed payments") & (churn >= 45.0),
        (driver.isin(["usage decline", "low adoption"])) & (churn >= 35.0),
        (scored["renewal_near_flag"] == 1) & (tier.isin(["medium", "high", "critical"])),
        tier.isin(["high", "critical"]),
        (tier == "medium") & (driver.isin(["support burden", "low NPS"])),
    ]
    choices = [
        "executive save motion",
        "billing intervention",
        "product adoption campaign",
        "renewal conversation",
        "customer success outreach",
        "customer success outreach",
    ]
    return pd.Series(np.select(conditions, choices, default="monitor only"), index=scored.index)


def recommendation_context(scored: pd.DataFrame) -> pd.Series:
    return (
        "Tier=" + scored["risk_tier"]
        + "; driver=" + scored["main_risk_driver"]
        + "; churn_risk=" + scored["churn_risk_score"].round(1).astype(str)
        + "; customer_value=" + scored["customer_value_score"].round(1).astype(str)
        + "; priority=" + scored["retention_priority_score"].round(1).astype(str)
        + "; current_mrr=$" + scored["current_mrr"].round(2).astype(str)
    )


def compute_scores(features: pd.DataFrame) -> pd.DataFrame:
    _validate_feature_input(features)
    scored = features[features["churn_flag"] == 0].copy()
    if scored.empty:
        return pd.DataFrame(columns=SCORE_OUTPUT_COLUMNS)

    signals = normalize_signals(scored)

    scored["churn_risk_score"] = churn_risk_score(scored, signals).values
    scored["customer_value_score"] = customer_value_score(scored).values
    scored["retention_priority_score"] = (
        scored["churn_risk_score"] * (0.65 + 0.35 * scored["customer_value_score"] / 100.0)
    ).round(2)

    scored["risk_tier"] = assign_tiers(
        scored["retention_priority_score"],
        scored["churn_risk_score"],
        scored["customer_value_score"],
    ).values
    scored["main_risk_driver"] = main_risk_driver(signals).values
    scored["recommended_action"] = recommend_actions(scored).values
    scored["recommendation_context"] = recommendation_context(scored).values

    scored = scored.sort_values(
        ["retention_priority_score", "current_mrr"], ascending=[False, False]
    ).reset_index(drop=True)

    return scored[SCORE_OUTPUT_COLUMNS]


def risk_tier_summary(scored: pd.DataFrame) -> pd.DataFrame:
    if scored.empty:
        return pd.DataFrame(
            [
                {
                    "risk_tier": tier,
                    "customers": 0,
                    "share_of_scored_base": 0.0,
                    "total_current_mrr": 0.0,
                    "avg_churn_risk_score": 0.0,
                    "avg_customer_value_score": 0.0,
                    "avg_retention_priority_score": 0.0,
                }
                for tier in ["critical", "high", "medium", "low"]
            ],
            columns=TIER_SUMMARY_COLUMNS,
        )

    summary = scored.groupby("risk_tier", as_index=False).agg(
        customers=("customer_id", "count"),
        share_of_scored_base=("customer_id", lambda s: len(s) / len(scored)),
        total_current_mrr=("current_mrr", "sum"),
        avg_churn_risk_score=("churn_risk_score", "mean"),
        avg_customer_value_score=("customer_value_score", "mean"),
        avg_retention_priority_score=("retention_priority_score", "mean"),
    )
    tier_order = pd.Categorical(
        summary["risk_tier"], categories=["critical", "high", "medium", "low"], ordered=True
    )
    summary = summary.assign(_order=tier_order).sort_values("_order").drop(columns=["_order"])
    summary["share_of_scored_base"] = summary["share_of_scored_base"].round(6)
    summary["total_current_mrr"] = summary["total_current_mrr"].round(2)
    summary["avg_churn_risk_score"] = summary["avg_churn_risk_score"].round(2)
    summary["avg_customer_value_score"] = summary["avg_customer_value_score"].round(2)
    summary["avg_retention_priority_score"] = summary["avg_retention_priority_score"].round(2)
    return summary.reset_index(drop=True)


def write_methodology_note() -> None:
    out = docs_dir() / "methodology" / "risk_scoring_methodology.md"
    out.parent.mkdir(parents=True, exist_ok=True)
    weights_block = "\n".join(f"  - `{k}` → {v:.2f}" for k, v in SIGNAL_WEIGHTS.items())
    cutoffs_block = "\n".join(f"  - `{k}` → priority ≥ {v}" for k, v in TIER_CUTOFFS.items())
    text = f"""# Risk Scoring Methodology

Open accounts only (`churn_flag = 0`). `at_risk_flag` is excluded from the
score to avoid label leakage from the simulator's own status field.

## Signals (0-1, higher = more risk)
- `usage_decline   = clip(-usage_trend / 5, 0, 1)`
- `failed_payments = clip(failed_payments_90d / 1.5, 0, 1)`
- `support_burden  = clip((support_tickets_90d - 3) / 6, 0, 1)`
- `low_nps         = clip((20 - nps_score_recent) / 25, 0, 1)`
- `low_adoption    = clip((50 - feature_adoption_score_recent) / 25, 0, 1)`

The thresholds are policy choices for this synthetic case. The score is an
interpretable prioritisation index, not a calibrated churn probability.

## Churn risk score (0-100)
```
churn_risk = 100 * sum(signal_i * weight_i)
           + 4 * max(0, count(signals >= 0.5) - 2)        # concentration bonus
           + renewal_near_flag * min(base, 60) * 0.10     # renewal amplifier
           + (recent_sessions_30d == 0) * 5
clip to [0, 100]
```

Weights:
{weights_block}

The concentration bonus encodes the policy assumption that co-firing signals
warrant more attention than isolated ones; it adds 4 points per signal beyond
the second.

## Customer value score (0-100)
```
customer_value = 100 * (0.60 * rank_pct(current_mrr)
                      + 0.30 * rank_pct(avg_monthly_revenue)
                      + 0.10 * rank_pct(lifetime_revenue))
```

## Retention priority (0-100)
```
retention_priority = churn_risk * (0.65 + 0.35 * customer_value / 100)
```
Customer value ranks risky accounts; it cannot create a priority signal when
behavioural churn risk is zero.

## Tiering
{cutoffs_block}
  - `critical` override: `churn_risk >= 45 AND customer_value >= 70`

## Main risk driver
The signal with the largest weighted contribution. Accounts with no positive
signal are labelled `no material signal`.

## Recommended actions
First match wins:
- `executive save motion` — critical tier with customer_value ≥ 70
- `billing intervention` — failed payments driver, churn_risk ≥ 45
- `product adoption campaign` — usage / adoption driver, churn_risk ≥ 35
- `renewal conversation` — renewal_near_flag + tier ≥ medium
- `customer success outreach` — any remaining high/critical, plus support/NPS medium
- `monitor only` — otherwise

## Outputs
- `data/processed/customer_risk_scores.csv` — one row per scored customer
- `outputs/tables/risk_tier_summary.csv` — tier-level rollup
"""
    out.write_text(text, encoding="utf-8")


def main() -> None:
    processed = processed_dir()
    outputs = outputs_tables_dir()
    outputs.mkdir(parents=True, exist_ok=True)
    processed.mkdir(parents=True, exist_ok=True)

    features = pd.read_csv(processed / "customer_retention_features.csv")
    scored = compute_scores(features)
    summary = risk_tier_summary(scored)

    scored.to_csv(processed / "customer_risk_scores.csv", index=False)
    summary.to_csv(outputs / "risk_tier_summary.csv", index=False)
    write_methodology_note()

    print(f"Scored {len(scored)} customers across {summary['risk_tier'].nunique()} tiers.")
    if scored.empty:
        print("Top priority: none (no open customers to score).")
    else:
        print(f"Top priority: {scored.iloc[0]['customer_id']} @ {scored.iloc[0]['retention_priority_score']:.1f}")


if __name__ == "__main__":
    main()
