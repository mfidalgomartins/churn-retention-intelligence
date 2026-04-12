from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd


def pct(value: float) -> str:
    return f"{value * 100:.1f}%"


def money(value: float) -> str:
    return f"${value:,.0f}"


def load_inputs(project_root: Path) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    features = pd.read_csv(project_root / "data" / "processed" / "customer_retention_features.csv")
    cohort = pd.read_csv(
        project_root / "data" / "processed" / "cohort_retention_table.csv",
        parse_dates=["cohort_month", "observation_month"],
    )
    subscriptions = pd.read_csv(
        project_root / "data" / "raw" / "subscriptions.csv",
        parse_dates=["subscription_start_date", "subscription_end_date"],
    )
    return features, cohort, subscriptions


def monthly_retention_trend(subscriptions: pd.DataFrame, snapshot_date: pd.Timestamp) -> pd.DataFrame:
    start_month = subscriptions["subscription_start_date"].min().to_period("M")
    end_month = snapshot_date.to_period("M")

    months = pd.period_range(start=start_month, end=end_month, freq="M")
    rows: list[dict] = []

    for period in months:
        month_start = period.to_timestamp()
        month_end = month_start + pd.offsets.MonthEnd(1)

        active_start_mask = (
            (subscriptions["subscription_start_date"] <= month_start)
            & (
                subscriptions["subscription_end_date"].isna()
                | (subscriptions["subscription_end_date"] >= month_start)
            )
        )
        active_start = int(active_start_mask.sum())
        active_start_mrr = float(subscriptions.loc[active_start_mask, "monthly_revenue"].sum())

        churn_mask = subscriptions["subscription_end_date"].notna() & (
            (subscriptions["subscription_end_date"] >= month_start)
            & (subscriptions["subscription_end_date"] <= month_end)
        )
        churned_customers = int(churn_mask.sum())
        churned_mrr = float(subscriptions.loc[churn_mask, "monthly_revenue"].sum())

        customer_churn_rate = churned_customers / active_start if active_start > 0 else np.nan
        revenue_churn_rate = churned_mrr / active_start_mrr if active_start_mrr > 0 else np.nan

        rows.append(
            {
                "month": month_start.date().isoformat(),
                "active_customers_start": active_start,
                "active_mrr_start": round(active_start_mrr, 2),
                "churned_customers": churned_customers,
                "churned_mrr": round(churned_mrr, 2),
                "customer_churn_rate": round(customer_churn_rate, 6),
                "revenue_churn_rate": round(revenue_churn_rate, 6),
                "retention_rate": round(1 - customer_churn_rate, 6) if not np.isnan(customer_churn_rate) else np.nan,
            }
        )

    return pd.DataFrame(rows)


def monthly_dimensional_trend(features: pd.DataFrame, subscriptions: pd.DataFrame, snapshot_date: pd.Timestamp) -> pd.DataFrame:
    dims = ["segment", "region", "acquisition_channel", "plan_type"]
    dim_map = features[["customer_id", "segment", "region", "acquisition_channel", "plan_type", "churn_flag"]].copy()
    dim_map = dim_map.drop(columns=["churn_flag"])

    subs = subscriptions.merge(dim_map, on="customer_id", how="left")
    start_month = subs["subscription_start_date"].min().to_period("M")
    end_month = snapshot_date.to_period("M")
    months = pd.period_range(start=start_month, end=end_month, freq="M")

    rows: list[dict] = []
    for period in months:
        month_start = period.to_timestamp()
        month_end = month_start + pd.offsets.MonthEnd(1)

        active = subs[
            (subs["subscription_start_date"] <= month_start)
            & (subs["subscription_end_date"].isna() | (subs["subscription_end_date"] >= month_start))
        ].copy()
        churned = subs[
            subs["subscription_end_date"].notna()
            & (subs["subscription_end_date"] >= month_start)
            & (subs["subscription_end_date"] <= month_end)
        ].copy()

        active_g = (
            active.groupby(dims, as_index=False)
            .agg(
                active_customers_start=("customer_id", "count"),
                active_mrr_start=("monthly_revenue", "sum"),
            )
        )
        churn_g = (
            churned.groupby(dims, as_index=False)
            .agg(
                churned_customers=("customer_id", "count"),
                churned_mrr=("monthly_revenue", "sum"),
            )
        )

        merged = active_g.merge(churn_g, on=dims, how="outer")
        merged["month"] = month_start.date().isoformat()
        merged["active_customers_start"] = merged["active_customers_start"].fillna(0).astype(int)
        merged["active_mrr_start"] = merged["active_mrr_start"].fillna(0.0)
        merged["churned_customers"] = merged["churned_customers"].fillna(0).astype(int)
        merged["churned_mrr"] = merged["churned_mrr"].fillna(0.0)

        rows.extend(merged.to_dict(orient="records"))

    out = pd.DataFrame(rows)
    out = out[
        [
            "month",
            "segment",
            "region",
            "acquisition_channel",
            "plan_type",
            "active_customers_start",
            "active_mrr_start",
            "churned_customers",
            "churned_mrr",
        ]
    ].copy()
    out["active_mrr_start"] = out["active_mrr_start"].round(2)
    out["churned_mrr"] = out["churned_mrr"].round(2)
    return out


def analyze_overall_health(features: pd.DataFrame, subscriptions: pd.DataFrame) -> tuple[dict, pd.DataFrame]:
    snapshot_date = pd.Timestamp(max(subscriptions["subscription_start_date"].max(), subscriptions["subscription_end_date"].max()))

    total_customers = int(len(features))
    active_customers = int((features["churn_flag"] == 0).sum())
    churned_customers = int((features["churn_flag"] == 1).sum())
    at_risk_customers = int((features["at_risk_flag"] == 1).sum())

    customer_churn_rate = churned_customers / total_customers if total_customers else 0.0

    total_monthly_value = float(features["avg_monthly_revenue"].sum())
    churned_monthly_value = float(features.loc[features["churn_flag"] == 1, "avg_monthly_revenue"].sum())
    at_risk_mrr = float(features.loc[features["at_risk_flag"] == 1, "current_mrr"].sum())

    revenue_churn_rate = churned_monthly_value / total_monthly_value if total_monthly_value > 0 else 0.0

    trend = monthly_retention_trend(subscriptions, snapshot_date)
    trend_valid = trend[trend["active_customers_start"] >= 100].copy()
    trend_last_12 = trend_valid.tail(12)

    if len(trend_last_12) >= 3:
        idx = np.arange(len(trend_last_12))
        customer_churn_slope = float(np.polyfit(idx, trend_last_12["customer_churn_rate"], 1)[0])
        revenue_churn_slope = float(np.polyfit(idx, trend_last_12["revenue_churn_rate"], 1)[0])
    else:
        customer_churn_slope = 0.0
        revenue_churn_slope = 0.0

    return (
        {
            "snapshot_date": snapshot_date.date().isoformat(),
            "total_customers": total_customers,
            "active_customers": active_customers,
            "churned_customers": churned_customers,
            "at_risk_customers": at_risk_customers,
            "customer_churn_rate": customer_churn_rate,
            "total_monthly_value": total_monthly_value,
            "churned_monthly_value": churned_monthly_value,
            "revenue_churn_rate": revenue_churn_rate,
            "at_risk_mrr": at_risk_mrr,
            "customer_vs_revenue_churn_delta": revenue_churn_rate - customer_churn_rate,
            "avg_customer_churn_last12": float(trend_last_12["customer_churn_rate"].mean()) if len(trend_last_12) else np.nan,
            "avg_revenue_churn_last12": float(trend_last_12["revenue_churn_rate"].mean()) if len(trend_last_12) else np.nan,
            "customer_churn_slope_last12": customer_churn_slope,
            "revenue_churn_slope_last12": revenue_churn_slope,
        },
        trend,
    )


def analyze_cohorts(cohort: pd.DataFrame) -> dict:
    cohort = cohort.copy()
    cohort["cohort_age_months"] = (
        (cohort["observation_month"].dt.year - cohort["cohort_month"].dt.year) * 12
        + (cohort["observation_month"].dt.month - cohort["cohort_month"].dt.month)
    )

    six_month = cohort[cohort["cohort_age_months"] == 6].sort_values("cohort_month").copy()

    if len(six_month) >= 6:
        window = min(6, len(six_month) // 2)
        early = six_month.head(window)
        recent = six_month.tail(window)
        retention_delta = float(recent["retention_rate"].mean() - early["retention_rate"].mean())
        revenue_retention_delta = float(recent["revenue_retention"].mean() - early["revenue_retention"].mean())
    else:
        retention_delta = 0.0
        revenue_retention_delta = 0.0

    if retention_delta > 0.02:
        trend_label = "improving"
    elif retention_delta < -0.02:
        trend_label = "deteriorating"
    else:
        trend_label = "mixed/stable"

    result = {
        "cohort_count": int(cohort["cohort_month"].nunique()),
        "mature_cohort_count_at_6m": int(len(six_month)),
        "avg_6m_retention": float(six_month["retention_rate"].mean()) if len(six_month) else np.nan,
        "avg_6m_revenue_retention": float(six_month["revenue_retention"].mean()) if len(six_month) else np.nan,
        "retention_delta_recent_vs_early_6m": retention_delta,
        "revenue_retention_delta_recent_vs_early_6m": revenue_retention_delta,
        "cohort_trend_label": trend_label,
    }

    return result


def churn_by_dimension(features: pd.DataFrame, dimension: str) -> pd.DataFrame:
    tmp = features.copy()
    tmp["churned_revenue_component"] = np.where(tmp["churn_flag"] == 1, tmp["avg_monthly_revenue"], 0.0)

    out = tmp.groupby(dimension, as_index=False).agg(
        customers=("customer_id", "count"),
        churned_customers=("churn_flag", "sum"),
        churn_rate=("churn_flag", "mean"),
        churned_revenue=("churned_revenue_component", "sum"),
        avg_monthly_revenue=("avg_monthly_revenue", "mean"),
    )
    out = out.sort_values("churn_rate", ascending=False).reset_index(drop=True)
    return out


def behavioral_relationships(features: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    q75_tickets = float(features["support_tickets_90d"].quantile(0.75))
    q25_nps = float(features["nps_score_recent"].quantile(0.25))
    q25_adoption = float(features["feature_adoption_score_recent"].quantile(0.25))

    conditions = {
        "usage_decline_flag": features["usage_trend"] < 0,
        "high_support_ticket_flag": features["support_tickets_90d"] >= q75_tickets,
        "failed_payment_flag": features["payment_failure_flag"] == 1,
        "low_nps_flag": features["nps_score_recent"] <= q25_nps,
        "low_feature_adoption_flag": features["feature_adoption_score_recent"] <= q25_adoption,
    }

    rows: list[dict] = []
    for name, mask in conditions.items():
        mask = mask.fillna(False)
        in_group = int(mask.sum())
        out_group = int((~mask).sum())

        churn_rate_in = float(features.loc[mask, "churn_flag"].mean()) if in_group else np.nan
        churn_rate_out = float(features.loc[~mask, "churn_flag"].mean()) if out_group else np.nan

        rows.append(
            {
                "relationship": name,
                "customers_in_group": in_group,
                "share_of_base": round(in_group / len(features), 6),
                "churn_rate_in_group": round(churn_rate_in, 6),
                "churn_rate_out_group": round(churn_rate_out, 6),
                "churn_rate_lift": round((churn_rate_in / churn_rate_out), 6) if churn_rate_out and churn_rate_out > 0 else np.nan,
                "churn_rate_delta_pp": round((churn_rate_in - churn_rate_out) * 100, 3) if pd.notna(churn_rate_in) and pd.notna(churn_rate_out) else np.nan,
            }
        )

    return pd.DataFrame(rows).sort_values("churn_rate_lift", ascending=False), {
        "q75_tickets": q75_tickets,
        "q25_nps": q25_nps,
        "q25_adoption": q25_adoption,
    }


def rank_churn_drivers(features: pd.DataFrame, thresholds: dict) -> pd.DataFrame:
    baseline = float(features["churn_flag"].mean())

    conditions: list[tuple[str, pd.Series]] = [
        ("usage_decline_flag", features["usage_trend"] < 0),
        ("high_support_ticket_flag", features["support_tickets_90d"] >= thresholds["q75_tickets"]),
        ("failed_payment_flag", features["payment_failure_flag"] == 1),
        ("low_nps_flag", features["nps_score_recent"] <= thresholds["q25_nps"]),
        ("low_feature_adoption_flag", features["feature_adoption_score_recent"] <= thresholds["q25_adoption"]),
    ]

    for col in ["segment", "region", "acquisition_channel", "plan_type"]:
        group = features.groupby(col, as_index=False).agg(churn_rate=("churn_flag", "mean"))
        risky = group[group["churn_rate"] > baseline]
        for value in risky[col].tolist():
            conditions.append((f"{col}={value}", features[col] == value))

    rows: list[dict] = []
    for name, mask in conditions:
        mask = mask.fillna(False)
        impacted = int(mask.sum())
        if impacted == 0:
            continue

        churn_rate = float(features.loc[mask, "churn_flag"].mean())
        if churn_rate <= baseline:
            continue

        lift = churn_rate / baseline if baseline > 0 else np.nan
        avg_value = float(features.loc[mask, "avg_monthly_revenue"].mean())

        excess_churn = max(churn_rate - baseline, 0.0)
        impact_score = excess_churn * impacted * avg_value

        rows.append(
            {
                "driver": name,
                "impacted_customers": impacted,
                "share_of_base": round(impacted / len(features), 6),
                "churn_rate": round(churn_rate, 6),
                "baseline_churn_rate": round(baseline, 6),
                "churn_rate_lift": round(lift, 4),
                "avg_monthly_revenue": round(avg_value, 2),
                "estimated_excess_mrr_loss": round(impact_score, 2),
            }
        )

    ranking = pd.DataFrame(rows).drop_duplicates(subset=["driver"]).sort_values(
        ["estimated_excess_mrr_loss", "churn_rate_lift"], ascending=False
    )
    return ranking.reset_index(drop=True)


def analyze_revenue_at_risk(features: pd.DataFrame) -> tuple[dict, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    q75_tickets = float(features["support_tickets_90d"].quantile(0.75))
    q25_nps = float(features["nps_score_recent"].quantile(0.25))

    hidden_risk_mask = (
        (features["churn_flag"] == 0)
        & (features["at_risk_flag"] == 0)
        & (
            (features["payment_failure_flag"] == 1)
            | (
                (features["usage_trend"] < 0)
                & ((features["support_tickets_90d"] >= q75_tickets) | (features["nps_score_recent"] <= q25_nps))
            )
        )
    )

    at_risk_mrr = float(features.loc[features["at_risk_flag"] == 1, "current_mrr"].sum())
    hidden_at_risk_mrr = float(features.loc[hidden_risk_mask, "current_mrr"].sum())
    churned_revenue = float(features.loc[features["churn_flag"] == 1, "avg_monthly_revenue"].sum())

    future_revenue_at_risk = at_risk_mrr + hidden_at_risk_mrr

    at_risk_customers = features[features["at_risk_flag"] == 1].copy()
    hv_threshold = float(at_risk_customers["current_mrr"].quantile(0.75)) if len(at_risk_customers) else 0.0
    high_value_at_risk = at_risk_customers[at_risk_customers["current_mrr"] >= hv_threshold].copy()
    high_value_at_risk = high_value_at_risk.sort_values("current_mrr", ascending=False)

    tmp = features.copy()
    tmp["at_risk_mrr_component"] = np.where(tmp["at_risk_flag"] == 1, tmp["current_mrr"], 0.0)
    tmp["churned_revenue_component"] = np.where(tmp["churn_flag"] == 1, tmp["avg_monthly_revenue"], 0.0)

    seg_loss = tmp.groupby("segment", as_index=False).agg(
        customers=("customer_id", "count"),
        at_risk_customers=("at_risk_flag", "sum"),
        churned_customers=("churn_flag", "sum"),
        at_risk_mrr=("at_risk_mrr_component", "sum"),
        churned_revenue=("churned_revenue_component", "sum"),
    )
    seg_loss["future_revenue_risk"] = seg_loss["at_risk_mrr"]
    seg_loss["total_revenue_loss_proxy"] = seg_loss["at_risk_mrr"] + seg_loss["churned_revenue"]
    seg_loss = seg_loss.sort_values("total_revenue_loss_proxy", ascending=False).reset_index(drop=True)

    value_labels = ["Low", "Mid-Low", "Mid-High", "High"]
    tmp["value_tier"] = pd.qcut(tmp["avg_monthly_revenue"], q=4, labels=value_labels, duplicates="drop")
    tmp["churned_revenue_component"] = np.where(tmp["churn_flag"] == 1, tmp["avg_monthly_revenue"], 0.0)

    tier_stats = tmp.groupby("value_tier", as_index=False).agg(
        customers=("customer_id", "count"),
        churned_customers=("churn_flag", "sum"),
        churn_rate=("churn_flag", "mean"),
        churned_revenue=("churned_revenue_component", "sum"),
        avg_monthly_revenue=("avg_monthly_revenue", "mean"),
    )

    tier_stats["share_of_churned_customers"] = tier_stats["churned_customers"] / max(
        int(tier_stats["churned_customers"].sum()), 1
    )
    tier_stats["share_of_churned_revenue"] = tier_stats["churned_revenue"] / max(
        float(tier_stats["churned_revenue"].sum()), 1.0
    )

    summary = {
        "at_risk_mrr": at_risk_mrr,
        "hidden_at_risk_mrr": hidden_at_risk_mrr,
        "future_revenue_at_risk": future_revenue_at_risk,
        "realized_churned_revenue": churned_revenue,
        "high_value_at_risk_count": int(len(high_value_at_risk)),
        "high_value_at_risk_mrr": float(high_value_at_risk["current_mrr"].sum()) if len(high_value_at_risk) else 0.0,
        "high_value_threshold": hv_threshold,
    }

    return summary, high_value_at_risk, seg_loss, tier_stats


def build_intervention_priorities(features: pd.DataFrame) -> pd.DataFrame:
    q75_tickets = float(features["support_tickets_90d"].quantile(0.75))
    q25_nps = float(features["nps_score_recent"].quantile(0.25))
    q40_adoption = float(features["feature_adoption_score_recent"].quantile(0.40))

    risk_signal = (
        (features["at_risk_flag"] == 1)
        | (features["payment_failure_flag"] == 1)
        | (features["usage_trend"] < 0)
        | (features["nps_score_recent"] <= q25_nps)
    )

    definitions = [
        {
            "name": "Payment Rescue",
            "recoverable_condition": (features["churn_flag"] == 0) & (features["payment_failure_flag"] == 1),
            "benchmark_condition": (features["payment_failure_flag"] == 1),
            "action": "Collections + payment method refresh + retry sequencing",
        },
        {
            "name": "Renewal Save Desk",
            "recoverable_condition": (features["churn_flag"] == 0) & (features["renewal_near_flag"] == 1) & risk_signal,
            "benchmark_condition": risk_signal,
            "action": "Pre-renewal save playbook with tailored offers and success plans",
        },
        {
            "name": "Adoption Reactivation",
            "recoverable_condition": (features["churn_flag"] == 0)
            & (features["usage_trend"] < 0)
            & (features["feature_adoption_score_recent"] <= q40_adoption),
            "benchmark_condition": (features["usage_trend"] < 0) & (features["feature_adoption_score_recent"] <= q40_adoption),
            "action": "Usage coaching, onboarding refresh, and feature activation campaign",
        },
        {
            "name": "Service Recovery",
            "recoverable_condition": (features["churn_flag"] == 0)
            & ((features["support_tickets_90d"] >= q75_tickets) | (features["nps_score_recent"] <= q25_nps))
            & ((features["at_risk_flag"] == 1) | (features["usage_trend"] < 0)),
            "benchmark_condition": ((features["support_tickets_90d"] >= q75_tickets) | (features["nps_score_recent"] <= q25_nps)),
            "action": "Escalated support workflow and proactive success outreach",
        },
    ]

    rows: list[dict] = []
    for definition in definitions:
        name = definition["name"]
        recoverable = definition["recoverable_condition"].fillna(False)
        benchmark_cond = definition["benchmark_condition"].fillna(False)
        action = definition["action"]

        recoverable_customers = int(recoverable.sum())
        recoverable_mrr = float(features.loc[recoverable, "current_mrr"].sum())

        benchmark_churn_rate = float(features.loc[benchmark_cond, "churn_flag"].mean()) if int(benchmark_cond.sum()) > 0 else 0.0
        priority_score = recoverable_mrr * benchmark_churn_rate

        top_segments = (
            features.loc[recoverable].groupby("segment")["current_mrr"].sum().sort_values(ascending=False).head(2)
        )
        segment_focus = ", ".join([f"{idx} ({money(val)})" for idx, val in top_segments.items()]) if len(top_segments) else "n/a"

        rows.append(
            {
                "opportunity": name,
                "recoverable_customers": recoverable_customers,
                "recoverable_mrr": round(recoverable_mrr, 2),
                "benchmark_churn_rate": round(benchmark_churn_rate, 6),
                "priority_score": round(priority_score, 2),
                "segment_focus": segment_focus,
                "recommended_action": action,
            }
        )

    out = pd.DataFrame(rows).sort_values("priority_score", ascending=False).reset_index(drop=True)
    return out


def build_structured_findings(
    overall: dict,
    cohort_result: dict,
    top_segment_row: pd.Series,
    top_region_row: pd.Series,
    top_channel_row: pd.Series,
    top_plan_row: pd.Series,
    relationships: pd.DataFrame,
    revenue_risk: dict,
    seg_loss: pd.DataFrame,
    tier_stats: pd.DataFrame,
    interventions: pd.DataFrame,
) -> pd.DataFrame:
    highest_driver = relationships.sort_values("churn_rate_lift", ascending=False).iloc[0]
    top_loss_segment = seg_loss.iloc[0]

    high_tiers = tier_stats[tier_stats["value_tier"].isin(["Mid-High", "High"])]
    high_tier_churn_share = float(high_tiers["share_of_churned_customers"].sum()) if len(high_tiers) else np.nan

    rows = [
        {
            "section": "1. Overall Retention Health",
            "question": "What is the current retention health and how severe is revenue loss?",
            "metrics_used": "active customers, churn rate, revenue churn rate, at-risk MRR",
            "result": (
                f"Active base {overall['active_customers']:,} / {overall['total_customers']:,}; "
                f"customer churn {pct(overall['customer_churn_rate'])}; revenue churn {pct(overall['revenue_churn_rate'])}; "
                f"at-risk MRR {money(overall['at_risk_mrr'])}."
            ),
            "business_interpretation": "Revenue loss pressure is material and should be managed as both a churn and pre-churn prevention problem.",
            "caveat": "Revenue churn uses monthly-value proxy from engineered features rather than full contract ARR accounting.",
        },
        {
            "section": "2. Cohort Retention",
            "question": "Are new cohorts retaining better or worse than older cohorts?",
            "metrics_used": "6-month retention, 6-month revenue retention, recent-vs-early cohort delta",
            "result": (
                f"6-month retention {pct(cohort_result['avg_6m_retention'])}; 6-month revenue retention "
                f"{pct(cohort_result['avg_6m_revenue_retention'])}; trend is {cohort_result['cohort_trend_label']}."
            ),
            "business_interpretation": "Acquisition and onboarding quality can be assessed by cohort movement at a fixed age milestone.",
            "caveat": "Recent cohorts are right-censored and cannot be fairly compared at long horizons yet.",
        },
        {
            "section": "3. Churn Drivers",
            "question": "Which customer groups and behaviors are most associated with churn?",
            "metrics_used": "dimension-level churn rates, behavioral churn-rate lift",
            "result": (
                f"Highest segment churn: {top_segment_row['segment']} ({pct(top_segment_row['churn_rate'])}); "
                f"region: {top_region_row['region']} ({pct(top_region_row['churn_rate'])}); "
                f"channel: {top_channel_row['acquisition_channel']} ({pct(top_channel_row['churn_rate'])}); "
                f"plan: {top_plan_row['plan_type']} ({pct(top_plan_row['churn_rate'])}); "
                f"strongest behavioral lift: {highest_driver['relationship']} ({highest_driver['churn_rate_lift']:.2f}x)."
            ),
            "business_interpretation": "Churn is driven by both acquisition mix and deteriorating account health signals.",
            "caveat": "These are associative relationships; causal effects require controlled experiments.",
        },
        {
            "section": "4. Revenue at Risk",
            "question": "Where is future revenue most exposed and is churn concentrated in high-value accounts?",
            "metrics_used": "future revenue at risk, high-value at-risk MRR, segment loss concentration, value-tier churn mix",
            "result": (
                f"Future revenue at risk {money(revenue_risk['future_revenue_at_risk'])}; "
                f"high-value at-risk MRR {money(revenue_risk['high_value_at_risk_mrr'])}; "
                f"largest loss segment {top_loss_segment['segment']} ({money(top_loss_segment['total_revenue_loss_proxy'])}); "
                f"high-tier share of churned customers {pct(high_tier_churn_share)}."
            ),
            "business_interpretation": "Revenue risk is concentrated enough to justify targeted save motions rather than broad, low-precision campaigns.",
            "caveat": "Value-tier concentration depends on quantile-based tiering, which should be recalibrated on real production data.",
        },
        {
            "section": "5. Retention Opportunities",
            "question": "Which intervention plays are likely to return the highest retention ROI now?",
            "metrics_used": "recoverable customers, recoverable MRR, benchmark churn rate, priority score",
            "result": (
                f"Top play: {interventions.iloc[0]['opportunity']} with recoverable MRR {money(interventions.iloc[0]['recoverable_mrr'])} "
                f"and benchmark churn {pct(interventions.iloc[0]['benchmark_churn_rate'])}."
            ),
            "business_interpretation": "Operational plays should be sequenced by recoverable MRR times churn propensity to maximize near-term impact.",
            "caveat": "Priority scores are heuristic and should be validated against intervention conversion data.",
        },
    ]

    return pd.DataFrame(rows)


def main() -> None:
    project_root = Path(__file__).resolve().parents[2]
    outputs_dir = project_root / "outputs" / "tables"
    docs_dir = project_root / "docs"

    outputs_dir.mkdir(parents=True, exist_ok=True)

    features, cohort, subscriptions = load_inputs(project_root)

    overall, trend = analyze_overall_health(features, subscriptions)
    snapshot_date = pd.Timestamp(max(subscriptions["subscription_start_date"].max(), subscriptions["subscription_end_date"].max()))
    trend_dim = monthly_dimensional_trend(features, subscriptions, snapshot_date)
    cohort_result = analyze_cohorts(cohort)

    churn_segment = churn_by_dimension(features, "segment")
    churn_region = churn_by_dimension(features, "region")
    churn_channel = churn_by_dimension(features, "acquisition_channel")
    churn_plan = churn_by_dimension(features, "plan_type")

    relationships, thresholds = behavioral_relationships(features)
    drivers_ranked = rank_churn_drivers(features, thresholds)

    revenue_risk, _, seg_loss, value_tier_stats = analyze_revenue_at_risk(features)
    interventions = build_intervention_priorities(features)

    findings = build_structured_findings(
        overall=overall,
        cohort_result=cohort_result,
        top_segment_row=churn_segment.iloc[0],
        top_region_row=churn_region.iloc[0],
        top_channel_row=churn_channel.iloc[0],
        top_plan_row=churn_plan.iloc[0],
        relationships=relationships,
        revenue_risk=revenue_risk,
        seg_loss=seg_loss,
        tier_stats=value_tier_stats,
        interventions=interventions,
    )

    trend.to_csv(outputs_dir / "overall_retention_trend_monthly.csv", index=False)
    trend_dim.to_csv(outputs_dir / "monthly_dimensional_trend.csv", index=False)
    churn_segment.to_csv(outputs_dir / "churn_by_segment.csv", index=False)
    churn_region.to_csv(outputs_dir / "churn_by_region.csv", index=False)
    churn_channel.to_csv(outputs_dir / "churn_by_acquisition_channel.csv", index=False)
    churn_plan.to_csv(outputs_dir / "churn_by_plan_type.csv", index=False)
    relationships.to_csv(outputs_dir / "behavioral_churn_relationships.csv", index=False)
    drivers_ranked.to_csv(outputs_dir / "main_analysis_churn_driver_ranking.csv", index=False)
    seg_loss.to_csv(outputs_dir / "segment_revenue_risk_contribution.csv", index=False)
    interventions.to_csv(outputs_dir / "main_analysis_intervention_priorities.csv", index=False)
    findings.to_csv(outputs_dir / "main_analysis_structured_findings.csv", index=False)

    print("Main analysis completed.")
    print(
        "Key metrics -> customer_churn:",
        round(overall["customer_churn_rate"], 4),
        ", revenue_churn:",
        round(overall["revenue_churn_rate"], 4),
        ", at_risk_mrr:",
        round(overall["at_risk_mrr"], 2),
    )
    print(
        "Outputs -> findings:",
        outputs_dir / "main_analysis_structured_findings.csv",
    )


if __name__ == "__main__":
    main()
