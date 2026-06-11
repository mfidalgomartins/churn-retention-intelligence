"""Portfolio-level churn analysis: trends, drivers, revenue at risk, intervention plays."""
from __future__ import annotations

import numpy as np
import pandas as pd

from churn.common import (
    infer_snapshot_date,
    last_complete_month_start,
    outputs_tables_dir,
    processed_dir,
    raw_dir,
)


def _pct(x: float) -> str:
    return f"{x * 100:.1f}%"


def _money(x: float) -> str:
    return f"${x:,.0f}"


def load_inputs() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    features = pd.read_csv(processed_dir() / "customer_retention_features.csv")
    cohort = pd.read_csv(
        processed_dir() / "cohort_retention_table.csv",
        parse_dates=["cohort_month", "observation_month"],
    )
    subscriptions = pd.read_csv(
        raw_dir() / "subscriptions.csv",
        parse_dates=["subscription_start_date", "subscription_end_date"],
    )
    return features, cohort, subscriptions


def monthly_retention_trend(subs: pd.DataFrame, snapshot: pd.Timestamp) -> pd.DataFrame:
    last_complete_month = last_complete_month_start(snapshot)
    months = pd.period_range(
        start=subs["subscription_start_date"].min().to_period("M"),
        end=last_complete_month.to_period("M"),
        freq="M",
    )
    rows: list[dict] = []
    for period in months:
        m_start = period.to_timestamp()
        m_end = m_start + pd.offsets.MonthEnd(1)

        active_mask = (
            (subs["subscription_start_date"] <= m_start)
            & (subs["subscription_end_date"].isna() | (subs["subscription_end_date"] >= m_start))
        )
        churn_mask = subs["subscription_end_date"].notna() & subs["subscription_end_date"].between(m_start, m_end)

        active = int(active_mask.sum())
        active_mrr = float(subs.loc[active_mask, "monthly_revenue"].sum())
        churned = int(churn_mask.sum())
        churned_mrr = float(subs.loc[churn_mask, "monthly_revenue"].sum())

        cust_churn = churned / active if active else np.nan
        rev_churn = churned_mrr / active_mrr if active_mrr else np.nan

        rows.append({
            "month": m_start.date().isoformat(),
            "active_customers_start": active,
            "active_mrr_start": round(active_mrr, 2),
            "churned_customers": churned,
            "churned_mrr": round(churned_mrr, 2),
            "customer_churn_rate": round(cust_churn, 6) if not np.isnan(cust_churn) else np.nan,
            "revenue_churn_rate": round(rev_churn, 6) if not np.isnan(rev_churn) else np.nan,
            "retention_rate": round(1 - cust_churn, 6) if not np.isnan(cust_churn) else np.nan,
        })
    return pd.DataFrame(rows)


def monthly_dimensional_trend(features: pd.DataFrame, subs: pd.DataFrame, snapshot: pd.Timestamp) -> pd.DataFrame:
    dims = ["segment", "region", "acquisition_channel", "plan_type"]
    enriched = subs.merge(
        features[["customer_id", *dims]], on="customer_id", how="left",
    )
    last_complete_month = last_complete_month_start(snapshot)
    months = pd.period_range(
        start=subs["subscription_start_date"].min().to_period("M"),
        end=last_complete_month.to_period("M"),
        freq="M",
    )

    rows: list[dict] = []
    for period in months:
        m_start = period.to_timestamp()
        m_end = m_start + pd.offsets.MonthEnd(1)

        active = enriched[
            (enriched["subscription_start_date"] <= m_start)
            & (enriched["subscription_end_date"].isna() | (enriched["subscription_end_date"] >= m_start))
        ]
        churned = enriched[
            enriched["subscription_end_date"].notna()
            & enriched["subscription_end_date"].between(m_start, m_end)
        ]

        active_agg = active.groupby(dims, as_index=False).agg(
            active_customers_start=("customer_id", "count"),
            active_mrr_start=("monthly_revenue", "sum"),
        )
        churn_agg = churned.groupby(dims, as_index=False).agg(
            churned_customers=("customer_id", "count"),
            churned_mrr=("monthly_revenue", "sum"),
        )
        merged = active_agg.merge(churn_agg, on=dims, how="outer")
        merged["month"] = m_start.date().isoformat()
        merged["active_customers_start"] = merged["active_customers_start"].fillna(0).astype(int)
        merged["active_mrr_start"] = merged["active_mrr_start"].fillna(0.0).round(2)
        merged["churned_customers"] = merged["churned_customers"].fillna(0).astype(int)
        merged["churned_mrr"] = merged["churned_mrr"].fillna(0.0).round(2)
        rows.extend(merged.to_dict(orient="records"))

    out = pd.DataFrame(rows)
    return out[["month", *dims, "active_customers_start", "active_mrr_start", "churned_customers", "churned_mrr"]]


def overall_health(features: pd.DataFrame, subs: pd.DataFrame, snapshot: pd.Timestamp) -> tuple[dict, pd.DataFrame]:
    total = len(features)
    churned = int((features["churn_flag"] == 1).sum())
    at_risk = int((features["at_risk_flag"] == 1).sum())
    cumulative_customer_churn_share = churned / total if total else 0.0

    total_value = float(features["avg_monthly_revenue"].sum())
    churned_value = float(features.loc[features["churn_flag"] == 1, "avg_monthly_revenue"].sum())
    at_risk_mrr = float(features.loc[features["at_risk_flag"] == 1, "current_mrr"].sum())
    cumulative_revenue_loss_share = churned_value / total_value if total_value else 0.0

    trend = monthly_retention_trend(subs, snapshot)
    mature = trend[trend["active_customers_start"] >= 100].tail(12)
    if len(mature) >= 3:
        idx = np.arange(len(mature))
        cust_slope = float(np.polyfit(idx, mature["customer_churn_rate"], 1)[0])
        rev_slope = float(np.polyfit(idx, mature["revenue_churn_rate"], 1)[0])
    else:
        cust_slope = rev_slope = 0.0

    summary = {
        "snapshot_date": snapshot.date().isoformat(),
        "total_customers": total,
        "active_customers": total - churned,
        "churned_customers": churned,
        "at_risk_customers": at_risk,
        "cumulative_customer_churn_share": cumulative_customer_churn_share,
        "portfolio_monthly_value_proxy": total_value,
        "churned_monthly_value_proxy": churned_value,
        "cumulative_revenue_loss_share": cumulative_revenue_loss_share,
        "at_risk_mrr": at_risk_mrr,
        "cumulative_customer_vs_revenue_loss_delta": (
            cumulative_revenue_loss_share - cumulative_customer_churn_share
        ),
        "avg_customer_churn_last12": float(mature["customer_churn_rate"].mean()) if len(mature) else np.nan,
        "avg_revenue_churn_last12": float(mature["revenue_churn_rate"].mean()) if len(mature) else np.nan,
        "customer_churn_slope_last12": cust_slope,
        "revenue_churn_slope_last12": rev_slope,
    }
    return summary, trend


def cohort_movement(cohort: pd.DataFrame) -> dict:
    c = cohort.copy()
    c["age_months"] = (
        (c["observation_month"].dt.year - c["cohort_month"].dt.year) * 12
        + (c["observation_month"].dt.month - c["cohort_month"].dt.month)
    )
    six_m = c[c["age_months"] == 6].sort_values("cohort_month")

    if len(six_m) >= 6:
        w = min(6, len(six_m) // 2)
        early, recent = six_m.head(w), six_m.tail(w)
        ret_delta = float(recent["retention_rate"].mean() - early["retention_rate"].mean())
        rev_delta = float(recent["revenue_retention"].mean() - early["revenue_retention"].mean())
    else:
        ret_delta = rev_delta = 0.0

    if ret_delta > 0.02:
        label = "improving"
    elif ret_delta < -0.02:
        label = "deteriorating"
    else:
        label = "mixed/stable"

    return {
        "cohort_count": int(c["cohort_month"].nunique()),
        "mature_cohort_count_at_6m": len(six_m),
        "avg_6m_retention": float(six_m["retention_rate"].mean()) if len(six_m) else np.nan,
        "avg_6m_revenue_retention": float(six_m["revenue_retention"].mean()) if len(six_m) else np.nan,
        "retention_delta_recent_vs_early_6m": ret_delta,
        "revenue_retention_delta_recent_vs_early_6m": rev_delta,
        "cohort_trend_label": label,
    }


def churn_by_dimension(features: pd.DataFrame, dim: str) -> pd.DataFrame:
    df = features.copy()
    df["churned_revenue"] = np.where(df["churn_flag"] == 1, df["avg_monthly_revenue"], 0.0)
    out = df.groupby(dim, as_index=False).agg(
        customers=("customer_id", "count"),
        churned_customers=("churn_flag", "sum"),
        cumulative_churn_share=("churn_flag", "mean"),
        churned_revenue=("churned_revenue", "sum"),
        avg_monthly_revenue=("avg_monthly_revenue", "mean"),
    )
    return out.sort_values("cumulative_churn_share", ascending=False).reset_index(drop=True)


def behavioral_relationships(features: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    q75_tickets = float(features["support_tickets_90d"].quantile(0.75))
    q25_nps = float(features["nps_score_recent"].quantile(0.25))
    q25_adoption = float(features["feature_adoption_score_recent"].quantile(0.25))

    conditions = {
        "usage_decline_flag":       features["usage_trend"] < 0,
        "high_support_ticket_flag": features["support_tickets_90d"] >= q75_tickets,
        "failed_payment_flag":      features["payment_failure_flag"] == 1,
        "low_nps_flag":             features["nps_score_recent"] <= q25_nps,
        "low_feature_adoption_flag": features["feature_adoption_score_recent"] <= q25_adoption,
    }

    rows: list[dict] = []
    for name, mask in conditions.items():
        mask = mask.fillna(False)
        in_n = int(mask.sum())
        out_n = int((~mask).sum())
        in_churn = float(features.loc[mask, "churn_flag"].mean()) if in_n else np.nan
        out_churn = float(features.loc[~mask, "churn_flag"].mean()) if out_n else np.nan
        rows.append({
            "relationship": name,
            "customers_in_group": in_n,
            "share_of_base": round(in_n / len(features), 6),
            "churn_rate_in_group": round(in_churn, 6),
            "churn_rate_out_group": round(out_churn, 6),
            "churn_rate_lift": round(in_churn / out_churn, 6) if out_churn and out_churn > 0 else np.nan,
            "churn_rate_delta_pp": round((in_churn - out_churn) * 100, 3)
                                   if pd.notna(in_churn) and pd.notna(out_churn) else np.nan,
        })

    return (
        pd.DataFrame(rows).sort_values("churn_rate_lift", ascending=False),
        {"q75_tickets": q75_tickets, "q25_nps": q25_nps, "q25_adoption": q25_adoption},
    )


def rank_drivers(features: pd.DataFrame, thresholds: dict) -> pd.DataFrame:
    baseline = float(features["churn_flag"].mean())
    conditions: list[tuple[str, pd.Series]] = [
        ("usage_decline_flag",       features["usage_trend"] < 0),
        ("high_support_ticket_flag", features["support_tickets_90d"] >= thresholds["q75_tickets"]),
        ("failed_payment_flag",      features["payment_failure_flag"] == 1),
        ("low_nps_flag",             features["nps_score_recent"] <= thresholds["q25_nps"]),
        ("low_feature_adoption_flag", features["feature_adoption_score_recent"] <= thresholds["q25_adoption"]),
    ]
    for col in ["segment", "region", "acquisition_channel", "plan_type"]:
        risky = features.groupby(col, as_index=False).agg(rate=("churn_flag", "mean"))
        for v in risky[risky["rate"] > baseline][col]:
            conditions.append((f"{col}={v}", features[col] == v))

    rows: list[dict] = []
    for name, mask in conditions:
        mask = mask.fillna(False)
        n = int(mask.sum())
        if n == 0:
            continue
        rate = float(features.loc[mask, "churn_flag"].mean())
        if rate <= baseline:
            continue
        avg_value = float(features.loc[mask, "avg_monthly_revenue"].mean())
        rows.append({
            "driver": name,
            "impacted_customers": n,
            "share_of_base": round(n / len(features), 6),
            "churn_rate": round(rate, 6),
            "baseline_churn_rate": round(baseline, 6),
            "churn_rate_lift": round(rate / baseline, 4) if baseline else np.nan,
            "avg_monthly_revenue": round(avg_value, 2),
            "excess_mrr_association_proxy": round(max(rate - baseline, 0) * n * avg_value, 2),
        })
    return (pd.DataFrame(rows).drop_duplicates(subset=["driver"])
            .sort_values(["excess_mrr_association_proxy", "churn_rate_lift"], ascending=False)
            .reset_index(drop=True))


def revenue_at_risk(features: pd.DataFrame) -> tuple[dict, pd.DataFrame, pd.DataFrame]:
    q75_tickets = float(features["support_tickets_90d"].quantile(0.75))
    q25_nps = float(features["nps_score_recent"].quantile(0.25))

    hidden_risk = (
        (features["churn_flag"] == 0)
        & (features["at_risk_flag"] == 0)
        & ((features["payment_failure_flag"] == 1)
           | ((features["usage_trend"] < 0)
              & ((features["support_tickets_90d"] >= q75_tickets)
                 | (features["nps_score_recent"] <= q25_nps))))
    )

    at_risk_mrr = float(features.loc[features["at_risk_flag"] == 1, "current_mrr"].sum())
    hidden_mrr = float(features.loc[hidden_risk, "current_mrr"].sum())
    churned_value = float(features.loc[features["churn_flag"] == 1, "avg_monthly_revenue"].sum())

    df = features.copy()
    df["at_risk_mrr_component"] = np.where(df["at_risk_flag"] == 1, df["current_mrr"], 0.0)
    df["churned_revenue_component"] = np.where(df["churn_flag"] == 1, df["avg_monthly_revenue"], 0.0)
    seg_loss = df.groupby("segment", as_index=False).agg(
        customers=("customer_id", "count"),
        at_risk_customers=("at_risk_flag", "sum"),
        churned_customers=("churn_flag", "sum"),
        at_risk_mrr=("at_risk_mrr_component", "sum"),
        churned_revenue=("churned_revenue_component", "sum"),
    )
    seg_loss = seg_loss.rename(columns={
        "at_risk_mrr": "current_mrr_at_risk",
        "churned_revenue": "churned_monthly_value_proxy",
    })
    seg_loss = seg_loss.sort_values("current_mrr_at_risk", ascending=False).reset_index(drop=True)

    df["value_tier"] = pd.qcut(df["avg_monthly_revenue"], q=4,
                                labels=["Low", "Mid-Low", "Mid-High", "High"], duplicates="drop")
    tier_stats = df.groupby("value_tier", as_index=False).agg(
        customers=("customer_id", "count"),
        churned_customers=("churn_flag", "sum"),
        churn_rate=("churn_flag", "mean"),
        churned_revenue=("churned_revenue_component", "sum"),
        avg_monthly_revenue=("avg_monthly_revenue", "mean"),
    )
    tier_stats["share_of_churned_customers"] = tier_stats["churned_customers"] / max(int(tier_stats["churned_customers"].sum()), 1)
    tier_stats["share_of_churned_revenue"] = tier_stats["churned_revenue"] / max(float(tier_stats["churned_revenue"].sum()), 1.0)

    summary = {
        "at_risk_mrr": at_risk_mrr,
        "behavioral_signal_mrr": hidden_mrr,
        "current_mrr_exposure": at_risk_mrr + hidden_mrr,
        "churned_monthly_value_proxy": churned_value,
    }
    return summary, seg_loss, tier_stats


def intervention_priorities(features: pd.DataFrame) -> pd.DataFrame:
    q75_tickets = float(features["support_tickets_90d"].quantile(0.75))
    q25_nps = float(features["nps_score_recent"].quantile(0.25))
    q40_adoption = float(features["feature_adoption_score_recent"].quantile(0.40))

    risk_signal = (
        (features["at_risk_flag"] == 1)
        | (features["payment_failure_flag"] == 1)
        | (features["usage_trend"] < 0)
        | (features["nps_score_recent"] <= q25_nps)
    )

    plays = [
        ("Payment Rescue",
         (features["churn_flag"] == 0) & (features["payment_failure_flag"] == 1),
         (features["payment_failure_flag"] == 1),
         "Collections + payment method refresh + retry sequencing"),
        ("Renewal Save Desk",
         (features["churn_flag"] == 0) & (features["renewal_near_flag"] == 1) & risk_signal,
         risk_signal,
         "Pre-renewal save playbook with tailored offers and success plans"),
        ("Adoption Reactivation",
         (features["churn_flag"] == 0) & (features["usage_trend"] < 0)
         & (features["feature_adoption_score_recent"] <= q40_adoption),
         (features["usage_trend"] < 0) & (features["feature_adoption_score_recent"] <= q40_adoption),
         "Usage coaching, onboarding refresh, and feature activation campaign"),
        ("Service Recovery",
         (features["churn_flag"] == 0)
         & ((features["support_tickets_90d"] >= q75_tickets) | (features["nps_score_recent"] <= q25_nps))
         & ((features["at_risk_flag"] == 1) | (features["usage_trend"] < 0)),
         ((features["support_tickets_90d"] >= q75_tickets) | (features["nps_score_recent"] <= q25_nps)),
         "Escalated support workflow and proactive success outreach"),
    ]

    rows: list[dict] = []
    for name, candidate, benchmark, action in plays:
        candidate = candidate.fillna(False)
        benchmark = benchmark.fillna(False)
        candidate_n = int(candidate.sum())
        current_mrr_scope = float(features.loc[candidate, "current_mrr"].sum())
        bench_rate = float(features.loc[benchmark, "churn_flag"].mean()) if int(benchmark.sum()) else 0.0
        top_segments = (features.loc[candidate].groupby("segment")["current_mrr"]
                        .sum().sort_values(ascending=False).head(2))
        focus = ", ".join(f"{seg} ({_money(val)})" for seg, val in top_segments.items()) or "n/a"
        rows.append({
            "opportunity": name,
            "candidate_customers": candidate_n,
            "current_mrr_scope": round(current_mrr_scope, 2),
            "historical_churn_share": round(bench_rate, 6),
            "mrr_exposure_proxy": round(current_mrr_scope * bench_rate, 2),
            "segment_focus": focus,
            "recommended_action": action,
        })
    return pd.DataFrame(rows).sort_values("mrr_exposure_proxy", ascending=False).reset_index(drop=True)


def structured_findings(
    overall: dict, cohort: dict,
    top_segment: pd.Series, top_region: pd.Series,
    top_channel: pd.Series, top_plan: pd.Series,
    relationships: pd.DataFrame, revenue_risk: dict,
    seg_loss: pd.DataFrame, tier_stats: pd.DataFrame,
    interventions: pd.DataFrame,
) -> pd.DataFrame:
    top_driver = relationships.sort_values("churn_rate_lift", ascending=False).iloc[0]
    top_loss = seg_loss.iloc[0]
    high_tier_share = float(tier_stats[tier_stats["value_tier"].isin(["Mid-High", "High"])]
                            ["share_of_churned_customers"].sum())

    return pd.DataFrame([
        {
            "section": "1. Overall Retention Health",
            "question": "How large are cumulative loss and current pre-churn pressure?",
            "metrics_used": "active customers, cumulative churn share, cumulative revenue loss share, at-risk MRR",
            "result": (
                f"Active base {overall['active_customers']:,} / {overall['total_customers']:,}; "
                f"cumulative customer churn {_pct(overall['cumulative_customer_churn_share'])}; "
                f"cumulative revenue loss share {_pct(overall['cumulative_revenue_loss_share'])}; "
                f"at-risk MRR {_money(overall['at_risk_mrr'])}."
            ),
            "business_interpretation": "Use monthly trends for rate monitoring and the open-account queue for prevention.",
            "caveat": "Cumulative revenue loss uses a monthly-value proxy, not full contract ARR.",
        },
        {
            "section": "2. Cohort Retention",
            "question": "Are recent cohorts retaining better or worse than older ones?",
            "metrics_used": "6-month retention, 6-month revenue retention, recent-vs-early delta",
            "result": (
                f"6-month retention {_pct(cohort['avg_6m_retention'])}; "
                f"revenue retention {_pct(cohort['avg_6m_revenue_retention'])}; "
                f"trend is {cohort['cohort_trend_label']}."
            ),
            "business_interpretation": "Cohort movement at a fixed age signals acquisition and onboarding quality drift.",
            "caveat": "Recent cohorts are right-censored — long-horizon comparisons aren't fair yet.",
        },
        {
            "section": "3. Churn Drivers",
            "question": "Which customer groups and behaviours associate with churn most strongly?",
            "metrics_used": "dimension cumulative churn shares, behavioural churn-share lift",
            "result": (
                f"Highest cumulative churn share: {top_segment['segment']} ({_pct(top_segment['cumulative_churn_share'])}); "
                f"region: {top_region['region']} ({_pct(top_region['cumulative_churn_share'])}); "
                f"channel: {top_channel['acquisition_channel']} ({_pct(top_channel['cumulative_churn_share'])}); "
                f"plan: {top_plan['plan_type']} ({_pct(top_plan['cumulative_churn_share'])}); "
                f"strongest behavioural lift: {top_driver['relationship']} ({top_driver['churn_rate_lift']:.2f}x)."
            ),
            "business_interpretation": "Acquisition mix and pre-churn account health both separate retained from churned accounts.",
            "caveat": "Relationships are associative; causal effects require controlled experiments.",
        },
        {
            "section": "4. Revenue at Risk",
            "question": "Where is current MRR exposed and is loss concentrated in high-value accounts?",
            "metrics_used": "current MRR exposure, segment loss concentration, value-tier churn mix",
            "result": (
                f"Current MRR exposure {_money(revenue_risk['current_mrr_exposure'])}; "
                f"largest explicitly at-risk segment {top_loss['segment']} ({_money(top_loss['current_mrr_at_risk'])}); "
                f"high-tier share of churned customers {_pct(high_tier_share)}."
            ),
            "business_interpretation": "Concentration justifies targeted save motions over broad campaigns.",
            "caveat": "Value-tier concentration depends on quantile tiering; recalibrate on real data.",
        },
        {
            "section": "5. Retention Opportunities",
            "question": "Which intervention queues carry the largest weighted MRR exposure?",
            "metrics_used": "candidate customers, current MRR scope, historical churn share, MRR exposure proxy",
            "result": (
                f"Top play: {interventions.iloc[0]['opportunity']} with current MRR scope "
                f"{_money(interventions.iloc[0]['current_mrr_scope'])} "
                f"and historical churn share {_pct(interventions.iloc[0]['historical_churn_share'])}."
            ),
            "business_interpretation": "Use the exposure proxy to sequence tests, not to claim expected ROI.",
            "caveat": "Exposure proxies are heuristic; validate them against intervention conversion data.",
        },
    ])


def main() -> None:
    features, cohort, subs = load_inputs()
    snapshot = infer_snapshot_date(
        subs["subscription_start_date"], subs["subscription_end_date"],
    )

    overall, trend = overall_health(features, subs, snapshot)
    trend_dim = monthly_dimensional_trend(features, subs, snapshot)
    cohort_result = cohort_movement(cohort)

    seg = churn_by_dimension(features, "segment")
    reg = churn_by_dimension(features, "region")
    chn = churn_by_dimension(features, "acquisition_channel")
    pln = churn_by_dimension(features, "plan_type")

    relationships, thresholds = behavioral_relationships(features)
    drivers = rank_drivers(features, thresholds)
    risk_summary, seg_loss, tier_stats = revenue_at_risk(features)
    plays = intervention_priorities(features)

    findings = structured_findings(
        overall, cohort_result,
        seg.iloc[0], reg.iloc[0], chn.iloc[0], pln.iloc[0],
        relationships, risk_summary, seg_loss, tier_stats, plays,
    )

    out = outputs_tables_dir()
    out.mkdir(parents=True, exist_ok=True)
    trend.to_csv(out / "overall_retention_trend_monthly.csv", index=False)
    trend_dim.to_csv(out / "monthly_dimensional_trend.csv", index=False)
    seg.to_csv(out / "churn_by_segment.csv", index=False)
    reg.to_csv(out / "churn_by_region.csv", index=False)
    chn.to_csv(out / "churn_by_acquisition_channel.csv", index=False)
    pln.to_csv(out / "churn_by_plan_type.csv", index=False)
    relationships.to_csv(out / "behavioral_churn_relationships.csv", index=False)
    drivers.to_csv(out / "main_analysis_churn_driver_ranking.csv", index=False)
    seg_loss.to_csv(out / "segment_revenue_risk_contribution.csv", index=False)
    plays.to_csv(out / "main_analysis_intervention_priorities.csv", index=False)
    findings.to_csv(out / "main_analysis_structured_findings.csv", index=False)

    print(f"Analysis complete. cumulative_customer_churn="
          f"{overall['cumulative_customer_churn_share']:.3f}, "
          f"cumulative_revenue_loss={overall['cumulative_revenue_loss_share']:.3f}, "
          f"at_risk_mrr={_money(overall['at_risk_mrr'])}.")


if __name__ == "__main__":
    main()
