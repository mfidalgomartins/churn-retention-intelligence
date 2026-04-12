from __future__ import annotations

from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd


CYCLE_TO_MONTHS = {"Monthly": 1, "Quarterly": 3, "Annual": 12}


def load_raw_tables(raw_dir: Path) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    customers = pd.read_csv(raw_dir / "customers.csv", parse_dates=["signup_date"])
    subscriptions = pd.read_csv(
        raw_dir / "subscriptions.csv",
        parse_dates=["subscription_start_date", "subscription_end_date"],
    )
    product_usage = pd.read_csv(raw_dir / "product_usage.csv", parse_dates=["usage_date"])
    payments = pd.read_csv(raw_dir / "payments.csv", parse_dates=["payment_date"])
    return customers, subscriptions, product_usage, payments


def infer_snapshot_date(subscriptions: pd.DataFrame, usage: pd.DataFrame, payments: pd.DataFrame) -> pd.Timestamp:
    candidates = [
        subscriptions["subscription_start_date"].max(),
        subscriptions["subscription_end_date"].max(),
        usage["usage_date"].max(),
        payments["payment_date"].max(),
    ]
    candidates = [d for d in candidates if pd.notna(d)]
    return pd.Timestamp(max(candidates))


def compute_usage_aggregates(usage: pd.DataFrame, snapshot_date: pd.Timestamp) -> pd.DataFrame:
    w30 = snapshot_date - pd.Timedelta(days=30)
    w60 = snapshot_date - pd.Timedelta(days=60)
    w90 = snapshot_date - pd.Timedelta(days=90)

    recent_30 = usage[usage["usage_date"] > w30]
    recent_90 = usage[usage["usage_date"] > w90]
    prior_30 = usage[(usage["usage_date"] > w60) & (usage["usage_date"] <= w30)]

    agg_30 = (
        recent_30.groupby("customer_id", as_index=False)
        .agg(
            recent_sessions_30d=("sessions", "sum"),
            support_tickets_30d=("support_tickets", "sum"),
            feature_adoption_score_recent=("feature_adoption_score", "mean"),
            recent_30_avg_sessions=("sessions", "mean"),
        )
        .astype({"recent_sessions_30d": int, "support_tickets_30d": int})
    )

    agg_90 = (
        recent_90.groupby("customer_id", as_index=False)
        .agg(
            recent_sessions_90d=("sessions", "sum"),
            support_tickets_90d=("support_tickets", "sum"),
            nps_score_recent=("nps_score", "mean"),
        )
        .astype({"recent_sessions_90d": int, "support_tickets_90d": int})
    )

    prior_30_avg = prior_30.groupby("customer_id", as_index=False).agg(prior_30_avg_sessions=("sessions", "mean"))

    usage_agg = agg_30.merge(agg_90, on="customer_id", how="outer").merge(prior_30_avg, on="customer_id", how="outer")

    usage_agg["recent_sessions_30d"] = usage_agg["recent_sessions_30d"].fillna(0).astype(int)
    usage_agg["recent_sessions_90d"] = usage_agg["recent_sessions_90d"].fillna(0).astype(int)
    usage_agg["support_tickets_30d"] = usage_agg["support_tickets_30d"].fillna(0).astype(int)
    usage_agg["support_tickets_90d"] = usage_agg["support_tickets_90d"].fillna(0).astype(int)
    usage_agg["feature_adoption_score_recent"] = usage_agg["feature_adoption_score_recent"].fillna(0.0)
    usage_agg["nps_score_recent"] = usage_agg["nps_score_recent"].fillna(0.0)
    usage_agg["recent_30_avg_sessions"] = usage_agg["recent_30_avg_sessions"].fillna(0.0)
    usage_agg["prior_30_avg_sessions"] = usage_agg["prior_30_avg_sessions"].fillna(0.0)

    usage_agg["usage_trend"] = usage_agg["recent_30_avg_sessions"] - usage_agg["prior_30_avg_sessions"]
    usage_agg["usage_trend"] = usage_agg["usage_trend"].round(4)

    return usage_agg[
        [
            "customer_id",
            "recent_sessions_30d",
            "recent_sessions_90d",
            "usage_trend",
            "feature_adoption_score_recent",
            "support_tickets_30d",
            "support_tickets_90d",
            "nps_score_recent",
        ]
    ]


def next_renewal_date(start_date: pd.Timestamp, snapshot_date: pd.Timestamp, cycle_months: int) -> pd.Timestamp:
    if pd.isna(start_date):
        return pd.NaT

    if start_date > snapshot_date:
        return start_date

    elapsed_months = (snapshot_date.year - start_date.year) * 12 + (snapshot_date.month - start_date.month)
    cycles_elapsed = elapsed_months // cycle_months
    candidate = start_date + pd.DateOffset(months=(cycles_elapsed * cycle_months))
    if candidate <= snapshot_date:
        candidate = candidate + pd.DateOffset(months=cycle_months)
    return pd.Timestamp(candidate)


def compute_payment_aggregates(payments: pd.DataFrame, subscriptions: pd.DataFrame, snapshot_date: pd.Timestamp) -> pd.DataFrame:
    pay = payments.merge(
        subscriptions[["customer_id", "billing_cycle"]],
        on="customer_id",
        how="left",
    )
    pay["cycle_months"] = pay["billing_cycle"].map(CYCLE_TO_MONTHS).fillna(1).astype(int)
    pay["monthly_equivalent_amount"] = pay["amount"] / pay["cycle_months"]

    w90 = snapshot_date - pd.Timedelta(days=90)
    w180 = snapshot_date - pd.Timedelta(days=180)

    recent_90 = pay[pay["payment_date"] > w90].copy()
    prior_90 = pay[(pay["payment_date"] > w180) & (pay["payment_date"] <= w90)].copy()

    recent_failed = recent_90.groupby("customer_id")["payment_status"].apply(lambda s: int((s == "failed").sum()))

    paid_all = pay[pay["payment_status"] == "paid"].groupby("customer_id", as_index=False).agg(
        lifetime_revenue=("amount", "sum"),
        avg_monthly_revenue_calc=("monthly_equivalent_amount", "mean"),
    )

    paid_recent_monthly = (
        recent_90[recent_90["payment_status"] == "paid"]
        .groupby("customer_id", as_index=False)
        .agg(recent_paid_monthly_equiv=("monthly_equivalent_amount", "mean"))
    )

    paid_prior_monthly = (
        prior_90[prior_90["payment_status"] == "paid"]
        .groupby("customer_id", as_index=False)
        .agg(prior_paid_monthly_equiv=("monthly_equivalent_amount", "mean"))
    )

    pay_agg = paid_all.merge(paid_recent_monthly, on="customer_id", how="left").merge(
        paid_prior_monthly, on="customer_id", how="left"
    )

    pay_agg["failed_payments_90d"] = pay_agg["customer_id"].map(recent_failed).fillna(0).astype(int)
    pay_agg["payment_failure_flag"] = (pay_agg["failed_payments_90d"] > 0).astype(int)

    pay_agg["recent_paid_monthly_equiv"] = pay_agg["recent_paid_monthly_equiv"].fillna(0.0)
    pay_agg["prior_paid_monthly_equiv"] = pay_agg["prior_paid_monthly_equiv"].fillna(0.0)

    pay_agg["contraction_flag"] = (
        (pay_agg["prior_paid_monthly_equiv"] > 0)
        & (pay_agg["recent_paid_monthly_equiv"] < pay_agg["prior_paid_monthly_equiv"] * 0.85)
    ).astype(int)

    return pay_agg[
        [
            "customer_id",
            "lifetime_revenue",
            "avg_monthly_revenue_calc",
            "failed_payments_90d",
            "payment_failure_flag",
            "contraction_flag",
        ]
    ]


def build_customer_retention_features(
    customers: pd.DataFrame,
    subscriptions: pd.DataFrame,
    usage: pd.DataFrame,
    payments: pd.DataFrame,
    snapshot_date: pd.Timestamp,
) -> pd.DataFrame:
    usage_agg = compute_usage_aggregates(usage, snapshot_date)
    payment_agg = compute_payment_aggregates(payments, subscriptions, snapshot_date)

    base = customers.merge(subscriptions, on="customer_id", how="inner")

    end_for_tenure = base["subscription_end_date"].fillna(snapshot_date)
    base["tenure_days"] = (end_for_tenure - base["subscription_start_date"]).dt.days.clip(lower=0)

    base["churn_flag"] = (base["status"] == "churned").astype(int)
    base["at_risk_flag"] = (base["status"] == "at_risk").astype(int)

    base["current_mrr"] = np.where(base["churn_flag"] == 1, 0.0, base["monthly_revenue"])

    cycle_months = base["billing_cycle"].map(CYCLE_TO_MONTHS).fillna(1).astype(int)
    base["next_renewal_date"] = [
        next_renewal_date(s, snapshot_date, c)
        for s, c in zip(base["subscription_start_date"], cycle_months)
    ]

    base["renewal_near_flag"] = (
        (base["churn_flag"] == 0)
        & ((base["next_renewal_date"] - snapshot_date).dt.days.between(0, 45, inclusive="both"))
    ).astype(int)

    features = (
        base.merge(usage_agg, on="customer_id", how="left")
        .merge(payment_agg, on="customer_id", how="left")
        .copy()
    )

    fill_defaults = {
        "recent_sessions_30d": 0,
        "recent_sessions_90d": 0,
        "usage_trend": 0.0,
        "feature_adoption_score_recent": 0.0,
        "support_tickets_30d": 0,
        "support_tickets_90d": 0,
        "nps_score_recent": 0.0,
        "lifetime_revenue": 0.0,
        "failed_payments_90d": 0,
        "payment_failure_flag": 0,
        "contraction_flag": 0,
    }
    for col, val in fill_defaults.items():
        features[col] = features[col].fillna(val)

    features["avg_monthly_revenue"] = features["avg_monthly_revenue_calc"].fillna(features["monthly_revenue"])

    features["feature_adoption_score_recent"] = features["feature_adoption_score_recent"].round(4)
    features["nps_score_recent"] = features["nps_score_recent"].round(4)
    features["avg_monthly_revenue"] = features["avg_monthly_revenue"].round(2)
    features["lifetime_revenue"] = features["lifetime_revenue"].round(2)
    features["current_mrr"] = features["current_mrr"].round(2)

    out_cols = [
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
        "recent_sessions_90d",
        "usage_trend",
        "feature_adoption_score_recent",
        "support_tickets_30d",
        "support_tickets_90d",
        "nps_score_recent",
        "failed_payments_90d",
        "payment_failure_flag",
        "renewal_near_flag",
        "contraction_flag",
        "churn_flag",
        "at_risk_flag",
    ]

    features = features[out_cols].copy()

    int_cols = [
        "tenure_days",
        "recent_sessions_30d",
        "recent_sessions_90d",
        "support_tickets_30d",
        "support_tickets_90d",
        "failed_payments_90d",
        "payment_failure_flag",
        "renewal_near_flag",
        "contraction_flag",
        "churn_flag",
        "at_risk_flag",
    ]
    for c in int_cols:
        features[c] = features[c].astype(int)

    return features


def build_cohort_retention_table(
    subscriptions: pd.DataFrame,
    snapshot_date: pd.Timestamp,
) -> pd.DataFrame:
    subs = subscriptions.copy()
    subs["cohort_month"] = subs["subscription_start_date"].dt.to_period("M").dt.to_timestamp()

    all_months = pd.period_range(
        start=subs["cohort_month"].min(),
        end=snapshot_date.to_period("M"),
        freq="M",
    ).to_timestamp()

    rows: list[dict] = []

    for cohort_month, cohort_df in subs.groupby("cohort_month"):
        cohort_size = int(len(cohort_df))
        cohort_initial_mrr = float(cohort_df["monthly_revenue"].sum())

        valid_months = all_months[all_months >= cohort_month]
        for observation_month in valid_months:
            month_end = observation_month + pd.offsets.MonthEnd(1)
            retained_mask = cohort_df["subscription_end_date"].isna() | (cohort_df["subscription_end_date"] > month_end)
            retained_customers = int(retained_mask.sum())

            retained_mrr = float(cohort_df.loc[retained_mask, "monthly_revenue"].sum())
            retention_rate = retained_customers / cohort_size if cohort_size else 0.0
            revenue_retention = retained_mrr / cohort_initial_mrr if cohort_initial_mrr > 0 else 0.0

            rows.append(
                {
                    "cohort_month": cohort_month.date().isoformat(),
                    "observation_month": observation_month.date().isoformat(),
                    "active_customers": cohort_size,
                    "retained_customers": retained_customers,
                    "retention_rate": round(retention_rate, 6),
                    "revenue_retention": round(revenue_retention, 6),
                }
            )

    return pd.DataFrame(rows).sort_values(["cohort_month", "observation_month"]).reset_index(drop=True)


def build_segment_retention_summary(features: pd.DataFrame) -> pd.DataFrame:
    df = features.copy()

    def summarise(group: pd.DataFrame) -> pd.Series:
        active_customers = int((group["churn_flag"] == 0).sum())
        churned_customers = int((group["churn_flag"] == 1).sum())
        total = len(group)
        churn_rate = churned_customers / total if total else 0.0

        revenue_at_risk = float(
            group.loc[group["at_risk_flag"] == 1, "current_mrr"].sum()
            + group.loc[group["churn_flag"] == 1, "avg_monthly_revenue"].sum()
        )

        return pd.Series(
            {
                "active_customers": active_customers,
                "churned_customers": churned_customers,
                "churn_rate": round(churn_rate, 6),
                "revenue_at_risk": round(revenue_at_risk, 2),
                "avg_tenure": round(group["tenure_days"].mean(), 2),
                "avg_nps": round(group["nps_score_recent"].mean(), 2),
                "avg_usage_trend": round(group["usage_trend"].mean(), 4),
            }
        )

    summary = df.groupby("segment").apply(summarise, include_groups=False).reset_index()
    summary = summary.drop(columns=["index"], errors="ignore")
    summary["active_customers"] = summary["active_customers"].astype(int)
    summary["churned_customers"] = summary["churned_customers"].astype(int)
    return summary.sort_values("segment").reset_index(drop=True)


def write_feature_dictionary(docs_dir: Path) -> None:
    methodology_dir = docs_dir / "methodology"
    methodology_dir.mkdir(parents=True, exist_ok=True)
    text = """# Feature Dictionary

## Transparent Flag Definitions
- `churn_flag`: 1 when `subscriptions.status == 'churned'`, else 0.
- `at_risk_flag`: 1 when `subscriptions.status == 'at_risk'`, else 0.

## Table: customer_retention_features
- `customer_id`: Stable customer key.
- `segment`: Commercial segment from customer master data.
- `region`: Geographic region of account.
- `acquisition_channel`: Source channel used to acquire the account.
- `plan_type`: Product plan tier.
- `tenure_days`: Days from subscription start to churn date (if churned) or snapshot date.
- `current_mrr`: Current monthly recurring revenue proxy; zeroed for churned accounts.
- `avg_monthly_revenue`: Mean of paid monthly-equivalent payment amounts (`amount / billing_cycle_months`), with fallback to `monthly_revenue` when needed.
- `lifetime_revenue`: Sum of successful (`payment_status='paid'`) payment amounts.
- `recent_sessions_30d`: Sum of sessions in the last 30 days from snapshot date.
- `recent_sessions_90d`: Sum of sessions in the last 90 days from snapshot date.
- `usage_trend`: Difference between average sessions in recent 30 days and prior 30-day window (days 31-60).
- `feature_adoption_score_recent`: Mean feature adoption score in last 30 days.
- `support_tickets_30d`: Sum of support tickets in last 30 days.
- `support_tickets_90d`: Sum of support tickets in last 90 days.
- `nps_score_recent`: Mean NPS in last 90 days.
- `failed_payments_90d`: Count of failed payments in last 90 days.
- `payment_failure_flag`: 1 if `failed_payments_90d > 0`, else 0.
- `renewal_near_flag`: 1 when non-churned account has next billing-cycle renewal in next 45 days.
- `contraction_flag`: 1 when recent 90-day paid monthly-equivalent amount is <85% of prior 90-day paid monthly-equivalent amount.

## Table: cohort_retention_table
- `cohort_month`: Month of subscription start (`subscription_start_date`, month grain).
- `observation_month`: Month where retention is evaluated.
- `active_customers`: Initial cohort size (denominator) for that cohort.
- `retained_customers`: Customers in cohort not churned by observation month end.
- `retention_rate`: `retained_customers / active_customers`.
- `revenue_retention`: Sum of retained-customer `monthly_revenue` divided by initial cohort `monthly_revenue`.

## Table: segment_retention_summary
- `segment`: Customer segment.
- `active_customers`: Non-churned customers (`churn_flag=0`) in segment.
- `churned_customers`: Churned customers (`churn_flag=1`) in segment.
- `churn_rate`: `churned_customers / total_customers_in_segment`.
- `revenue_at_risk`: `sum(current_mrr for at_risk)` + `sum(avg_monthly_revenue for churned)`.
- `avg_tenure`: Mean `tenure_days` in segment.
- `avg_nps`: Mean `nps_score_recent` in segment.
- `avg_usage_trend`: Mean `usage_trend` in segment.
"""
    (methodology_dir / "feature_dictionary.md").write_text(text, encoding="utf-8")


def main() -> None:
    project_root = Path(__file__).resolve().parents[2]
    raw_dir = project_root / "data" / "raw"
    processed_dir = project_root / "data" / "processed"
    docs_dir = project_root / "docs"

    processed_dir.mkdir(parents=True, exist_ok=True)

    customers, subscriptions, usage, payments = load_raw_tables(raw_dir)
    snapshot_date = infer_snapshot_date(subscriptions, usage, payments)

    customer_features = build_customer_retention_features(
        customers=customers,
        subscriptions=subscriptions,
        usage=usage,
        payments=payments,
        snapshot_date=snapshot_date,
    )
    cohort_table = build_cohort_retention_table(subscriptions, snapshot_date)
    segment_summary = build_segment_retention_summary(customer_features)

    customer_features.to_csv(processed_dir / "customer_retention_features.csv", index=False)
    cohort_table.to_csv(processed_dir / "cohort_retention_table.csv", index=False)
    segment_summary.to_csv(processed_dir / "segment_retention_summary.csv", index=False)

    write_feature_dictionary(docs_dir)

    print("Feature engineering completed.")
    print(f"Snapshot date: {snapshot_date.date().isoformat()}")
    print(
        "Rows -> customer_retention_features:",
        len(customer_features),
        ", cohort_retention_table:",
        len(cohort_table),
        ", segment_retention_summary:",
        len(segment_summary),
    )


if __name__ == "__main__":
    main()
