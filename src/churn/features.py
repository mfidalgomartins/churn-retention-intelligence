"""Feature engineering for customer-level retention modelling.

Produces three governed tables anchored on a single snapshot date:
    customer_retention_features  — one row per customer at snapshot
    cohort_retention_table       — customer & revenue retention by cohort × age
    segment_retention_summary    — segment-level rollup with revenue-at-risk
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from churn.common import (
    docs_dir,
    infer_snapshot_date,
    last_complete_month_start,
    processed_dir,
    raw_dir,
)

CYCLE_TO_MONTHS = {"Monthly": 1, "Quarterly": 3, "Annual": 12}


def load_raw() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    base = raw_dir()
    customers = pd.read_csv(base / "customers.csv", parse_dates=["signup_date"])
    subscriptions = pd.read_csv(
        base / "subscriptions.csv",
        parse_dates=["subscription_start_date", "subscription_end_date"],
    )
    usage = pd.read_csv(base / "product_usage.csv", parse_dates=["usage_date"])
    payments = pd.read_csv(base / "payments.csv", parse_dates=["payment_date"])
    return customers, subscriptions, usage, payments


def customer_observation_dates(
    subscriptions: pd.DataFrame,
    snapshot: pd.Timestamp,
) -> pd.DataFrame:
    """Use churn date for closed accounts and the portfolio snapshot for open accounts."""
    dates = subscriptions[["customer_id", "subscription_end_date"]].copy()
    dates["observation_date"] = dates["subscription_end_date"].fillna(snapshot).clip(upper=snapshot)
    return dates[["customer_id", "observation_date"]]


def usage_aggregates(
    usage: pd.DataFrame,
    observation_dates: pd.DataFrame,
) -> pd.DataFrame:
    observed = usage.merge(
        observation_dates,
        on="customer_id",
        how="inner",
        validate="many_to_one",
    )
    observed["days_before_observation"] = (
        observed["observation_date"] - observed["usage_date"]
    ).dt.days
    observed = observed[observed["days_before_observation"] >= 0]

    recent_30 = observed[observed["days_before_observation"] < 30]
    recent_90 = observed[observed["days_before_observation"] < 90]
    prior_30 = observed[observed["days_before_observation"].between(30, 59)]

    agg_30 = recent_30.groupby("customer_id", as_index=False).agg(
        recent_sessions_30d=("sessions", "sum"),
        support_tickets_30d=("support_tickets", "sum"),
        feature_adoption_score_recent=("feature_adoption_score", "mean"),
        recent_30_avg_sessions=("sessions", "mean"),
    )
    agg_90 = recent_90.groupby("customer_id", as_index=False).agg(
        recent_sessions_90d=("sessions", "sum"),
        support_tickets_90d=("support_tickets", "sum"),
        nps_score_recent=("nps_score", "mean"),
    )
    prior = prior_30.groupby("customer_id", as_index=False).agg(
        prior_30_avg_sessions=("sessions", "mean"),
    )

    out = agg_30.merge(agg_90, on="customer_id", how="outer").merge(prior, on="customer_id", how="outer")

    fill = {
        "recent_sessions_30d": 0, "recent_sessions_90d": 0,
        "support_tickets_30d": 0, "support_tickets_90d": 0,
        "feature_adoption_score_recent": 0.0, "nps_score_recent": 0.0,
        "recent_30_avg_sessions": 0.0, "prior_30_avg_sessions": 0.0,
    }
    for col, val in fill.items():
        out[col] = out[col].fillna(val)

    int_cols = ["recent_sessions_30d", "recent_sessions_90d",
                "support_tickets_30d", "support_tickets_90d"]
    for col in int_cols:
        out[col] = out[col].astype(int)

    out["usage_trend"] = (out["recent_30_avg_sessions"] - out["prior_30_avg_sessions"]).round(4)
    return out[[
        "customer_id", "recent_sessions_30d", "recent_sessions_90d", "usage_trend",
        "feature_adoption_score_recent", "support_tickets_30d", "support_tickets_90d",
        "nps_score_recent",
    ]]


def next_renewal_date(start: pd.Series, snapshot: pd.Timestamp, cycle_months: pd.Series) -> pd.Series:
    """Next billing-cycle anniversary strictly after the snapshot."""
    months_elapsed = ((snapshot.year - start.dt.year) * 12
                      + (snapshot.month - start.dt.month))
    cycles_done = (months_elapsed // cycle_months).clip(lower=0)
    exact = []
    for s, c, n in zip(start, cycle_months, cycles_done, strict=False):
        if pd.isna(s):
            exact.append(pd.NaT)
            continue
        candidate = s + pd.DateOffset(months=int(n * c))
        if candidate <= snapshot:
            candidate = candidate + pd.DateOffset(months=int(c))
        exact.append(pd.Timestamp(candidate))
    return pd.Series(exact, index=start.index, dtype="datetime64[ns]")


def payment_aggregates(
    payments: pd.DataFrame,
    subscriptions: pd.DataFrame,
    observation_dates: pd.DataFrame,
) -> pd.DataFrame:
    pay = payments.merge(
        subscriptions[["customer_id", "billing_cycle"]],
        on="customer_id",
        how="left",
        validate="many_to_one",
    ).merge(
        observation_dates,
        on="customer_id",
        how="inner",
        validate="many_to_one",
    )
    pay["cycle_months"] = pay["billing_cycle"].map(CYCLE_TO_MONTHS).fillna(1).astype(int)
    pay["monthly_equivalent"] = pay["amount"] / pay["cycle_months"]
    pay["days_before_observation"] = (pay["observation_date"] - pay["payment_date"]).dt.days
    pay = pay[pay["days_before_observation"] >= 0]

    recent = pay[pay["days_before_observation"] < 90]

    paid_all = pay[pay["payment_status"] == "paid"].groupby("customer_id", as_index=False).agg(
        lifetime_revenue=("amount", "sum"),
        avg_monthly_revenue_calc=("monthly_equivalent", "mean"),
    )
    failed_recent = (recent.assign(is_failed=lambda d: (d["payment_status"] == "failed").astype(int))
                     .groupby("customer_id", as_index=False)
                     .agg(failed_payments_90d=("is_failed", "sum")))

    out = (
        observation_dates[["customer_id"]]
        .merge(paid_all, on="customer_id", how="left", validate="one_to_one")
        .merge(failed_recent, on="customer_id", how="left", validate="one_to_one")
    )

    out["lifetime_revenue"] = out["lifetime_revenue"].fillna(0.0)
    out["failed_payments_90d"] = out["failed_payments_90d"].fillna(0).astype(int)
    out["payment_failure_flag"] = (out["failed_payments_90d"] > 0).astype(int)

    return out[[
        "customer_id", "lifetime_revenue", "avg_monthly_revenue_calc",
        "failed_payments_90d", "payment_failure_flag",
    ]]


def build_customer_features(
    customers: pd.DataFrame,
    subscriptions: pd.DataFrame,
    usage: pd.DataFrame,
    payments: pd.DataFrame,
    snapshot: pd.Timestamp,
) -> pd.DataFrame:
    base = customers.merge(subscriptions, on="customer_id")
    base["tenure_days"] = (
        base["subscription_end_date"].fillna(snapshot) - base["subscription_start_date"]
    ).dt.days.clip(lower=0)
    base["churn_flag"] = (base["status"] == "churned").astype(int)
    base["at_risk_flag"] = (base["status"] == "at_risk").astype(int)
    base["current_mrr"] = np.where(base["churn_flag"] == 1, 0.0, base["monthly_revenue"])
    observation_dates = customer_observation_dates(subscriptions, snapshot)
    base = base.merge(observation_dates, on="customer_id", how="left", validate="one_to_one")

    cycle = base["billing_cycle"].map(CYCLE_TO_MONTHS).fillna(1).astype(int)
    base["next_renewal_date"] = next_renewal_date(base["subscription_start_date"], snapshot, cycle)
    days_to_renewal = (base["next_renewal_date"] - snapshot).dt.days
    base["renewal_near_flag"] = (
        (base["churn_flag"] == 0) & days_to_renewal.between(0, 45)
    ).astype(int)

    features = (base
                .merge(usage_aggregates(usage, observation_dates), on="customer_id", how="left")
                .merge(payment_aggregates(payments, subscriptions, observation_dates), on="customer_id", how="left"))

    defaults = {
        "recent_sessions_30d": 0, "recent_sessions_90d": 0, "usage_trend": 0.0,
        "feature_adoption_score_recent": 0.0,
        "support_tickets_30d": 0, "support_tickets_90d": 0, "nps_score_recent": 0.0,
        "lifetime_revenue": 0.0, "failed_payments_90d": 0,
        "payment_failure_flag": 0,
    }
    for col, val in defaults.items():
        features[col] = features[col].fillna(val)

    features["avg_monthly_revenue"] = features["avg_monthly_revenue_calc"].fillna(features["monthly_revenue"]).round(2)
    features["lifetime_revenue"] = features["lifetime_revenue"].round(2)
    features["current_mrr"] = features["current_mrr"].round(2)
    features["feature_adoption_score_recent"] = features["feature_adoption_score_recent"].round(4)
    features["nps_score_recent"] = features["nps_score_recent"].round(4)
    features["observation_date"] = pd.to_datetime(features["observation_date"]).dt.date.astype(str)

    cols = [
        "customer_id", "observation_date", "segment", "region", "acquisition_channel", "plan_type",
        "tenure_days", "current_mrr", "avg_monthly_revenue", "lifetime_revenue",
        "recent_sessions_30d", "recent_sessions_90d", "usage_trend",
        "feature_adoption_score_recent",
        "support_tickets_30d", "support_tickets_90d", "nps_score_recent",
        "failed_payments_90d", "payment_failure_flag",
        "renewal_near_flag",
        "churn_flag", "at_risk_flag",
    ]
    features = features[cols].copy()

    for col in ["tenure_days", "recent_sessions_30d", "recent_sessions_90d",
                "support_tickets_30d", "support_tickets_90d", "failed_payments_90d",
                "payment_failure_flag", "renewal_near_flag",
                "churn_flag", "at_risk_flag"]:
        features[col] = features[col].astype(int)

    return features


def build_cohort_table(subscriptions: pd.DataFrame, snapshot: pd.Timestamp) -> pd.DataFrame:
    subs = subscriptions.copy()
    subs["cohort_month"] = subs["subscription_start_date"].dt.to_period("M").dt.to_timestamp()
    last_complete_month = last_complete_month_start(snapshot)
    all_months = pd.period_range(
        start=subs["cohort_month"].min(),
        end=last_complete_month.to_period("M"),
        freq="M",
    ).to_timestamp()

    rows: list[dict] = []
    for cohort_month, group in subs.groupby("cohort_month"):
        cohort_size = len(group)
        cohort_mrr = float(group["monthly_revenue"].sum())
        for obs_month in all_months[all_months >= cohort_month]:
            month_end = obs_month + pd.offsets.MonthEnd(1)
            retained = group["subscription_end_date"].isna() | (group["subscription_end_date"] > month_end)
            retained_n = int(retained.sum())
            retained_mrr = float(group.loc[retained, "monthly_revenue"].sum())
            rows.append({
                "cohort_month": cohort_month.date().isoformat(),
                "observation_month": obs_month.date().isoformat(),
                "active_customers": cohort_size,
                "retained_customers": retained_n,
                "retention_rate": round(retained_n / cohort_size, 6) if cohort_size else 0.0,
                "revenue_retention": round(retained_mrr / cohort_mrr, 6) if cohort_mrr else 0.0,
            })
    return pd.DataFrame(rows).sort_values(["cohort_month", "observation_month"]).reset_index(drop=True)


def build_segment_summary(features: pd.DataFrame) -> pd.DataFrame:
    def summarise(group: pd.DataFrame) -> pd.Series:
        active = int((group["churn_flag"] == 0).sum())
        churned = int((group["churn_flag"] == 1).sum())
        total = len(group)
        return pd.Series({
            "active_customers": active,
            "churned_customers": churned,
            "cumulative_churn_share": round(churned / total, 6) if total else 0.0,
            "current_mrr_at_risk": round(float(
                group.loc[group["at_risk_flag"] == 1, "current_mrr"].sum()
            ), 2),
            "churned_monthly_value_proxy": round(float(
                group.loc[group["churn_flag"] == 1, "avg_monthly_revenue"].sum()
            ), 2),
            "avg_tenure": round(group["tenure_days"].mean(), 2),
            "avg_nps": round(group["nps_score_recent"].mean(), 2),
            "avg_usage_trend": round(group["usage_trend"].mean(), 4),
        })

    out = features.groupby("segment").apply(summarise, include_groups=False).reset_index()
    return out.sort_values("segment").reset_index(drop=True)


def write_feature_dictionary() -> None:
    out = docs_dir() / "methodology" / "feature_dictionary.md"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("""# Feature Dictionary

## customer_retention_features (one row per customer at snapshot)

| Column | Definition |
|---|---|
| `customer_id` | Stable customer key. |
| `observation_date` | Churn date for closed accounts; portfolio snapshot for open accounts. |
| `segment`, `region`, `acquisition_channel`, `plan_type` | Commercial dimensions from customer master. |
| `tenure_days` | Days from subscription start to churn date (if churned) or snapshot. |
| `current_mrr` | Monthly recurring revenue proxy; zero for churned accounts. |
| `avg_monthly_revenue` | Mean of paid monthly-equivalent payments; falls back to `monthly_revenue`. |
| `lifetime_revenue` | Sum of successful payments. |
| `recent_sessions_30d`, `recent_sessions_90d` | Sessions in trailing 30 / 90 days. |
| `usage_trend` | Avg sessions in last 30d minus avg sessions in days 31-60. |
| `feature_adoption_score_recent` | Mean adoption score in last 30 days. |
| `support_tickets_30d`, `support_tickets_90d` | Tickets in trailing 30 / 90 days. |
| `nps_score_recent` | Mean NPS in last 90 days. |
| `failed_payments_90d` | Failed payments in last 90 days. |
| `payment_failure_flag` | `1` if any failed payment in last 90 days. |
| `renewal_near_flag` | `1` if non-churned account renews within 45 days. |
| `churn_flag` | `1` if `status == "churned"`. |
| `at_risk_flag` | `1` if `status == "at_risk"`. |

## cohort_retention_table (one row per cohort × observation month)

`active_customers` is the **initial cohort size** (denominator), not currently active.
`retained_customers` is the number not yet churned by the observation month-end.
`retention_rate = retained_customers / active_customers`.
`revenue_retention = sum(retained MRR) / sum(initial MRR)`.
Only fully observed calendar months are included.

## segment_retention_summary (one row per segment)

`current_mrr_at_risk` and `churned_monthly_value_proxy` are reported separately
to avoid mixing future exposure with realised churn.
""", encoding="utf-8")


def main() -> None:
    customers, subscriptions, usage, payments = load_raw()
    snapshot = infer_snapshot_date(
        subscriptions["subscription_start_date"],
        subscriptions["subscription_end_date"],
        usage["usage_date"],
        payments["payment_date"],
    )

    features = build_customer_features(customers, subscriptions, usage, payments, snapshot)
    cohort = build_cohort_table(subscriptions, snapshot)
    summary = build_segment_summary(features)

    out = processed_dir()
    out.mkdir(parents=True, exist_ok=True)
    features.to_csv(out / "customer_retention_features.csv", index=False)
    cohort.to_csv(out / "cohort_retention_table.csv", index=False)
    summary.to_csv(out / "segment_retention_summary.csv", index=False)
    write_feature_dictionary()

    print(f"Features built (snapshot={snapshot.date()}). "
          f"customers={len(features):,}, cohort_rows={len(cohort):,}, segments={len(summary)}.")


if __name__ == "__main__":
    main()
