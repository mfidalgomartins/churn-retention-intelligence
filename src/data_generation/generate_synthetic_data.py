from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd


SEED = 42
N_CUSTOMERS = 3500
REFERENCE_DATE = pd.Timestamp("2026-03-01")


def sigmoid(x: np.ndarray) -> np.ndarray:
    return 1.0 / (1.0 + np.exp(-x))


def generate_customers(rng: np.random.Generator) -> pd.DataFrame:
    customer_ids = [f"C{idx:06d}" for idx in range(1, N_CUSTOMERS + 1)]

    signup_start = pd.Timestamp("2022-01-01")
    signup_span_days = (REFERENCE_DATE - signup_start).days
    signup_offsets = rng.integers(0, signup_span_days, size=N_CUSTOMERS)
    signup_dates = signup_start + pd.to_timedelta(signup_offsets, unit="D")

    segment_options = np.array(["Startup", "SMB", "Mid-Market", "Enterprise"])
    segment_probs = np.array([0.16, 0.39, 0.29, 0.16])
    segments = rng.choice(segment_options, p=segment_probs, size=N_CUSTOMERS)

    region_options = np.array(["North America", "Europe", "LATAM", "APAC"])
    region_probs = np.array([0.39, 0.31, 0.16, 0.14])
    regions = rng.choice(region_options, p=region_probs, size=N_CUSTOMERS)

    channel_options = np.array(
        ["Organic", "Referral", "Partner", "Paid Search", "Affiliate", "Outbound"]
    )
    channel_probs_by_segment = {
        "Startup": np.array([0.18, 0.11, 0.07, 0.34, 0.20, 0.10]),
        "SMB": np.array([0.22, 0.12, 0.09, 0.28, 0.14, 0.15]),
        "Mid-Market": np.array([0.25, 0.11, 0.18, 0.17, 0.06, 0.23]),
        "Enterprise": np.array([0.21, 0.09, 0.28, 0.10, 0.02, 0.30]),
    }
    acquisition_channels = np.array(
        [rng.choice(channel_options, p=channel_probs_by_segment[s]) for s in segments]
    )

    plan_options = np.array(["Basic", "Growth", "Pro", "Enterprise"])
    plan_probs_by_segment = {
        "Startup": np.array([0.53, 0.33, 0.13, 0.01]),
        "SMB": np.array([0.36, 0.41, 0.20, 0.03]),
        "Mid-Market": np.array([0.14, 0.38, 0.37, 0.11]),
        "Enterprise": np.array([0.03, 0.14, 0.43, 0.40]),
    }
    plan_types = np.array([rng.choice(plan_options, p=plan_probs_by_segment[s]) for s in segments])

    customers = pd.DataFrame(
        {
            "customer_id": customer_ids,
            "signup_date": signup_dates,
            "segment": segments,
            "region": regions,
            "acquisition_channel": acquisition_channels,
            "plan_type": plan_types,
        }
    )

    return customers


def generate_subscriptions(customers: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
    n = len(customers)

    subscription_ids = [f"SUB{idx:06d}" for idx in range(1, n + 1)]
    start_lag_days = rng.integers(0, 15, size=n)
    subscription_start = customers["signup_date"] + pd.to_timedelta(start_lag_days, unit="D")
    # Avoid future-dated starts that can create invalid churn timelines.
    subscription_start = subscription_start.clip(upper=REFERENCE_DATE)

    monthly_revenue_base = {
        "Basic": 55.0,
        "Growth": 160.0,
        "Pro": 460.0,
        "Enterprise": 1400.0,
    }
    revenue_sigma = {
        "Basic": 0.45,
        "Growth": 0.40,
        "Pro": 0.42,
        "Enterprise": 0.55,
    }
    monthly_revenue = np.array(
        [
            monthly_revenue_base[p] * rng.lognormal(mean=0.0, sigma=revenue_sigma[p])
            for p in customers["plan_type"].to_numpy()
        ]
    )
    monthly_revenue = np.round(np.clip(monthly_revenue, 20, 20000), 2)

    contract_type = []
    billing_cycle = []
    for seg, plan in zip(customers["segment"], customers["plan_type"]):
        if plan == "Enterprise" or seg == "Enterprise":
            ctype = rng.choice(["Annual", "Monthly"], p=[0.78, 0.22])
        elif plan == "Pro":
            ctype = rng.choice(["Annual", "Monthly"], p=[0.42, 0.58])
        else:
            ctype = rng.choice(["Annual", "Monthly"], p=[0.16, 0.84])

        if ctype == "Annual":
            cycle = rng.choice(["Annual", "Quarterly"], p=[0.82, 0.18])
        else:
            cycle = rng.choice(["Monthly", "Quarterly"], p=[0.86, 0.14])

        contract_type.append(ctype)
        billing_cycle.append(cycle)

    tenure_months = ((REFERENCE_DATE - subscription_start).dt.days / 30.4).to_numpy()

    segment_coef = {
        "Startup": 1.00,
        "SMB": 0.62,
        "Mid-Market": 0.18,
        "Enterprise": -0.42,
    }
    channel_coef = {
        "Organic": 0.00,
        "Referral": -0.38,
        "Partner": -0.24,
        "Paid Search": 0.74,
        "Affiliate": 0.86,
        "Outbound": 0.34,
    }
    region_coef = {
        "North America": 0.00,
        "Europe": 0.11,
        "LATAM": 0.52,
        "APAC": 0.31,
    }
    plan_coef = {
        "Basic": 0.64,
        "Growth": 0.23,
        "Pro": -0.07,
        "Enterprise": -0.45,
    }

    tenure_coef = np.select(
        [tenure_months < 6, tenure_months < 12, tenure_months < 24],
        [0.56, 0.26, 0.00],
        default=-0.20,
    )

    log_revenue = np.log(monthly_revenue)
    revenue_coef = -0.20 * (log_revenue - np.mean(log_revenue))

    churn_logit = (
        -2.05
        + np.array([segment_coef[s] for s in customers["segment"]])
        + np.array([channel_coef[c] for c in customers["acquisition_channel"]])
        + np.array([region_coef[r] for r in customers["region"]])
        + np.array([plan_coef[p] for p in customers["plan_type"]])
        + tenure_coef
        + revenue_coef
    )

    churn_probability = np.clip(sigmoid(churn_logit), 0.03, 0.82)
    max_lifetime_days = (REFERENCE_DATE - subscription_start).dt.days.to_numpy()
    churn_eligible = max_lifetime_days >= 90
    churned = (rng.random(n) < churn_probability) & churn_eligible

    churn_offsets = np.zeros(n, dtype=int)
    eligible_idx = np.where(churned)[0]
    for i in eligible_idx:
        max_days = int(max_lifetime_days[i])
        # Skew churn timing closer to present so pre-churn deterioration is observable.
        sampled = int(rng.beta(12.0, 1.4) * max_days)
        upper = max(max_days - 1, 60)
        churn_offsets[i] = int(np.clip(sampled, 60, upper))

    subscription_end = pd.Series(pd.NaT, index=customers.index, dtype="datetime64[ns]")
    subscription_end[churned] = subscription_start[churned] + pd.to_timedelta(
        churn_offsets[churned], unit="D"
    )

    at_risk_logit = churn_logit + 0.52
    at_risk_probability = np.clip(sigmoid(at_risk_logit) * 0.56, 0.05, 0.75)
    at_risk = (~churned) & (rng.random(n) < at_risk_probability)

    status = np.where(churned, "churned", np.where(at_risk, "at_risk", "active"))

    subscriptions = pd.DataFrame(
        {
            "subscription_id": subscription_ids,
            "customer_id": customers["customer_id"],
            "subscription_start_date": subscription_start,
            "subscription_end_date": subscription_end,
            "monthly_revenue": monthly_revenue,
            "contract_type": contract_type,
            "billing_cycle": billing_cycle,
            "status": status,
        }
    )

    return subscriptions


def generate_payments(
    customers: pd.DataFrame,
    subscriptions: pd.DataFrame,
    rng: np.random.Generator,
) -> pd.DataFrame:
    cycle_months = {"Monthly": 1, "Quarterly": 3, "Annual": 12}

    churned_ids = set(
        subscriptions.loc[subscriptions["status"] == "churned", "customer_id"].to_list()
    )
    forced_failed_ids = {cid for cid in churned_ids if rng.random() < 0.55}

    payment_rows = []
    payment_idx = 1

    merged = customers.merge(subscriptions, on="customer_id", how="inner")

    for row in merged.itertuples(index=False):
        cid = row.customer_id
        start_date = pd.Timestamp(row.subscription_start_date)
        churn_date = pd.Timestamp(row.subscription_end_date) if pd.notna(row.subscription_end_date) else None
        end_date = churn_date if churn_date is not None else REFERENCE_DATE

        cycle = row.billing_cycle
        months = cycle_months[cycle]

        current = start_date
        forced_failed_written = False

        while current <= end_date:
            amount = row.monthly_revenue * months * rng.normal(1.0, 0.025)
            amount = round(float(np.clip(amount, row.monthly_revenue * months * 0.7, row.monthly_revenue * months * 1.3)), 2)

            fail_prob = 0.018
            if row.status == "at_risk" and (REFERENCE_DATE - current).days <= 90:
                fail_prob += 0.03

            if churn_date is not None and 0 <= (churn_date - current).days <= 90:
                fail_prob += 0.26
                if row.segment in {"Startup", "SMB"}:
                    fail_prob += 0.08

            failed = rng.random() < fail_prob

            if (
                cid in forced_failed_ids
                and churn_date is not None
                and 0 <= (churn_date - current).days <= 60
                and not forced_failed_written
            ):
                failed = True
                forced_failed_written = True

            payment_rows.append(
                {
                    "payment_id": f"PAY{payment_idx:08d}",
                    "customer_id": cid,
                    "payment_date": current,
                    "amount": amount,
                    "payment_status": "failed" if failed else "paid",
                }
            )
            payment_idx += 1

            current = current + pd.DateOffset(months=months)

    payments = pd.DataFrame(payment_rows)
    return payments


def generate_product_usage(
    customers: pd.DataFrame,
    subscriptions: pd.DataFrame,
    rng: np.random.Generator,
) -> pd.DataFrame:
    merged = customers.merge(subscriptions, on="customer_id", how="inner")

    plan_session_base = {"Basic": 8.0, "Growth": 13.0, "Pro": 22.0, "Enterprise": 36.0}
    segment_session_mult = {"Startup": 0.90, "SMB": 1.00, "Mid-Market": 1.18, "Enterprise": 1.35}

    plan_adoption_base = {"Basic": 38.0, "Growth": 54.0, "Pro": 71.0, "Enterprise": 81.0}
    segment_adoption_adj = {"Startup": -4.0, "SMB": 0.0, "Mid-Market": 4.0, "Enterprise": 6.0}

    segment_ticket_lambda = {"Startup": 0.32, "SMB": 0.42, "Mid-Market": 0.30, "Enterprise": 0.24}

    plan_nps_base = {"Basic": 16.0, "Growth": 26.0, "Pro": 36.0, "Enterprise": 44.0}

    usage_rows = []
    usage_idx = 1

    for row in merged.itertuples(index=False):
        start_date = pd.Timestamp(row.subscription_start_date)
        churn_date = pd.Timestamp(row.subscription_end_date) if pd.notna(row.subscription_end_date) else None
        end_date = churn_date if churn_date is not None else REFERENCE_DATE

        usage_dates = pd.date_range(start=start_date, end=end_date, freq="7D")

        customer_intensity = rng.lognormal(mean=0.0, sigma=0.34)
        customer_baseline_sessions = (
            plan_session_base[row.plan_type]
            * segment_session_mult[row.segment]
            * customer_intensity
        )

        for d in usage_dates:
            seasonality = 1.0 + 0.08 * np.sin((d.dayofyear / 365.25) * 2.0 * np.pi)
            decay_multiplier = 1.0
            nps_decay = 0.0
            ticket_boost = 1.0

            if churn_date is not None:
                days_to_churn = (churn_date - d).days
                if 0 <= days_to_churn <= 180:
                    decay_multiplier *= 0.24 + 0.76 * (days_to_churn / 180)
                    nps_decay += (180 - days_to_churn) * 0.20
                    if row.segment in {"Startup", "SMB"}:
                        ticket_boost *= 4.0
                    else:
                        ticket_boost *= 2.8

            if row.status == "at_risk":
                days_to_reference = (REFERENCE_DATE - d).days
                if 0 <= days_to_reference <= 90:
                    decay_multiplier *= 0.78 + 0.22 * (days_to_reference / 90)
                    nps_decay += (90 - days_to_reference) * 0.07
                    ticket_boost *= 1.2

            expected_sessions = max(0.5, customer_baseline_sessions * seasonality * decay_multiplier)
            sessions = int(rng.poisson(expected_sessions))

            feature_adoption = (
                plan_adoption_base[row.plan_type]
                + segment_adoption_adj[row.segment]
                + 0.48 * sessions
                + rng.normal(0, 8)
                - nps_decay * 0.45
            )
            feature_adoption = float(np.clip(feature_adoption, 0, 100))

            lambda_tickets = segment_ticket_lambda[row.segment] * ticket_boost
            support_tickets = int(rng.poisson(lambda_tickets))

            nps_score = (
                plan_nps_base[row.plan_type]
                + rng.normal(0, 11)
                - nps_decay
                - support_tickets * 2.5
            )
            nps_score = int(np.clip(round(nps_score), -100, 100))

            usage_rows.append(
                {
                    "usage_id": f"USG{usage_idx:09d}",
                    "customer_id": row.customer_id,
                    "usage_date": d,
                    "sessions": sessions,
                    "feature_adoption_score": round(feature_adoption, 2),
                    "support_tickets": support_tickets,
                    "nps_score": nps_score,
                }
            )
            usage_idx += 1

    usage = pd.DataFrame(usage_rows)
    return usage


def write_outputs(
    customers: pd.DataFrame,
    subscriptions: pd.DataFrame,
    product_usage: pd.DataFrame,
    payments: pd.DataFrame,
) -> None:
    root = Path(__file__).resolve().parents[2]
    raw_dir = root / "data" / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    customers_out = customers.copy()
    customers_out["signup_date"] = pd.to_datetime(customers_out["signup_date"]).dt.date

    subscriptions_out = subscriptions.copy()
    subscriptions_out["subscription_start_date"] = pd.to_datetime(
        subscriptions_out["subscription_start_date"]
    ).dt.date
    subscriptions_out["subscription_end_date"] = pd.to_datetime(
        subscriptions_out["subscription_end_date"]
    ).dt.date

    usage_out = product_usage.copy()
    usage_out["usage_date"] = pd.to_datetime(usage_out["usage_date"]).dt.date

    payments_out = payments.copy()
    payments_out["payment_date"] = pd.to_datetime(payments_out["payment_date"]).dt.date

    customers_out.to_csv(raw_dir / "customers.csv", index=False)
    subscriptions_out.to_csv(raw_dir / "subscriptions.csv", index=False)
    usage_out.to_csv(raw_dir / "product_usage.csv", index=False)
    payments_out.to_csv(raw_dir / "payments.csv", index=False)


def main() -> None:
    rng = np.random.default_rng(SEED)

    customers = generate_customers(rng)
    subscriptions = generate_subscriptions(customers, rng)
    payments = generate_payments(customers, subscriptions, rng)
    product_usage = generate_product_usage(customers, subscriptions, rng)

    write_outputs(customers, subscriptions, product_usage, payments)

    status_mix = subscriptions["status"].value_counts(normalize=True).mul(100).round(1)
    print("Synthetic data generated with fixed seed:", SEED)
    print("Rows -> customers:", len(customers), ", subscriptions:", len(subscriptions), ", product_usage:", len(product_usage), ", payments:", len(payments))
    print("Subscription status mix (%):", status_mix.to_dict())


if __name__ == "__main__":
    main()
