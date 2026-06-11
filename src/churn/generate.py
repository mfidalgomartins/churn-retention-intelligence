"""Synthetic SaaS dataset: customers, subscriptions, weekly product usage, payments.

Deterministic given (SEED, REFERENCE_DATE). The simulator embeds realistic
commercial heterogeneity and pre-churn deterioration patterns so downstream
analytics behaves like a real retention problem.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from churn.common import REFERENCE_DATE, SEED, raw_dir

N_CUSTOMERS = 3500


def _sigmoid(x: np.ndarray) -> np.ndarray:
    return 1.0 / (1.0 + np.exp(-x))


def generate_customers(rng: np.random.Generator) -> pd.DataFrame:
    customer_ids = [f"C{idx:06d}" for idx in range(1, N_CUSTOMERS + 1)]

    signup_start = pd.Timestamp("2022-01-01")
    signup_span_days = (REFERENCE_DATE - signup_start).days
    signup_dates = signup_start + pd.to_timedelta(
        rng.integers(0, signup_span_days, size=N_CUSTOMERS), unit="D"
    )

    segments = rng.choice(
        ["Startup", "SMB", "Mid-Market", "Enterprise"],
        p=[0.16, 0.39, 0.29, 0.16],
        size=N_CUSTOMERS,
    )
    regions = rng.choice(
        ["North America", "Europe", "LATAM", "APAC"],
        p=[0.39, 0.31, 0.16, 0.14],
        size=N_CUSTOMERS,
    )

    channels = np.array(["Organic", "Referral", "Partner", "Paid Search", "Affiliate", "Outbound"])
    channel_mix = {
        "Startup":    [0.18, 0.11, 0.07, 0.34, 0.20, 0.10],
        "SMB":        [0.22, 0.12, 0.09, 0.28, 0.14, 0.15],
        "Mid-Market": [0.25, 0.11, 0.18, 0.17, 0.06, 0.23],
        "Enterprise": [0.21, 0.09, 0.28, 0.10, 0.02, 0.30],
    }
    acquisition_channels = np.array(
        [rng.choice(channels, p=channel_mix[s]) for s in segments]
    )

    plans = np.array(["Basic", "Growth", "Pro", "Enterprise"])
    plan_mix = {
        "Startup":    [0.53, 0.33, 0.13, 0.01],
        "SMB":        [0.36, 0.41, 0.20, 0.03],
        "Mid-Market": [0.14, 0.38, 0.37, 0.11],
        "Enterprise": [0.03, 0.14, 0.43, 0.40],
    }
    plan_types = np.array([rng.choice(plans, p=plan_mix[s]) for s in segments])

    return pd.DataFrame({
        "customer_id": customer_ids,
        "signup_date": signup_dates,
        "segment": segments,
        "region": regions,
        "acquisition_channel": acquisition_channels,
        "plan_type": plan_types,
    })


def generate_subscriptions(customers: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
    n = len(customers)

    start_lag = rng.integers(0, 15, size=n)
    sub_start = customers["signup_date"] + pd.to_timedelta(start_lag, unit="D")
    sub_start = sub_start.clip(upper=REFERENCE_DATE)

    base_price = {"Basic": 55.0, "Growth": 160.0, "Pro": 460.0, "Enterprise": 1400.0}
    sigma = {"Basic": 0.45, "Growth": 0.40, "Pro": 0.42, "Enterprise": 0.55}
    monthly_revenue = np.round(np.clip(
        np.array([base_price[p] * rng.lognormal(0.0, sigma[p])
                  for p in customers["plan_type"]]),
        20, 20000,
    ), 2)

    contract_type, billing_cycle = [], []
    for seg, plan in zip(customers["segment"], customers["plan_type"], strict=False):
        if plan == "Enterprise" or seg == "Enterprise":
            c = rng.choice(["Annual", "Monthly"], p=[0.78, 0.22])
        elif plan == "Pro":
            c = rng.choice(["Annual", "Monthly"], p=[0.42, 0.58])
        else:
            c = rng.choice(["Annual", "Monthly"], p=[0.16, 0.84])
        if c == "Annual":
            b = rng.choice(["Annual", "Quarterly"], p=[0.82, 0.18])
        else:
            b = rng.choice(["Monthly", "Quarterly"], p=[0.86, 0.14])
        contract_type.append(c)
        billing_cycle.append(b)

    tenure_months = ((REFERENCE_DATE - sub_start).dt.days / 30.4).to_numpy()

    seg_coef = {"Startup": 1.00, "SMB": 0.62, "Mid-Market": 0.18, "Enterprise": -0.42}
    chn_coef = {"Organic": 0.00, "Referral": -0.38, "Partner": -0.24,
                "Paid Search": 0.74, "Affiliate": 0.86, "Outbound": 0.34}
    reg_coef = {"North America": 0.00, "Europe": 0.11, "LATAM": 0.52, "APAC": 0.31}
    pln_coef = {"Basic": 0.64, "Growth": 0.23, "Pro": -0.07, "Enterprise": -0.45}
    ten_coef = np.select(
        [tenure_months < 6, tenure_months < 12, tenure_months < 24],
        [0.56, 0.26, 0.00],
        default=-0.20,
    )
    log_rev = np.log(monthly_revenue)
    rev_coef = -0.20 * (log_rev - np.mean(log_rev))

    churn_logit = (
        -2.05
        + np.array([seg_coef[s] for s in customers["segment"]])
        + np.array([chn_coef[c] for c in customers["acquisition_channel"]])
        + np.array([reg_coef[r] for r in customers["region"]])
        + np.array([pln_coef[p] for p in customers["plan_type"]])
        + ten_coef
        + rev_coef
    )
    churn_prob = np.clip(_sigmoid(churn_logit), 0.03, 0.82)

    max_life_days = (REFERENCE_DATE - sub_start).dt.days.to_numpy()
    eligible = max_life_days >= 90
    churned = (rng.random(n) < churn_prob) & eligible

    churn_offsets = np.zeros(n, dtype=int)
    for i in np.where(churned)[0]:
        max_days = int(max_life_days[i])
        sampled = int(rng.beta(12.0, 1.4) * max_days)
        churn_offsets[i] = int(np.clip(sampled, 60, max(max_days - 1, 60)))

    sub_end = pd.Series(pd.NaT, index=customers.index, dtype="datetime64[ns]")
    sub_end[churned] = sub_start[churned] + pd.to_timedelta(churn_offsets[churned], unit="D")

    at_risk_prob = np.clip(_sigmoid(churn_logit + 0.52) * 0.56, 0.05, 0.75)
    at_risk = (~churned) & (rng.random(n) < at_risk_prob)
    status = np.where(churned, "churned", np.where(at_risk, "at_risk", "active"))

    return pd.DataFrame({
        "subscription_id": [f"SUB{i:06d}" for i in range(1, n + 1)],
        "customer_id": customers["customer_id"],
        "subscription_start_date": sub_start,
        "subscription_end_date": sub_end,
        "monthly_revenue": monthly_revenue,
        "contract_type": contract_type,
        "billing_cycle": billing_cycle,
        "status": status,
    })


def generate_payments(
    customers: pd.DataFrame,
    subscriptions: pd.DataFrame,
    rng: np.random.Generator,
) -> pd.DataFrame:
    cycle_months = {"Monthly": 1, "Quarterly": 3, "Annual": 12}
    churned_ids = set(subscriptions.loc[subscriptions["status"] == "churned", "customer_id"])
    forced_failed_ids = {cid for cid in sorted(churned_ids) if rng.random() < 0.55}

    rows: list[dict] = []
    idx = 1
    merged = customers.merge(subscriptions, on="customer_id")

    for row in merged.itertuples(index=False):
        cid = row.customer_id
        start = pd.Timestamp(row.subscription_start_date)
        churn_date = pd.Timestamp(row.subscription_end_date) if pd.notna(row.subscription_end_date) else None
        end = churn_date if churn_date is not None else REFERENCE_DATE
        months = cycle_months[row.billing_cycle]

        current = start
        forced_written = False
        while current <= end:
            amount = row.monthly_revenue * months * rng.normal(1.0, 0.025)
            amount = round(float(np.clip(amount, row.monthly_revenue * months * 0.7,
                                                 row.monthly_revenue * months * 1.3)), 2)

            fail_prob = 0.018
            if row.status == "at_risk" and (REFERENCE_DATE - current).days <= 90:
                fail_prob += 0.03
            if churn_date is not None and 0 <= (churn_date - current).days <= 90:
                fail_prob += 0.26
                if row.segment in {"Startup", "SMB"}:
                    fail_prob += 0.08

            failed = rng.random() < fail_prob
            if (cid in forced_failed_ids and churn_date is not None
                    and 0 <= (churn_date - current).days <= 60
                    and not forced_written):
                failed = True
                forced_written = True

            rows.append({
                "payment_id": f"PAY{idx:08d}",
                "customer_id": cid,
                "payment_date": current,
                "amount": amount,
                "payment_status": "failed" if failed else "paid",
            })
            idx += 1
            current = current + pd.DateOffset(months=months)

    return pd.DataFrame(rows)


def generate_product_usage(
    customers: pd.DataFrame,
    subscriptions: pd.DataFrame,
    rng: np.random.Generator,
) -> pd.DataFrame:
    merged = customers.merge(subscriptions, on="customer_id")

    plan_sessions = {"Basic": 8.0, "Growth": 13.0, "Pro": 22.0, "Enterprise": 36.0}
    seg_sessions = {"Startup": 0.90, "SMB": 1.00, "Mid-Market": 1.18, "Enterprise": 1.35}
    plan_adoption = {"Basic": 38.0, "Growth": 54.0, "Pro": 71.0, "Enterprise": 81.0}
    seg_adoption = {"Startup": -4.0, "SMB": 0.0, "Mid-Market": 4.0, "Enterprise": 6.0}
    seg_tickets = {"Startup": 0.32, "SMB": 0.42, "Mid-Market": 0.30, "Enterprise": 0.24}
    plan_nps = {"Basic": 16.0, "Growth": 26.0, "Pro": 36.0, "Enterprise": 44.0}

    rows: list[dict] = []
    idx = 1
    for row in merged.itertuples(index=False):
        start = pd.Timestamp(row.subscription_start_date)
        churn_date = pd.Timestamp(row.subscription_end_date) if pd.notna(row.subscription_end_date) else None
        end = churn_date if churn_date is not None else REFERENCE_DATE

        intensity = rng.lognormal(0.0, 0.34)
        baseline = plan_sessions[row.plan_type] * seg_sessions[row.segment] * intensity

        for d in pd.date_range(start, end, freq="7D"):
            seasonal = 1.0 + 0.08 * np.sin((d.dayofyear / 365.25) * 2.0 * np.pi)
            decay = 1.0
            nps_decay = 0.0
            ticket_boost = 1.0

            if churn_date is not None:
                days_to_churn = (churn_date - d).days
                if 0 <= days_to_churn <= 180:
                    decay *= 0.24 + 0.76 * (days_to_churn / 180)
                    nps_decay += (180 - days_to_churn) * 0.20
                    ticket_boost *= 4.0 if row.segment in {"Startup", "SMB"} else 2.8

            if row.status == "at_risk":
                days_to_ref = (REFERENCE_DATE - d).days
                if 0 <= days_to_ref <= 90:
                    decay *= 0.78 + 0.22 * (days_to_ref / 90)
                    nps_decay += (90 - days_to_ref) * 0.07
                    ticket_boost *= 1.2

            expected = max(0.5, baseline * seasonal * decay)
            sessions = int(rng.poisson(expected))

            adoption = (plan_adoption[row.plan_type] + seg_adoption[row.segment]
                        + 0.48 * sessions + rng.normal(0, 8) - nps_decay * 0.45)
            adoption = float(np.clip(adoption, 0, 100))

            tickets = int(rng.poisson(seg_tickets[row.segment] * ticket_boost))
            nps = int(np.clip(round(plan_nps[row.plan_type] + rng.normal(0, 11)
                                     - nps_decay - tickets * 2.5), -100, 100))

            rows.append({
                "usage_id": f"USG{idx:09d}",
                "customer_id": row.customer_id,
                "usage_date": d,
                "sessions": sessions,
                "feature_adoption_score": round(adoption, 2),
                "support_tickets": tickets,
                "nps_score": nps,
            })
            idx += 1

    return pd.DataFrame(rows)


def _write(customers, subscriptions, usage, payments) -> None:
    out = raw_dir()
    out.mkdir(parents=True, exist_ok=True)
    for df, name, date_cols in [
        (customers, "customers", ["signup_date"]),
        (subscriptions, "subscriptions", ["subscription_start_date", "subscription_end_date"]),
        (usage, "product_usage", ["usage_date"]),
        (payments, "payments", ["payment_date"]),
    ]:
        df = df.copy()
        for col in date_cols:
            df[col] = pd.to_datetime(df[col]).dt.date
        df.to_csv(out / f"{name}.csv", index=False)


def main() -> None:
    rng = np.random.default_rng(SEED)
    customers = generate_customers(rng)
    subscriptions = generate_subscriptions(customers, rng)
    payments = generate_payments(customers, subscriptions, rng)
    usage = generate_product_usage(customers, subscriptions, rng)
    _write(customers, subscriptions, usage, payments)

    status_mix = subscriptions["status"].value_counts(normalize=True).mul(100).round(1).to_dict()
    print(f"Generated synthetic dataset (seed={SEED}, reference={REFERENCE_DATE.date()}).")
    print(f"  customers={len(customers):,}  subscriptions={len(subscriptions):,}  "
          f"usage={len(usage):,}  payments={len(payments):,}")
    print(f"  status mix (%): {status_mix}")


if __name__ == "__main__":
    main()
