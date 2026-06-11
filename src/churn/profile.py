"""Data quality profiling on the raw tables.

Runs a fixed set of integrity checks and writes a status table. Failures here
should be considered blockers — anything downstream assumes these pass.
"""
from __future__ import annotations

import sys
from dataclasses import dataclass

import numpy as np
import pandas as pd

from churn.common import infer_snapshot_date, outputs_tables_dir, raw_dir


@dataclass
class Check:
    table: str
    name: str
    failed: int
    denominator: int
    severity: str
    note: str

    def to_row(self) -> dict:
        rate = (self.failed / self.denominator) if self.denominator else 0.0
        return {
            "table_name": self.table,
            "check_name": self.name,
            "failed_rows": self.failed,
            "denominator": self.denominator,
            "failure_rate": round(rate, 6),
            "severity": self.severity,
            "status": "FAIL" if self.failed > 0 else "PASS",
            "details": self.note,
        }


def load_raw() -> dict[str, pd.DataFrame]:
    base = raw_dir()
    return {
        "customers": pd.read_csv(base / "customers.csv", parse_dates=["signup_date"]),
        "subscriptions": pd.read_csv(
            base / "subscriptions.csv",
            parse_dates=["subscription_start_date", "subscription_end_date"],
        ),
        "product_usage": pd.read_csv(base / "product_usage.csv", parse_dates=["usage_date"]),
        "payments": pd.read_csv(base / "payments.csv", parse_dates=["payment_date"]),
    }


def run_checks(tables: dict[str, pd.DataFrame], snapshot: pd.Timestamp) -> pd.DataFrame:
    customers = tables["customers"]
    subscriptions = tables["subscriptions"]
    usage = tables["product_usage"]
    payments = tables["payments"]

    checks: list[Check] = []

    # Subscriptions: temporal integrity, value sanity, status vocabulary.
    subs = subscriptions.sort_values(["customer_id", "subscription_start_date", "subscription_end_date"]).copy()
    subs["effective_end"] = subs["subscription_end_date"].fillna(snapshot)
    subs["prev_end"] = subs.groupby("customer_id")["effective_end"].shift(1)
    overlap = subs["prev_end"].notna() & (subs["subscription_start_date"] <= subs["prev_end"])
    checks.append(Check("subscriptions", "overlapping_subscriptions", int(overlap.sum()),
                        subs["customer_id"].nunique(), "high",
                        "A new subscription must start after the previous one ends."))

    bad_rev = (~np.isfinite(subscriptions["monthly_revenue"])) | (subscriptions["monthly_revenue"] <= 0)
    checks.append(Check("subscriptions", "impossible_revenue_values", int(bad_rev.sum()),
                        len(subscriptions), "high", "monthly_revenue must be positive and finite."))

    valid_status = {"active", "at_risk", "churned"}
    bad_status = ~subscriptions["status"].isin(valid_status)
    checks.append(Check("subscriptions", "invalid_subscription_status", int(bad_status.sum()),
                        len(subscriptions), "high", f"status must be one of {sorted(valid_status)}."))

    bad_contract = ~subscriptions["contract_type"].isin({"Annual", "Monthly"})
    checks.append(Check("subscriptions", "invalid_contract_type", int(bad_contract.sum()),
                        len(subscriptions), "medium", "contract_type must be Annual or Monthly."))

    bad_billing = ~subscriptions["billing_cycle"].isin({"Monthly", "Quarterly", "Annual"})
    checks.append(Check("subscriptions", "invalid_billing_cycle", int(bad_billing.sum()),
                        len(subscriptions), "medium", "billing_cycle must be Monthly, Quarterly, or Annual."))

    end_before_start = (subscriptions["subscription_end_date"].notna()
                        & (subscriptions["subscription_end_date"] < subscriptions["subscription_start_date"]))
    checks.append(Check("subscriptions", "subscription_end_before_start", int(end_before_start.sum()),
                        len(subscriptions), "high", "Subscription end date cannot precede its start."))

    # Product usage: dates in active period, ranges.
    usage_merged = usage.merge(
        subscriptions[["customer_id", "subscription_start_date", "subscription_end_date"]],
        on="customer_id", how="left",
    )
    usage_outside = (
        usage_merged["subscription_start_date"].isna()
        | (usage_merged["usage_date"] < usage_merged["subscription_start_date"])
        | (usage_merged["subscription_end_date"].notna()
           & (usage_merged["usage_date"] > usage_merged["subscription_end_date"]))
    )
    checks.append(Check("product_usage", "usage_outside_subscription_period",
                        int(usage_outside.sum()), len(usage_merged), "high",
                        "Usage must fall inside the subscription's active interval."))

    checks.append(Check("product_usage", "negative_sessions",
                        int((usage["sessions"] < 0).sum()), len(usage), "high",
                        "sessions must be non-negative."))

    bad_adoption = (usage["feature_adoption_score"] < 0) | (usage["feature_adoption_score"] > 100)
    checks.append(Check("product_usage", "feature_adoption_out_of_range",
                        int(bad_adoption.sum()), len(usage), "high",
                        "feature_adoption_score must be in [0, 100]."))

    checks.append(Check("product_usage", "negative_support_tickets",
                        int((usage["support_tickets"] < 0).sum()), len(usage), "high",
                        "support_tickets must be non-negative."))

    bad_nps = (usage["nps_score"] < -100) | (usage["nps_score"] > 100)
    checks.append(Check("product_usage", "nps_out_of_range", int(bad_nps.sum()), len(usage),
                        "high", "nps_score must be in [-100, 100]."))

    # Payments: amounts, status, timing.
    payments_merged = payments.merge(
        subscriptions[["customer_id", "subscription_start_date", "subscription_end_date"]],
        on="customer_id", how="left",
    )
    bad_status_pay = ~payments["payment_status"].isin({"paid", "failed"})
    bad_amt = (~np.isfinite(payments["amount"])) | (payments["amount"] <= 0)
    pay_before = payments_merged["subscription_start_date"].notna() & (
        payments_merged["payment_date"] < payments_merged["subscription_start_date"]
    )
    pay_after = payments_merged["subscription_end_date"].notna() & (
        payments_merged["payment_date"] > payments_merged["subscription_end_date"]
    )
    orphan = payments_merged["subscription_start_date"].isna()
    pay_bad = bad_status_pay | bad_amt | pay_before | pay_after | orphan
    checks.append(Check("payments", "payment_inconsistencies", int(pay_bad.sum()),
                        len(payments), "high",
                        "Payment status/amount/date must align with a known subscription window."))

    checks.append(Check("payments", "duplicate_payment_id",
                        int(payments["payment_id"].duplicated().sum()), len(payments),
                        "high", "payment_id must be unique."))

    # Customers: signup sanity.
    checks.append(Check("customers", "signup_date_in_future",
                        int((customers["signup_date"] > snapshot).sum()), len(customers),
                        "medium", "signup_date should not be after the dataset snapshot."))

    return pd.DataFrame([c.to_row() for c in checks])


def main() -> int:
    tables = load_raw()
    snapshot = infer_snapshot_date(
        tables["customers"]["signup_date"],
        tables["subscriptions"]["subscription_start_date"],
        tables["subscriptions"]["subscription_end_date"],
        tables["product_usage"]["usage_date"],
        tables["payments"]["payment_date"],
    )
    checks = run_checks(tables, snapshot)

    out = outputs_tables_dir()
    out.mkdir(parents=True, exist_ok=True)
    checks.to_csv(out / "data_quality_checks.csv", index=False)

    fails = int((checks["status"] == "FAIL").sum())
    print(f"Profiled raw tables (snapshot={snapshot.date()}). "
          f"Checks={len(checks)}, failed={fails}.")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
