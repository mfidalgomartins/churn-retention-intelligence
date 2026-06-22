"""Regression tests for cohort-table performance changes."""
from __future__ import annotations

import unittest

import pandas as pd
from pandas.testing import assert_frame_equal

from churn.common import last_complete_month_start
from churn.features import build_cohort_table

SNAPSHOT = pd.Timestamp("2026-03-01")


def _reference_cohort_table(subscriptions: pd.DataFrame, snapshot: pd.Timestamp) -> pd.DataFrame:
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
            retained = group["subscription_end_date"].isna() | (
                group["subscription_end_date"] > month_end
            )
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

    return pd.DataFrame(rows).sort_values(["cohort_month", "observation_month"]).reset_index(
        drop=True
    )


class TestCohortTablePerformance(unittest.TestCase):
    def test_build_cohort_table_matches_reference_loop_on_small_fixture(self) -> None:
        subscriptions = pd.DataFrame([
            {
                "customer_id": "C001",
                "subscription_start_date": pd.Timestamp("2025-01-15"),
                "subscription_end_date": pd.NaT,
                "monthly_revenue": 100.0,
            },
            {
                "customer_id": "C002",
                "subscription_start_date": pd.Timestamp("2025-01-20"),
                "subscription_end_date": pd.Timestamp("2025-08-10"),
                "monthly_revenue": 200.0,
            },
            {
                "customer_id": "C003",
                "subscription_start_date": pd.Timestamp("2025-02-01"),
                "subscription_end_date": pd.Timestamp("2025-02-28"),
                "monthly_revenue": 50.0,
            },
            {
                "customer_id": "C004",
                "subscription_start_date": pd.Timestamp("2025-02-18"),
                "subscription_end_date": pd.Timestamp("2026-02-01"),
                "monthly_revenue": 150.0,
            },
            {
                "customer_id": "C005",
                "subscription_start_date": pd.Timestamp("2025-03-05"),
                "subscription_end_date": pd.NaT,
                "monthly_revenue": 0.0,
            },
            {
                "customer_id": "C006",
                "subscription_start_date": pd.Timestamp("2025-03-22"),
                "subscription_end_date": pd.Timestamp("2025-04-01"),
                "monthly_revenue": 0.0,
            },
        ])

        expected = _reference_cohort_table(subscriptions, SNAPSHOT)
        actual = build_cohort_table(subscriptions, SNAPSHOT)

        assert_frame_equal(actual, expected)


if __name__ == "__main__":
    unittest.main()
