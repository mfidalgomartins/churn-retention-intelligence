"""Unit tests for feature engineering."""
from __future__ import annotations

import unittest

import pandas as pd

from churn.features import (
    build_cohort_table,
    build_segment_summary,
    next_renewal_date,
    payment_aggregates,
    usage_aggregates,
)

SNAPSHOT = pd.Timestamp("2026-03-01")


def _observation_dates(*customer_ids: str, date: pd.Timestamp = SNAPSHOT) -> pd.DataFrame:
    return pd.DataFrame({
        "customer_id": list(customer_ids),
        "observation_date": [date] * len(customer_ids),
    })


class TestNextRenewalDate(unittest.TestCase):
    def test_monthly_cycle_lands_on_anniversary(self) -> None:
        start = pd.Series([pd.Timestamp("2025-01-15")])
        cycle = pd.Series([1])
        result = next_renewal_date(start, SNAPSHOT, cycle)
        # 2025-01-15 → next monthly anniversary strictly after 2026-03-01 is 2026-03-15
        self.assertEqual(result.iloc[0], pd.Timestamp("2026-03-15"))

    def test_annual_cycle_lands_on_yearly_anniversary(self) -> None:
        start = pd.Series([pd.Timestamp("2024-06-10")])
        cycle = pd.Series([12])
        result = next_renewal_date(start, SNAPSHOT, cycle)
        # 2024-06-10 + 12m = 2025-06-10 (past), + another 12m = 2026-06-10
        self.assertEqual(result.iloc[0], pd.Timestamp("2026-06-10"))

    def test_future_start_returns_first_anniversary(self) -> None:
        start = pd.Series([pd.Timestamp("2026-04-01")])
        cycle = pd.Series([1])
        result = next_renewal_date(start, SNAPSHOT, cycle)
        # start is after snapshot → first anniversary is the start itself
        self.assertEqual(result.iloc[0], pd.Timestamp("2026-04-01"))


class TestUsageAggregates(unittest.TestCase):
    def _usage(self, **rows) -> pd.DataFrame:
        return pd.DataFrame([
            {"customer_id": cid, "usage_date": pd.Timestamp(d),
             "sessions": s, "support_tickets": t,
             "feature_adoption_score": a, "nps_score": n}
            for cid, recs in rows.items()
            for d, s, t, a, n in recs
        ])

    def test_recent_30d_counts(self) -> None:
        usage = self._usage(
            A=[("2026-02-15", 10, 1, 50, 20),  # in 30d
               ("2026-01-20", 5, 0, 40, 10),    # in 90d, not 30d
               ("2025-11-20", 7, 2, 30, 0)],    # outside 90d
        )
        agg = usage_aggregates(usage, _observation_dates("A"))
        row = agg.iloc[0]
        self.assertEqual(row["recent_sessions_30d"], 10)
        self.assertEqual(row["recent_sessions_90d"], 15)

    def test_usage_trend_is_recent_minus_prior(self) -> None:
        # snapshot = 2026-03-01; window 0-30d (Feb 1-Mar 1), 30-60d (Jan 2-Feb 1)
        usage = self._usage(
            A=[("2026-02-15", 20, 0, 80, 50),  # recent_30
               ("2026-01-15", 5, 0, 80, 50)],  # prior_30
        )
        agg = usage_aggregates(usage, _observation_dates("A"))
        self.assertAlmostEqual(agg.iloc[0]["usage_trend"], 15.0)

    def test_windows_are_anchored_to_customer_observation_date(self) -> None:
        usage = self._usage(
            A=[("2025-06-15", 20, 0, 80, 50), ("2026-02-15", 1, 0, 10, -50)],
        )
        observation = _observation_dates("A", date=pd.Timestamp("2025-07-01"))
        agg = usage_aggregates(usage, observation)
        self.assertEqual(agg.iloc[0]["recent_sessions_30d"], 20)


class TestPaymentAggregates(unittest.TestCase):
    def test_failed_payments_90d_count(self) -> None:
        subs = pd.DataFrame([{"customer_id": "A", "billing_cycle": "Monthly"}])
        payments = pd.DataFrame([
            {"customer_id": "A", "payment_date": pd.Timestamp("2025-12-10"),
             "amount": 100, "payment_status": "paid"},
            {"customer_id": "A", "payment_date": pd.Timestamp("2026-02-10"),
             "amount": 100, "payment_status": "failed"},
            {"customer_id": "A", "payment_date": pd.Timestamp("2026-01-10"),
             "amount": 100, "payment_status": "failed"},
            {"customer_id": "A", "payment_date": pd.Timestamp("2025-11-10"),
             "amount": 100, "payment_status": "failed"},
        ])
        agg = payment_aggregates(payments, subs, _observation_dates("A"))
        a = agg[agg["customer_id"] == "A"].iloc[0]
        self.assertEqual(a["failed_payments_90d"], 2)
        self.assertEqual(a["payment_failure_flag"], 1)

    def test_failed_payment_window_uses_customer_observation_date(self) -> None:
        subs = pd.DataFrame([{"customer_id": "A", "billing_cycle": "Monthly"}])
        payments = pd.DataFrame([
            {"customer_id": "A", "payment_date": pd.Timestamp("2025-06-10"),
             "amount": 100, "payment_status": "failed"},
            {"customer_id": "A", "payment_date": pd.Timestamp("2026-02-10"),
             "amount": 100, "payment_status": "failed"},
        ])
        observation = _observation_dates("A", date=pd.Timestamp("2025-07-01"))
        agg = payment_aggregates(payments, subs, observation)
        self.assertEqual(agg[agg["customer_id"] == "A"].iloc[0]["failed_payments_90d"], 1)


class TestCohortTable(unittest.TestCase):
    def _subs(self, rows) -> pd.DataFrame:
        return pd.DataFrame([
            {"customer_id": f"C{i}",
             "subscription_start_date": pd.Timestamp(s),
             "subscription_end_date": pd.Timestamp(e) if e else pd.NaT,
             "monthly_revenue": r}
            for i, (s, e, r) in enumerate(rows)
        ])

    def test_retention_rate_is_retained_over_cohort_size(self) -> None:
        subs = self._subs([
            ("2025-01-15", None, 100),    # active throughout
            ("2025-01-20", "2025-08-10", 100),  # churned in month 7
        ])
        cohort = build_cohort_table(subs, SNAPSHOT)
        # Look up the August 2025 observation for cohort 2025-01
        row = cohort[(cohort["cohort_month"] == "2025-01-01")
                     & (cohort["observation_month"] == "2025-08-01")].iloc[0]
        self.assertEqual(row["active_customers"], 2)
        self.assertEqual(row["retained_customers"], 1)
        self.assertAlmostEqual(row["retention_rate"], 0.5)

    def test_retention_rate_starts_at_one(self) -> None:
        subs = self._subs([("2025-06-15", None, 100), ("2025-06-20", "2026-01-15", 100)])
        cohort = build_cohort_table(subs, SNAPSHOT)
        first = cohort[(cohort["cohort_month"] == "2025-06-01")
                       & (cohort["observation_month"] == "2025-06-01")].iloc[0]
        self.assertEqual(first["retention_rate"], 1.0)

    def test_partial_snapshot_month_is_excluded(self) -> None:
        subs = self._subs([("2025-06-15", None, 100)])
        cohort = build_cohort_table(subs, SNAPSHOT)
        self.assertEqual(cohort["observation_month"].max(), "2026-02-01")


class TestSegmentSummary(unittest.TestCase):
    def test_churn_rate_per_segment(self) -> None:
        features = pd.DataFrame([
            {"customer_id": "A", "segment": "SMB", "churn_flag": 1, "at_risk_flag": 0,
             "current_mrr": 0, "avg_monthly_revenue": 100, "tenure_days": 365,
             "nps_score_recent": 30, "usage_trend": 0},
            {"customer_id": "B", "segment": "SMB", "churn_flag": 0, "at_risk_flag": 1,
             "current_mrr": 200, "avg_monthly_revenue": 200, "tenure_days": 200,
             "nps_score_recent": 20, "usage_trend": -1},
            {"customer_id": "C", "segment": "Enterprise", "churn_flag": 0, "at_risk_flag": 0,
             "current_mrr": 1000, "avg_monthly_revenue": 1000, "tenure_days": 800,
             "nps_score_recent": 50, "usage_trend": 2},
        ])
        summary = build_segment_summary(features)
        smb = summary[summary["segment"] == "SMB"].iloc[0]
        self.assertEqual(smb["cumulative_churn_share"], 0.5)
        self.assertEqual(smb["current_mrr_at_risk"], 200.0)
        self.assertEqual(smb["churned_monthly_value_proxy"], 100.0)


if __name__ == "__main__":
    unittest.main()
