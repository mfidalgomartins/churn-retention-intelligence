"""Unit tests for the churn analysis layer."""
from __future__ import annotations

import unittest

import pandas as pd

from churn.analyze import (
    behavioral_relationships,
    churn_by_dimension,
    cohort_movement,
    intervention_priorities,
    overall_health,
    rank_drivers,
    revenue_at_risk,
)


def _features(rows: list[dict]) -> pd.DataFrame:
    """Build a feature frame with sane defaults."""
    defaults = {
        "customer_id": "X", "segment": "SMB", "region": "Europe",
        "acquisition_channel": "Organic", "plan_type": "Growth",
        "tenure_days": 400, "current_mrr": 200.0,
        "avg_monthly_revenue": 200.0, "lifetime_revenue": 4000.0,
        "recent_sessions_30d": 20, "recent_sessions_90d": 60, "usage_trend": 1.0,
        "feature_adoption_score_recent": 70.0,
        "support_tickets_30d": 1, "support_tickets_90d": 2,
        "nps_score_recent": 30.0,
        "failed_payments_90d": 0, "payment_failure_flag": 0,
        "renewal_near_flag": 0, "contraction_flag": 0,
        "churn_flag": 0, "at_risk_flag": 0,
    }
    return pd.DataFrame([{**defaults, **r} for r in rows])


class TestChurnByDimension(unittest.TestCase):
    def test_churn_rate_per_segment(self) -> None:
        features = _features([
            {"customer_id": "A", "segment": "SMB", "churn_flag": 1, "avg_monthly_revenue": 100},
            {"customer_id": "B", "segment": "SMB", "churn_flag": 0, "avg_monthly_revenue": 100},
            {"customer_id": "C", "segment": "Enterprise", "churn_flag": 0, "avg_monthly_revenue": 1000},
        ])
        out = churn_by_dimension(features, "segment")
        smb = out[out["segment"] == "SMB"].iloc[0]
        ent = out[out["segment"] == "Enterprise"].iloc[0]
        self.assertEqual(smb["churn_rate"], 0.5)
        self.assertEqual(ent["churn_rate"], 0.0)
        self.assertEqual(smb["churned_revenue"], 100.0)

    def test_result_sorted_by_churn_rate_descending(self) -> None:
        features = _features([
            {"customer_id": "A", "segment": "Low", "churn_flag": 0},
            {"customer_id": "B", "segment": "High", "churn_flag": 1},
        ])
        out = churn_by_dimension(features, "segment")
        self.assertEqual(out.iloc[0]["segment"], "High")


class TestBehavioralRelationships(unittest.TestCase):
    def test_lift_is_in_group_over_out_group(self) -> None:
        # 5 customers; 2 with failed_payment, both churned. 3 without, 0 churned.
        features = _features([
            {"customer_id": str(i), "payment_failure_flag": 1, "churn_flag": 1}
            for i in range(2)
        ] + [
            {"customer_id": str(i + 10), "payment_failure_flag": 0, "churn_flag": 0}
            for i in range(3)
        ])
        rels, _ = behavioral_relationships(features)
        fp = rels[rels["relationship"] == "failed_payment_flag"].iloc[0]
        self.assertEqual(fp["churn_rate_in_group"], 1.0)
        self.assertEqual(fp["churn_rate_out_group"], 0.0)


class TestRankDrivers(unittest.TestCase):
    def test_drivers_below_baseline_are_excluded(self) -> None:
        # Baseline churn = 0.5. Only drivers with > 0.5 churn should appear.
        features = _features([
            {"customer_id": "A", "churn_flag": 1, "payment_failure_flag": 1},
            {"customer_id": "B", "churn_flag": 0, "payment_failure_flag": 0},
        ])
        _rels, thresholds = behavioral_relationships(features)
        out = rank_drivers(features, thresholds)
        # All drivers in ranking should have churn_rate > baseline
        self.assertTrue((out["churn_rate"] > out["baseline_churn_rate"]).all())


class TestOverallHealth(unittest.TestCase):
    def test_summary_has_required_keys(self) -> None:
        features = _features([
            {"customer_id": "A", "churn_flag": 1, "avg_monthly_revenue": 100, "current_mrr": 0},
            {"customer_id": "B", "churn_flag": 0, "at_risk_flag": 1, "current_mrr": 200, "avg_monthly_revenue": 200},
            {"customer_id": "C", "churn_flag": 0, "current_mrr": 300, "avg_monthly_revenue": 300},
        ])
        subs = pd.DataFrame([
            {"customer_id": "A", "subscription_start_date": pd.Timestamp("2024-01-01"),
             "subscription_end_date": pd.Timestamp("2025-06-01"), "monthly_revenue": 100},
            {"customer_id": "B", "subscription_start_date": pd.Timestamp("2024-01-01"),
             "subscription_end_date": pd.NaT, "monthly_revenue": 200},
            {"customer_id": "C", "subscription_start_date": pd.Timestamp("2024-01-01"),
             "subscription_end_date": pd.NaT, "monthly_revenue": 300},
        ])
        summary, trend = overall_health(features, subs, pd.Timestamp("2026-03-01"))
        self.assertEqual(summary["total_customers"], 3)
        self.assertEqual(summary["churned_customers"], 1)
        self.assertEqual(summary["at_risk_customers"], 1)
        self.assertAlmostEqual(summary["customer_churn_rate"], 1 / 3)
        self.assertGreater(len(trend), 0)


class TestRevenueAtRisk(unittest.TestCase):
    def test_at_risk_mrr_sums_at_risk_current_mrr(self) -> None:
        # Need 4+ customers for qcut into value tiers.
        features = _features([
            {"customer_id": "A", "at_risk_flag": 1, "current_mrr": 500, "avg_monthly_revenue": 500},
            {"customer_id": "B", "at_risk_flag": 0, "current_mrr": 200, "avg_monthly_revenue": 200},
            {"customer_id": "C", "at_risk_flag": 0, "current_mrr": 800, "avg_monthly_revenue": 800},
            {"customer_id": "D", "at_risk_flag": 0, "current_mrr": 1200, "avg_monthly_revenue": 1200},
        ])
        summary, _, _ = revenue_at_risk(features)
        self.assertEqual(summary["at_risk_mrr"], 500.0)


class TestInterventionPriorities(unittest.TestCase):
    def test_payment_rescue_targets_payment_failures(self) -> None:
        features = _features([
            {"customer_id": "A", "payment_failure_flag": 1, "current_mrr": 300, "churn_flag": 1, "avg_monthly_revenue": 300},
            {"customer_id": "B", "payment_failure_flag": 1, "current_mrr": 500, "churn_flag": 0, "avg_monthly_revenue": 500},
        ])
        out = intervention_priorities(features)
        rescue = out[out["opportunity"] == "Payment Rescue"].iloc[0]
        # Only the non-churned customer is "recoverable"
        self.assertEqual(rescue["recoverable_customers"], 1)
        self.assertEqual(rescue["recoverable_mrr"], 500.0)


class TestCohortMovement(unittest.TestCase):
    def test_label_reflects_delta(self) -> None:
        # Build a synthetic cohort table with 12 cohorts at age 6, retention rising over time
        cohorts = []
        for i in range(12):
            month = pd.Timestamp("2024-01-01") + pd.DateOffset(months=i)
            cohorts.append({
                "cohort_month": month,
                "observation_month": month + pd.DateOffset(months=6),
                "active_customers": 100,
                "retained_customers": 80 + i,  # rising trend
                "retention_rate": (80 + i) / 100,
                "revenue_retention": (80 + i) / 100,
            })
        result = cohort_movement(pd.DataFrame(cohorts))
        self.assertEqual(result["cohort_trend_label"], "improving")


if __name__ == "__main__":
    unittest.main()
