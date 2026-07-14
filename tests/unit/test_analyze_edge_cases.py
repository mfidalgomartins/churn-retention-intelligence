"""Edge-case unit tests for reusable churn analysis functions."""

from __future__ import annotations

import unittest

import pandas as pd

from churn.analyze import (
    behavioral_relationships,
    cohort_movement,
    intervention_priorities,
    monthly_dimensional_trend,
    monthly_retention_trend,
    revenue_at_risk,
)


def _features(rows: list[dict]) -> pd.DataFrame:
    defaults = {
        "customer_id": "X",
        "segment": "SMB",
        "region": "Europe",
        "acquisition_channel": "Organic",
        "plan_type": "Growth",
        "tenure_days": 400,
        "current_mrr": 200.0,
        "avg_monthly_revenue": 200.0,
        "lifetime_revenue": 4000.0,
        "recent_sessions_30d": 20,
        "recent_sessions_90d": 60,
        "usage_trend": 1.0,
        "feature_adoption_score_recent": 70.0,
        "support_tickets_30d": 1,
        "support_tickets_90d": 2,
        "nps_score_recent": 30.0,
        "failed_payments_90d": 0,
        "payment_failure_flag": 0,
        "renewal_near_flag": 0,
        "churn_flag": 0,
        "at_risk_flag": 0,
    }
    return pd.DataFrame([{**defaults, **row} for row in rows])


class TestMonthlyRetentionTrendEdgeCases(unittest.TestCase):
    def test_zero_active_mrr_returns_nan_revenue_churn(self) -> None:
        subs = pd.DataFrame(
            [
                {
                    "customer_id": "A",
                    "subscription_start_date": pd.Timestamp("2025-01-01"),
                    "subscription_end_date": pd.Timestamp("2025-01-31"),
                    "monthly_revenue": 0.0,
                },
                {
                    "customer_id": "B",
                    "subscription_start_date": pd.Timestamp("2025-02-01"),
                    "subscription_end_date": pd.NaT,
                    "monthly_revenue": 100.0,
                },
            ]
        )

        trend = monthly_retention_trend(subs, pd.Timestamp("2025-03-15"))
        jan = trend[trend["month"] == "2025-01-01"].iloc[0]

        self.assertEqual(jan["active_customers_start"], 1)
        self.assertEqual(jan["churned_customers"], 1)
        self.assertEqual(jan["customer_churn_rate"], 1.0)
        self.assertTrue(pd.isna(jan["revenue_churn_rate"]))

    def test_month_start_churn_counts_in_same_month(self) -> None:
        subs = pd.DataFrame(
            [
                {
                    "customer_id": "A",
                    "subscription_start_date": pd.Timestamp("2025-01-01"),
                    "subscription_end_date": pd.Timestamp("2025-02-01"),
                    "monthly_revenue": 100.0,
                },
            ]
        )

        trend = monthly_retention_trend(subs, pd.Timestamp("2025-03-20"))
        feb = trend[trend["month"] == "2025-02-01"].iloc[0]

        self.assertEqual(feb["active_customers_start"], 1)
        self.assertEqual(feb["churned_customers"], 1)
        self.assertEqual(feb["retention_rate"], 0.0)

    def test_same_month_acquisition_and_churn_is_excluded_from_opening_base_rate(self) -> None:
        subs = pd.DataFrame(
            [
                {
                    "customer_id": "A",
                    "subscription_start_date": pd.Timestamp("2025-01-15"),
                    "subscription_end_date": pd.Timestamp("2025-01-20"),
                    "monthly_revenue": 100.0,
                },
            ]
        )

        trend = monthly_retention_trend(subs, pd.Timestamp("2025-02-15"))
        jan = trend[trend["month"] == "2025-01-01"].iloc[0]

        self.assertEqual(jan["active_customers_start"], 0)
        self.assertEqual(jan["churned_customers"], 0)
        self.assertEqual(jan["churned_mrr"], 0.0)
        self.assertTrue(pd.isna(jan["customer_churn_rate"]))


class TestMonthlyDimensionalTrendEdgeCases(unittest.TestCase):
    def test_same_month_start_and_churn_is_excluded_from_opening_base(self) -> None:
        features = _features(
            [
                {"customer_id": "A", "segment": "Enterprise", "current_mrr": 100.0},
            ]
        )
        subs = pd.DataFrame(
            [
                {
                    "customer_id": "A",
                    "subscription_start_date": pd.Timestamp("2025-01-15"),
                    "subscription_end_date": pd.Timestamp("2025-01-20"),
                    "monthly_revenue": 100.0,
                },
            ]
        )

        out = monthly_dimensional_trend(features, subs, pd.Timestamp("2025-02-15"))

        self.assertTrue(out.empty)
        self.assertEqual(
            list(out.columns),
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
            ],
        )


class TestBehavioralRelationshipsEdgeCases(unittest.TestCase):
    def test_all_customers_in_signal_group_has_nan_out_group_metrics(self) -> None:
        features = _features(
            [
                {"customer_id": "A", "payment_failure_flag": 1, "churn_flag": 1},
                {"customer_id": "B", "payment_failure_flag": 1, "churn_flag": 0},
                {"customer_id": "C", "payment_failure_flag": 1, "churn_flag": 0},
            ]
        )

        relationships, _ = behavioral_relationships(features)
        failed_payment = relationships[relationships["relationship"] == "failed_payment_flag"].iloc[
            0
        ]

        self.assertEqual(failed_payment["customers_in_group"], 3)
        self.assertEqual(failed_payment["share_of_base"], 1.0)
        self.assertEqual(failed_payment["churn_rate_in_group"], 0.333333)
        self.assertTrue(pd.isna(failed_payment["churn_rate_out_group"]))
        self.assertTrue(pd.isna(failed_payment["churn_rate_lift"]))
        self.assertTrue(pd.isna(failed_payment["churn_rate_delta_pp"]))


class TestCohortMovementEdgeCases(unittest.TestCase):
    def test_no_six_month_cohorts_returns_stable_nan_averages(self) -> None:
        cohort = pd.DataFrame(
            [
                {
                    "cohort_month": pd.Timestamp("2025-01-01"),
                    "observation_month": pd.Timestamp("2025-04-01"),
                    "active_customers": 100,
                    "retained_customers": 80,
                    "retention_rate": 0.8,
                    "revenue_retention": 0.9,
                },
            ]
        )

        result = cohort_movement(cohort)

        self.assertEqual(result["cohort_count"], 1)
        self.assertEqual(result["mature_cohort_count_at_6m"], 0)
        self.assertTrue(pd.isna(result["avg_6m_retention"]))
        self.assertTrue(pd.isna(result["avg_6m_revenue_retention"]))
        self.assertEqual(result["cohort_trend_label"], "mixed/stable")


class TestRevenueAtRiskEdgeCases(unittest.TestCase):
    def test_hidden_risk_excludes_churned_and_explicitly_at_risk_customers(self) -> None:
        features = _features(
            [
                {
                    "customer_id": "A",
                    "payment_failure_flag": 1,
                    "current_mrr": 300.0,
                    "avg_monthly_revenue": 100.0,
                },
                {
                    "customer_id": "B",
                    "payment_failure_flag": 1,
                    "churn_flag": 1,
                    "current_mrr": 400.0,
                    "avg_monthly_revenue": 200.0,
                },
                {
                    "customer_id": "C",
                    "payment_failure_flag": 1,
                    "at_risk_flag": 1,
                    "current_mrr": 500.0,
                    "avg_monthly_revenue": 300.0,
                },
                {"customer_id": "D", "current_mrr": 600.0, "avg_monthly_revenue": 400.0},
            ]
        )

        summary, _, _ = revenue_at_risk(features)

        self.assertEqual(summary["behavioral_signal_mrr"], 300.0)
        self.assertEqual(summary["at_risk_mrr"], 500.0)
        self.assertEqual(summary["current_mrr_exposure"], 800.0)


class TestInterventionPrioritiesEdgeCases(unittest.TestCase):
    def test_zero_candidate_play_keeps_numeric_zeroes_and_na_focus(self) -> None:
        features = _features(
            [
                {
                    "customer_id": "A",
                    "payment_failure_flag": 1,
                    "churn_flag": 1,
                    "current_mrr": 300.0,
                },
                {
                    "customer_id": "B",
                    "payment_failure_flag": 0,
                    "churn_flag": 0,
                    "current_mrr": 500.0,
                },
            ]
        )

        out = intervention_priorities(features)
        payment_rescue = out[out["opportunity"] == "Payment Rescue"].iloc[0]

        self.assertEqual(payment_rescue["candidate_customers"], 0)
        self.assertEqual(payment_rescue["current_mrr_scope"], 0.0)
        self.assertEqual(payment_rescue["mrr_exposure_proxy"], 0.0)
        self.assertEqual(payment_rescue["segment_focus"], "n/a")


if __name__ == "__main__":
    unittest.main()
