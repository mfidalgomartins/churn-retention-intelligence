"""Edge-case unit tests for feature engineering."""
from __future__ import annotations

import unittest

import pandas as pd

from churn.features import (
    build_cohort_table,
    build_customer_features,
    customer_observation_dates,
    payment_aggregates,
    usage_aggregates,
)

SNAPSHOT = pd.Timestamp("2026-03-01")


def _empty_usage() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "customer_id",
            "usage_date",
            "sessions",
            "support_tickets",
            "feature_adoption_score",
            "nps_score",
        ]
    ).astype({"usage_date": "datetime64[ns]"})


def _empty_payments() -> pd.DataFrame:
    return pd.DataFrame(
        columns=["customer_id", "payment_date", "amount", "payment_status"]
    ).astype({"payment_date": "datetime64[ns]"})


class TestObservationDateEdgeCases(unittest.TestCase):
    def test_churn_dates_after_snapshot_are_clipped_to_snapshot(self) -> None:
        subscriptions = pd.DataFrame(
            [
                {
                    "customer_id": "A",
                    "subscription_end_date": pd.Timestamp("2026-04-15"),
                },
                {"customer_id": "B", "subscription_end_date": pd.NaT},
            ]
        )

        result = customer_observation_dates(subscriptions, SNAPSHOT)

        self.assertEqual(
            result.set_index("customer_id")["observation_date"].to_dict(),
            {"A": SNAPSHOT, "B": SNAPSHOT},
        )


class TestNoEventFeatureDefaults(unittest.TestCase):
    def test_customer_features_default_cleanly_when_usage_and_payments_are_empty(self) -> None:
        customers = pd.DataFrame(
            [
                {
                    "customer_id": "A",
                    "signup_date": pd.Timestamp("2025-01-01"),
                    "segment": "SMB",
                    "region": "EU",
                    "acquisition_channel": "Inbound",
                }
            ]
        )
        subscriptions = pd.DataFrame(
            [
                {
                    "customer_id": "A",
                    "subscription_start_date": pd.Timestamp("2025-01-01"),
                    "subscription_end_date": pd.NaT,
                    "status": "active",
                    "monthly_revenue": 250.0,
                    "billing_cycle": "Monthly",
                    "plan_type": "Pro",
                }
            ]
        )

        features = build_customer_features(
            customers,
            subscriptions,
            _empty_usage(),
            _empty_payments(),
            SNAPSHOT,
        )

        row = features.iloc[0]
        self.assertEqual(row["customer_id"], "A")
        self.assertEqual(row["observation_date"], "2026-03-01")
        self.assertEqual(row["avg_monthly_revenue"], 250.0)
        self.assertEqual(row["lifetime_revenue"], 0.0)
        self.assertEqual(row["recent_sessions_30d"], 0)
        self.assertEqual(row["recent_sessions_90d"], 0)
        self.assertEqual(row["usage_trend"], 0.0)
        self.assertEqual(row["feature_adoption_score_recent"], 0.0)
        self.assertEqual(row["support_tickets_30d"], 0)
        self.assertEqual(row["support_tickets_90d"], 0)
        self.assertEqual(row["nps_score_recent"], 0.0)
        self.assertEqual(row["failed_payments_90d"], 0)
        self.assertEqual(row["payment_failure_flag"], 0)


class TestUsageAggregationEdgeCases(unittest.TestCase):
    def test_empty_usage_returns_empty_aggregate_with_expected_columns(self) -> None:
        observation_dates = pd.DataFrame(
            [{"customer_id": "A", "observation_date": SNAPSHOT}]
        )

        result = usage_aggregates(_empty_usage(), observation_dates)

        self.assertTrue(result.empty)
        self.assertEqual(
            list(result.columns),
            [
                "customer_id",
                "recent_sessions_30d",
                "recent_sessions_90d",
                "usage_trend",
                "feature_adoption_score_recent",
                "support_tickets_30d",
                "support_tickets_90d",
                "nps_score_recent",
            ],
        )

    def test_future_usage_events_are_excluded_from_all_windows(self) -> None:
        usage = pd.DataFrame(
            [
                {
                    "customer_id": "A",
                    "usage_date": pd.Timestamp("2026-03-02"),
                    "sessions": 50,
                    "support_tickets": 3,
                    "feature_adoption_score": 90,
                    "nps_score": 80,
                }
            ]
        )
        observation_dates = pd.DataFrame(
            [{"customer_id": "A", "observation_date": SNAPSHOT}]
        )

        result = usage_aggregates(usage, observation_dates)

        self.assertTrue(result.empty)


class TestPaymentAggregationEdgeCases(unittest.TestCase):
    def test_customer_with_only_failed_payments_has_no_revenue_and_failure_flag(self) -> None:
        subscriptions = pd.DataFrame(
            [{"customer_id": "A", "billing_cycle": "Monthly"}]
        )
        payments = pd.DataFrame(
            [
                {
                    "customer_id": "A",
                    "payment_date": pd.Timestamp("2026-02-20"),
                    "amount": 100.0,
                    "payment_status": "failed",
                }
            ]
        )
        observation_dates = pd.DataFrame(
            [{"customer_id": "A", "observation_date": SNAPSHOT}]
        )

        result = payment_aggregates(payments, subscriptions, observation_dates)
        row = result.iloc[0]

        self.assertEqual(row["lifetime_revenue"], 0.0)
        self.assertTrue(pd.isna(row["avg_monthly_revenue_calc"]))
        self.assertEqual(row["failed_payments_90d"], 1)
        self.assertEqual(row["payment_failure_flag"], 1)

    def test_unknown_billing_cycle_falls_back_to_monthly_equivalent(self) -> None:
        subscriptions = pd.DataFrame(
            [{"customer_id": "A", "billing_cycle": "Custom"}]
        )
        payments = pd.DataFrame(
            [
                {
                    "customer_id": "A",
                    "payment_date": pd.Timestamp("2026-02-01"),
                    "amount": 300.0,
                    "payment_status": "paid",
                }
            ]
        )
        observation_dates = pd.DataFrame(
            [{"customer_id": "A", "observation_date": SNAPSHOT}]
        )

        result = payment_aggregates(payments, subscriptions, observation_dates)
        row = result.iloc[0]

        self.assertEqual(row["lifetime_revenue"], 300.0)
        self.assertEqual(row["avg_monthly_revenue_calc"], 300.0)
        self.assertEqual(row["failed_payments_90d"], 0)
        self.assertEqual(row["payment_failure_flag"], 0)


class TestCohortAggregationEdgeCases(unittest.TestCase):
    def test_zero_mrr_cohort_uses_zero_revenue_retention(self) -> None:
        subscriptions = pd.DataFrame(
            [
                {
                    "customer_id": "A",
                    "subscription_start_date": pd.Timestamp("2025-01-10"),
                    "subscription_end_date": pd.NaT,
                    "monthly_revenue": 0.0,
                },
                {
                    "customer_id": "B",
                    "subscription_start_date": pd.Timestamp("2025-01-20"),
                    "subscription_end_date": pd.NaT,
                    "monthly_revenue": 0.0,
                },
            ]
        )

        cohort = build_cohort_table(subscriptions, SNAPSHOT)
        first_month = cohort.iloc[0]

        self.assertEqual(first_month["active_customers"], 2)
        self.assertEqual(first_month["retained_customers"], 2)
        self.assertEqual(first_month["retention_rate"], 1.0)
        self.assertEqual(first_month["revenue_retention"], 0.0)


if __name__ == "__main__":
    unittest.main()
