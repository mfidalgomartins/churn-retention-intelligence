"""Tests for the synthetic data-generating process."""

from __future__ import annotations

import unittest

import numpy as np

from churn.analyze import monthly_retention_trend
from churn.common import REFERENCE_DATE
from churn.generate import generate_customers, generate_subscriptions


class TestSubscriptionSimulation(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        rng = np.random.default_rng(42)
        cls.customers = generate_customers(rng)
        cls.subscriptions = generate_subscriptions(cls.customers, rng)

    def test_event_dates_respect_subscription_window(self) -> None:
        churned = self.subscriptions[self.subscriptions["status"] == "churned"]

        self.assertGreater(len(churned), 0)
        self.assertTrue(
            (churned["subscription_end_date"] >= churned["subscription_start_date"]).all()
        )
        self.assertTrue((churned["subscription_end_date"] <= REFERENCE_DATE).all())

    def test_churn_is_not_artificially_concentrated_at_snapshot(self) -> None:
        trend = monthly_retention_trend(self.subscriptions, REFERENCE_DATE)
        mature = trend[trend["active_customers_start"] >= 100]
        positive_month_share = (mature["churned_customers"] > 0).mean()
        median_rate = mature["customer_churn_rate"].median()
        latest_rate = mature.iloc[-1]["customer_churn_rate"]

        self.assertGreaterEqual(positive_month_share, 0.90)
        self.assertLessEqual(latest_rate, median_rate * 3.0)

    def test_commercial_risk_differentiates_churn_outcomes(self) -> None:
        result = self.customers[["customer_id", "segment"]].merge(
            self.subscriptions[["customer_id", "status"]],
            on="customer_id",
            validate="one_to_one",
        )
        rates = (
            result.assign(churned=result["status"].eq("churned"))
            .groupby("segment")["churned"]
            .mean()
        )

        self.assertGreater(rates["Startup"], rates["Enterprise"])


if __name__ == "__main__":
    unittest.main()
