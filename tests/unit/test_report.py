"""Unit tests for the pure numeric-formatting helpers in the report builder.

The PDF assembly itself (layout, charts, reportlab canvas) is exercised by the
integration suite via `make release` and CI's generated-artifact diff, not
unit-tested here — see pyproject.toml's coverage `omit` list.
"""

from __future__ import annotations

import unittest

import pandas as pd

from churn.common import processed_dir
from churn.report import floor_multiple, load_metrics, usd


class TestUsd(unittest.TestCase):
    def test_plain_dollar_format(self) -> None:
        self.assertEqual(usd(1234.9), "$1,235")
        self.assertEqual(usd(0), "$0")
        self.assertEqual(usd(-130.5), "-$130")

    def test_thousands_abbreviation(self) -> None:
        self.assertEqual(usd(190640.98, k=True), "$191k")
        self.assertEqual(usd(999, k=True), "$1k")


class TestFloorMultiple(unittest.TestCase):
    def test_spells_out_single_digit_floor(self) -> None:
        # A "more than N times" claim must floor, not round: 7.7x is not "more
        # than 8 times" -- this guards the regression fixed in this report.
        self.assertEqual(floor_multiple(7.7), "seven")

    def test_two_digit_values_use_numerals(self) -> None:
        self.assertEqual(floor_multiple(25.7), "25")
        self.assertEqual(floor_multiple(30.2), "30")

    def test_boundary_values(self) -> None:
        self.assertEqual(floor_multiple(9.99), "nine")
        self.assertEqual(floor_multiple(10.0), "10")
        self.assertEqual(floor_multiple(0.5), "zero")


class TestReportMetrics(unittest.TestCase):
    def test_snapshot_mrr_and_revenue_loss_use_canonical_denominators(self) -> None:
        features = pd.read_csv(processed_dir() / "customer_retention_features.csv")
        metrics = load_metrics()

        expected_active_mrr = features.loc[features["churn_flag"] == 0, "current_mrr"].sum()
        expected_loss_share = (
            features.loc[features["churn_flag"] == 1, "avg_monthly_revenue"].sum()
            / features["avg_monthly_revenue"].sum()
            * 100
        )

        self.assertAlmostEqual(metrics["active_mrr"], expected_active_mrr, places=6)
        self.assertAlmostEqual(metrics["cum_revenue_loss_pct"], expected_loss_share, places=6)
        self.assertGreater(metrics["model_roc_auc"], 0.70)
        self.assertGreater(metrics["average_monthly_nrr"], 0)
        self.assertEqual(
            metrics["experiment_eligible"],
            metrics["experiment_treatment"] + metrics["experiment_control"],
        )


if __name__ == "__main__":
    unittest.main()
