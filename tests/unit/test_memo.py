"""Tests for the generated executive decision memo."""

from __future__ import annotations

import unittest

from churn.memo import MemoMetrics, MemoPlay, render_memo


class TestDecisionMemo(unittest.TestCase):
    def test_render_uses_supplied_metrics_and_labels_proxy(self) -> None:
        metrics = MemoMetrics(
            snapshot_date="2026-03-01",
            total_customers=100,
            active_customers=80,
            cumulative_churn_share=0.20,
            cumulative_revenue_loss_share=0.10,
            at_risk_mrr=10_000,
            current_mrr_exposure=12_500,
            avg_6m_retention=0.90,
            avg_6m_revenue_retention=0.95,
            cohort_trend="mixed/stable",
            top_dimensions=(("Segment", "SMB", 0.30),),
            strongest_relationship="low NPS",
            strongest_relationship_lift=4.2,
            critical_customers=3,
            high_customers=7,
            critical_high_mrr=25_000,
            plays=(
                MemoPlay(
                    name="Payment Rescue",
                    candidates=5,
                    current_mrr_scope=8_000,
                    weighted_exposure=4_000,
                    action="Refresh payment method",
                ),
            ),
            model_roc_auc=0.80,
            model_average_precision=0.30,
            model_brier_score=0.04,
            average_monthly_nrr=0.99,
            gross_margin_rate=0.75,
            blended_cac=900,
            experiment_eligible=20,
            treatment_customers=10,
            holdout_customers=10,
            simulated_saved_mrr=5_000,
            simulated_saved_mrr_ci_lower=1_000,
            simulated_saved_mrr_ci_upper=9_000,
            monitoring_alerts=0,
        )

        memo = render_memo(metrics)

        self.assertIn("20.0%", memo)
        self.assertIn("$25,000", memo)
        self.assertIn("Payment Rescue", memo)
        self.assertIn("not expected saved revenue or ROI", memo)
        self.assertIn("Out-of-time ROC AUC is 0.800", memo)
        self.assertIn("synthetic outcome demonstration", memo)
        self.assertIn("incremental saved-MRR estimate", memo)


if __name__ == "__main__":
    unittest.main()
