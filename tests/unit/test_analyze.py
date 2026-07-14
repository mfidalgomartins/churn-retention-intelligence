"""Unit tests for the churn analysis layer."""

from __future__ import annotations

import unittest

import pandas as pd

from churn.analyze import (
    assign_intervention_plays,
    behavioral_relationships,
    churn_by_dimension,
    cohort_movement,
    intervention_priorities,
    monthly_retention_trend,
    overall_health,
    rank_drivers,
    revenue_at_risk,
    structured_findings,
)


def _features(rows: list[dict]) -> pd.DataFrame:
    """Build a feature frame with sane defaults."""
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
    return pd.DataFrame([{**defaults, **r} for r in rows])


class TestChurnByDimension(unittest.TestCase):
    def test_churn_rate_per_segment(self) -> None:
        features = _features(
            [
                {"customer_id": "A", "segment": "SMB", "churn_flag": 1, "avg_monthly_revenue": 100},
                {"customer_id": "B", "segment": "SMB", "churn_flag": 0, "avg_monthly_revenue": 100},
                {
                    "customer_id": "C",
                    "segment": "Enterprise",
                    "churn_flag": 0,
                    "avg_monthly_revenue": 1000,
                },
            ]
        )
        out = churn_by_dimension(features, "segment")
        smb = out[out["segment"] == "SMB"].iloc[0]
        ent = out[out["segment"] == "Enterprise"].iloc[0]
        self.assertEqual(smb["cumulative_churn_share"], 0.5)
        self.assertEqual(ent["cumulative_churn_share"], 0.0)
        self.assertEqual(smb["churned_revenue"], 100.0)

    def test_result_sorted_by_churn_rate_descending(self) -> None:
        features = _features(
            [
                {"customer_id": "A", "segment": "Low", "churn_flag": 0},
                {"customer_id": "B", "segment": "High", "churn_flag": 1},
            ]
        )
        out = churn_by_dimension(features, "segment")
        self.assertEqual(out.iloc[0]["segment"], "High")


class TestBehavioralRelationships(unittest.TestCase):
    def test_lift_is_in_group_over_out_group(self) -> None:
        # 5 customers; 2 with failed_payment, both churned. 3 without, 0 churned.
        features = _features(
            [{"customer_id": str(i), "payment_failure_flag": 1, "churn_flag": 1} for i in range(2)]
            + [
                {"customer_id": str(i + 10), "payment_failure_flag": 0, "churn_flag": 0}
                for i in range(3)
            ]
        )
        rels, _ = behavioral_relationships(features)
        fp = rels[rels["relationship"] == "failed_payment_flag"].iloc[0]
        self.assertEqual(fp["churn_rate_in_group"], 1.0)
        self.assertEqual(fp["churn_rate_out_group"], 0.0)


class TestRankDrivers(unittest.TestCase):
    def test_drivers_below_baseline_are_excluded(self) -> None:
        # Baseline churn = 0.5. Only drivers with > 0.5 churn should appear.
        features = _features(
            [
                {"customer_id": "A", "churn_flag": 1, "payment_failure_flag": 1},
                {"customer_id": "B", "churn_flag": 0, "payment_failure_flag": 0},
            ]
        )
        _rels, thresholds = behavioral_relationships(features)
        out = rank_drivers(features, thresholds)
        # All drivers in ranking should have churn_rate > baseline
        self.assertTrue((out["churn_rate"] > out["baseline_churn_rate"]).all())


class TestOverallHealth(unittest.TestCase):
    def test_summary_has_required_keys(self) -> None:
        features = _features(
            [
                {"customer_id": "A", "churn_flag": 1, "avg_monthly_revenue": 100, "current_mrr": 0},
                {
                    "customer_id": "B",
                    "churn_flag": 0,
                    "at_risk_flag": 1,
                    "current_mrr": 200,
                    "avg_monthly_revenue": 200,
                },
                {
                    "customer_id": "C",
                    "churn_flag": 0,
                    "current_mrr": 300,
                    "avg_monthly_revenue": 300,
                },
            ]
        )
        subs = pd.DataFrame(
            [
                {
                    "customer_id": "A",
                    "subscription_start_date": pd.Timestamp("2024-01-01"),
                    "subscription_end_date": pd.Timestamp("2025-06-01"),
                    "monthly_revenue": 100,
                },
                {
                    "customer_id": "B",
                    "subscription_start_date": pd.Timestamp("2024-01-01"),
                    "subscription_end_date": pd.NaT,
                    "monthly_revenue": 200,
                },
                {
                    "customer_id": "C",
                    "subscription_start_date": pd.Timestamp("2024-01-01"),
                    "subscription_end_date": pd.NaT,
                    "monthly_revenue": 300,
                },
            ]
        )
        summary, trend = overall_health(features, subs, pd.Timestamp("2026-03-01"))
        self.assertEqual(summary["total_customers"], 3)
        self.assertEqual(summary["churned_customers"], 1)
        self.assertEqual(summary["at_risk_customers"], 1)
        self.assertAlmostEqual(summary["cumulative_customer_churn_share"], 1 / 3)
        self.assertGreater(len(trend), 0)

    def test_monthly_trend_excludes_partial_snapshot_month(self) -> None:
        subs = pd.DataFrame(
            [
                {
                    "customer_id": "A",
                    "subscription_start_date": pd.Timestamp("2025-01-01"),
                    "subscription_end_date": pd.NaT,
                    "monthly_revenue": 100,
                },
            ]
        )
        trend = monthly_retention_trend(subs, pd.Timestamp("2026-03-01"))
        self.assertEqual(trend.iloc[-1]["month"], "2026-02-01")


class TestRevenueAtRisk(unittest.TestCase):
    def test_at_risk_mrr_sums_at_risk_current_mrr(self) -> None:
        # Need 4+ customers for qcut into value tiers.
        features = _features(
            [
                {
                    "customer_id": "A",
                    "at_risk_flag": 1,
                    "current_mrr": 500,
                    "avg_monthly_revenue": 500,
                },
                {
                    "customer_id": "B",
                    "at_risk_flag": 0,
                    "current_mrr": 200,
                    "avg_monthly_revenue": 200,
                },
                {
                    "customer_id": "C",
                    "at_risk_flag": 0,
                    "current_mrr": 800,
                    "avg_monthly_revenue": 800,
                },
                {
                    "customer_id": "D",
                    "at_risk_flag": 0,
                    "current_mrr": 1200,
                    "avg_monthly_revenue": 1200,
                },
            ]
        )
        summary, _, _ = revenue_at_risk(features)
        self.assertEqual(summary["at_risk_mrr"], 500.0)


class TestInterventionPriorities(unittest.TestCase):
    def test_payment_rescue_targets_payment_failures(self) -> None:
        features = _features(
            [
                {
                    "customer_id": "A",
                    "payment_failure_flag": 1,
                    "current_mrr": 300,
                    "churn_flag": 1,
                    "avg_monthly_revenue": 300,
                },
                {
                    "customer_id": "B",
                    "payment_failure_flag": 1,
                    "current_mrr": 500,
                    "churn_flag": 0,
                    "avg_monthly_revenue": 500,
                },
            ]
        )
        out = intervention_priorities(features)
        rescue = out[out["opportunity"] == "Payment Rescue"].iloc[0]
        # Only the non-churned customer belongs in the intervention queue.
        self.assertEqual(rescue["candidate_customers"], 1)
        self.assertEqual(rescue["current_mrr_scope"], 500.0)

    def test_each_account_receives_at_most_one_play(self) -> None:
        features = _features(
            [
                {
                    "customer_id": "A",
                    "payment_failure_flag": 1,
                    "renewal_near_flag": 1,
                    "usage_trend": -5,
                    "feature_adoption_score_recent": 10,
                    "support_tickets_90d": 20,
                    "nps_score_recent": -30,
                    "at_risk_flag": 1,
                },
                {
                    "customer_id": "B",
                    "renewal_near_flag": 1,
                    "usage_trend": -5,
                    "feature_adoption_score_recent": 10,
                    "support_tickets_90d": 20,
                    "nps_score_recent": -30,
                    "at_risk_flag": 1,
                },
            ]
        )

        assignments = assign_intervention_plays(features)

        self.assertEqual(assignments.loc[0], "Payment Rescue")
        self.assertEqual(assignments.loc[1], "Service Recovery")
        self.assertEqual(assignments.notna().sum(), 2)


class TestCohortMovement(unittest.TestCase):
    def test_label_reflects_delta(self) -> None:
        # Build a synthetic cohort table with 12 cohorts at age 6, retention rising over time
        cohorts = []
        for i in range(12):
            month = pd.Timestamp("2024-01-01") + pd.DateOffset(months=i)
            cohorts.append(
                {
                    "cohort_month": month,
                    "observation_month": month + pd.DateOffset(months=6),
                    "active_customers": 100,
                    "retained_customers": 80 + i,  # rising trend
                    "retention_rate": (80 + i) / 100,
                    "revenue_retention": (80 + i) / 100,
                }
            )
        result = cohort_movement(pd.DataFrame(cohorts))
        self.assertEqual(result["cohort_trend_label"], "improving")

    def _cohort_table(self, retained_by_age6) -> pd.DataFrame:
        rows = []
        for i, retained in enumerate(retained_by_age6):
            month = pd.Timestamp("2024-01-01") + pd.DateOffset(months=i)
            rows.append(
                {
                    "cohort_month": month,
                    "observation_month": month + pd.DateOffset(months=6),
                    "active_customers": 100,
                    "retained_customers": retained,
                    "retention_rate": retained / 100,
                    "revenue_retention": retained / 100,
                }
            )
        return pd.DataFrame(rows)

    def test_label_deteriorating_when_recent_cohorts_retain_worse(self) -> None:
        result = cohort_movement(self._cohort_table([90 - i for i in range(12)]))
        self.assertEqual(result["cohort_trend_label"], "deteriorating")

    def test_label_stable_when_delta_within_band(self) -> None:
        result = cohort_movement(self._cohort_table([85] * 12))
        self.assertEqual(result["cohort_trend_label"], "mixed/stable")


class TestRankDriversDimension(unittest.TestCase):
    def test_dimension_value_above_baseline_becomes_driver(self) -> None:
        # Baseline churn 0.5; the "Bad" segment churns at 1.0 → must surface as a driver.
        features = _features(
            [{"customer_id": f"bad{i}", "segment": "Bad", "churn_flag": 1} for i in range(2)]
            + [{"customer_id": f"ok{i}", "segment": "Good", "churn_flag": 0} for i in range(2)]
        )
        _rels, thresholds = behavioral_relationships(features)
        out = rank_drivers(features, thresholds)
        self.assertIn("segment=Bad", set(out["driver"]))
        self.assertNotIn("segment=Good", set(out["driver"]))


class TestStructuredFindings(unittest.TestCase):
    def _scenario(self):
        # Varied feature frame: ≥4 distinct revenues for qcut, mixed churn and dimensions.
        features = _features(
            [
                {
                    "customer_id": "A",
                    "segment": "Startup",
                    "region": "LATAM",
                    "acquisition_channel": "Affiliate",
                    "plan_type": "Basic",
                    "churn_flag": 1,
                    "current_mrr": 0,
                    "avg_monthly_revenue": 100,
                    "nps_score_recent": 5,
                    "usage_trend": -2.0,
                    "payment_failure_flag": 1,
                    "failed_payments_90d": 2,
                },
                {
                    "customer_id": "B",
                    "segment": "SMB",
                    "region": "Europe",
                    "acquisition_channel": "Organic",
                    "plan_type": "Growth",
                    "churn_flag": 0,
                    "at_risk_flag": 1,
                    "current_mrr": 400,
                    "avg_monthly_revenue": 400,
                    "renewal_near_flag": 1,
                    "usage_trend": -1.0,
                    "nps_score_recent": 10,
                },
                {
                    "customer_id": "C",
                    "segment": "Mid-Market",
                    "region": "NA",
                    "acquisition_channel": "Sales",
                    "plan_type": "Enterprise",
                    "churn_flag": 0,
                    "current_mrr": 900,
                    "avg_monthly_revenue": 900,
                },
                {
                    "customer_id": "D",
                    "segment": "SMB",
                    "region": "APAC",
                    "acquisition_channel": "Paid",
                    "plan_type": "Growth",
                    "churn_flag": 0,
                    "current_mrr": 1500,
                    "avg_monthly_revenue": 1500,
                },
            ]
        )
        # 120 long-lived subscriptions so the trend has ≥3 mature months (slope branch).
        subs = pd.DataFrame(
            [
                {
                    "customer_id": f"S{i}",
                    "subscription_start_date": pd.Timestamp("2024-01-01"),
                    "subscription_end_date": pd.NaT,
                    "monthly_revenue": 200.0,
                }
                for i in range(120)
            ]
        )
        cohorts = []
        for i in range(12):
            month = pd.Timestamp("2024-01-01") + pd.DateOffset(months=i)
            cohorts.append(
                {
                    "cohort_month": month,
                    "observation_month": month + pd.DateOffset(months=6),
                    "active_customers": 100,
                    "retained_customers": 80,
                    "retention_rate": 0.80,
                    "revenue_retention": 0.82,
                }
            )
        return features, subs, pd.DataFrame(cohorts)

    def test_findings_cover_all_five_sections(self) -> None:
        features, subs, cohort = self._scenario()
        import math

        overall, _trend = overall_health(features, subs, pd.Timestamp("2026-03-01"))
        # ≥100 active customers across ≥12 months exercises the polyfit slope branch.
        self.assertTrue(math.isfinite(overall["customer_churn_slope_last12"]))
        cohort_result = cohort_movement(cohort)
        seg = churn_by_dimension(features, "segment")
        reg = churn_by_dimension(features, "region")
        chn = churn_by_dimension(features, "acquisition_channel")
        pln = churn_by_dimension(features, "plan_type")
        relationships, _ = behavioral_relationships(features)
        risk_summary, seg_loss, tier_stats = revenue_at_risk(features)
        plays = intervention_priorities(features)

        findings = structured_findings(
            overall,
            cohort_result,
            seg.iloc[0],
            reg.iloc[0],
            chn.iloc[0],
            pln.iloc[0],
            relationships,
            risk_summary,
            seg_loss,
            tier_stats,
            plays,
        )
        self.assertEqual(len(findings), 5)
        self.assertEqual(findings.iloc[0]["section"], "1. Overall Retention Health")
        # Formatters ran: percentages and money rendered into the result text.
        self.assertTrue(any("%" in r for r in findings["result"]))
        self.assertTrue(any("$" in r for r in findings["result"]))


if __name__ == "__main__":
    unittest.main()
