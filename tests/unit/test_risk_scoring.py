"""Unit tests for risk scoring."""

from __future__ import annotations

import contextlib
import io
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import pandas as pd

from churn import risk
from churn.risk import (
    SCORE_OUTPUT_COLUMNS,
    SIGNAL_WEIGHTS,
    TIER_SUMMARY_COLUMNS,
    assign_tiers,
    churn_risk_score,
    compute_scores,
    customer_value_score,
    main_risk_driver,
    normalize_signals,
    risk_tier_summary,
)


def _baseline_row(**overrides) -> dict:
    """A 'clean' customer with no risk signals."""
    base = {
        "customer_id": "C000001",
        "segment": "SMB",
        "region": "Europe",
        "acquisition_channel": "Organic",
        "plan_type": "Growth",
        "tenure_days": 720,
        "current_mrr": 200.0,
        "avg_monthly_revenue": 200.0,
        "lifetime_revenue": 4800.0,
        "usage_trend": 2.0,
        "feature_adoption_score_recent": 80.0,
        "support_tickets_30d": 0,
        "support_tickets_90d": 1,
        "nps_score_recent": 50.0,
        "recent_sessions_30d": 30,
        "recent_sessions_90d": 100,
        "failed_payments_90d": 0,
        "payment_failure_flag": 0,
        "renewal_near_flag": 0,
        "churn_flag": 0,
        "at_risk_flag": 0,
    }
    base.update(overrides)
    return base


def _frame(*rows) -> pd.DataFrame:
    return pd.DataFrame(list(rows))


class TestNormalizeSignals(unittest.TestCase):
    def test_clean_customer_has_no_signals(self) -> None:
        df = _frame(_baseline_row())
        signals = normalize_signals(df)
        for col in SIGNAL_WEIGHTS:
            self.assertEqual(signals[col].iloc[0], 0.0, f"{col} should be 0 for a clean customer")

    def test_failed_payment_saturates_at_two(self) -> None:
        df = _frame(
            _baseline_row(failed_payments_90d=0),
            _baseline_row(failed_payments_90d=1),
            _baseline_row(failed_payments_90d=3),
        )
        s = normalize_signals(df)["failed_payments"]
        self.assertEqual(s.iloc[0], 0.0)
        self.assertGreater(s.iloc[1], 0.5)
        self.assertEqual(s.iloc[2], 1.0)

    def test_usage_decline_signal_clips_at_one(self) -> None:
        df = _frame(_baseline_row(usage_trend=-10.0))
        self.assertEqual(normalize_signals(df)["usage_decline"].iloc[0], 1.0)

    def test_signals_are_in_unit_interval(self) -> None:
        df = _frame(
            _baseline_row(
                usage_trend=-50,
                failed_payments_90d=10,
                support_tickets_90d=50,
                nps_score_recent=-100,
                feature_adoption_score_recent=0,
            ),
        )
        s = normalize_signals(df)
        for col in SIGNAL_WEIGHTS:
            self.assertGreaterEqual(s[col].iloc[0], 0.0)
            self.assertLessEqual(s[col].iloc[0], 1.0)


class TestChurnRiskScore(unittest.TestCase):
    def test_clean_customer_scores_zero(self) -> None:
        df = _frame(_baseline_row())
        s = normalize_signals(df)
        self.assertEqual(churn_risk_score(df, s).iloc[0], 0.0)

    def test_score_is_bounded(self) -> None:
        df = _frame(
            _baseline_row(
                usage_trend=-30,
                failed_payments_90d=5,
                support_tickets_90d=20,
                nps_score_recent=-100,
                feature_adoption_score_recent=0,
                renewal_near_flag=1,
                recent_sessions_30d=0,
            ),
        )
        s = normalize_signals(df)
        score = churn_risk_score(df, s).iloc[0]
        self.assertGreaterEqual(score, 0.0)
        self.assertLessEqual(score, 100.0)

    def test_concentration_bonus_rewards_co_firing(self) -> None:
        single = _frame(_baseline_row(usage_trend=-10))  # only 1 signal firing strongly
        many = _frame(
            _baseline_row(
                usage_trend=-10,
                failed_payments_90d=2,
                nps_score_recent=-10,
                feature_adoption_score_recent=10,
            )
        )
        s_single = churn_risk_score(single, normalize_signals(single)).iloc[0]
        s_many = churn_risk_score(many, normalize_signals(many)).iloc[0]
        self.assertGreater(s_many, s_single + 4.0)


class TestCustomerValueScore(unittest.TestCase):
    def test_higher_mrr_gets_higher_score(self) -> None:
        df = _frame(
            _baseline_row(
                customer_id="low", current_mrr=50.0, avg_monthly_revenue=50.0, lifetime_revenue=600
            ),
            _baseline_row(
                customer_id="mid",
                current_mrr=300.0,
                avg_monthly_revenue=300.0,
                lifetime_revenue=8000,
            ),
            _baseline_row(
                customer_id="high",
                current_mrr=2000.0,
                avg_monthly_revenue=2000.0,
                lifetime_revenue=80000,
            ),
        )
        s = customer_value_score(df)
        self.assertLess(s.iloc[0], s.iloc[1])
        self.assertLess(s.iloc[1], s.iloc[2])


class TestAssignTiers(unittest.TestCase):
    def test_priority_thresholds_map_to_tiers(self) -> None:
        priority = pd.Series([10, 20, 30, 40])
        churn = pd.Series([10, 30, 40, 50])
        revenue = pd.Series([20, 20, 20, 20])
        tiers = assign_tiers(priority, churn, revenue)
        self.assertEqual(list(tiers), ["low", "medium", "high", "critical"])

    def test_critical_override_fires_when_both_scores_high(self) -> None:
        # priority < 35 but churn >= 45 and customer value >= 70 → critical override
        tiers = assign_tiers(pd.Series([30.0]), pd.Series([50.0]), pd.Series([80.0]))
        self.assertEqual(tiers.iloc[0], "critical")


class TestMainRiskDriver(unittest.TestCase):
    def test_driver_is_the_signal_with_largest_weighted_contribution(self) -> None:
        # Failed payments has weight 0.22; a saturated signal there should outrank low_nps at 0.20
        df = _frame(_baseline_row(failed_payments_90d=2, nps_score_recent=-100))
        signals = normalize_signals(df)
        driver = main_risk_driver(signals).iloc[0]
        self.assertEqual(driver, "failed payments")

    def test_clean_customer_has_no_material_driver(self) -> None:
        df = _frame(_baseline_row())
        driver = main_risk_driver(normalize_signals(df)).iloc[0]
        self.assertEqual(driver, "no material signal")


class TestRecommendActions(unittest.TestCase):
    def test_critical_high_revenue_triggers_executive_save(self) -> None:
        df = _frame(
            _baseline_row(
                current_mrr=1500.0,
                avg_monthly_revenue=1500.0,
                lifetime_revenue=30000,
                failed_payments_90d=2,
                nps_score_recent=-10,
                feature_adoption_score_recent=20,
                usage_trend=-5,
            )
        )
        scored = compute_scores(df)
        self.assertEqual(scored.iloc[0]["risk_tier"], "critical")
        self.assertEqual(scored.iloc[0]["recommended_action"], "executive save motion")

    def test_failed_payments_with_meaningful_risk_triggers_billing(self) -> None:
        # Need a small population so the target customer doesn't land in the top
        # revenue percentile (which would trigger executive save motion instead).
        df = _frame(
            _baseline_row(
                customer_id="big",
                current_mrr=10000,
                avg_monthly_revenue=10000,
                lifetime_revenue=200000,
            ),
            _baseline_row(
                customer_id="target",
                failed_payments_90d=2,
                support_tickets_90d=12,
                nps_score_recent=0,
            ),
            _baseline_row(
                customer_id="other",
                current_mrr=5000,
                avg_monthly_revenue=5000,
                lifetime_revenue=80000,
            ),
        )
        scored = compute_scores(df)
        target = scored[scored["customer_id"] == "target"].iloc[0]
        self.assertEqual(target["main_risk_driver"], "failed payments")
        self.assertEqual(target["recommended_action"], "billing intervention")

    def test_low_risk_clean_customer_just_monitors(self) -> None:
        # customer_value_score is percentile-based, so we need a population for it
        # to be meaningful; the clean baseline customer should rank in the middle.
        df = _frame(
            _baseline_row(
                customer_id="poor", current_mrr=10.0, avg_monthly_revenue=10.0, lifetime_revenue=120
            ),
            _baseline_row(customer_id="clean"),
            _baseline_row(
                customer_id="big",
                current_mrr=2000,
                avg_monthly_revenue=2000,
                lifetime_revenue=50000,
            ),
        )
        scored = compute_scores(df)
        clean = scored[scored["customer_id"] == "clean"].iloc[0]
        self.assertEqual(clean["risk_tier"], "low")
        self.assertEqual(clean["recommended_action"], "monitor only")

    def test_customer_value_cannot_create_priority_without_churn_risk(self) -> None:
        df = _frame(
            _baseline_row(
                customer_id="small", current_mrr=10, avg_monthly_revenue=10, lifetime_revenue=100
            ),
            _baseline_row(
                customer_id="large",
                current_mrr=10000,
                avg_monthly_revenue=10000,
                lifetime_revenue=500000,
            ),
        )
        scored = compute_scores(df)
        self.assertTrue((scored["retention_priority_score"] == 0).all())
        self.assertTrue((scored["risk_tier"] == "low").all())


class TestComputeScoresContract(unittest.TestCase):
    def test_excludes_churned_customers(self) -> None:
        df = _frame(
            _baseline_row(customer_id="A", churn_flag=0),
            _baseline_row(customer_id="B", churn_flag=1),
        )
        scored = compute_scores(df)
        self.assertEqual(list(scored["customer_id"]), ["A"])

    def test_output_is_sorted_by_priority_descending(self) -> None:
        df = _frame(
            _baseline_row(customer_id="A"),
            _baseline_row(customer_id="B", failed_payments_90d=2, nps_score_recent=-20),
            _baseline_row(customer_id="C", usage_trend=-3),
        )
        scored = compute_scores(df)
        priorities = scored["retention_priority_score"].tolist()
        self.assertEqual(priorities, sorted(priorities, reverse=True))

    def test_required_columns_present(self) -> None:
        df = _frame(_baseline_row())
        scored = compute_scores(df)
        required = {
            "customer_id",
            "churn_risk_score",
            "customer_value_score",
            "retention_priority_score",
            "risk_tier",
            "main_risk_driver",
            "recommended_action",
        }
        self.assertTrue(required.issubset(scored.columns))

    def test_missing_required_columns_fail_fast(self) -> None:
        df = _frame(_baseline_row()).drop(columns=["usage_trend"])
        with self.assertRaisesRegex(ValueError, "missing required columns"):
            compute_scores(df)

    def test_all_churned_population_returns_empty_stable_schema(self) -> None:
        df = _frame(
            _baseline_row(customer_id="A", churn_flag=1),
            _baseline_row(customer_id="B", churn_flag=1),
        )
        scored = compute_scores(df)
        self.assertEqual(scored.empty, True)
        self.assertEqual(scored.columns.tolist(), SCORE_OUTPUT_COLUMNS)


class TestRiskTierSummary(unittest.TestCase):
    def test_empty_summary_keeps_all_tiers_and_columns(self) -> None:
        summary = risk_tier_summary(pd.DataFrame(columns=SCORE_OUTPUT_COLUMNS))
        self.assertEqual(summary.columns.tolist(), TIER_SUMMARY_COLUMNS)
        self.assertEqual(summary["risk_tier"].tolist(), ["critical", "high", "medium", "low"])
        self.assertEqual(summary["customers"].sum(), 0)

    def test_non_empty_summary_orders_tiers_and_rounds_metrics(self) -> None:
        scored = compute_scores(
            _frame(
                _baseline_row(
                    customer_id="critical",
                    current_mrr=2000,
                    avg_monthly_revenue=2000,
                    lifetime_revenue=100000,
                    failed_payments_90d=2,
                    usage_trend=-5,
                    nps_score_recent=-5,
                    feature_adoption_score_recent=10,
                ),
                _baseline_row(
                    customer_id="high",
                    current_mrr=1000,
                    avg_monthly_revenue=1000,
                    lifetime_revenue=50000,
                    failed_payments_90d=2,
                    nps_score_recent=0,
                ),
                _baseline_row(customer_id="low", current_mrr=100, avg_monthly_revenue=100),
            )
        )

        summary = risk_tier_summary(scored)

        self.assertEqual(summary["risk_tier"].tolist(), ["critical", "high", "low"])
        self.assertEqual(summary["customers"].sum(), 3)
        self.assertAlmostEqual(float(summary["share_of_scored_base"].sum()), 1.0, places=5)
        self.assertTrue((summary["total_current_mrr"] >= 0).all())


class TestRiskMain(unittest.TestCase):
    def test_main_writes_scores_summary_and_methodology_note(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            processed = root / "processed"
            outputs = root / "outputs"
            docs = root / "docs"
            processed.mkdir()
            pd.DataFrame(
                [
                    _baseline_row(
                        customer_id="A",
                        failed_payments_90d=2,
                        nps_score_recent=0,
                        feature_adoption_score_recent=20,
                        usage_trend=-4,
                    ),
                    _baseline_row(customer_id="B", churn_flag=1),
                ]
            ).to_csv(processed / "customer_retention_features.csv", index=False)

            with (
                mock.patch.object(risk, "processed_dir", return_value=processed),
                mock.patch.object(risk, "outputs_tables_dir", return_value=outputs),
                mock.patch.object(risk, "docs_dir", return_value=docs),
                contextlib.redirect_stdout(io.StringIO()) as stdout,
            ):
                risk.main()

            scored = pd.read_csv(processed / "customer_risk_scores.csv")
            summary = pd.read_csv(outputs / "risk_tier_summary.csv")
            note = docs / "methodology" / "risk_scoring_methodology.md"

            self.assertEqual(scored["customer_id"].tolist(), ["A"])
            self.assertEqual(summary["customers"].sum(), 1)
            self.assertIn("Top priority: A", stdout.getvalue())
            self.assertIn("Risk Scoring Methodology", note.read_text(encoding="utf-8"))

    def test_main_handles_no_open_customers(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            processed = root / "processed"
            outputs = root / "outputs"
            docs = root / "docs"
            processed.mkdir()
            pd.DataFrame(
                [
                    _baseline_row(customer_id="A", churn_flag=1),
                    _baseline_row(customer_id="B", churn_flag=1),
                ]
            ).to_csv(processed / "customer_retention_features.csv", index=False)

            with (
                mock.patch.object(risk, "processed_dir", return_value=processed),
                mock.patch.object(risk, "outputs_tables_dir", return_value=outputs),
                mock.patch.object(risk, "docs_dir", return_value=docs),
                contextlib.redirect_stdout(io.StringIO()) as stdout,
            ):
                risk.main()

            scored = pd.read_csv(processed / "customer_risk_scores.csv")
            self.assertEqual(scored.columns.tolist(), SCORE_OUTPUT_COLUMNS)
            self.assertTrue(scored.empty)
            self.assertIn("Top priority: none", stdout.getvalue())


if __name__ == "__main__":
    unittest.main()
