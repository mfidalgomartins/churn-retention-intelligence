"""Tests for the production ingestion, model, economics, experiment, and monitoring layers."""

from __future__ import annotations

import json
import tempfile
import unittest
import zipfile
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd
import yaml

from churn.economics import (
    build_account_month_ledger,
    build_revenue_bridge,
    build_summary,
    build_unit_economics,
)
from churn.economics import load_inputs as load_economic_inputs
from churn.experiments import (
    assign_intervention,
    build_balance_table,
    build_incrementality_table,
    build_outcome_monitoring,
    build_pending_outcomes,
    load_existing_production_assignment,
    load_existing_production_outcomes,
    load_population,
    merge_observed_outcomes,
    resolve_assignment_date,
    resolve_outcomes,
    simulate_outcomes,
    validate_experiment,
)
from churn.experiments import (
    load_config as load_experiment_config,
)
from churn.ingest import (
    CsvSourceAdapter,
    _validated_identifier,
    build_adapter,
    publish_frames,
    validate_source_frames,
    verify_ingestion_manifest,
)
from churn.modeling import (
    _write_model_artifact,
    build_prediction_outputs,
    calibration_table,
    categorical_psi,
    coefficient_table,
    feature_drift_table,
    fit_model,
    load_snapshots,
    monitoring_table,
    numeric_psi,
    performance_table,
    split_snapshots,
    validate_model_performance,
)
from churn.modeling import (
    load_config as load_model_config,
)
from churn.monitor import (
    build_alerts,
    build_monitoring_summary,
    build_portfolio_trend,
    build_transition_history,
)
from churn.monitor import (
    load_config as load_monitoring_config,
)
from churn.monitor import (
    load_inputs as load_monitoring_inputs,
)
from churn.snapshot import publish_snapshot
from churn.snapshots import _window_sums, build_monthly_snapshots

ROOT = Path(__file__).resolve().parents[2]


class TestEconomics(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.inputs = load_economic_inputs(ROOT)
        cls.ledger = build_account_month_ledger(
            cls.inputs["customers"],
            cls.inputs["subscriptions"],
            cls.inputs["movements"],
            cls.inputs["service_costs"],
            pd.Timestamp("2026-03-01"),
        )

    def test_event_ledger_reconciles_to_monthly_bridge(self) -> None:
        bridge = build_revenue_bridge(self.ledger, include_reactivation=True)
        self.assertLessEqual(bridge["reconciliation_diff"].abs().max(), 0.01)
        self.assertTrue((bridge["closing_mrr"] >= 0).all())

    def test_current_mrr_ties_to_open_subscriptions(self) -> None:
        ledger_total = self.ledger.groupby("customer_id")["closing_mrr"].last().sum()
        expected = (
            self.inputs["subscriptions"]
            .loc[self.inputs["subscriptions"]["subscription_end_date"].isna(), "monthly_revenue"]
            .sum()
        )
        self.assertAlmostEqual(ledger_total, expected, places=2)

    def test_unit_economics_exclude_incomplete_period_and_reconcile_formulas(self) -> None:
        channel, segment, cohort, account = build_unit_economics(
            self.ledger,
            self.inputs["customers"],
            self.inputs["spend"],
            horizon_months=24,
            complete_month=pd.Timestamp("2026-02-01"),
        )
        bridge = build_revenue_bridge(self.ledger, include_reactivation=True)
        summary = build_summary(
            bridge,
            channel,
            account,
            horizon_months=24,
            complete_month=pd.Timestamp("2026-02-01"),
        )
        expected_cac = channel["total_acquisition_spend"] / channel["acquired_customers"]
        np.testing.assert_allclose(channel["cac"], expected_cac, atol=1e-4)
        self.assertEqual(set(segment["segment"]), {"Startup", "SMB", "Mid-Market", "Enterprise"})
        self.assertGreater(len(cohort), 12)
        self.assertIn("blended_cac", set(summary["metric"]))


class TestTemporalModel(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.config = load_model_config(ROOT)
        cls.snapshots = load_snapshots(ROOT)
        cls.splits = split_snapshots(cls.snapshots, cls.config)
        cls.base_model, cls.calibrator = fit_model(cls.splits, cls.config)

    def test_temporal_splits_are_label_safe_and_separated(self) -> None:
        config = self.config
        splits = self.splits
        horizon = pd.Timedelta(days=int(config["outcome_horizon_days"]))
        self.assertLess(
            splits["train"]["observation_date"].max() + horizon,
            splits["calibration"]["observation_date"].min(),
        )
        self.assertLess(
            splits["calibration"]["observation_date"].max() + horizon,
            splits["test"]["observation_date"].min(),
        )
        self.assertTrue(
            all(frame["churn_within_horizon"].nunique() == 2 for frame in splits.values())
        )

    def test_model_outputs_are_calibrated_bounded_and_serializable(self) -> None:
        performance = performance_table(self.splits, self.base_model, self.calibrator, self.config)
        validate_model_performance(performance, self.config)
        self.assertFalse(coefficient_table(self.base_model).empty)

        features = pd.read_csv(ROOT / "data/processed/customer_retention_features.csv")
        history, current = build_prediction_outputs(
            self.snapshots,
            features,
            self.base_model,
            self.calibrator,
            self.config,
        )
        self.assertTrue(current["churn_probability_90d"].between(0, 1).all())
        self.assertFalse(monitoring_table(history).empty)

        test_probability = history.loc[
            history["prediction_scope"].eq("out_of_time_test"), "churn_probability_90d"
        ].to_numpy()
        calibration = calibration_table(
            self.splits["test"]["churn_within_horizon"], test_probability
        )
        self.assertEqual(calibration["rows"].sum(), len(self.splits["test"]))

        current_features = features[features["churn_flag"].eq(0)]
        drift = feature_drift_table(
            self.splits["train"], self.splits["test"], current_features, self.config
        )
        self.assertEqual(set(drift["scope"]), {"out_of_time_test", "current_snapshot"})

        with tempfile.TemporaryDirectory() as temporary:
            model_path, metadata = _write_model_artifact(
                self.base_model,
                self.calibrator,
                self.config,
                performance,
                Path(temporary),
            )
            self.assertTrue(model_path.is_file())
            self.assertEqual(len(metadata["model_sha256"]), 64)

    def test_window_sums_exclude_events_on_lower_boundary(self) -> None:
        observations = np.array(["2026-03-01"], dtype="datetime64[D]")
        events = np.array(["2026-01-30", "2026-01-31", "2026-03-01"], dtype="datetime64[D]")
        totals, counts = _window_sums(observations, events, np.array([10.0, 20.0, 30.0]), 30)
        self.assertEqual(totals.tolist(), [50.0])
        self.assertEqual(counts.tolist(), [2])

    def test_point_in_time_snapshot_builder_uses_only_available_events(self) -> None:
        customers = pd.DataFrame(
            {
                "customer_id": ["C1"],
                "signup_date": pd.to_datetime(["2025-12-20"]),
                "segment": ["SMB"],
                "region": ["Europe"],
                "acquisition_channel": ["Organic"],
                "plan_type": ["Growth"],
            }
        )
        subscriptions = pd.DataFrame(
            {
                "customer_id": ["C1"],
                "subscription_start_date": pd.to_datetime(["2026-01-01"]),
                "subscription_end_date": pd.to_datetime([pd.NaT]),
                "contract_type": ["Monthly"],
            }
        )
        usage = pd.DataFrame(
            {
                "customer_id": ["C1", "C1", "C1"],
                "usage_date": pd.to_datetime(["2026-01-15", "2026-02-15", "2026-04-01"]),
                "sessions": [5, 10, 1000],
                "feature_adoption_score": [40.0, 50.0, 100.0],
                "support_tickets": [0, 1, 100],
                "nps_score": [20, 10, -100],
            }
        )
        payments = pd.DataFrame(
            {
                "customer_id": ["C1", "C1"],
                "payment_date": pd.to_datetime(["2026-02-01", "2026-04-01"]),
                "payment_status": ["failed", "failed"],
            }
        )
        account_month = pd.DataFrame(
            {
                "customer_id": ["C1", "C1", "C1"],
                "month": pd.to_datetime(["2026-01-01", "2026-02-01", "2026-03-01"]),
                "closing_mrr": [100.0, 100.0, 100.0],
            }
        )
        result = build_monthly_snapshots(
            {
                "customers": customers,
                "subscriptions": subscriptions,
                "usage": usage,
                "payments": payments,
                "account_month": account_month,
            },
            {"outcome_horizon_days": 30, "minimum_history_days": 0},
            pd.Timestamp("2026-03-31"),
        )
        march = result.loc[result["observation_date"].eq(pd.Timestamp("2026-03-31"))].iloc[0]
        self.assertLess(march["recent_sessions_90d"], 1000)
        self.assertEqual(march["failed_payments_90d"], 1)
        self.assertEqual(march["label_available"], 0)

    def test_model_quality_gate_accepts_release_metrics_and_rejects_weak_model(self) -> None:
        config = load_model_config(ROOT)
        performance = pd.read_csv(ROOT / "outputs/tables/model_performance.csv")
        validate_model_performance(performance, config)
        weak = performance.copy()
        weak.loc[weak["split"].eq("test"), "roc_auc"] = 0.50
        with self.assertRaisesRegex(ValueError, "roc_auc"):
            validate_model_performance(weak, config)

    def test_population_stability_index_behaviour(self) -> None:
        expected = pd.Series(np.arange(100))
        self.assertAlmostEqual(numeric_psi(expected, expected), 0.0)
        self.assertGreater(numeric_psi(expected, pd.Series(np.arange(100) + 200)), 0.25)
        categories = pd.Series(["a"] * 50 + ["b"] * 50)
        self.assertAlmostEqual(categorical_psi(categories, categories), 0.0)


class TestExperimentation(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.config = load_experiment_config(ROOT)
        cls.assignments = assign_intervention(load_population(ROOT), cls.config)
        cls.outcomes = simulate_outcomes(cls.assignments, cls.config)

    def test_assignment_is_deterministic_stratified_and_balanced(self) -> None:
        repeated = assign_intervention(load_population(ROOT), self.config)
        pd.testing.assert_frame_equal(self.assignments, repeated)
        balance = build_balance_table(self.assignments)
        validate_experiment(self.assignments, balance, self.config)
        self.assertEqual(set(self.assignments["assignment"]), {"treatment", "control"})
        self.assertLessEqual(abs(self.assignments["assignment"].eq("treatment").mean() - 0.5), 0.01)

    def test_outcomes_disclose_simulation_and_saved_mrr_reconciles(self) -> None:
        self.assertEqual(set(self.outcomes["outcome_status"]), {"simulated"})
        self.assertEqual(
            set(self.outcomes["outcome_source"]), {"synthetic_counterfactual_simulation"}
        )
        effects = build_incrementality_table(self.outcomes)
        saved = effects.loc[effects["metric"].eq("lost_mrr_90d")].iloc[0]
        self.assertAlmostEqual(
            saved["incremental_saved_mrr"],
            -saved["treatment_minus_control"] * saved["treatment_n"],
            places=3,
        )

    def test_outcome_monitoring_detects_no_contamination(self) -> None:
        monitoring = build_outcome_monitoring(self.outcomes)
        self.assertTrue(monitoring["outcome_completeness"].eq(1).all())
        self.assertTrue(monitoring["delivery_contamination"].eq(0).all())

    def test_production_outcomes_are_pending_until_observed(self) -> None:
        assignments = assign_intervention(
            load_population(ROOT),
            self.config,
            source_adapter="csv",
            assignment_date="2026-03-01",
        )
        pending = build_pending_outcomes(assignments, self.config)
        self.assertEqual(set(pending["outcome_status"]), {"pending"})
        self.assertNotIn("simulated", set(pending["outcome_status"]))
        pending_monitoring = build_outcome_monitoring(pending)
        self.assertTrue(pending_monitoring["outcome_completeness"].eq(0).all())
        self.assertTrue(pending_monitoring["delivery_contamination"].eq(0).all())
        effects = build_incrementality_table(pending)
        self.assertEqual(set(effects["estimation_status"]), {"not_estimable"})
        self.assertTrue(effects["incremental_saved_mrr"].isna().all())

        observed = assignments[["customer_id", "assignment"]].copy()
        observed["assigned_action_delivered"] = observed["assignment"]
        observed["churned_90d"] = 0
        observed["lost_mrr_90d"] = 0.0
        merged = merge_observed_outcomes(assignments, observed, self.config)
        self.assertEqual(set(merged["outcome_status"]), {"observed"})
        observed_effects = build_incrementality_table(merged)
        self.assertEqual(set(observed_effects["estimation_status"]), {"estimable"})

    def test_external_adapter_cannot_enter_simulation_path(self) -> None:
        assignments = assign_intervention(
            load_population(ROOT),
            self.config,
            source_adapter="postgresql",
            assignment_date="2026-03-01",
        )
        env_name = self.config["observed_outcomes"]["file_env"]
        with patch.dict("os.environ", {env_name: ""}):
            outcomes = resolve_outcomes(assignments, self.config, "postgresql")
        self.assertEqual(set(outcomes["outcome_status"]), {"pending"})
        self.assertEqual(set(outcomes["source_adapter"]), {"postgresql"})
        self.assertNotIn(
            "synthetic_counterfactual_simulation",
            set(outcomes["outcome_source"]),
        )
        existing = outcomes.copy()
        customer_id = existing.loc[0, "customer_id"]
        existing.loc[0, "outcome_status"] = "observed"
        existing.loc[0, "outcome_source"] = "observed_outcome_file"
        existing.loc[0, "assigned_action_delivered"] = existing.loc[0, "assignment"]
        existing.loc[0, "churned_90d"] = 0
        existing.loc[0, "retained_90d"] = 1
        existing.loc[0, "lost_mrr_90d"] = 0.0
        existing.loc[0, "ending_mrr_90d"] = existing.loc[0, "current_mrr"]
        with patch.dict("os.environ", {env_name: ""}):
            preserved = resolve_outcomes(
                assignments,
                self.config,
                "postgresql",
                existing,
            )
        status = preserved.set_index("customer_id").loc[customer_id, "outcome_status"]
        self.assertEqual(status, "observed")

    def test_assignment_date_and_persisted_ledgers_are_adapter_aware(self) -> None:
        self.assertEqual(
            resolve_assignment_date(self.config, {"adapter": "synthetic"}),
            "2026-03-01",
        )
        env_name = self.config["assignment_date_env"]
        with patch.dict("os.environ", {env_name: "2026-04-15"}):
            self.assertEqual(
                resolve_assignment_date(
                    self.config,
                    {"adapter": "postgresql", "reference_date": "2026-04-01"},
                ),
                "2026-04-15",
            )

        assignments = assign_intervention(
            load_population(ROOT),
            self.config,
            source_adapter="csv",
            assignment_date="2026-03-01",
        )
        outcomes = build_pending_outcomes(assignments, self.config)
        with tempfile.TemporaryDirectory() as temporary:
            assignment_path = Path(temporary) / "assignments.csv"
            outcome_path = Path(temporary) / "outcomes.csv"
            assignments.to_csv(assignment_path, index=False)
            outcomes.to_csv(outcome_path, index=False)
            loaded_assignments = load_existing_production_assignment(
                assignment_path,
                "csv",
                self.config["experiment_id"],
            )
            self.assertIsNotNone(loaded_assignments)
            loaded_outcomes = load_existing_production_outcomes(
                outcome_path,
                "csv",
                assignments,
            )
            self.assertIsNotNone(loaded_outcomes)

    def test_assignment_rejects_singleton_strata(self) -> None:
        population = load_population(ROOT).head(1).copy()
        population["probability_risk_tier"] = "high"
        with self.assertRaisesRegex(ValueError, "at least two"):
            assign_intervention(population, self.config)


class TestMonitoring(unittest.TestCase):
    def test_monthly_transition_monitoring_and_alerts(self) -> None:
        config = load_monitoring_config(ROOT)
        inputs = load_monitoring_inputs(ROOT)
        portfolio = build_portfolio_trend(inputs["history"])
        transitions, matrix = build_transition_history(inputs["history"], config["risk_tier_order"])
        alerts = build_alerts(portfolio, transitions, inputs["outcomes"], inputs["drift"], config)
        summary = build_monitoring_summary(portfolio, transitions, alerts)
        complete = transitions[transitions["complete_monthly_interval"].eq(1)]
        self.assertTrue(complete["interval_days"].ge(27).all())
        np.testing.assert_allclose(
            matrix.groupby("from_tier", observed=True)["transition_share"].sum(),
            np.ones(matrix["from_tier"].nunique()),
            atol=2e-6,
        )
        self.assertEqual(summary["open_alerts"].iloc[0], alerts["status"].eq("alert").sum())


class TestProductionAdapters(unittest.TestCase):
    def setUp(self) -> None:
        self.contracts = {
            "raw_customers": {
                "path": "data/raw/customers.csv",
                "primary_key": "customer_id",
                "required_columns": ["customer_id", "segment"],
                "allowed_values": {"segment": ["SMB", "Enterprise"]},
            }
        }
        self.frame = pd.DataFrame({"customer_id": ["C1", "C2"], "segment": ["SMB", "Enterprise"]})

    def test_csv_adapter_validates_and_publishes_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "source"
            source.mkdir()
            self.frame.to_csv(source / "customers.csv", index=False)
            frames = CsvSourceAdapter(source).read(self.contracts)
            validate_source_frames(frames, self.contracts)
            manifest = publish_frames(
                frames,
                self.contracts,
                root,
                "csv",
                datetime(2026, 3, 1, tzinfo=UTC),
            )
            self.assertEqual(manifest["datasets"]["customers"]["rows"], 2)
            self.assertTrue((root / "data/raw/_ingestion_manifest.json").is_file())
            self.assertEqual(verify_ingestion_manifest(root, self.contracts)["adapter"], "csv")
            manifest_path = root / "data/raw/_ingestion_manifest.json"
            changed_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            changed_manifest["datasets"]["customers"]["rows"] = 3
            manifest_path.write_text(json.dumps(changed_manifest), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "row count mismatch"):
                verify_ingestion_manifest(root, self.contracts)
            changed_manifest["datasets"]["customers"]["rows"] = 2
            changed_manifest["contract_version"] = 999
            manifest_path.write_text(json.dumps(changed_manifest), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "contract version mismatch"):
                verify_ingestion_manifest(root, self.contracts)
            changed_manifest["contract_version"] = 1
            manifest_path.write_text(json.dumps(changed_manifest), encoding="utf-8")
            (root / "data/raw/customers.csv").write_text(
                "customer_id,segment\nC9,SMB\n", encoding="utf-8"
            )
            with self.assertRaisesRegex(ValueError, "checksum mismatch"):
                verify_ingestion_manifest(root, self.contracts)

    def test_adapter_rejects_duplicate_primary_key(self) -> None:
        duplicate = pd.concat([self.frame.iloc[[0]], self.frame.iloc[[0]]], ignore_index=True)
        with self.assertRaisesRegex(ValueError, "primary key"):
            validate_source_frames({"raw_customers": duplicate}, self.contracts)

    def test_adapter_factory_and_identifier_guards(self) -> None:
        config = {
            "csv": {"source_directory_env": "TEST_CHURN_SOURCE"},
            "postgresql": {"dsn_env": "TEST_DSN", "schema": "analytics", "tables": {}},
        }
        with tempfile.TemporaryDirectory() as temporary:
            with patch.dict("os.environ", {"TEST_CHURN_SOURCE": temporary}):
                adapter = build_adapter("csv", config)
            self.assertIsInstance(adapter, CsvSourceAdapter)
        with (
            patch.dict("os.environ", {"TEST_CHURN_SOURCE": ""}),
            self.assertRaisesRegex(ValueError, "Set TEST_CHURN_SOURCE"),
        ):
            build_adapter("csv", config)
        with self.assertRaisesRegex(ValueError, "Unsupported adapter"):
            build_adapter("unknown", config)
        self.assertEqual(_validated_identifier("safe_table_1"), "safe_table_1")
        with self.assertRaisesRegex(ValueError, "Unsafe SQL identifier"):
            _validated_identifier("unsafe;drop table")

    def test_snapshot_is_content_addressed_and_deterministic(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "config").mkdir()
            (root / "data/raw").mkdir(parents=True)
            (root / "data/raw/customers.csv").write_text("customer_id\nC1\n", encoding="utf-8")
            config = {
                "snapshot": {
                    "include": ["data/raw/*.csv"],
                    "exclude": [],
                }
            }
            (root / "config/pipeline.yml").write_text(yaml.safe_dump(config), encoding="utf-8")
            first_archive, first_manifest = publish_snapshot(root, "2026-03-01")
            second_archive, second_manifest = publish_snapshot(root, "2026-03-01")
            self.assertEqual(first_archive, second_archive)
            self.assertEqual(first_archive.read_bytes(), second_archive.read_bytes())
            self.assertEqual(first_manifest, second_manifest)
            with zipfile.ZipFile(first_archive) as archive:
                manifest = json.loads(archive.read("manifest.json"))
            self.assertEqual(manifest["file_count"], 1)
            self.assertEqual(manifest["source_adapter"], "synthetic")


if __name__ == "__main__":
    unittest.main()
