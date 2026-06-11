from __future__ import annotations

import csv
import json
import os
import subprocess
import sys
import unittest
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class TestIntegration(unittest.TestCase):
    def test_required_structure_exists(self) -> None:
        required_dirs = [
            "data/raw",
            "data/processed",
            "src",
            "outputs/tables",
            "outputs/dashboard",
            "assets",
            "assets/vendor",
            "docs",
            "docs/architecture",
            "docs/methodology",
            "docs/governance",
            "config/contracts",
            "config/governance",
            "tests",
        ]
        required_files = [
            "README.md",
            "LICENSE",
            "pyproject.toml",
            ".gitignore",
            "Makefile",
            "config/contracts/data_contracts.json",
            "config/governance/release_policy.yml",
            "config/governance/score_stability_baseline.json",
        ]

        for rel in required_dirs:
            self.assertTrue((ROOT / rel).exists(), f"Missing required directory: {rel}")
        for rel in required_files:
            self.assertTrue((ROOT / rel).exists(), f"Missing required file: {rel}")

    def test_sql_reference_models_are_temporally_aligned_postgresql(self) -> None:
        readme = (ROOT / "sql/README.md").read_text(encoding="utf-8").lower()
        features = (ROOT / "sql/marts/customer_retention_features.sql").read_text(
            encoding="utf-8"
        ).lower()
        kpis = (ROOT / "sql/marts/churn_kpis.sql").read_text(encoding="utf-8").lower()
        combined = "\n".join((features, kpis))

        self.assertIn("postgresql 15+", readme)
        self.assertIn("observation_date", features)
        self.assertIn("complete calendar months", kpis)
        self.assertEqual(combined.count("with params as"), 2)
        self.assertNotIn("datediff(", combined)
        self.assertNotIn("current_date", combined)

    def test_no_invalid_subscription_date_ranges(self) -> None:
        path = ROOT / "data/raw/subscriptions.csv"
        with path.open("r", encoding="utf-8", newline="") as f:
            rows = list(csv.DictReader(f))

        invalid = 0
        for row in rows:
            start = date.fromisoformat(row["subscription_start_date"])
            end_raw = row.get("subscription_end_date", "").strip()
            if end_raw:
                end = date.fromisoformat(end_raw)
                if end < start:
                    invalid += 1

        self.assertEqual(invalid, 0, f"Found {invalid} subscriptions with end_date < start_date")

    def test_validation_has_no_failures(self) -> None:
        path = ROOT / "outputs/tables/final_validation_issues.csv"
        with path.open("r", encoding="utf-8", newline="") as f:
            issues = list(csv.DictReader(f))

        fail_rows = [r for r in issues if r.get("status") == "FAIL"]
        self.assertEqual(
            len(fail_rows),
            0,
            f"Validation has FAIL rows: {[r.get('check_name') for r in fail_rows]}",
        )

    def test_risk_scores_are_bounded(self) -> None:
        path = ROOT / "data/processed/customer_risk_scores.csv"
        with path.open("r", encoding="utf-8", newline="") as f:
            rows = list(csv.DictReader(f))

        allowed_tiers = {"low", "medium", "high", "critical"}
        for r in rows:
            churn_score = float(r["churn_risk_score"])
            value_score = float(r["customer_value_score"])
            priority_score = float(r["retention_priority_score"])
            self.assertTrue(0 <= churn_score <= 100)
            self.assertTrue(0 <= value_score <= 100)
            self.assertTrue(0 <= priority_score <= 100)
            self.assertIn(r["risk_tier"], allowed_tiers)

    def test_risk_score_output_is_canonical(self) -> None:
        duplicate_path = ROOT / "data/processed/customer_risk_priority_ranked.csv"
        self.assertFalse(duplicate_path.exists(), "Risk scores are already priority-ranked; duplicate ranked export should not exist.")

    def test_validation_schema_and_blockers(self) -> None:
        checks_path = ROOT / "outputs/tables/final_validation_checks.csv"
        issues_path = ROOT / "outputs/tables/final_validation_issues.csv"

        with checks_path.open("r", encoding="utf-8", newline="") as f:
            checks = list(csv.DictReader(f))
        with issues_path.open("r", encoding="utf-8", newline="") as f:
            issues = list(csv.DictReader(f))

        required_check_cols = {"category", "check_name", "status", "severity", "gate_level", "is_blocker", "evidence"}
        required_issue_cols = {"category", "check_name", "severity", "gate_level", "is_blocker", "status", "evidence", "fix_applied"}
        self.assertTrue(required_check_cols.issubset(set(checks[0].keys())))
        if issues:
            self.assertTrue(required_issue_cols.issubset(set(issues[0].keys())))

        blocker_fails = [r for r in issues if r.get("is_blocker") == "True" and r.get("status") == "FAIL"]
        self.assertEqual(len(blocker_fails), 0, f"Blocker failures detected: {[r.get('check_name') for r in blocker_fails]}")

    def test_data_contract_validation_outputs(self) -> None:
        checks_path = ROOT / "outputs/tables/data_contract_checks.csv"
        issues_path = ROOT / "outputs/tables/data_contract_issues.csv"
        with checks_path.open("r", encoding="utf-8", newline="") as f:
            checks = list(csv.DictReader(f))
        with issues_path.open("r", encoding="utf-8", newline="") as f:
            issues = list(csv.DictReader(f))

        self.assertGreater(len(checks), 0, "Expected data contract checks to be populated.")
        required_cols = {"dataset", "check_name", "status", "severity", "evidence"}
        self.assertTrue(required_cols.issubset(set(checks[0].keys())))
        if issues:
            self.assertTrue(required_cols.issubset(set(issues[0].keys())))

        fail_rows = [r for r in checks if r.get("status") == "FAIL"]
        self.assertEqual(len(fail_rows), 0, f"Data contract failures found: {[r.get('check_name') for r in fail_rows]}")

    def test_release_readiness_matrix(self) -> None:
        path = ROOT / "outputs/tables/release_readiness_matrix.csv"
        with path.open("r", encoding="utf-8", newline="") as f:
            rows = list(csv.DictReader(f))

        expected_states = {
            "technically valid",
            "analytically acceptable",
            "decision-support only",
            "screening-grade only",
            "not committee-grade",
            "publish-blocked",
        }
        observed = {r.get("state") for r in rows}
        self.assertEqual(observed, expected_states)

        publish_blocked = next(r for r in rows if r.get("state") == "publish-blocked")
        self.assertEqual(publish_blocked.get("active"), "False")
        analytically_acceptable = next(r for r in rows if r.get("state") == "analytically acceptable")
        decision_support = next(r for r in rows if r.get("state") == "decision-support only")
        self.assertIn("limit=1", analytically_acceptable["evidence"])
        self.assertIn("limit=3", decision_support["evidence"])

    def test_dashboard_wires_filters_and_core_views(self) -> None:
        builder_path = ROOT / "src/churn/dashboard.py"
        html_path = ROOT / "outputs/dashboard/executive-retention-command-center.html"

        builder_text = builder_path.read_text(encoding="utf-8")
        html_text = html_path.read_text(encoding="utf-8")

        # Filters that drive every slice.
        for marker in (
            'id="filterStartMonth"',
            'id="filterEndMonth"',
            'id="filterPeriodPreset"',
            'id="filterSegment"',
            'id="filterChannel"',
            'id="filterRiskTier"',
            'type="date"',
        ):
            self.assertIn(marker, html_text)

        # Core charts and tables present.
        for marker in (
            'id="chartTrend"',
            'id="chartChurnSegment"',
            'id="chartChurnChannel"',
            'id="chartCohort"',
            'id="queueTable"',
        ):
            self.assertIn(marker, html_text)

        # Builder reads only governed locations.
        self.assertIn("processed", builder_text)
        self.assertIn("outputs", builder_text)
        self.assertNotIn("data/raw", builder_text)

    def test_dashboard_output_is_unique_and_self_contained(self) -> None:
        dashboard_dir = ROOT / "outputs/dashboard"
        html_files = sorted(dashboard_dir.glob("*.html"))
        self.assertEqual(len(html_files), 1, f"Expected one official dashboard HTML, found {[p.name for p in html_files]}")
        self.assertEqual(html_files[0].name, "executive-retention-command-center.html")

        html_text = html_files[0].read_text(encoding="utf-8")
        self.assertNotIn("src=\"http://", html_text)
        self.assertNotIn("src=\"https://", html_text)
        self.assertNotIn("href=\"http://", html_text)
        self.assertNotIn("href=\"https://", html_text)
        self.assertIn('id="periodLabel"', html_text)
        self.assertIn('id="filterPeriodPreset"', html_text)
        self.assertIn("const DATA =", html_text)

    def test_only_one_project_html_outside_virtualenv(self) -> None:
        html_files = [
            p for p in ROOT.rglob("*.html")
            if not {".venv", "build", "src"}.intersection(p.parts)
        ]
        rel = sorted(str(p.relative_to(ROOT)) for p in html_files)
        self.assertEqual(
            rel,
            [
                "docs/index.html",
                "index.html",
                "outputs/dashboard/executive-retention-command-center.html",
            ],
        )

    def test_dashboard_payload_size_sanity(self) -> None:
        html_path = ROOT / "outputs/dashboard/executive-retention-command-center.html"
        size_bytes = html_path.stat().st_size
        self.assertGreaterEqual(size_bytes, 250_000)
        self.assertLessEqual(size_bytes, 3_000_000)

    def test_dashboard_build_is_deterministic(self) -> None:
        from churn.dashboard import build_html, load_data

        chart_js = (ROOT / "assets/vendor/chart.umd.min.js").read_text(encoding="utf-8")
        first = json.dumps(load_data(ROOT), separators=(",", ":"), ensure_ascii=False)
        second = json.dumps(load_data(ROOT), separators=(",", ":"), ensure_ascii=False)

        self.assertEqual(first, second)
        self.assertEqual(build_html(first, chart_js), build_html(second, chart_js))
        self.assertNotIn("generated_at_utc", first)

    def test_dashboard_priority_scope_count_matches_priority_mrr_scope(self) -> None:
        import pandas as pd

        from churn.dashboard import ALL_TOKEN, load_data

        payload = load_data(ROOT)
        all_row = next(
            row
            for row in payload["risk_kpi_cube"]
            if all(
                row[dim] == ALL_TOKEN
                for dim in (
                    "segment",
                    "region",
                    "acquisition_channel",
                    "plan_type",
                    "risk_tier_filter",
                )
            )
        )
        scored = pd.read_csv(ROOT / "data/processed/customer_risk_scores.csv")
        priority = (scored["at_risk_flag"] == 1) | scored["risk_tier"].isin(
            ["high", "critical"]
        )

        self.assertEqual(all_row["priority_accounts"], int(priority.sum()))
        self.assertAlmostEqual(
            all_row["priority_mrr_exposure"],
            float(scored.loc[priority, "current_mrr"].sum()),
            places=2,
        )

    def test_generator_is_hash_seed_independent(self) -> None:
        code = """
import hashlib
import numpy as np
from churn.common import SEED
from churn.generate import generate_customers, generate_payments, generate_subscriptions

rng = np.random.default_rng(SEED)
customers = generate_customers(rng)
subscriptions = generate_subscriptions(customers, rng)
payments = generate_payments(customers, subscriptions, rng)
print(hashlib.sha256(payments.to_csv(index=False).encode("utf-8")).hexdigest())
"""

        hashes = []
        for hash_seed in ("1", "2"):
            env = {**os.environ, "PYTHONHASHSEED": hash_seed}
            hashes.append(subprocess.check_output(
                [sys.executable, "-c", code],
                cwd=ROOT,
                env=env,
                text=True,
            ).strip())
        self.assertEqual(hashes[0], hashes[1])

    def test_pages_entrypoints_redirect_to_official_dashboard(self) -> None:
        root_index = (ROOT / "index.html").read_text(encoding="utf-8")
        docs_index = (ROOT / "docs/index.html").read_text(encoding="utf-8")
        dashboard_html = (ROOT / "outputs/dashboard/executive-retention-command-center.html").read_text(encoding="utf-8")

        self.assertIn("http-equiv=\"refresh\"", root_index)
        self.assertIn("./outputs/dashboard/executive-retention-command-center.html", root_index)
        self.assertIn("http-equiv=\"refresh\"", docs_index)
        self.assertIn("../outputs/dashboard/executive-retention-command-center.html", docs_index)

        self.assertNotIn("const DATA =", root_index)
        self.assertNotIn("const DATA =", docs_index)
        self.assertIn("const DATA =", dashboard_html)

    def test_make_validate_includes_contract_gate(self) -> None:
        makefile_text = (ROOT / "Makefile").read_text(encoding="utf-8")
        self.assertIn("$(MOD).contracts", makefile_text)
        self.assertIn("$(MOD).validate", makefile_text)
        self.assertIn("all: data profile features analyze risk dashboard validate\n\t$(MOD).dashboard", makefile_text)
        self.assertIn("release: all report", makefile_text)

    def test_published_release_artifacts_exist(self) -> None:
        graph_files = sorted((ROOT / "outputs/graphs").glob("*.png"))
        self.assertEqual(len(graph_files), 18)
        self.assertTrue(
            (ROOT / "outputs/reports/churn-retention-intelligence-report.pdf").exists()
        )
        self.assertTrue(
            (ROOT / "outputs/dashboard/executive-retention-command-center.html").exists()
        )


if __name__ == "__main__":
    unittest.main()
