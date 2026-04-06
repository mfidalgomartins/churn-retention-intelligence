from __future__ import annotations

import csv
import unittest
from datetime import date
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class TestProjectIntegrity(unittest.TestCase):
    def test_required_structure_exists(self) -> None:
        required_dirs = [
            "data/raw",
            "data/processed",
            "notebooks",
            "src",
            "src/retention_analysis",
            "outputs/charts",
            "outputs/tables",
            "outputs/dashboard",
            "assets",
            "assets/vendor",
            "docs",
            "docs/architecture",
            "docs/methodology",
            "docs/reports",
            "docs/governance",
            "config/contracts",
            "config/governance",
            "tests",
        ]
        required_files = [
            "README.md",
            "requirements.txt",
            ".gitignore",
            "Makefile",
            "config/contracts/data_contracts.yml",
            "config/contracts/data_contracts.json",
            "config/governance/release_policy.yml",
            "config/governance/score_stability_baseline.json",
        ]

        for rel in required_dirs:
            self.assertTrue((ROOT / rel).exists(), f"Missing required directory: {rel}")
        for rel in required_files:
            self.assertTrue((ROOT / rel).exists(), f"Missing required file: {rel}")

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
        report_path = ROOT / "docs/governance/data_contract_validation_report.md"

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
        self.assertTrue(report_path.exists(), "Data contract validation report is missing.")

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

    def test_dashboard_has_region_chart_and_date_filtered_customers(self) -> None:
        builder_path = ROOT / "src/dashboard_builder/build_executive_dashboard.py"
        html_path = ROOT / "outputs/dashboard/churn_retention_command_center.html"

        builder_text = builder_path.read_text(encoding="utf-8")
        html_text = html_path.read_text(encoding="utf-8")

        self.assertIn("chartChurnRegion", html_text)
        self.assertIn("getTrendRows(filters)", html_text)
        self.assertIn("getFilteredSnapshot(filters)", html_text)
        self.assertIn("getFilteredScored(filters)", html_text)
        self.assertIn("id=\"filterStartMonth\"", html_text)
        self.assertIn("id=\"filterEndMonth\"", html_text)
        self.assertIn("type=\"date\"", html_text)
        self.assertIn("id=\"filterPeriodPreset\"", html_text)
        self.assertIn("applyPeriodPreset(", html_text)
        self.assertIn("processed", builder_text)
        self.assertIn("outputs", builder_text)
        self.assertNotIn("data/raw", builder_text)

    def test_dashboard_output_is_unique_and_self_contained(self) -> None:
        dashboard_dir = ROOT / "outputs/dashboard"
        html_files = sorted(dashboard_dir.glob("*.html"))
        self.assertEqual(len(html_files), 1, f"Expected one official dashboard HTML, found {[p.name for p in html_files]}")
        self.assertEqual(html_files[0].name, "churn_retention_command_center.html")

        html_text = html_files[0].read_text(encoding="utf-8")
        self.assertNotIn("src=\"http://", html_text)
        self.assertNotIn("src=\"https://", html_text)
        self.assertNotIn("href=\"http://", html_text)
        self.assertNotIn("href=\"https://", html_text)
        self.assertIn("id=\"coverageText\"", html_text)
        self.assertIn("id=\"selectedPeriodText\"", html_text)
        self.assertIn("id=\"filterPeriodPreset\"", html_text)
        self.assertNotIn("Dashboard version:", html_text)
        self.assertNotIn("Builder version:", html_text)
        self.assertNotIn("Generated:", html_text)
        self.assertIn("const DATA =", html_text)

    def test_only_one_project_html_outside_virtualenv(self) -> None:
        html_files = [p for p in ROOT.rglob("*.html") if ".venv" not in p.parts]
        rel = [str(p.relative_to(ROOT)) for p in html_files]
        self.assertEqual(rel, ["outputs/dashboard/churn_retention_command_center.html"])

    def test_dashboard_payload_size_sanity(self) -> None:
        html_path = ROOT / "outputs/dashboard/churn_retention_command_center.html"
        size_bytes = html_path.stat().st_size
        self.assertGreaterEqual(size_bytes, 250_000)
        self.assertLessEqual(size_bytes, 3_000_000)

    def test_make_validate_includes_contract_gate(self) -> None:
        makefile_path = ROOT / "Makefile"
        makefile_text = makefile_path.read_text(encoding="utf-8")
        self.assertIn("validate_data_contracts.py", makefile_text)
        self.assertIn("run_final_validation.py", makefile_text)


if __name__ == "__main__":
    unittest.main()
