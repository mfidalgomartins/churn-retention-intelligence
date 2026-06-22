"""Unit tests for data contract validation."""
from __future__ import annotations

import csv
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from churn.contracts import evaluate_dataset, main


def _write_rows(path: Path, rows: list[dict[str, str]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


class TestEvaluateDataset(unittest.TestCase):
    def test_valid_dataset_passes_required_checks(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _write_rows(
                root / "data" / "customers.csv",
                [{"customer_id": "C001", "segment": "SMB"}],
                ["customer_id", "segment"],
            )

            checks = evaluate_dataset(
                "customers",
                {
                    "path": "data/customers.csv",
                    "primary_key": "customer_id",
                    "required_columns": ["customer_id", "segment"],
                },
                root,
            )

        self.assertTrue(all(check.status == "PASS" for check in checks))
        self.assertEqual({check.check_name for check in checks}, {
            "dataset_exists",
            "required_columns_present",
            "row_count_nonzero",
            "primary_key_not_null",
            "primary_key_unique",
        })

    def test_missing_dataset_returns_only_existence_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            checks = evaluate_dataset(
                "missing",
                {
                    "path": "data/missing.csv",
                    "primary_key": "id",
                    "required_columns": ["id"],
                },
                Path(tmp),
            )

        self.assertEqual(len(checks), 1)
        self.assertEqual(checks[0].check_name, "dataset_exists")
        self.assertEqual(checks[0].status, "FAIL")
        self.assertEqual(checks[0].severity, "blocker")

    def test_missing_column_duplicate_key_and_null_key_fail(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _write_rows(
                root / "data" / "customers.csv",
                [
                    {"customer_id": "C001", "segment": "SMB"},
                    {"customer_id": "C001", "segment": "Enterprise"},
                    {"customer_id": "", "segment": "Mid-Market"},
                ],
                ["customer_id", "segment"],
            )

            checks = evaluate_dataset(
                "customers",
                {
                    "path": "data/customers.csv",
                    "primary_key": "customer_id",
                    "required_columns": ["customer_id", "segment", "region"],
                },
                root,
            )

        statuses = {check.check_name: check.status for check in checks}
        self.assertEqual(statuses["required_columns_present"], "FAIL")
        self.assertEqual(statuses["primary_key_not_null"], "FAIL")
        self.assertEqual(statuses["primary_key_unique"], "FAIL")

    def test_missing_primary_key_declaration_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _write_rows(
                root / "data" / "customers.csv",
                [{"customer_id": "C001"}],
                ["customer_id"],
            )

            checks = evaluate_dataset(
                "customers",
                {
                    "path": "data/customers.csv",
                    "primary_key": "missing_id",
                    "required_columns": ["customer_id"],
                },
                root,
            )

        pk_check = next(check for check in checks if check.check_name == "primary_key_declared_and_present")
        self.assertEqual(pk_check.status, "FAIL")
        self.assertEqual(pk_check.severity, "blocker")

    def test_allowed_values_numeric_ranges_and_date_order_fail_fast(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _write_rows(
                root / "data" / "subscriptions.csv",
                [
                    {
                        "subscription_id": "S001",
                        "customer_id": "C001",
                        "subscription_start_date": "2026-01-10",
                        "subscription_end_date": "2026-01-09",
                        "monthly_revenue": "-1",
                        "status": "paused",
                    }
                ],
                [
                    "subscription_id",
                    "customer_id",
                    "subscription_start_date",
                    "subscription_end_date",
                    "monthly_revenue",
                    "status",
                ],
            )

            checks = evaluate_dataset(
                "subscriptions",
                {
                    "path": "data/subscriptions.csv",
                    "primary_key": "subscription_id",
                    "required_columns": ["subscription_id", "customer_id"],
                    "allowed_values": {"status": ["active", "at_risk", "churned"]},
                    "numeric_ranges": {"monthly_revenue": {"min": 0}},
                    "date_order_checks": [
                        {
                            "start_column": "subscription_start_date",
                            "end_column": "subscription_end_date",
                        }
                    ],
                },
                root,
            )

        statuses = {check.check_name: check.status for check in checks}
        self.assertEqual(statuses["allowed_values:status"], "FAIL")
        self.assertEqual(statuses["numeric_range:monthly_revenue"], "FAIL")
        self.assertEqual(
            statuses["date_order:subscription_start_date_lte_subscription_end_date"],
            "FAIL",
        )

    def test_foreign_key_checks_reference_dataset_membership(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _write_rows(
                root / "data" / "customers.csv",
                [{"customer_id": "C001"}],
                ["customer_id"],
            )
            _write_rows(
                root / "data" / "payments.csv",
                [
                    {"payment_id": "P001", "customer_id": "C001"},
                    {"payment_id": "P002", "customer_id": "C999"},
                ],
                ["payment_id", "customer_id"],
            )
            configs = {
                "customers": {
                    "path": "data/customers.csv",
                    "primary_key": "customer_id",
                    "required_columns": ["customer_id"],
                },
                "payments": {
                    "path": "data/payments.csv",
                    "primary_key": "payment_id",
                    "required_columns": ["payment_id", "customer_id"],
                    "foreign_keys": [
                        {
                            "column": "customer_id",
                            "references_dataset": "customers",
                            "references_column": "customer_id",
                        }
                    ],
                },
            }

            checks = evaluate_dataset("payments", configs["payments"], root, configs)

        fk_check = next(
            check for check in checks if check.check_name == "foreign_key:customer_id->customers.customer_id"
        )
        self.assertEqual(fk_check.status, "FAIL")
        self.assertEqual(fk_check.severity, "blocker")


class TestContractsMain(unittest.TestCase):
    def test_main_writes_check_and_issue_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            outputs = root / "outputs" / "tables"
            _write_rows(
                root / "data" / "customers.csv",
                [{"customer_id": "C001", "segment": "SMB"}],
                ["customer_id", "segment"],
            )
            contract_path = root / "config" / "contracts" / "data_contracts.json"
            contract_path.parent.mkdir(parents=True, exist_ok=True)
            contract_path.write_text(
                json.dumps({
                    "datasets": {
                        "customers": {
                            "path": "data/customers.csv",
                            "primary_key": "customer_id",
                            "required_columns": ["customer_id", "segment"],
                        }
                    }
                }),
                encoding="utf-8",
            )

            with (
                patch("churn.contracts.project_root", return_value=root),
                patch("churn.contracts.outputs_tables_dir", return_value=outputs),
            ):
                exit_code = main()

            self.assertEqual(exit_code, 0)
            self.assertTrue((outputs / "data_contract_checks.csv").exists())
            self.assertTrue((outputs / "data_contract_issues.csv").exists())


if __name__ == "__main__":
    unittest.main()
