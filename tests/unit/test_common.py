"""Unit tests for shared helpers."""
from __future__ import annotations

import unittest

import pandas as pd

from churn.common import (
    _parse_reference_date,
    _parse_seed,
    docs_dir,
    infer_snapshot_date,
    last_complete_month_start,
    outputs_dashboard_dir,
    outputs_tables_dir,
    processed_dir,
    project_root,
    raw_dir,
)


class TestInferSnapshotDate(unittest.TestCase):
    def test_returns_latest_non_null(self) -> None:
        s1 = pd.Series(pd.to_datetime(["2025-01-01", "2025-03-15"]))
        s2 = pd.Series(pd.to_datetime(["2024-12-01", "2025-06-01"]))
        self.assertEqual(infer_snapshot_date(s1, s2), pd.Timestamp("2025-06-01"))

    def test_ignores_nat(self) -> None:
        s = pd.Series([pd.NaT, pd.Timestamp("2025-02-01"), pd.NaT])
        self.assertEqual(infer_snapshot_date(s), pd.Timestamp("2025-02-01"))

    def test_raises_when_no_dates(self) -> None:
        with self.assertRaises(ValueError):
            infer_snapshot_date(pd.Series([pd.NaT]))


class TestLastCompleteMonthStart(unittest.TestCase):
    def test_partial_month_returns_previous_month(self) -> None:
        self.assertEqual(
            last_complete_month_start(pd.Timestamp("2026-03-01")),
            pd.Timestamp("2026-02-01"),
        )

    def test_month_end_returns_current_month(self) -> None:
        self.assertEqual(
            last_complete_month_start(pd.Timestamp("2026-03-31")),
            pd.Timestamp("2026-03-01"),
        )


class TestEnvironmentParsing(unittest.TestCase):
    def test_reference_date_parser_normalizes_valid_dates(self) -> None:
        self.assertEqual(_parse_reference_date("2026-03-15"), pd.Timestamp("2026-03-15"))

    def test_reference_date_parser_rejects_invalid_dates(self) -> None:
        with self.assertRaisesRegex(ValueError, "CHURN_REFERENCE_DATE"):
            _parse_reference_date("not-a-date")

    def test_seed_parser_accepts_non_negative_integer(self) -> None:
        self.assertEqual(_parse_seed("123"), 123)

    def test_seed_parser_rejects_negative_integer(self) -> None:
        with self.assertRaisesRegex(ValueError, "CHURN_SEED"):
            _parse_seed("-1")


class TestProjectPaths(unittest.TestCase):
    def test_project_root_resolves_repository_root(self) -> None:
        self.assertTrue((project_root() / "pyproject.toml").exists())

    def test_data_and_output_paths_are_under_project_root(self) -> None:
        root = project_root()
        self.assertEqual(raw_dir(), root / "data" / "raw")
        self.assertEqual(processed_dir(), root / "data" / "processed")
        self.assertEqual(outputs_tables_dir(), root / "outputs" / "tables")
        self.assertEqual(outputs_dashboard_dir(), root / "outputs" / "dashboard")
        self.assertEqual(docs_dir(), root / "docs")


if __name__ == "__main__":
    unittest.main()
