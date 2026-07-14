"""Unit tests for validation helpers and the release-state machine."""

from __future__ import annotations

import datetime as dt
import unittest

from churn.validate import (
    Check,
    count_duplicates,
    gate_level_for_check,
    is_blocker_check,
    month_end,
    month_range,
    month_start,
    null_counts,
    parse_date,
    pct,
    release_matrix,
    severity_for_check,
    to_float,
    to_int,
)


class TestParsingHelpers(unittest.TestCase):
    def test_parse_date_handles_missing_and_blank(self) -> None:
        self.assertIsNone(parse_date(None))
        self.assertIsNone(parse_date(""))
        self.assertIsNone(parse_date("   "))
        self.assertEqual(parse_date(" 2025-01-02 "), dt.date(2025, 1, 2))

    def test_to_float_defaults_to_zero(self) -> None:
        self.assertEqual(to_float(None), 0.0)
        self.assertEqual(to_float(""), 0.0)
        self.assertEqual(to_float(" 1.5 "), 1.5)

    def test_to_int_truncates_via_float(self) -> None:
        self.assertEqual(to_int(None), 0)
        self.assertEqual(to_int(""), 0)
        self.assertEqual(to_int(" 4 "), 4)
        self.assertEqual(to_int("3.0"), 3)

    def test_pct_guards_zero_denominator(self) -> None:
        self.assertEqual(pct(1, 2), 0.5)
        self.assertEqual(pct(1, 0), 0.0)


class TestRowHelpers(unittest.TestCase):
    def test_count_duplicates(self) -> None:
        rows = [{"id": "a"}, {"id": "a"}, {"id": "b"}, {"id": "a"}]
        self.assertEqual(count_duplicates(rows, "id"), 2)  # "a" appears 3x → 2 extra

    def test_null_counts(self) -> None:
        rows = [{"x": "1", "y": ""}, {"x": "", "y": "2"}, {"x": "3", "y": None}]
        self.assertEqual(null_counts(rows, ["x", "y"]), {"x": 1, "y": 2})


class TestMonthHelpers(unittest.TestCase):
    def test_month_start_and_end(self) -> None:
        self.assertEqual(month_start("2025-03"), dt.date(2025, 3, 1))
        self.assertEqual(month_end("2025-02"), dt.date(2025, 2, 28))
        self.assertEqual(month_end("2024-02"), dt.date(2024, 2, 29))  # leap year
        self.assertEqual(month_end("2025-12"), dt.date(2025, 12, 31))  # year rollover

    def test_month_range_crosses_year_boundary(self) -> None:
        self.assertEqual(
            month_range("2024-11", "2025-02"),
            ["2024-11", "2024-12", "2025-01", "2025-02"],
        )


class TestCheckClassification(unittest.TestCase):
    def test_gate_level_mapping(self) -> None:
        self.assertEqual(gate_level_for_check("Data Quality"), "technical_validity")
        self.assertEqual(gate_level_for_check("Metric Correctness"), "analytical_validity")
        self.assertEqual(gate_level_for_check("Dashboard Review"), "decision_product_quality")
        self.assertEqual(gate_level_for_check("Something Else"), "general")

    def test_is_blocker_check(self) -> None:
        self.assertTrue(is_blocker_check("Data Quality", "Duplicate handling"))
        self.assertFalse(is_blocker_check("Data Quality", "Not a real check"))

    def test_severity_levels(self) -> None:
        self.assertEqual(
            severity_for_check("FAIL", "Data Quality", "Duplicate handling"), "blocker"
        )
        self.assertEqual(
            severity_for_check("FAIL", "Data Quality", "Non-blocker check"), "critical"
        )
        self.assertEqual(
            severity_for_check("WARN", "Analytical Integrity", "Survivorship bias risk"), "major"
        )
        self.assertEqual(severity_for_check("WARN", "Data Quality", "Minor thing"), "minor")
        self.assertEqual(severity_for_check("PASS", "Data Quality", "Duplicate handling"), "info")


class TestReleaseMatrix(unittest.TestCase):
    def test_clean_run_is_analytically_acceptable_for_real_data(self) -> None:
        matrix, recommended = release_matrix([], synthetic_data=False)
        self.assertEqual(recommended, "analytically acceptable")
        self.assertEqual(len(matrix), 6)
        states = {row["state"]: row["active"] for row in matrix}
        self.assertTrue(states["technically valid"])
        self.assertFalse(states["publish-blocked"])

    def test_blocker_failure_blocks_publication(self) -> None:
        checks = [Check("Data Quality", "Duplicate handling", "FAIL", "2 duplicate ids")]
        matrix, recommended = release_matrix(checks)
        self.assertEqual(recommended, "publish-blocked")
        states = {row["state"]: row["active"] for row in matrix}
        self.assertTrue(states["publish-blocked"])
        self.assertFalse(states["technically valid"])

    def test_major_warnings_degrade_to_decision_support(self) -> None:
        checks = [
            Check("Analytical Integrity", "Survivorship bias risk", "WARN", "a"),
            Check("Analytical Integrity", "Overclaiming risk", "WARN", "b"),
        ]
        _matrix, recommended = release_matrix(checks, synthetic_data=False)
        self.assertEqual(recommended, "decision-support only")

    def test_excess_major_warnings_fall_to_screening_grade(self) -> None:
        checks = [
            Check("Data Quality", "Usage dates outside subscription periods", "WARN", "a"),
            Check("Data Quality", "Payment consistency", "WARN", "b"),
            Check("Metric Correctness", "Cohort logic correctness", "WARN", "c"),
            Check("Analytical Integrity", "Incomplete period comparison risk", "WARN", "d"),
        ]
        _matrix, recommended = release_matrix(checks)
        self.assertEqual(recommended, "screening-grade only")


if __name__ == "__main__":
    unittest.main()
