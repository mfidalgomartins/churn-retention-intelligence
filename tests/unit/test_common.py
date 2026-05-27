"""Unit tests for shared helpers."""
from __future__ import annotations

import unittest

import pandas as pd

from churn.common import infer_snapshot_date


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


if __name__ == "__main__":
    unittest.main()
