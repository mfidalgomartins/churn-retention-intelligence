"""Shared constants and helpers used by the pipeline modules."""
from __future__ import annotations

# Canonical reference date for the synthetic generator. Every downstream snapshot
# falls on or before this date. Override via CHURN_REFERENCE_DATE in the env.
import os
from pathlib import Path

import pandas as pd

REFERENCE_DATE: pd.Timestamp = pd.Timestamp(
    os.environ.get("CHURN_REFERENCE_DATE", "2026-03-01")
)

SEED: int = int(os.environ.get("CHURN_SEED", "42"))


def project_root() -> Path:
    """Repository root, resolved from this module's location."""
    return Path(__file__).resolve().parents[2]


def raw_dir() -> Path:
    return project_root() / "data" / "raw"


def processed_dir() -> Path:
    return project_root() / "data" / "processed"


def outputs_tables_dir() -> Path:
    return project_root() / "outputs" / "tables"


def outputs_dashboard_dir() -> Path:
    return project_root() / "outputs" / "dashboard"


def docs_dir() -> Path:
    return project_root() / "docs"


def infer_snapshot_date(*date_series: pd.Series) -> pd.Timestamp:
    """Latest non-null date across the supplied series.

    Used to pin every downstream computation to the same observation point.
    """
    maxima = [s.max() for s in date_series if s is not None]
    maxima = [m for m in maxima if pd.notna(m)]
    if not maxima:
        raise ValueError("infer_snapshot_date requires at least one non-empty date series")
    return pd.Timestamp(max(maxima))
