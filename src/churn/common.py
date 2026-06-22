"""Shared constants and helpers used by the pipeline modules."""
from __future__ import annotations

# Canonical reference date for the synthetic generator. Every downstream snapshot
# falls on or before this date. Override via CHURN_REFERENCE_DATE in the env.
import os
from pathlib import Path

import pandas as pd

DEFAULT_REFERENCE_DATE = "2026-03-01"
DEFAULT_SEED = "42"


def _parse_reference_date(value: str | None) -> pd.Timestamp:
    raw = value or DEFAULT_REFERENCE_DATE
    try:
        parsed = pd.Timestamp(raw).normalize()
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"CHURN_REFERENCE_DATE must be a valid date, got {raw!r}"
        ) from exc
    if pd.isna(parsed):
        raise ValueError(f"CHURN_REFERENCE_DATE must be a valid date, got {raw!r}")
    return parsed


def _parse_seed(value: str | None) -> int:
    raw = value or DEFAULT_SEED
    try:
        seed = int(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"CHURN_SEED must be an integer, got {raw!r}") from exc
    if seed < 0:
        raise ValueError(f"CHURN_SEED must be non-negative, got {seed}")
    return seed


REFERENCE_DATE: pd.Timestamp = _parse_reference_date(os.environ.get("CHURN_REFERENCE_DATE"))

SEED: int = _parse_seed(os.environ.get("CHURN_SEED"))


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


def last_complete_month_start(snapshot: pd.Timestamp) -> pd.Timestamp:
    """Month start for the latest fully observed calendar month."""
    snapshot = pd.Timestamp(snapshot).normalize()
    current_month_end = snapshot + pd.offsets.MonthEnd(0)
    if snapshot == current_month_end:
        return snapshot.to_period("M").to_timestamp()
    return (snapshot.to_period("M") - 1).to_timestamp()
