from __future__ import annotations

import csv
import json
import re
import sys
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from itertools import pairwise
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from churn.common import outputs_tables_dir, processed_dir, project_root, raw_dir

DATE_FMT = "%Y-%m-%d"
RISK_TIERS = ("critical", "high", "medium", "low")
ALLOWED_SUBSCRIPTION_STATUS = {"active", "at_risk", "churned"}
ALLOWED_PAYMENT_STATUS = {"paid", "failed"}
BLOCKER_CHECKS: set[tuple[str, str]] = {
    ("Data Quality", "Duplicate handling"),
    ("Data Quality", "Status consistency"),
    ("Data Quality", "Impossible date logic"),
    ("Data Quality", "Overlapping subscriptions where not expected"),
    ("Metric Correctness", "churn_flag logic"),
    ("Metric Correctness", "at_risk_flag logic"),
    ("Metric Correctness", "Feature observation date logic"),
    ("Metric Correctness", "Cumulative customer churn share calculation"),
    ("Metric Correctness", "Cumulative revenue loss share calculation"),
    ("Metric Correctness", "Monthly trend metric correctness"),
    ("Metric Correctness", "Completed-period trend logic"),
    ("Analytical Integrity", "Join inflation risk"),
    ("Analytical Integrity", "Denominator correctness"),
    ("Dashboard Review", "Governed data-source usage"),
    ("Dashboard Review", "Dashboard payload integrity"),
    ("Dashboard Review", "Consistency between KPI cards and trend charts"),
    ("Dashboard Review", "Version stamping and traceability"),
}
MAJOR_WARN_CHECKS: set[tuple[str, str]] = {
    ("Data Quality", "Usage dates outside subscription periods"),
    ("Data Quality", "Payment consistency"),
    ("Metric Correctness", "Cohort logic correctness"),
    ("Analytical Integrity", "Incomplete period comparison risk"),
    ("Analytical Integrity", "Survivorship bias risk"),
    ("Analytical Integrity", "Overclaiming risk"),
    ("Dashboard Review", "Filtered vs aggregated output consistency"),
    ("Dashboard Review", "Responsive/layout safety"),
    ("Dashboard Review", "Payload size/performance sanity"),
}


@dataclass(frozen=True)
class Check:
    category: str
    check_name: str
    status: str
    evidence: str


@dataclass(frozen=True)
class ValidationData:
    root: Path
    frames: dict[str, pd.DataFrame]
    source_adapter: str

    def __getitem__(self, name: str) -> pd.DataFrame:
        return self.frames[name]


def parse_date(value: str | None) -> date | None:
    if value is None or not value.strip():
        return None
    return datetime.strptime(value.strip(), DATE_FMT).date()


def to_float(value: str | None) -> float:
    if value is None or not value.strip():
        return 0.0
    return float(value.strip())


def to_int(value: str | None) -> int:
    if value is None or not value.strip():
        return 0
    return int(float(value.strip()))


def load_csv(path: Path) -> tuple[list[dict[str, str]], list[str]]:
    with path.open("r", encoding="utf-8", newline="") as file:
        reader = csv.DictReader(file)
        return list(reader), list(reader.fieldnames or [])


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def count_duplicates(rows: list[dict[str, str]], key: str) -> int:
    counts = Counter(row.get(key, "") for row in rows)
    return sum(count - 1 for count in counts.values() if count > 1)


def null_counts(rows: list[dict[str, str]], fields: list[str]) -> dict[str, int]:
    return {field: sum(not str(row.get(field) or "").strip() for row in rows) for field in fields}


def pct(numerator: float, denominator: float) -> float:
    return numerator / denominator if denominator else 0.0


def month_start(month: str) -> date:
    return datetime.strptime(month + "-01", DATE_FMT).date()


def month_end(month: str) -> date:
    first = month_start(month)
    next_first = (
        date(first.year + 1, 1, 1) if first.month == 12 else date(first.year, first.month + 1, 1)
    )
    return next_first - timedelta(days=1)


def month_range(start_month: str, end_month: str) -> list[str]:
    months: list[str] = []
    current = month_start(start_month)
    end = month_start(end_month)
    while current <= end:
        months.append(f"{current.year:04d}-{current.month:02d}")
        current = (
            date(current.year + 1, 1, 1)
            if current.month == 12
            else date(current.year, current.month + 1, 1)
        )
    return months


def is_blocker_check(category: str, check_name: str) -> bool:
    return (category, check_name) in BLOCKER_CHECKS


def gate_level_for_check(category: str) -> str:
    return {
        "Data Quality": "technical_validity",
        "Metric Correctness": "analytical_validity",
        "Analytical Integrity": "analytical_validity",
        "Dashboard Review": "decision_product_quality",
    }.get(category, "general")


def severity_for_check(status: str, category: str, check_name: str) -> str:
    if status == "FAIL":
        return "blocker" if is_blocker_check(category, check_name) else "critical"
    if status == "WARN":
        return "major" if (category, check_name) in MAJOR_WARN_CHECKS else "minor"
    return "info"


def release_matrix(
    checks: list[Check],
    synthetic_data: bool = True,
    analytical_warn_limit: int = 1,
    decision_support_warn_limit: int = 3,
) -> tuple[list[dict[str, Any]], str]:
    failures = [check for check in checks if check.status == "FAIL"]
    warnings = [check for check in checks if check.status == "WARN"]
    blocker_fails = [
        check for check in failures if is_blocker_check(check.category, check.check_name)
    ]
    major_warns = [
        check
        for check in warnings
        if severity_for_check(check.status, check.category, check.check_name) == "major"
    ]
    technical_failures = [
        check
        for check in failures
        if gate_level_for_check(check.category)
        in {"technical_validity", "decision_product_quality"}
    ]
    analytical_failures = [
        check for check in failures if gate_level_for_check(check.category) == "analytical_validity"
    ]

    technically_valid = not technical_failures and not blocker_fails
    analytically_acceptable = (
        technically_valid and not analytical_failures and len(major_warns) <= analytical_warn_limit
    )
    decision_support_only = (
        technically_valid
        and not analytical_failures
        and len(major_warns) <= decision_support_warn_limit
        and (bool(warnings) or synthetic_data)
    )
    screening_grade_only = (
        technically_valid and not analytically_acceptable and not decision_support_only
    )
    publish_blocked = bool(blocker_fails or failures)
    states = {
        "technically valid": technically_valid,
        "analytically acceptable": analytically_acceptable,
        "decision-support only": decision_support_only,
        "screening-grade only": screening_grade_only,
        "not committee-grade": synthetic_data or bool(warnings),
        "publish-blocked": publish_blocked,
    }
    criteria = {
        "technically valid": "No blocker failures in technical and product-quality gates.",
        "analytically acceptable": "Technically valid and no analytical failures within the acceptance warning limit.",
        "decision-support only": "Technically valid decision support within the configured warning limit, with explicit caveats.",
        "screening-grade only": "Technically stable but analytically below decision-support threshold.",
        "not committee-grade": "Any unresolved caveat or synthetic-data limitation prevents committee-grade claims.",
        "publish-blocked": "Any FAIL or blocker fail blocks publication-ready claim.",
    }
    evidence = {
        "technically valid": f"blocker_fails={len(blocker_fails)}, technical_failures={len(technical_failures)}",
        "analytically acceptable": f"analytical_failures={len(analytical_failures)}, major_warns={len(major_warns)}, limit={analytical_warn_limit}",
        "decision-support only": f"synthetic_data={synthetic_data}, major_warns={len(major_warns)}, limit={decision_support_warn_limit}",
        "screening-grade only": f"technically_valid={technically_valid}, analytically_acceptable={analytically_acceptable}",
        "not committee-grade": f"synthetic_data={synthetic_data}, warns={len(warnings)}",
        "publish-blocked": f"fail_count={len(failures)}, blocker_fails={len(blocker_fails)}",
    }
    matrix = [
        {
            "state": state,
            "active": active,
            "criterion": criteria[state],
            "evidence": evidence[state],
        }
        for state, active in states.items()
    ]

    if publish_blocked:
        recommended = "publish-blocked"
    elif screening_grade_only:
        recommended = "screening-grade only"
    elif decision_support_only:
        recommended = "decision-support only"
    elif analytically_acceptable:
        recommended = "analytically acceptable"
    elif technically_valid:
        recommended = "technically valid"
    else:
        recommended = "not committee-grade"
    return matrix, recommended


def _read_frame(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, keep_default_na=False)


def load_validation_data(root: Path | None = None) -> ValidationData:
    root = root or project_root()
    raw = raw_dir() if root == project_root() else root / "data" / "raw"
    processed = processed_dir() if root == project_root() else root / "data" / "processed"
    tables = outputs_tables_dir() if root == project_root() else root / "outputs" / "tables"
    paths = {
        "customers": raw / "customers.csv",
        "subscriptions": raw / "subscriptions.csv",
        "usage": raw / "product_usage.csv",
        "payments": raw / "payments.csv",
        "features": processed / "customer_retention_features.csv",
        "cohort": processed / "cohort_retention_table.csv",
        "segment_summary": processed / "segment_retention_summary.csv",
        "risk_scores": processed / "customer_risk_scores.csv",
        "trend": tables / "overall_retention_trend_monthly.csv",
        "churn_by_segment": tables / "churn_by_segment.csv",
        "churn_by_region": tables / "churn_by_region.csv",
        "churn_by_channel": tables / "churn_by_acquisition_channel.csv",
        "churn_by_plan": tables / "churn_by_plan_type.csv",
        "behavioral": tables / "behavioral_churn_relationships.csv",
        "findings": tables / "main_analysis_structured_findings.csv",
        "segment_risk": tables / "segment_revenue_risk_contribution.csv",
        "risk_summary": tables / "risk_tier_summary.csv",
        "revenue_bridge": tables / "revenue_movement_bridge.csv",
        "channel_economics": tables / "unit_economics_by_channel.csv",
        "model_performance": tables / "model_performance.csv",
        "model_calibration": tables / "model_calibration.csv",
        "model_drift": tables / "model_feature_drift.csv",
        "probabilities": processed / "customer_churn_probabilities.csv",
        "experiment_assignments": processed / "intervention_assignments.csv",
        "experiment_outcomes": processed / "intervention_outcome_ledger.csv",
        "intervention_incrementality": tables / "intervention_incrementality.csv",
        "intervention_balance": tables / "intervention_balance.csv",
        "risk_transitions": tables / "risk_tier_transition_history.csv",
        "monitoring_alerts": tables / "monitoring_alerts.csv",
    }
    manifest_path = raw / "_ingestion_manifest.json"
    source_adapter = "unknown"
    if manifest_path.is_file():
        source_adapter = str(json.loads(manifest_path.read_text(encoding="utf-8"))["adapter"])
    return ValidationData(
        root=root,
        frames={name: _read_frame(path) for name, path in paths.items()},
        source_adapter=source_adapter,
    )


def _check(category: str, name: str, passed: bool, evidence: str, fail: str = "FAIL") -> Check:
    return Check(category, name, "PASS" if passed else fail, evidence)


def _numeric(frame: pd.DataFrame, column: str) -> pd.Series:
    return pd.to_numeric(frame[column], errors="coerce").fillna(0.0)


def _dates(frame: pd.DataFrame, column: str) -> pd.Series:
    return pd.to_datetime(frame[column], errors="coerce")


def data_quality_checks(data: ValidationData) -> list[Check]:
    customers = data["customers"]
    subscriptions = data["subscriptions"]
    usage = data["usage"]
    payments = data["payments"]
    checks: list[Check] = []

    shapes = {
        "customers": customers.shape,
        "subscriptions": subscriptions.shape,
        "product_usage": usage.shape,
        "payments": payments.shape,
    }
    schema_widths_ok = (
        shapes["customers"][1] == 6
        and shapes["subscriptions"][1] == 8
        and shapes["product_usage"][1] == 7
        and shapes["payments"][1] == 5
    )
    if data.source_adapter == "synthetic":
        volume_ok = (
            shapes["customers"][0] == 3500
            and shapes["subscriptions"][0] == 3500
            and 200_000 <= shapes["product_usage"][0] <= 900_000
            and 20_000 <= shapes["payments"][0] <= 90_000
        )
    else:
        volume_ok = all(rows > 0 for rows, _columns in shapes.values())
    shape_ok = schema_widths_ok and volume_ok
    checks.append(
        _check(
            "Data Quality",
            "Row/column count sanity",
            shape_ok,
            f"Observed shapes: {shapes}; adapter={data.source_adapter}; checked against schema widths and adapter-appropriate row ranges.",
            "WARN",
        )
    )

    duplicate_counts = {
        "customers.customer_id": int(customers["customer_id"].duplicated().sum()),
        "subscriptions.subscription_id": int(subscriptions["subscription_id"].duplicated().sum()),
        "product_usage.usage_id": int(usage["usage_id"].duplicated().sum()),
        "payments.payment_id": int(payments["payment_id"].duplicated().sum()),
    }
    checks.append(
        _check(
            "Data Quality",
            "Duplicate handling",
            not any(duplicate_counts.values()),
            f"Primary-key duplicate counts: {duplicate_counts}.",
        )
    )

    blank = subscriptions.astype(str).apply(lambda series: series.str.strip().eq("").sum())
    unexpected_nulls = {
        column: int(count)
        for column, count in blank.items()
        if column != "subscription_end_date" and count > 0
    }
    checks.append(
        _check(
            "Data Quality",
            "Null handling",
            not unexpected_nulls,
            f"subscription_end_date nulls={int(blank.get('subscription_end_date', 0))} (expected for open accounts); unexpected subscription nulls={unexpected_nulls}.",
            "WARN",
        )
    )

    sub_statuses = set(subscriptions["status"].astype(str))
    payment_statuses = set(payments["payment_status"].astype(str))
    invalid_sub_status = sorted(sub_statuses - ALLOWED_SUBSCRIPTION_STATUS)
    invalid_payment_status = sorted(payment_statuses - ALLOWED_PAYMENT_STATUS)
    checks.append(
        _check(
            "Data Quality",
            "Status consistency",
            not invalid_sub_status and not invalid_payment_status,
            f"Subscription statuses={sorted(sub_statuses)}; payment statuses={sorted(payment_statuses)}; invalid subscription={invalid_sub_status}; invalid payment={invalid_payment_status}.",
        )
    )

    joined = subscriptions.merge(
        customers[["customer_id", "signup_date"]], on="customer_id", how="left"
    )
    start = _dates(joined, "subscription_start_date")
    end = _dates(joined, "subscription_end_date")
    signup = _dates(joined, "signup_date")
    end_before_start = int((end.notna() & (end < start)).sum())
    signup_after_start = int((signup.notna() & start.notna() & (signup > start)).sum())
    checks.append(
        _check(
            "Data Quality",
            "Impossible date logic",
            end_before_start == 0 and signup_after_start == 0,
            f"subscription_end_before_start={end_before_start}; signup_after_subscription_start={signup_after_start}.",
        )
    )

    invalid_revenue = int((_numeric(subscriptions, "monthly_revenue") <= 0).sum())
    checks.append(
        _check(
            "Data Quality",
            "Impossible revenue values",
            invalid_revenue == 0,
            f"subscriptions.monthly_revenue <= 0 rows: {invalid_revenue}.",
        )
    )

    intervals = subscriptions[["customer_id"]].copy()
    intervals["start"] = _dates(subscriptions, "subscription_start_date")
    intervals["end"] = _dates(subscriptions, "subscription_end_date").fillna(pd.Timestamp.max)
    intervals = intervals.sort_values(["customer_id", "start"])
    previous_end = intervals.groupby("customer_id")["end"].shift()
    overlap_customers = int(
        intervals.loc[intervals["start"] <= previous_end, "customer_id"].nunique()
    )
    checks.append(
        _check(
            "Data Quality",
            "Overlapping subscriptions where not expected",
            overlap_customers == 0,
            f"Customers with overlapping subscription intervals: {overlap_customers}.",
        )
    )

    windows = subscriptions[["customer_id"]].copy()
    windows["start"] = _dates(subscriptions, "subscription_start_date")
    windows["end"] = _dates(subscriptions, "subscription_end_date")
    windows = windows.groupby("customer_id", as_index=False).agg(
        start=("start", "min"),
        end=("end", "max"),
        open_subscription=("end", lambda values: bool(values.isna().any())),
    )
    usage_window = usage[["customer_id", "usage_date"]].merge(windows, on="customer_id", how="left")
    usage_date = _dates(usage_window, "usage_date")
    usage_valid = (
        usage_window["start"].notna()
        & (usage_date >= usage_window["start"])
        & (usage_window["open_subscription"] | (usage_date <= usage_window["end"]))
    )
    usage_outside = int((~usage_valid).sum())
    checks.append(
        _check(
            "Data Quality",
            "Usage dates outside subscription periods",
            usage_outside == 0,
            f"Usage rows outside active subscription window: {usage_outside}.",
            "WARN",
        )
    )

    payment_window = payments[["customer_id", "payment_date"]].merge(
        windows, on="customer_id", how="left"
    )
    payment_date = _dates(payment_window, "payment_date")
    invalid_amount = int((_numeric(payments, "amount") <= 0).sum())
    payment_before_start = int(
        (payment_window["start"].notna() & (payment_date < payment_window["start"])).sum()
    )
    payment_after_end = int(
        (
            ~payment_window["open_subscription"]
            & payment_window["end"].notna()
            & (payment_date > payment_window["end"])
        ).sum()
    )
    payment_ok = invalid_amount == 0 and payment_before_start == 0 and payment_after_end == 0
    checks.append(
        _check(
            "Data Quality",
            "Payment consistency",
            payment_ok,
            f"payment.amount<=0 rows={invalid_amount}; payment_before_subscription_start={payment_before_start}; payment_after_subscription_end={payment_after_end}.",
            "WARN",
        )
    )
    return checks


def metric_correctness_checks(data: ValidationData) -> list[Check]:
    subscriptions = data["subscriptions"]
    features = data["features"]
    cohort = data["cohort"]
    segment_summary = data["segment_summary"]
    trend = data["trend"]
    churn_by_segment = data["churn_by_segment"]
    checks: list[Check] = []

    status = features[["customer_id", "churn_flag", "at_risk_flag"]].merge(
        subscriptions[["customer_id", "status", "subscription_end_date"]],
        on="customer_id",
        how="left",
    )
    churn = _numeric(status, "churn_flag").astype(int)
    at_risk = _numeric(status, "at_risk_flag").astype(int)
    expected_churn = status["status"].eq("churned").astype(int)
    expected_risk = status["status"].eq("at_risk").astype(int)
    churn_mismatch = int((churn != expected_churn).sum())
    risk_mismatch = int((at_risk != expected_risk).sum())
    both_flags = int(((churn == 1) & (at_risk == 1)).sum())
    checks.append(
        _check(
            "Metric Correctness",
            "churn_flag logic",
            churn_mismatch == 0 and both_flags == 0,
            f"churn_flag mismatches={churn_mismatch}; rows with churn_flag=1 and at_risk_flag=1={both_flags}.",
        )
    )
    checks.append(
        _check(
            "Metric Correctness",
            "at_risk_flag logic",
            risk_mismatch == 0,
            f"at_risk_flag mismatches={risk_mismatch}.",
        )
    )

    observations = features[["customer_id", "observation_date"]].merge(
        subscriptions[["customer_id", "status", "subscription_end_date"]],
        on="customer_id",
        how="left",
    )
    observation_date = _dates(observations, "observation_date")
    subscription_end = _dates(observations, "subscription_end_date")
    churned_mask = observations["status"].eq("churned")
    observation_mismatch = int(
        (observation_date.isna() | (churned_mask & (observation_date != subscription_end))).sum()
    )
    open_dates = sorted(observation_date[~churned_mask].dropna().dt.date.unique())
    checks.append(
        _check(
            "Metric Correctness",
            "Feature observation date logic",
            observation_mismatch == 0 and len(open_dates) == 1,
            f"observation_mismatches={observation_mismatch}; open_account_snapshot_dates={open_dates}.",
        )
    )

    total_customers = len(features)
    churned_customers = int(_numeric(features, "churn_flag").sum())
    feature_churn_share = pct(churned_customers, total_customers)
    segment_total = int(
        (
            _numeric(segment_summary, "active_customers")
            + _numeric(segment_summary, "churned_customers")
        ).sum()
    )
    segment_churned = int(_numeric(segment_summary, "churned_customers").sum())
    segment_churn_share = pct(segment_churned, segment_total)
    churn_share_diff = abs(feature_churn_share - segment_churn_share)
    checks.append(
        _check(
            "Metric Correctness",
            "Cumulative customer churn share calculation",
            churn_share_diff <= 1e-6,
            f"features cumulative_churn_share={feature_churn_share:.6f}; segment_summary implied={segment_churn_share:.6f}; diff={churn_share_diff:.8f}.",
        )
    )

    average_mrr = _numeric(features, "avg_monthly_revenue")
    churned_average_mrr = float(average_mrr[_numeric(features, "churn_flag").eq(1)].sum())
    dimensional_churned_mrr = float(_numeric(churn_by_segment, "churned_revenue").sum())
    revenue_diff = abs(churned_average_mrr - dimensional_churned_mrr)
    revenue_loss_share = pct(churned_average_mrr, float(average_mrr.sum()))
    checks.append(
        _check(
            "Metric Correctness",
            "Cumulative revenue loss share calculation",
            revenue_diff <= 1e-6,
            f"features churned_monthly_value={churned_average_mrr:.2f}; churn_by_segment churned_revenue sum={dimensional_churned_mrr:.2f}; cumulative_revenue_loss_share={revenue_loss_share:.6f}.",
        )
    )

    feature_values = features.assign(
        current_risk=_numeric(features, "current_mrr") * _numeric(features, "at_risk_flag"),
        churned_value=_numeric(features, "avg_monthly_revenue") * _numeric(features, "churn_flag"),
    )
    recomputed = feature_values.groupby("segment", as_index=False).agg(
        current_risk=("current_risk", "sum"), churned_value=("churned_value", "sum")
    )
    exposure = segment_summary.merge(recomputed, on="segment", how="outer").fillna(0)
    exposure_diff = max(
        float(
            (_numeric(exposure, "current_mrr_at_risk") - _numeric(exposure, "current_risk"))
            .abs()
            .max()
        ),
        float(
            (
                _numeric(exposure, "churned_monthly_value_proxy")
                - _numeric(exposure, "churned_value")
            )
            .abs()
            .max()
        ),
    )
    checks.append(
        _check(
            "Metric Correctness",
            "Segment exposure calculation",
            exposure_diff <= 0.01,
            f"Max segment-level absolute diff vs recompute: {exposure_diff:.4f}.",
        )
    )

    cohort_active = _numeric(cohort, "active_customers")
    cohort_retained = _numeric(cohort, "retained_customers")
    cohort_rate = _numeric(cohort, "retention_rate")
    recomputed_rate = cohort_retained.div(cohort_active.where(cohort_active.ne(0), 1))
    rate_mismatch = int(((cohort_rate - recomputed_rate).abs() > 1.1e-6).sum())
    bounds_violations = int(((cohort_retained > cohort_active) | ~cohort_rate.between(0, 1)).sum())
    checks.append(
        _check(
            "Metric Correctness",
            "Retention rate correctness",
            rate_mismatch == 0 and bounds_violations == 0,
            f"cohort retention mismatches={rate_mismatch}; bounds violations={bounds_violations}.",
        )
    )

    active = _numeric(trend, "active_customers_start")
    active_mrr = _numeric(trend, "active_mrr_start")
    customer_rate = _numeric(trend, "customer_churn_rate")
    revenue_rate = _numeric(trend, "revenue_churn_rate")
    retention_rate = _numeric(trend, "retention_rate")
    expected_customer_rate = _numeric(trend, "churned_customers").div(active.where(active.ne(0), 1))
    expected_revenue_rate = _numeric(trend, "churned_mrr").div(
        active_mrr.where(active_mrr.ne(0), 1)
    )
    valid_month = active.gt(0)
    trend_mismatch = int(
        (
            ((customer_rate - expected_customer_rate).abs() > 1.1e-6)
            | ((revenue_rate - expected_revenue_rate).abs() > 1.0e-4)
            | (valid_month & ((retention_rate - (1 - customer_rate)).abs() > 1.1e-6))
        ).sum()
    )
    checks.append(
        _check(
            "Metric Correctness",
            "Monthly trend metric correctness",
            trend_mismatch == 0,
            f"overall_retention_trend_monthly inconsistencies={trend_mismatch}.",
        )
    )

    snapshot_date = open_dates[0] if len(open_dates) == 1 else None
    expected_last_complete = None
    if snapshot_date:
        snapshot_month = f"{snapshot_date.year:04d}-{snapshot_date.month:02d}"
        expected_last_complete = (
            snapshot_date
            if snapshot_date == month_end(snapshot_month)
            else month_start(snapshot_month) - timedelta(days=1)
        )
    trend_latest = month_end(str(trend["month"].max())[:7]) if not trend.empty else None
    cohort_latest = (
        month_end(str(cohort["observation_month"].max())[:7]) if not cohort.empty else None
    )
    completed_periods_ok = (
        expected_last_complete is not None
        and trend_latest == expected_last_complete
        and cohort_latest == expected_last_complete
    )
    checks.append(
        _check(
            "Metric Correctness",
            "Completed-period trend logic",
            completed_periods_ok,
            f"snapshot={snapshot_date}; expected_last_complete_end={expected_last_complete}; trend_latest_end={trend_latest}; cohort_latest_end={cohort_latest}.",
        )
    )

    cohort_ordered = cohort.assign(
        cohort_date=_dates(cohort, "cohort_month"),
        observation_date=_dates(cohort, "observation_month"),
        retention_value=cohort_rate,
        revenue_value=_numeric(cohort, "revenue_retention"),
        active_value=cohort_active,
    ).sort_values(["cohort_month", "observation_date"])
    date_violations = int(
        (cohort_ordered["observation_date"] < cohort_ordered["cohort_date"]).sum()
    )
    denominator_violations = int(
        (cohort_ordered.groupby("cohort_month")["active_value"].nunique() > 1).sum()
    )
    retention_increases = cohort_ordered.groupby("cohort_month")["retention_value"].diff().gt(1e-9)
    revenue_increases = cohort_ordered.groupby("cohort_month")["revenue_value"].diff().gt(1e-9)
    monotonic_violations = int((retention_increases | revenue_increases).sum())
    cohort_ok = date_violations == denominator_violations == monotonic_violations == 0
    checks.append(
        _check(
            "Metric Correctness",
            "Cohort logic correctness",
            cohort_ok,
            f"observation_before_cohort={date_violations}; active_denominator_inconsistencies={denominator_violations}; monotonicity_violations={monotonic_violations}.",
            "WARN",
        )
    )
    return checks


def analytical_integrity_checks(data: ValidationData) -> list[Check]:
    customers = data["customers"]
    subscriptions = data["subscriptions"]
    features = data["features"]
    risk_scores = data["risk_scores"]
    trend = data["trend"]
    dimensions = [
        data["churn_by_segment"],
        data["churn_by_region"],
        data["churn_by_channel"],
        data["churn_by_plan"],
    ]
    behavioral = data["behavioral"]
    findings = data["findings"]
    checks: list[Check] = []

    unique_counts = {
        "customers": customers["customer_id"].nunique(),
        "subscriptions": subscriptions["customer_id"].nunique(),
        "features": features["customer_id"].nunique(),
        "risk_scores": risk_scores["customer_id"].nunique(),
    }
    no_inflation = all(
        unique_counts[name] == len(frame)
        for name, frame in (
            ("customers", customers),
            ("subscriptions", subscriptions),
            ("features", features),
            ("risk_scores", risk_scores),
        )
    )
    checks.append(
        _check(
            "Analytical Integrity",
            "Join inflation risk",
            no_inflation,
            f"Unique customer rows: {unique_counts}; physical rows={{'customers': {len(customers)}, 'subscriptions': {len(subscriptions)}, 'features': {len(features)}, 'risk_scores': {len(risk_scores)}}}.",
        )
    )

    low_denom_months = trend.loc[_numeric(trend, "active_customers_start") < 100, "month"].tolist()
    expected_prefix = trend["month"].head(len(low_denom_months)).tolist()
    checks.append(
        _check(
            "Analytical Integrity",
            "Incomplete period comparison risk",
            low_denom_months == expected_prefix,
            f"Low-denominator months form an initial prefix and are excluded from dashboard coverage: {low_denom_months}.",
            "WARN",
        )
    )

    dimension_frame = pd.concat(dimensions, ignore_index=True)
    customers_count = _numeric(dimension_frame, "customers")
    churned_count = _numeric(dimension_frame, "churned_customers")
    dimension_rate = _numeric(dimension_frame, "cumulative_churn_share")
    expected_rate = churned_count.div(customers_count.where(customers_count.ne(0), 1))
    denominator_issues = int(
        (
            customers_count.le(0)
            | churned_count.gt(customers_count)
            | ((dimension_rate - expected_rate).abs() > 1.1e-6)
        ).sum()
    )
    checks.append(
        _check(
            "Analytical Integrity",
            "Denominator correctness",
            denominator_issues == 0,
            f"Dimension-level denominator/rate inconsistencies={denominator_issues}.",
        )
    )

    non_churned = int((_numeric(features, "churn_flag") == 0).sum())
    checks.append(
        _check(
            "Analytical Integrity",
            "Survivorship bias risk",
            len(risk_scores) == non_churned,
            f"risk_scores rows={len(risk_scores)}; non-churned feature rows={non_churned}; scoring layer excludes churned accounts by design.",
            "WARN",
        )
    )

    monitored_relationships = {
        "usage_decline_flag",
        "high_support_ticket_flag",
        "failed_payment_flag",
        "low_nps_flag",
        "low_feature_adoption_flag",
    }
    neutral = {
        str(row.relationship): float(row.churn_rate_lift)
        for row in behavioral.itertuples()
        if row.relationship in monitored_relationships and float(row.churn_rate_lift) <= 1.0
    }
    checks.append(
        _check(
            "Analytical Integrity",
            "Overclaiming risk",
            not neutral,
            f"Behavior signals with churn lift <= 1.0: {neutral if neutral else 'none'}.",
            "WARN",
        )
    )

    dimensions_with_names = [
        (data["churn_by_segment"], "segment"),
        (data["churn_by_region"], "region"),
        (data["churn_by_channel"], "acquisition_channel"),
        (data["churn_by_plan"], "plan_type"),
    ]
    top_values = [
        str(frame.loc[_numeric(frame, "cumulative_churn_share").idxmax(), column])
        for frame, column in dimensions_with_names
    ]
    top_values.append(
        str(behavioral.loc[_numeric(behavioral, "churn_rate_lift").idxmax(), "relationship"])
    )
    section_three = findings[findings["section"].astype(str).str.startswith("3.")]
    finding_result = str(section_three.iloc[0]["result"]) if not section_three.empty else ""
    supported = all(value in finding_result for value in top_values)
    checks.append(
        _check(
            "Analytical Integrity",
            "Conclusions supported by evidence",
            supported,
            f"Section 3 finding includes computed top drivers={supported}; values={top_values}.",
            "WARN",
        )
    )
    return checks


def _dashboard_payload(html: str) -> tuple[dict[str, Any], str]:
    match = re.search(r"const DATA = (.+?);\nconst ALL", html, re.S)
    if not match:
        return {}, "Unable to locate embedded DATA payload in dashboard HTML."
    try:
        payload = json.loads(match.group(1))
    except (json.JSONDecodeError, TypeError) as exc:
        return {}, str(exc)
    return payload, ""


def dashboard_checks(data: ValidationData) -> list[Check]:
    root = data.root
    trend = data["trend"]
    risk_scores = data["risk_scores"]
    builder = (root / "src" / "churn" / "dashboard.py").read_text(encoding="utf-8")
    dashboard_path = root / "outputs" / "dashboard" / "executive-retention-command-center.html"
    html = dashboard_path.read_text(encoding="utf-8")
    root_index = (root / "index.html").read_text(encoding="utf-8")
    docs_index = (root / "docs" / "index.html").read_text(encoding="utf-8")
    dashboard_files = sorted((root / "outputs" / "dashboard").glob("*.html"))
    checks: list[Check] = []

    unique_output = (
        len(dashboard_files) == 1
        and dashboard_files[0].name == "executive-retention-command-center.html"
    )
    checks.append(
        _check(
            "Dashboard Review",
            "Official dashboard output uniqueness",
            unique_output,
            f"Dashboard HTML files detected: {[path.name for path in dashboard_files]}.",
            "WARN",
        )
    )

    redirect_ok = (
        'http-equiv="refresh"' in root_index
        and "./outputs/dashboard/executive-retention-command-center.html" in root_index
        and "const DATA =" not in root_index
        and 'http-equiv="refresh"' in docs_index
        and "../outputs/dashboard/executive-retention-command-center.html" in docs_index
        and "const DATA =" not in docs_index
    )
    checks.append(
        _check(
            "Dashboard Review",
            "Single dashboard payload copy",
            redirect_ok,
            "Root/docs entrypoints are lightweight redirects and only outputs/dashboard carries the full payload.",
        )
    )

    governed_sources = "data/raw" not in builder and "processed" in builder and "outputs" in builder
    checks.append(
        _check(
            "Dashboard Review",
            "Governed data-source usage",
            governed_sources,
            "Builder uses governed processed/output artifacts and does not query raw files.",
        )
    )

    region_coverage = "filterRegion" in html or "chartChurnRegion" in html
    checks.append(
        _check(
            "Dashboard Review",
            "Required diagnostics coverage (region)",
            region_coverage,
            "Region is accessible through a filter or dedicated diagnostic chart.",
        )
    )

    payload, payload_error = _dashboard_payload(html)
    required_payload = {
        "meta",
        "domains",
        "months",
        "monthly_fact_rows",
        "risk_kpi_cube",
        "snapshot_agg",
        "scored_customers",
        "cohort_rows",
    }
    payload_ok = bool(payload) and required_payload.issubset(payload)
    checks.append(
        _check(
            "Dashboard Review",
            "Dashboard payload integrity",
            payload_ok,
            "Embedded payload contains governed cubes and rendering tables."
            if payload_ok
            else f"Payload parse/integrity issue: {payload_error}",
        )
    )

    filters_ok = (
        all(
            token in html
            for token in (
                'id="filterStartMonth"',
                'id="filterEndMonth"',
                'id="filterSegment"',
                'id="filterRegion"',
                'id="filterChannel"',
                'id="filterPlan"',
                'id="filterRiskTier"',
                "getTrendRows(",
                "getRiskKpi(",
            )
        )
        and ("getSnapshot(" in html or "getFilteredSnapshot(" in html)
        and ("getScored(" in html or "getFilteredScored(" in html)
    )
    checks.append(
        _check(
            "Dashboard Review",
            "Filtered vs aggregated output consistency",
            filters_ok,
            "Date, dimension, and risk filters are connected to trend, risk, and diagnostic retrieval.",
        )
    )

    kpi_ok = False
    kpi_evidence = "Unable to compare dashboard and governed trend payloads."
    if payload_ok:
        months = payload.get("months", [])
        aggregated: dict[str, list[float]] = {}
        for row in payload.get("monthly_fact_rows", []):
            if len(row) < 9:
                continue
            month_index = int(float(row[0]))
            if not 0 <= month_index < len(months):
                continue
            values = aggregated.setdefault(str(months[month_index]), [0.0, 0.0, 0.0, 0.0])
            for index, source_index in enumerate((5, 6, 7, 8)):
                values[index] += float(row[source_index])
        source = {str(row.month)[:7]: row for row in trend.itertuples()}
        differences: list[tuple[float, float]] = []
        for month, values in aggregated.items():
            if month not in source:
                continue
            customer_rate = pct(values[2], values[0])
            revenue_rate = pct(values[3], values[1])
            differences.append(
                (
                    abs(customer_rate - float(source[month].customer_churn_rate or 0)),
                    abs(revenue_rate - float(source[month].revenue_churn_rate or 0)),
                )
            )
        if differences:
            max_customer_diff = max(value[0] for value in differences)
            max_revenue_diff = max(value[1] for value in differences)
            kpi_ok = max(max_customer_diff, max_revenue_diff) <= 1.1e-6
            kpi_evidence = f"Compared {len(differences)} overlapping months; max customer churn diff={max_customer_diff:.8f}, max revenue churn diff={max_revenue_diff:.8f}."
    checks.append(
        _check(
            "Dashboard Review",
            "Consistency between KPI cards and trend charts",
            kpi_ok,
            kpi_evidence,
        )
    )

    scored_payload = payload.get("scored_customers", []) if payload_ok else []
    cube_payload = payload.get("risk_kpi_cube", []) if payload_ok else []
    required_score_columns = {
        "customer_id",
        "segment",
        "region",
        "acquisition_channel",
        "plan_type",
        "current_mrr",
        "churn_risk_score",
        "customer_value_score",
        "retention_priority_score",
        "risk_tier",
        "main_risk_driver",
        "recommended_action",
        "at_risk_flag",
    }
    score_columns_ok = bool(scored_payload) and required_score_columns.issubset(scored_payload[0])
    cube_all_scope = any(
        all(
            row.get(key) == "__all__"
            for key in (
                "segment",
                "region",
                "acquisition_channel",
                "plan_type",
                "risk_tier_filter",
            )
        )
        for row in cube_payload
    )
    checks.append(
        _check(
            "Dashboard Review",
            "Risk chart/table logic consistency",
            score_columns_ok and cube_all_scope,
            f"scored_customers required columns={score_columns_ok}; risk_kpi_cube contains all-scope row={cube_all_scope}.",
        )
    )

    required_priority_columns = {
        "customer_id",
        "segment",
        "current_mrr",
        "churn_risk_score",
        "customer_value_score",
        "retention_priority_score",
        "main_risk_driver",
        "recommended_action",
    }
    checks.append(
        _check(
            "Dashboard Review",
            "Priority table schema consistency",
            required_priority_columns.issubset(risk_scores.columns),
            "Priority-table columns are governed by the risk-scoring output schema.",
        )
    )

    chart_count = len(set(re.findall(r'id="(chart[A-Za-z0-9_]+)"', html)))
    checks.append(
        _check(
            "Dashboard Review",
            "Chart density and readability scope",
            4 <= chart_count <= 6,
            f"Unique chart canvases detected: {chart_count}.",
            "WARN",
        )
    )

    layout_safe = all(
        token in html
        for token in (
            "minmax(0, 1fr)",
            "@media (max-width: 960px)",
            "@media print",
            "overflow: hidden",
        )
    )
    checks.append(
        _check(
            "Dashboard Review",
            "Responsive/layout safety",
            layout_safe,
            "Layout uses responsive grids with constrained overflow and print rules.",
            "WARN",
        )
    )

    self_contained = all(
        token not in html
        for token in (
            "__CHART_JS__",
            'src="http://',
            'src="https://',
            'href="http://',
            'href="https://',
        )
    )
    checks.append(
        _check(
            "Dashboard Review",
            "Offline/self-contained packaging",
            self_contained,
            "Dashboard is packaged without external network script or style dependencies.",
            "WARN",
        )
    )

    payload_bytes = dashboard_path.stat().st_size
    checks.append(
        _check(
            "Dashboard Review",
            "Payload size/performance sanity",
            250_000 <= payload_bytes <= 5_000_000,
            f"Dashboard HTML payload size={payload_bytes} bytes.",
            "WARN",
        )
    )

    versioned = all(
        token in html
        for token in (
            "dashboard_version",
            "data_snapshot_month",
            "coverage_start_month",
            "coverage_end_month",
            'id="filterPeriodPreset"',
            'id="periodLabel"',
        )
    )
    checks.append(
        _check(
            "Dashboard Review",
            "Version stamping and traceability",
            versioned,
            "Dashboard embeds governed version and period metadata.",
        )
    )
    return checks


def score_stability_check(risk_scores: pd.DataFrame, baseline_path: Path) -> Check:
    if not baseline_path.exists():
        return Check(
            "Governance & Release",
            "Score stability baseline drift",
            "WARN",
            f"Baseline not found at {baseline_path}.",
        )

    baseline = json.loads(baseline_path.read_text(encoding="utf-8"))
    current_share = risk_scores["risk_tier"].value_counts(normalize=True).to_dict()
    max_tier_drift = max(
        abs(float(current_share.get(tier, 0.0)) - float(baseline["tier_share"].get(tier, 0.0)))
        for tier in RISK_TIERS
    )
    current_average = float(_numeric(risk_scores, "retention_priority_score").mean())
    average_drift = abs(current_average - float(baseline["avg_priority_score"]))
    baseline_population = max(int(baseline["population"]), 1)
    population_drift = abs(len(risk_scores) - baseline_population) / baseline_population
    thresholds = baseline.get("thresholds", {})
    passed = (
        max_tier_drift <= float(thresholds.get("max_tier_share_drift", 0.03))
        and average_drift <= float(thresholds.get("max_avg_priority_drift", 3.0))
        and population_drift <= float(thresholds.get("max_population_relative_drift", 0.02))
    )
    return _check(
        "Governance & Release",
        "Score stability baseline drift",
        passed,
        f"max_tier_share_drift={max_tier_drift:.4f}; avg_priority_drift={average_drift:.4f}; population_relative_drift={population_drift:.4f}; baseline_version={baseline.get('baseline_version', 'n/a')}.",
        "WARN",
    )


def governance_checks(data: ValidationData) -> list[Check]:
    risk_scores = data["risk_scores"]
    risk_summary = data["risk_summary"]
    features = data["features"]
    segment_risk = data["segment_risk"]
    checks: list[Check] = []

    score_counts = risk_scores["risk_tier"].value_counts().to_dict()
    summary_counts = dict(
        zip(
            risk_summary["risk_tier"],
            _numeric(risk_summary, "customers").astype(int),
            strict=True,
        )
    )
    tier_count_diff = sum(
        abs(int(score_counts.get(tier, 0)) - int(summary_counts.get(tier, 0)))
        for tier in set(score_counts) | set(summary_counts)
    )
    checks.append(
        _check(
            "Governance & Release",
            "Risk tier summary cross-output consistency",
            tier_count_diff == 0,
            f"Tier count absolute diff between risk_scores and risk_tier_summary: {tier_count_diff}.",
        )
    )

    priority = _numeric(risk_scores, "retention_priority_score")
    ranking_violations = int(priority.diff().fillna(0).gt(1e-9).sum())
    checks.append(
        _check(
            "Governance & Release",
            "Priority ranking monotonicity",
            ranking_violations == 0,
            f"Rows violating non-increasing priority order: {ranking_violations}.",
        )
    )

    tier_average = risk_scores.assign(priority=priority).groupby("risk_tier")["priority"].mean()
    present_tiers = [tier for tier in RISK_TIERS if tier in tier_average]
    tier_violations = sum(
        float(tier_average.loc[first]) < float(tier_average.loc[second]) - 1e-9
        for first, second in pairwise(present_tiers)
    )
    checks.append(
        _check(
            "Governance & Release",
            "Risk tier score monotonicity",
            tier_violations == 0,
            f"Tier average priority monotonicity violations={tier_violations}; averages={tier_average.to_dict()}.",
        )
    )

    zero_risk = _numeric(risk_scores, "churn_risk_score").eq(0)
    zero_risk_violations = int(
        (
            zero_risk
            & (
                priority.ne(0)
                | risk_scores["risk_tier"].ne("low")
                | risk_scores["main_risk_driver"].ne("no material signal")
            )
        ).sum()
    )
    checks.append(
        _check(
            "Governance & Release",
            "Value cannot create churn priority",
            zero_risk_violations == 0,
            f"Zero-churn-risk rows with nonzero priority, non-low tier, or false driver: {zero_risk_violations}.",
        )
    )

    action = risk_scores["recommended_action"]
    tier = risk_scores["risk_tier"]
    driver = risk_scores["main_risk_driver"]
    churn_risk = _numeric(risk_scores, "churn_risk_score")
    customer_value = _numeric(risk_scores, "customer_value_score")
    renewal_near = _numeric(risk_scores, "renewal_near_flag").astype(int)
    action_mismatch = int(
        (
            (action.eq("executive save motion") & ~(tier.eq("critical") & customer_value.ge(70)))
            | (
                action.eq("billing intervention")
                & ~(driver.eq("failed payments") & churn_risk.ge(45))
            )
            | (
                action.eq("product adoption campaign")
                & ~(driver.isin({"usage decline", "low adoption"}) & churn_risk.ge(35))
            )
            | (
                action.eq("renewal conversation")
                & ~(renewal_near.eq(1) & tier.isin({"medium", "high", "critical"}))
            )
        ).sum()
    )
    checks.append(
        _check(
            "Governance & Release",
            "Recommended-action rule consistency",
            action_mismatch == 0,
            f"Action rows violating scoring policy rules: {action_mismatch}.",
        )
    )

    segment_future_risk = float(_numeric(segment_risk, "current_mrr_at_risk").sum())
    segment_churned_value = float(_numeric(segment_risk, "churned_monthly_value_proxy").sum())
    feature_future_risk = float(
        _numeric(features, "current_mrr")[_numeric(features, "at_risk_flag").eq(1)].sum()
    )
    feature_churned_value = float(
        _numeric(features, "avg_monthly_revenue")[_numeric(features, "churn_flag").eq(1)].sum()
    )
    financial_diff = max(
        abs(segment_future_risk - feature_future_risk),
        abs(segment_churned_value - feature_churned_value),
    )
    checks.append(
        _check(
            "Governance & Release",
            "Financial tie-out consistency",
            financial_diff <= 0.01,
            f"Max absolute diff between segment financial outputs and feature recomputation: {financial_diff:.4f}.",
        )
    )
    checks.append(
        score_stability_check(
            risk_scores,
            data.root / "config" / "governance" / "score_stability_baseline.json",
        )
    )
    return checks


def strategic_expansion_checks(data: ValidationData) -> list[Check]:
    checks: list[Check] = []
    bridge = data["revenue_bridge"]
    economics = data["channel_economics"]
    performance = data["model_performance"]
    calibration = data["model_calibration"]
    probabilities = data["probabilities"]
    assignments = data["experiment_assignments"]
    outcomes = data["experiment_outcomes"]
    incrementality = data["intervention_incrementality"]
    balance = data["intervention_balance"]
    transitions = data["risk_transitions"]
    alerts = data["monitoring_alerts"]

    max_bridge_diff = _numeric(bridge, "reconciliation_diff").abs().max()
    checks.append(
        _check(
            "Strategic Expansions",
            "Monthly MRR bridge reconciliation",
            max_bridge_diff <= 0.01,
            f"Maximum absolute bridge reconciliation difference={max_bridge_diff:.6f}.",
        )
    )

    expected_cac = _numeric(economics, "total_acquisition_spend") / _numeric(
        economics, "acquired_customers"
    ).replace(0, pd.NA)
    expected_ltv_ratio = _numeric(economics, "modelled_ltv_24m") / _numeric(
        economics, "cac"
    ).replace(0, pd.NA)
    economics_diff = max(
        float((expected_cac - _numeric(economics, "cac")).abs().max()),
        float((expected_ltv_ratio - _numeric(economics, "ltv_to_cac_24m")).abs().max()),
    )
    checks.append(
        _check(
            "Strategic Expansions",
            "Unit-economics formula reconciliation",
            economics_diff <= 0.001,
            f"Maximum CAC or LTV:CAC formula difference={economics_diff:.6f}.",
        )
    )

    model_config = yaml.safe_load(
        (data.root / "config" / "modeling.yml").read_text(encoding="utf-8")
    )
    test = performance.loc[performance["split"].eq("test")].iloc[0]
    thresholds = model_config["quality_thresholds"]
    model_gate = (
        float(test["roc_auc"]) >= float(thresholds["minimum_test_roc_auc"])
        and float(test["average_precision"]) >= float(thresholds["minimum_test_average_precision"])
        and float(test["brier_score"]) <= float(thresholds["maximum_test_brier_score"])
        and abs(float(test["calibration_intercept"]))
        <= float(thresholds["maximum_absolute_calibration_intercept"])
        and float(thresholds["minimum_calibration_slope"])
        <= float(test["calibration_slope"])
        <= float(thresholds["maximum_calibration_slope"])
    )
    checks.append(
        _check(
            "Strategic Expansions",
            "Out-of-time probability-model quality gate",
            model_gate,
            f"test_auc={float(test['roc_auc']):.4f}; AP={float(test['average_precision']):.4f}; brier={float(test['brier_score']):.4f}; calibration_intercept={float(test['calibration_intercept']):.4f}; slope={float(test['calibration_slope']):.4f}.",
        )
    )
    max_calibration_gap = _numeric(calibration, "calibration_gap").abs().max()
    probability = _numeric(probabilities, "churn_probability_90d")
    probability_ok = probability.between(0, 1, inclusive="both").all()
    checks.extend(
        [
            _check(
                "Strategic Expansions",
                "Calibrated probability bounds",
                bool(probability_ok),
                f"Scored rows={len(probabilities)}; min={probability.min():.6f}; max={probability.max():.6f}.",
            ),
            _check(
                "Strategic Expansions",
                "Calibration-bin reliability",
                max_calibration_gap <= 0.03,
                f"Maximum absolute test calibration-bin gap={max_calibration_gap:.6f}.",
            ),
        ]
    )

    assigned_ids = set(assignments["customer_id"])
    outcome_ids = set(outcomes["customer_id"])
    arm_counts = assignments["assignment"].value_counts()
    holdout_consistent = (
        assignments["holdout_flag"]
        .astype(str)
        .eq(assignments["assignment"].eq("control").astype(int).astype(str))
    )
    checks.append(
        _check(
            "Strategic Expansions",
            "Randomized holdout integrity",
            assigned_ids == outcome_ids
            and set(arm_counts.index) == {"treatment", "control"}
            and bool(holdout_consistent.all()),
            f"arm_counts={arm_counts.to_dict()}; assignment_outcome_id_diff={len(assigned_ids ^ outcome_ids)}; holdout_mismatches={int((~holdout_consistent).sum())}.",
        )
    )
    numeric_balance = balance[balance["statistic_name"].eq("standardized_mean_difference")]
    max_smd = _numeric(numeric_balance, "balance_statistic").abs().max()
    experiment_config = yaml.safe_load(
        (data.root / "config" / "experiments.yml").read_text(encoding="utf-8")
    )
    smd_threshold = float(
        experiment_config["quality_thresholds"]["maximum_absolute_standardized_mean_difference"]
    )
    checks.append(
        _check(
            "Strategic Expansions",
            "Experiment baseline balance",
            max_smd <= smd_threshold,
            f"Maximum absolute numeric SMD={max_smd:.6f}; threshold={smd_threshold:.6f}.",
        )
    )

    saved_row = incrementality.loc[incrementality["metric"].eq("lost_mrr_90d")].iloc[0]
    estimation_status = str(saved_row.get("estimation_status", "estimable"))
    if estimation_status == "estimable":
        expected_saved_mrr = -float(saved_row["treatment_minus_control"]) * int(
            saved_row["treatment_n"]
        )
        observed_saved_mrr = float(saved_row["incremental_saved_mrr"])
        saved_mrr_diff = abs(expected_saved_mrr - observed_saved_mrr)
        saved_mrr_ok = saved_mrr_diff <= 0.01
        saved_mrr_evidence = (
            f"Reported=${observed_saved_mrr:.2f}; recomputed=${expected_saved_mrr:.2f}; "
            f"absolute_diff={saved_mrr_diff:.6f}."
        )
    else:
        reported_saved_mrr = saved_row["incremental_saved_mrr"]
        effect_is_blank = pd.isna(reported_saved_mrr) or not str(reported_saved_mrr).strip()
        saved_mrr_ok = data.source_adapter in {"csv", "postgresql"} and effect_is_blank
        saved_mrr_evidence = (
            f"estimation_status={estimation_status}; adapter={data.source_adapter}; "
            f"completion_share={float(saved_row['outcome_completion_share']):.6f}."
        )
    if data.source_adapter == "synthetic":
        outcome_provenance_ok = set(outcomes["outcome_status"]) == {"simulated"} and set(
            outcomes["outcome_source"]
        ) == {"synthetic_counterfactual_simulation"}
    else:
        outcome_provenance_ok = (
            "simulated" not in set(outcomes["outcome_status"])
            and set(outcomes["outcome_status"]).issubset({"observed", "pending"})
            and set(outcomes["outcome_source"]).issubset(
                {"observed_outcome_file", "awaiting_observed_outcomes"}
            )
        )
    checks.extend(
        [
            _check(
                "Strategic Expansions",
                "Incremental saved-MRR identity",
                saved_mrr_ok,
                saved_mrr_evidence,
            ),
            _check(
                "Strategic Expansions",
                "Experiment outcome provenance",
                outcome_provenance_ok,
                f"adapter={data.source_adapter}; outcome_status={sorted(set(outcomes['outcome_status']))}; sources={sorted(set(outcomes['outcome_source']))}.",
            ),
        ]
    )

    complete_transitions = transitions[_numeric(transitions, "complete_monthly_interval").eq(1)]
    transitions_ok = (
        not complete_transitions.empty
        and _numeric(complete_transitions, "interval_days").ge(27).all()
    )
    alerts_ok = set(alerts["status"]).issubset({"ok", "alert"}) and set(
        alerts["severity"]
    ).issubset({"info", "medium", "high"})
    checks.extend(
        [
            _check(
                "Strategic Expansions",
                "Complete-period risk transition monitoring",
                bool(transitions_ok),
                f"Complete monthly transitions={len(complete_transitions)}; total transitions={len(transitions)}.",
            ),
            _check(
                "Strategic Expansions",
                "Monitoring alert schema",
                alerts_ok,
                f"status_values={sorted(set(alerts['status']))}; severity_values={sorted(set(alerts['severity']))}.",
            ),
        ]
    )
    return checks


def write_validation_outputs(checks: list[Check], root: Path) -> tuple[int, int, int]:
    check_rows: list[dict[str, Any]] = []
    issue_rows: list[dict[str, Any]] = []
    for check in checks:
        row = {
            "category": check.category,
            "check_name": check.check_name,
            "status": check.status,
            "severity": severity_for_check(check.status, check.category, check.check_name),
            "gate_level": gate_level_for_check(check.category),
            "is_blocker": is_blocker_check(check.category, check.check_name),
            "evidence": check.evidence,
        }
        check_rows.append(row)
        if check.status in {"FAIL", "WARN"}:
            issue_rows.append({**row, "fix_applied": "No (validation-only scope)"})

    tables = root / "outputs" / "tables"
    check_fields = [
        "category",
        "check_name",
        "status",
        "severity",
        "gate_level",
        "is_blocker",
        "evidence",
    ]
    issue_fields = [
        "category",
        "check_name",
        "severity",
        "gate_level",
        "is_blocker",
        "status",
        "evidence",
        "fix_applied",
    ]
    write_csv(tables / "final_validation_checks.csv", check_rows, check_fields)
    write_csv(tables / "final_validation_issues.csv", issue_rows, issue_fields)

    policy = yaml.safe_load(
        (root / "config" / "governance" / "release_policy.yml").read_text(encoding="utf-8")
    )
    warn_policy = policy["warn_policy"]
    manifest_path = root / "data" / "raw" / "_ingestion_manifest.json"
    source_adapter = "unknown"
    if manifest_path.is_file():
        source_adapter = str(json.loads(manifest_path.read_text(encoding="utf-8"))["adapter"])
    matrix, _recommended = release_matrix(
        checks,
        synthetic_data=source_adapter != "postgresql" and source_adapter != "csv",
        analytical_warn_limit=int(warn_policy["major_warn_threshold_for_analytical_acceptance"]),
        decision_support_warn_limit=int(warn_policy["major_warn_threshold_for_decision_support"]),
    )
    write_csv(
        tables / "release_readiness_matrix.csv",
        matrix,
        ["state", "active", "criterion", "evidence"],
    )
    return (
        sum(check.status == "PASS" for check in checks),
        sum(check.status == "WARN" for check in checks),
        sum(check.status == "FAIL" for check in checks),
    )


CHECK_SUITES: tuple[Callable[[ValidationData], list[Check]], ...] = (
    data_quality_checks,
    metric_correctness_checks,
    analytical_integrity_checks,
    dashboard_checks,
    governance_checks,
    strategic_expansion_checks,
)


def main() -> int:
    data = load_validation_data()
    checks = [check for suite in CHECK_SUITES for check in suite(data)]
    passed, warned, failed = write_validation_outputs(checks, data.root)
    confidence = (
        "Needs revision" if failed else "Share with caveats" if warned else "Ready to share"
    )
    print("Validation complete.")
    print(f"Checks: {len(checks)} | PASS: {passed} | WARN: {warned} | FAIL: {failed}")
    print(f"Confidence: {confidence}")
    return int(failed > 0)


if __name__ == "__main__":
    sys.exit(main())
