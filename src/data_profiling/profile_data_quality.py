from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class TableConfig:
    grain: str
    candidate_pk: str
    dimensions: List[str]
    metrics: List[str]
    temporal_fields: List[str]
    identifiers: List[str]
    booleans: List[str]
    text_fields: List[str]


TABLE_CONFIG: Dict[str, TableConfig] = {
    "customers": TableConfig(
        grain="One row per customer account",
        candidate_pk="customer_id",
        dimensions=["segment", "region", "acquisition_channel", "plan_type"],
        metrics=[],
        temporal_fields=["signup_date"],
        identifiers=["customer_id"],
        booleans=[],
        text_fields=[],
    ),
    "subscriptions": TableConfig(
        grain="One row per subscription record (current simulation has one subscription per customer)",
        candidate_pk="subscription_id",
        dimensions=["contract_type", "billing_cycle", "status"],
        metrics=["monthly_revenue"],
        temporal_fields=["subscription_start_date", "subscription_end_date"],
        identifiers=["subscription_id", "customer_id"],
        booleans=[],
        text_fields=[],
    ),
    "product_usage": TableConfig(
        grain="One row per customer usage event date (weekly cadence)",
        candidate_pk="usage_id",
        dimensions=["customer_id"],
        metrics=["sessions", "feature_adoption_score", "support_tickets", "nps_score"],
        temporal_fields=["usage_date"],
        identifiers=["usage_id", "customer_id"],
        booleans=[],
        text_fields=[],
    ),
    "payments": TableConfig(
        grain="One row per payment attempt",
        candidate_pk="payment_id",
        dimensions=["payment_status", "customer_id"],
        metrics=["amount"],
        temporal_fields=["payment_date"],
        identifiers=["payment_id", "customer_id"],
        booleans=[],
        text_fields=[],
    ),
}


def classify_column(column: str, table: str) -> str:
    cfg = TABLE_CONFIG[table]
    if column in cfg.identifiers:
        return "identifier"
    if column in cfg.dimensions:
        return "dimension"
    if column in cfg.metrics:
        return "metric"
    if column in cfg.temporal_fields:
        return "temporal"
    if column in cfg.booleans:
        return "boolean"
    if column in cfg.text_fields:
        return "text"
    return "dimension"


def load_tables(data_dir: Path) -> Dict[str, pd.DataFrame]:
    return {
        "customers": pd.read_csv(data_dir / "customers.csv", parse_dates=["signup_date"]),
        "subscriptions": pd.read_csv(
            data_dir / "subscriptions.csv",
            parse_dates=["subscription_start_date", "subscription_end_date"],
        ),
        "product_usage": pd.read_csv(data_dir / "product_usage.csv", parse_dates=["usage_date"]),
        "payments": pd.read_csv(data_dir / "payments.csv", parse_dates=["payment_date"]),
    }


def infer_snapshot_date(tables: Dict[str, pd.DataFrame]) -> pd.Timestamp:
    date_maxima = [
        tables["customers"]["signup_date"].max(),
        tables["subscriptions"]["subscription_start_date"].max(),
        tables["subscriptions"]["subscription_end_date"].max(),
        tables["product_usage"]["usage_date"].max(),
        tables["payments"]["payment_date"].max(),
    ]
    date_maxima = [d for d in date_maxima if pd.notna(d)]
    return pd.Timestamp(max(date_maxima))


def profile_table(
    table_name: str,
    df: pd.DataFrame,
) -> Tuple[dict, List[dict], List[dict], List[dict]]:
    cfg = TABLE_CONFIG[table_name]

    row_count = int(len(df))
    col_count = int(df.shape[1])
    duplicate_rows = int(df.duplicated().sum())

    pk = cfg.candidate_pk
    pk_nulls = int(df[pk].isna().sum())
    pk_duplicates = int(df[pk].duplicated().sum())
    pk_valid = pk_nulls == 0 and pk_duplicates == 0

    table_summary = {
        "table_name": table_name,
        "grain": cfg.grain,
        "row_count": row_count,
        "column_count": col_count,
        "candidate_primary_key": pk,
        "candidate_pk_valid": pk_valid,
        "candidate_pk_nulls": pk_nulls,
        "candidate_pk_duplicates": pk_duplicates,
        "duplicate_row_count": duplicate_rows,
    }

    column_rows = []
    for col in df.columns:
        null_count = int(df[col].isna().sum())
        null_rate = null_count / row_count if row_count else 0.0
        distinct_count = int(df[col].nunique(dropna=True))
        column_rows.append(
            {
                "table_name": table_name,
                "column_name": col,
                "dtype": str(df[col].dtype),
                "classification": classify_column(col, table_name),
                "null_count": null_count,
                "null_rate": round(null_rate, 6),
                "distinct_count": distinct_count,
            }
        )

    cardinality_rows = []
    for dim in cfg.dimensions:
        if dim in df.columns:
            cardinality_rows.append(
                {
                    "table_name": table_name,
                    "dimension": dim,
                    "cardinality": int(df[dim].nunique(dropna=True)),
                }
            )

    date_rows = []
    for dcol in cfg.temporal_fields:
        if dcol in df.columns:
            dmin = df[dcol].min()
            dmax = df[dcol].max()
            span_days = None
            if pd.notna(dmin) and pd.notna(dmax):
                span_days = int((dmax - dmin).days)
            date_rows.append(
                {
                    "table_name": table_name,
                    "date_field": dcol,
                    "min_date": dmin.date().isoformat() if pd.notna(dmin) else None,
                    "max_date": dmax.date().isoformat() if pd.notna(dmax) else None,
                    "coverage_days": span_days,
                }
            )

    return table_summary, column_rows, cardinality_rows, date_rows


def run_quality_checks(tables: Dict[str, pd.DataFrame], snapshot_date: pd.Timestamp) -> pd.DataFrame:
    customers = tables["customers"]
    subscriptions = tables["subscriptions"]
    usage = tables["product_usage"]
    payments = tables["payments"]

    checks = []

    def add_check(
        table_name: str,
        check_name: str,
        failed_rows: int,
        denominator: int,
        severity: str,
        details: str,
    ) -> None:
        failure_rate = (failed_rows / denominator) if denominator else 0.0
        checks.append(
            {
                "table_name": table_name,
                "check_name": check_name,
                "failed_rows": int(failed_rows),
                "denominator": int(denominator),
                "failure_rate": round(failure_rate, 6),
                "severity": severity,
                "status": "FAIL" if failed_rows > 0 else "PASS",
                "details": details,
            }
        )

    # Explicit check: overlapping subscriptions where not expected.
    subs = subscriptions.copy().sort_values(["customer_id", "subscription_start_date", "subscription_end_date"])
    subs["effective_end"] = subs["subscription_end_date"].fillna(snapshot_date)
    subs["previous_end"] = subs.groupby("customer_id")["effective_end"].shift(1)
    overlap_mask = subs["previous_end"].notna() & (subs["subscription_start_date"] <= subs["previous_end"])
    add_check(
        "subscriptions",
        "overlapping_subscriptions",
        int(overlap_mask.sum()),
        int(subs["customer_id"].nunique()),
        "high",
        "Subscription start date should occur after previous subscription end date for the same customer.",
    )

    # Explicit check: impossible revenue values.
    impossible_revenue = (~np.isfinite(subscriptions["monthly_revenue"])) | (subscriptions["monthly_revenue"] <= 0)
    add_check(
        "subscriptions",
        "impossible_revenue_values",
        int(impossible_revenue.sum()),
        int(len(subscriptions)),
        "high",
        "Monthly revenue must be positive and finite.",
    )

    # Explicit check: invalid status values.
    valid_statuses = {"active", "at_risk", "churned"}
    invalid_status = ~subscriptions["status"].isin(valid_statuses)
    add_check(
        "subscriptions",
        "invalid_subscription_status",
        int(invalid_status.sum()),
        int(len(subscriptions)),
        "high",
        "Status must be one of: active, at_risk, churned.",
    )

    # Explicit check: usage dates outside subscription periods.
    usage_subs = usage.merge(
        subscriptions[["customer_id", "subscription_start_date", "subscription_end_date"]],
        on="customer_id",
        how="left",
    )
    usage_outside = (
        usage_subs["subscription_start_date"].isna()
        | (usage_subs["usage_date"] < usage_subs["subscription_start_date"])
        | (
            usage_subs["subscription_end_date"].notna()
            & (usage_subs["usage_date"] > usage_subs["subscription_end_date"])
        )
    )
    add_check(
        "product_usage",
        "usage_outside_subscription_period",
        int(usage_outside.sum()),
        int(len(usage_subs)),
        "high",
        "Usage date must fall within the customer subscription active interval.",
    )

    # Explicit check: payment inconsistencies.
    pay_subs = payments.merge(
        subscriptions[["customer_id", "subscription_start_date", "subscription_end_date"]],
        on="customer_id",
        how="left",
    )
    invalid_payment_status = ~payments["payment_status"].isin({"paid", "failed"})
    invalid_payment_amount = (~np.isfinite(payments["amount"])) | (payments["amount"] <= 0)
    payment_before_subscription = pay_subs["subscription_start_date"].notna() & (
        pay_subs["payment_date"] < pay_subs["subscription_start_date"]
    )
    payment_after_churn = pay_subs["subscription_end_date"].notna() & (
        pay_subs["payment_date"] > pay_subs["subscription_end_date"]
    )
    orphan_payment_customer = pay_subs["subscription_start_date"].isna()

    payment_inconsistency = (
        invalid_payment_status
        | invalid_payment_amount
        | payment_before_subscription
        | payment_after_churn
        | orphan_payment_customer
    )
    add_check(
        "payments",
        "payment_inconsistencies",
        int(payment_inconsistency.sum()),
        int(len(payments)),
        "high",
        "Payment status/amount/date must be valid and aligned with a known subscription lifecycle.",
    )

    # Additional broad consistency checks.
    invalid_contract = ~subscriptions["contract_type"].isin({"Annual", "Monthly"})
    add_check(
        "subscriptions",
        "invalid_contract_type",
        int(invalid_contract.sum()),
        int(len(subscriptions)),
        "medium",
        "Contract type should be either Annual or Monthly.",
    )

    invalid_billing = ~subscriptions["billing_cycle"].isin({"Monthly", "Quarterly", "Annual"})
    add_check(
        "subscriptions",
        "invalid_billing_cycle",
        int(invalid_billing.sum()),
        int(len(subscriptions)),
        "medium",
        "Billing cycle should be Monthly, Quarterly, or Annual.",
    )

    bad_subscription_dates = subscriptions["subscription_end_date"].notna() & (
        subscriptions["subscription_end_date"] < subscriptions["subscription_start_date"]
    )
    add_check(
        "subscriptions",
        "subscription_end_before_start",
        int(bad_subscription_dates.sum()),
        int(len(subscriptions)),
        "high",
        "Subscription end date cannot be before start date.",
    )

    invalid_sessions = usage["sessions"] < 0
    add_check(
        "product_usage",
        "negative_sessions",
        int(invalid_sessions.sum()),
        int(len(usage)),
        "high",
        "Sessions must be non-negative.",
    )

    invalid_feature_adoption = (usage["feature_adoption_score"] < 0) | (usage["feature_adoption_score"] > 100)
    add_check(
        "product_usage",
        "feature_adoption_out_of_range",
        int(invalid_feature_adoption.sum()),
        int(len(usage)),
        "high",
        "Feature adoption score must be in [0, 100].",
    )

    invalid_tickets = usage["support_tickets"] < 0
    add_check(
        "product_usage",
        "negative_support_tickets",
        int(invalid_tickets.sum()),
        int(len(usage)),
        "high",
        "Support tickets must be non-negative.",
    )

    invalid_nps = (usage["nps_score"] < -100) | (usage["nps_score"] > 100)
    add_check(
        "product_usage",
        "nps_out_of_range",
        int(invalid_nps.sum()),
        int(len(usage)),
        "high",
        "NPS must be in [-100, 100].",
    )

    future_signup = customers["signup_date"] > snapshot_date
    add_check(
        "customers",
        "signup_date_in_future",
        int(future_signup.sum()),
        int(len(customers)),
        "medium",
        "Signup date should not be after dataset snapshot date.",
    )

    duplicate_payment_ids = int(payments["payment_id"].duplicated().sum())
    add_check(
        "payments",
        "duplicate_payment_id",
        duplicate_payment_ids,
        int(len(payments)),
        "high",
        "payment_id should be unique.",
    )

    return pd.DataFrame(checks)


def compute_suspicious_values(tables: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    subscriptions = tables["subscriptions"]
    usage = tables["product_usage"]
    payments = tables["payments"]

    rows = []

    revenue_threshold = float(subscriptions["monthly_revenue"].quantile(0.995) * 2.0)
    suspicious_high_revenue = int((subscriptions["monthly_revenue"] > revenue_threshold).sum())
    rows.append(
        {
            "table_name": "subscriptions",
            "issue": "extreme_high_monthly_revenue",
            "affected_rows": suspicious_high_revenue,
            "threshold_or_rule": f"monthly_revenue > {revenue_threshold:.2f}",
            "note": "Likely rare tail accounts; validate if enterprise pricing assumptions are intentional.",
        }
    )

    zero_session_rows = int((usage["sessions"] == 0).sum())
    rows.append(
        {
            "table_name": "product_usage",
            "issue": "zero_session_events",
            "affected_rows": zero_session_rows,
            "threshold_or_rule": "sessions == 0",
            "note": "Not impossible; can represent dormant weeks and is analytically useful for risk signals.",
        }
    )

    failed_payment_rows = int((payments["payment_status"] == "failed").sum())
    rows.append(
        {
            "table_name": "payments",
            "issue": "failed_payment_events",
            "affected_rows": failed_payment_rows,
            "threshold_or_rule": "payment_status == 'failed'",
            "note": "Expected risk signal; monitor concentration by segment/channel before modeling.",
        }
    )

    return pd.DataFrame(rows)


def build_column_classification_summary() -> pd.DataFrame:
    rows = []
    for table_name, cfg in TABLE_CONFIG.items():
        for col in cfg.identifiers:
            rows.append({"table_name": table_name, "column_name": col, "classification": "identifier"})
        for col in cfg.dimensions:
            rows.append({"table_name": table_name, "column_name": col, "classification": "dimension"})
        for col in cfg.metrics:
            rows.append({"table_name": table_name, "column_name": col, "classification": "metric"})
        for col in cfg.temporal_fields:
            rows.append({"table_name": table_name, "column_name": col, "classification": "temporal"})
        for col in cfg.booleans:
            rows.append({"table_name": table_name, "column_name": col, "classification": "boolean"})
        for col in cfg.text_fields:
            rows.append({"table_name": table_name, "column_name": col, "classification": "text"})
    return pd.DataFrame(rows).drop_duplicates().sort_values(["table_name", "column_name"])


def write_markdown_reports(
    docs_dir: Path,
    table_profile: pd.DataFrame,
    column_profile: pd.DataFrame,
    cardinality: pd.DataFrame,
    date_coverage: pd.DataFrame,
    quality_checks: pd.DataFrame,
    suspicious_values: pd.DataFrame,
) -> None:
    reports_dir = docs_dir / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)

    # Combined data quality + profiling report (single source of truth).
    lines = [
        "# Data Quality + Profiling Report",
        "",
        "## Dataset-Level Overview",
        "",
        "| table | row_count | column_count | candidate_pk | pk_valid | duplicate_rows |",
        "|---|---:|---:|---|:---:|---:|",
    ]
    for row in table_profile.itertuples(index=False):
        lines.append(
            f"| {row.table_name} | {row.row_count} | {row.column_count} | {row.candidate_primary_key} | {'yes' if row.candidate_pk_valid else 'no'} | {row.duplicate_row_count} |"
        )

    for row in table_profile.itertuples(index=False):
        table = row.table_name
        cfg = TABLE_CONFIG[table]
        lines.extend(
            [
                "",
                f"## {table}",
                "",
                f"- Grain: {row.grain}",
                f"- Candidate primary key: `{row.candidate_primary_key}`",
                f"- Nulls in candidate key: {row.candidate_pk_nulls}",
                f"- Duplicates in candidate key: {row.candidate_pk_duplicates}",
                f"- Useful dimensions: {', '.join(cfg.dimensions) if cfg.dimensions else 'None'}",
                f"- Useful metrics: {', '.join(cfg.metrics) if cfg.metrics else 'None'}",
            ]
        )

        this_dates = date_coverage[date_coverage["table_name"] == table]
        if not this_dates.empty:
            lines.append("- Date coverage:")
            for drow in this_dates.itertuples(index=False):
                lines.append(
                    f"  - `{drow.date_field}`: {drow.min_date} to {drow.max_date} ({drow.coverage_days} days)"
                )

        this_card = cardinality[cardinality["table_name"] == table]
        if not this_card.empty:
            lines.append("- Cardinality by relevant dimensions:")
            for crow in this_card.itertuples(index=False):
                lines.append(f"  - `{crow.dimension}`: {crow.cardinality}")

        this_cols = column_profile[column_profile["table_name"] == table].copy()
        lines.append("- Column classification:")
        class_order = ["identifier", "dimension", "metric", "temporal", "boolean", "text"]
        for cls in class_order:
            cols = this_cols.loc[this_cols["classification"] == cls, "column_name"].tolist()
            lines.append(f"  - {cls}: {', '.join(cols) if cols else 'None'}")

        null_cols = this_cols[this_cols["null_count"] > 0][["column_name", "null_count", "null_rate"]]
        if null_cols.empty:
            lines.append("- Null profile: no nulls detected.")
        else:
            lines.append("- Null profile (columns with nulls):")
            for nrow in null_cols.itertuples(index=False):
                lines.append(f"  - `{nrow.column_name}`: {nrow.null_count} ({nrow.null_rate:.2%})")

    lines.extend(
        [
            "",
            "## Data Quality Checks",
        ]
    )

    # Data quality issues report section.
    fail_checks = quality_checks[quality_checks["status"] == "FAIL"]
    pass_checks = quality_checks[quality_checks["status"] == "PASS"]

    lines.extend(
        [
            f"- Total checks: {len(quality_checks)}",
            f"- Passed checks: {len(pass_checks)}",
            f"- Failed checks: {len(fail_checks)}",
            "",
            "### Explicit Required Checks",
            "",
            "| check_name | table | failed_rows | failure_rate | status |",
            "|---|---|---:|---:|:---:|",
        ]
    )

    explicit_order = [
        "overlapping_subscriptions",
        "impossible_revenue_values",
        "invalid_subscription_status",
        "usage_outside_subscription_period",
        "payment_inconsistencies",
    ]
    explicit = quality_checks[quality_checks["check_name"].isin(explicit_order)].set_index("check_name")

    for check_name in explicit_order:
        if check_name in explicit.index:
            r = explicit.loc[check_name]
            lines.append(
                f"| {check_name} | {r['table_name']} | {int(r['failed_rows'])} | {r['failure_rate']:.4%} | {r['status']} |"
            )

    lines.extend(["", "### Failed Checks", ""])

    if fail_checks.empty:
        lines.append("No failed quality checks detected.")
    else:
        for r in fail_checks.itertuples(index=False):
            lines.append(
                f"- `{r.table_name}.{r.check_name}` failed with {r.failed_rows} records ({r.failure_rate:.2%}). Severity: {r.severity}. {r.details}"
            )

    lines.extend(["", "## Suspicious Values", ""])
    for r in suspicious_values.itertuples(index=False):
        lines.append(
            f"- `{r.table_name}.{r.issue}`: {r.affected_rows} rows (`{r.threshold_or_rule}`). {r.note}"
        )

    (reports_dir / "data_quality_profile_report.md").write_text("\n".join(lines), encoding="utf-8")

def main() -> None:
    project_root = Path(__file__).resolve().parents[2]
    data_dir = project_root / "data" / "raw"
    docs_dir = project_root / "docs"
    outputs_dir = project_root / "outputs" / "profiling"
    outputs_dir.mkdir(parents=True, exist_ok=True)

    tables = load_tables(data_dir)
    snapshot_date = infer_snapshot_date(tables)

    table_rows: List[dict] = []
    column_rows: List[dict] = []
    cardinality_rows: List[dict] = []
    date_rows: List[dict] = []

    for table_name, df in tables.items():
        trow, crows, card_rows, drows = profile_table(table_name, df)
        table_rows.append(trow)
        column_rows.extend(crows)
        cardinality_rows.extend(card_rows)
        date_rows.extend(drows)

    table_profile = pd.DataFrame(table_rows)
    column_profile = pd.DataFrame(column_rows)
    cardinality = pd.DataFrame(cardinality_rows)
    date_coverage = pd.DataFrame(date_rows)

    quality_checks = run_quality_checks(tables, snapshot_date)
    suspicious_values = compute_suspicious_values(tables)
    classification_summary = build_column_classification_summary()

    quality_checks.to_csv(outputs_dir / "data_quality_checks.csv", index=False)

    write_markdown_reports(
        docs_dir=docs_dir,
        table_profile=table_profile,
        column_profile=column_profile,
        cardinality=cardinality,
        date_coverage=date_coverage,
        quality_checks=quality_checks,
        suspicious_values=suspicious_values,
    )

    print("Profiling and data quality assessment completed.")
    print(f"Snapshot date inferred: {snapshot_date.date().isoformat()}")
    print(f"Quality checks executed: {len(quality_checks)} | Failed: {(quality_checks['status'] == 'FAIL').sum()}")


if __name__ == "__main__":
    main()
