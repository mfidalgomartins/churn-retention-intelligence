from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from itertools import product
from pathlib import Path

import numpy as np
import pandas as pd

ALL_TOKEN = "__all__"
RISK_ORDER = ["critical", "high", "medium", "low", "churned"]
OFFICIAL_DASHBOARD_FILENAME = "executive-retention-command-center.html"


def _json_records(df: pd.DataFrame) -> list[dict]:
    clean = df.replace({np.nan: None})
    return clean.to_dict(orient="records")


def _build_version(inputs: list[Path], builder_path: Path) -> str:
    h = hashlib.sha256()
    for p in sorted(inputs + [builder_path]):
        stat = p.stat()
        h.update(p.name.encode("utf-8"))
        h.update(str(stat.st_size).encode("utf-8"))
        h.update(str(int(stat.st_mtime)).encode("utf-8"))
    return h.hexdigest()[:12]


def _month_str(value: pd.Series) -> pd.Series:
    return pd.to_datetime(value).dt.strftime("%Y-%m")


def _expand_dims_with_all(df: pd.DataFrame, dims: list[str]) -> pd.DataFrame:
    expanded_frames: list[pd.DataFrame] = []
    for use_all_mask in product([0, 1], repeat=len(dims)):
        part = df.copy()
        for idx, dim in enumerate(dims):
            if use_all_mask[idx] == 1:
                part[dim] = ALL_TOKEN
        expanded_frames.append(part)
    return pd.concat(expanded_frames, ignore_index=True)


def _load_base_tables(project_root: Path) -> dict[str, pd.DataFrame]:
    processed = project_root / "data" / "processed"
    outputs = project_root / "outputs" / "tables"

    return {
        "features": pd.read_csv(processed / "customer_retention_features.csv"),
        "risk": pd.read_csv(processed / "customer_risk_scores.csv"),
        "monthly_dim": pd.read_csv(outputs / "monthly_dimensional_trend.csv", parse_dates=["month"]),
        "cohort": pd.read_csv(processed / "cohort_retention_table.csv", parse_dates=["cohort_month", "observation_month"]),
        "findings": pd.read_csv(outputs / "main_analysis_structured_findings.csv"),
    }


def _build_snapshot_tables(features: pd.DataFrame, risk: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    dims = ["segment", "region", "acquisition_channel", "plan_type"]
    risk_cols = [
        "customer_id",
        "churn_risk_score",
        "revenue_risk_score",
        "retention_priority_score",
        "risk_tier",
        "main_risk_driver",
        "recommended_action",
    ]

    snapshot = features.merge(risk[risk_cols], on="customer_id", how="left")
    snapshot["risk_tier"] = snapshot["risk_tier"].astype("object")
    missing_tier = snapshot["risk_tier"].isna()
    snapshot.loc[missing_tier & (snapshot["churn_flag"] == 1), "risk_tier"] = "churned"
    snapshot.loc[missing_tier & (snapshot["churn_flag"] == 0), "risk_tier"] = "low"
    snapshot["main_risk_driver"] = snapshot["main_risk_driver"].fillna("historical churn")
    snapshot["recommended_action"] = snapshot["recommended_action"].fillna("monitor only")

    snapshot["customer_count"] = 1
    snapshot["usage_sum"] = snapshot["usage_trend"]
    snapshot["support_sum"] = snapshot["support_tickets_90d"]
    snapshot["nps_sum"] = snapshot["nps_score_recent"]
    snapshot["payment_failure_sum"] = snapshot["payment_failure_flag"]
    snapshot["usage_decline_count"] = (snapshot["usage_trend"] < 0).astype(int)
    snapshot["at_risk_mrr_component"] = np.where(snapshot["at_risk_flag"] == 1, snapshot["current_mrr"], 0.0)
    snapshot["churned_revenue_component"] = np.where(snapshot["churn_flag"] == 1, snapshot["avg_monthly_revenue"], 0.0)

    snapshot_agg = (
        snapshot.groupby(dims + ["risk_tier", "churn_flag"], as_index=False)
        .agg(
            customer_count=("customer_count", "sum"),
            current_mrr_sum=("current_mrr", "sum"),
            usage_sum=("usage_sum", "sum"),
            support_sum=("support_sum", "sum"),
            nps_sum=("nps_sum", "sum"),
            payment_failure_sum=("payment_failure_sum", "sum"),
            usage_decline_count=("usage_decline_count", "sum"),
            at_risk_mrr_component=("at_risk_mrr_component", "sum"),
            churned_revenue_component=("churned_revenue_component", "sum"),
        )
        .reset_index(drop=True)
    )

    scored = snapshot[snapshot["churn_flag"] == 0].copy()
    scored = scored[
        [
            "customer_id",
            "segment",
            "region",
            "acquisition_channel",
            "plan_type",
            "current_mrr",
            "at_risk_flag",
            "usage_trend",
            "support_tickets_90d",
            "nps_score_recent",
            "payment_failure_flag",
            "churn_risk_score",
            "revenue_risk_score",
            "retention_priority_score",
            "risk_tier",
            "main_risk_driver",
            "recommended_action",
        ]
    ].copy()

    numeric_round = {
        "current_mrr": 2,
        "usage_trend": 2,
        "support_tickets_90d": 2,
        "nps_score_recent": 2,
        "churn_risk_score": 2,
        "revenue_risk_score": 2,
        "retention_priority_score": 2,
    }
    for col, digits in numeric_round.items():
        scored[col] = scored[col].round(digits)

    return snapshot_agg, scored


def _encode_monthly_fact(monthly_dim: pd.DataFrame, domains: dict[str, list[str]]) -> tuple[list[str], list[list[float]]]:
    dims = ["segment", "region", "acquisition_channel", "plan_type"]
    fact = monthly_dim[
        [
            "month",
            "segment",
            "region",
            "acquisition_channel",
            "plan_type",
            "active_customers_start",
            "active_mrr_start",
            "churned_customers",
            "churned_mrr",
        ]
    ].copy()
    fact["month"] = _month_str(fact["month"])

    months = sorted(fact["month"].unique().tolist())
    month_idx = {m: i for i, m in enumerate(months)}
    dim_idx = {d: {v: i for i, v in enumerate(domains[d])} for d in dims}

    rows: list[list[float]] = []
    for r in fact.itertuples(index=False):
        rows.append(
            [
                float(month_idx[r.month]),
                float(dim_idx["segment"][r.segment]),
                float(dim_idx["region"][r.region]),
                float(dim_idx["acquisition_channel"][r.acquisition_channel]),
                float(dim_idx["plan_type"][r.plan_type]),
                float(r.active_customers_start),
                round(float(r.active_mrr_start), 2),
                float(r.churned_customers),
                round(float(r.churned_mrr), 2),
            ]
        )

    return months, rows


def _build_risk_kpi_cube(scored: pd.DataFrame) -> pd.DataFrame:
    dims = ["segment", "region", "acquisition_channel", "plan_type"]

    base = scored.copy()
    base["scored_customers"] = 1
    base["high_risk_customers"] = base["risk_tier"].isin(["high", "critical"]).astype(int)
    base["critical_customers"] = (base["risk_tier"] == "critical").astype(int)
    base["priority_sum"] = base["retention_priority_score"]
    base["revenue_at_risk_component"] = np.where(
        (base["at_risk_flag"] == 1) | (base["risk_tier"].isin(["high", "critical"])),
        base["current_mrr"],
        0.0,
    )

    expanded_dims = _expand_dims_with_all(base, dims)

    all_rows = expanded_dims.copy()
    all_rows["risk_tier_filter"] = ALL_TOKEN

    tier_rows = expanded_dims.copy()
    tier_rows["risk_tier_filter"] = tier_rows["risk_tier"]

    cube_input = pd.concat([all_rows, tier_rows], ignore_index=True)

    cube = (
        cube_input.groupby(dims + ["risk_tier_filter"], as_index=False)
        .agg(
            scored_customers=("scored_customers", "sum"),
            total_current_mrr=("current_mrr", "sum"),
            revenue_at_risk=("revenue_at_risk_component", "sum"),
            high_risk_customers=("high_risk_customers", "sum"),
            critical_customers=("critical_customers", "sum"),
            priority_sum=("priority_sum", "sum"),
        )
        .reset_index(drop=True)
    )

    cube["avg_priority_score"] = np.where(
        cube["scored_customers"] > 0,
        cube["priority_sum"] / cube["scored_customers"],
        0.0,
    )

    for col in ["total_current_mrr", "revenue_at_risk", "avg_priority_score"]:
        cube[col] = cube[col].round(2)

    return cube


def _prepare_cohort_rows(cohort: pd.DataFrame) -> pd.DataFrame:
    rows = cohort.copy()
    rows["cohort_age_months"] = (
        (rows["observation_month"].dt.year - rows["cohort_month"].dt.year) * 12
        + (rows["observation_month"].dt.month - rows["cohort_month"].dt.month)
    )
    rows["cohort_month"] = rows["cohort_month"].dt.strftime("%Y-%m")
    rows["observation_month"] = rows["observation_month"].dt.strftime("%Y-%m")
    return rows[
        [
            "cohort_month",
            "observation_month",
            "cohort_age_months",
            "active_customers",
            "retained_customers",
            "retention_rate",
            "revenue_retention",
        ]
    ].copy()


def _executive_findings(findings: pd.DataFrame) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for row in findings.itertuples(index=False):
        title = str(row.section).split(". ", 1)[-1]
        out.append({"title": title, "body": str(row.result)})
    return out[:5]


def load_data(project_root: Path) -> dict:
    base = _load_base_tables(project_root)

    features = base["features"]
    risk = base["risk"]
    monthly_dim = base["monthly_dim"]
    cohort = base["cohort"]
    findings = base["findings"]

    snapshot_agg, scored_all = _build_snapshot_tables(features, risk)
    risk_kpi_cube = _build_risk_kpi_cube(scored_all)
    cohort_rows = _prepare_cohort_rows(cohort)

    dims = ["segment", "region", "acquisition_channel", "plan_type"]
    domains = {dim: sorted(features[dim].dropna().astype(str).unique().tolist()) for dim in dims}
    domains["risk_tier"] = ["critical", "high", "medium", "low", "churned"]
    months, monthly_fact_rows = _encode_monthly_fact(monthly_dim, domains)

    # Keep the ranking table payload intentionally bounded for fast dashboard load.
    scored = scored_all.sort_values(["retention_priority_score", "current_mrr"], ascending=[False, False]).head(800).copy()

    processed = project_root / "data" / "processed"
    outputs = project_root / "outputs" / "tables"
    builder_path = Path(__file__).resolve()
    source_inputs = [
        processed / "customer_retention_features.csv",
        processed / "customer_risk_scores.csv",
        processed / "cohort_retention_table.csv",
        outputs / "monthly_dimensional_trend.csv",
        outputs / "main_analysis_structured_findings.csv",
    ]
    version = _build_version(source_inputs, builder_path)

    data = {
        "meta": {
            "project": "Churn & Retention Intelligence System",
            "dashboard_version": version,
            "builder_version": "2.0.0",
            "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
            "coverage_start_month": months[0],
            "coverage_end_month": months[-1],
        },
        "domains": domains,
        "months": months,
        "executive_findings": _executive_findings(findings),
        "monthly_fact_rows": monthly_fact_rows,
        "risk_kpi_cube": _json_records(risk_kpi_cube),
        "snapshot_agg": _json_records(snapshot_agg),
        "scored_customers": _json_records(scored),
        "cohort_rows": _json_records(cohort_rows),
    }
    return data


def build_html(data_json: str, chart_js: str) -> str:
    # Defensive escaping for inline script boundaries.
    safe_chart_js = chart_js.replace("</script", "<\\/script")
    safe_data_json = data_json.replace("</script", "<\\/script")

    template = r'''<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Churn & Retention Command Center</title>
  <style>
    :root {
      --bg: #f2f5f9;
      --surface: #ffffff;
      --surface-soft: #f8fafc;
      --surface-elev: #ffffff;
      --ink: #0b1320;
      --ink-soft: #425466;
      --brand: #0f3b66;
      --brand-2: #1c5a8f;
      --danger: #b42318;
      --success: #0f766e;
      --warning: #a16207;
      --border: #d4dee9;
      --shadow: 0 12px 28px rgba(11, 19, 32, 0.08);
      --radius: 18px;
      --space: 16px;
      --font: "Avenir Next", "Segoe UI", "Helvetica Neue", sans-serif;
      --font-strong: "IBM Plex Sans", "Avenir Next", "Segoe UI", sans-serif;
      --input-bg: #f5fbff;
      --input-border: #8aaec4;
      --input-ink: #173a51;
      --thead-bg: #f8fafc;
      --row-alt: #fbfdff;
      --table-row-border: #edf2f7;
      --stack-border: #e2e8f0;
      --header-grad-1: #113f67;
      --header-grad-2: #1a4f77;
      --header-grad-3: #102f49;
      --header-ink: #f1f6fb;
      --header-soft: #d7e9f5;
      --header-soft-2: #cde0ee;
      --header-strong: #e8f3fb;
      --toggle-bg: rgba(255, 255, 255, 0.13);
      --toggle-border: rgba(226, 242, 255, 0.45);
      --toggle-ink: #eaf5ff;
    }

    body[data-theme="dark"] {
      --bg: #080f1a;
      --surface: #0f1725;
      --surface-soft: #0c1422;
      --surface-elev: #101b2c;
      --ink: #e6edf7;
      --ink-soft: #9db1c8;
      --brand: #9dc7e8;
      --brand-2: #63a1cf;
      --danger: #f07770;
      --success: #4bc7bc;
      --warning: #e5c167;
      --border: #203044;
      --shadow: 0 14px 30px rgba(0, 0, 0, 0.35);
      --input-bg: #122136;
      --input-border: #314962;
      --input-ink: #e4eef8;
      --thead-bg: #132033;
      --row-alt: #111b2b;
      --table-row-border: #24364d;
      --stack-border: #2e425b;
      --header-grad-1: #09121f;
      --header-grad-2: #0f1f34;
      --header-grad-3: #152942;
      --header-ink: #e7f0fb;
      --header-soft: #b9cde2;
      --header-soft-2: #9fb8d1;
      --header-strong: #f1f7ff;
      --toggle-bg: rgba(159, 200, 234, 0.16);
      --toggle-border: rgba(159, 200, 234, 0.45);
      --toggle-ink: #e6f3ff;
    }

    * { box-sizing: border-box; }

    body {
      margin: 0;
      font-family: var(--font);
      color: var(--ink);
      background:
        radial-gradient(1200px 480px at 100% 0%, rgba(20, 72, 120, 0.14), transparent 60%),
        linear-gradient(180deg, color-mix(in srgb, var(--bg) 88%, #ffffff 12%) 0%, var(--bg) 38%);
      line-height: 1.4;
    }

    .container {
      width: min(1720px, 100% - 38px);
      margin: 20px auto 32px;
      display: grid;
      gap: var(--space);
    }

    .panel {
      background: var(--surface);
      border: 1px solid var(--border);
      border-radius: var(--radius);
      box-shadow: var(--shadow);
      min-width: 0;
      overflow: hidden;
    }

    .header {
      background: linear-gradient(132deg, var(--header-grad-1) 0%, var(--header-grad-2) 42%, var(--header-grad-3) 100%);
      color: var(--header-ink);
      padding: 22px;
      display: grid;
      gap: 16px;
    }

    .header-top {
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto;
      gap: 12px;
      align-items: start;
      min-width: 0;
    }

    .title {
      margin: 0;
      font-size: clamp(22px, 2.3vw, 31px);
      line-height: 1.1;
      overflow-wrap: anywhere;
      font-family: var(--font-strong);
      letter-spacing: 0.2px;
    }

    .subtitle {
      margin: 6px 0 0;
      max-width: 980px;
      font-size: 14px;
      color: var(--header-soft);
      overflow-wrap: anywhere;
      max-width: 950px;
    }

    .header-meta {
      text-align: right;
      font-size: 12px;
      color: var(--header-soft-2);
      white-space: nowrap;
    }

    .header-meta strong { color: var(--header-strong); }

    .filters {
      display: grid;
      grid-template-columns: repeat(9, minmax(0, 1fr));
      gap: 10px;
      min-width: 0;
    }

    .filter {
      display: grid;
      gap: 4px;
      min-width: 0;
    }

    .filter label {
      font-size: 11px;
      letter-spacing: 0.35px;
      text-transform: uppercase;
      font-weight: 700;
      color: var(--header-soft);
    }

    .filter input,
    .filter select {
      min-height: 36px;
      width: 100%;
      border: 1px solid var(--input-border);
      border-radius: 9px;
      background: var(--input-bg);
      color: var(--input-ink);
      padding: 7px 10px;
      font-size: 13px;
      min-width: 0;
    }

    .theme-toggle-wrap {
      display: flex;
      align-items: end;
      justify-content: flex-end;
    }

    .header-actions {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 8px;
      width: 100%;
    }

    .action-btn {
      min-height: 36px;
      border-radius: 10px;
      border: 1px solid var(--toggle-border);
      background: var(--toggle-bg);
      color: var(--toggle-ink);
      font-size: 12px;
      font-weight: 700;
      padding: 7px 11px;
      width: 100%;
      cursor: pointer;
    }

    .action-btn:hover { filter: brightness(1.07); }

    .scope-note {
      font-size: 12px;
      color: var(--header-soft);
      overflow-wrap: anywhere;
    }

    .summary-strip {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 12px;
    }

    .summary-card {
      background: var(--surface);
      border: 1px solid var(--border);
      border-left: 4px solid var(--brand-2);
      border-radius: 12px;
      padding: 12px 13px;
      min-height: 98px;
      overflow: hidden;
    }

    .summary-card.summary-card-1 { border-left-color: #1d4e89; }
    .summary-card.summary-card-2 { border-left-color: #9f1239; }
    .summary-card.summary-card-3 { border-left-color: #0f766e; }
    .summary-card.summary-card-4 { border-left-color: #b45309; }

    .summary-title {
      margin: 0;
      font-size: 10px;
      text-transform: uppercase;
      letter-spacing: 0.6px;
      color: var(--ink-soft);
      font-weight: 700;
    }

    .summary-body {
      margin: 7px 0 0;
      font-size: 13px;
      color: var(--ink);
      font-weight: 650;
      overflow-wrap: anywhere;
    }

    .section-header {
      background: var(--surface-soft);
      padding: 14px 16px;
      border-bottom: 1px solid var(--border);
      border-left: 4px solid #c7d8e8;
    }

    .section-title {
      margin: 0;
      font-size: 18px;
      line-height: 1.2;
      font-family: var(--font-strong);
    }

    .section-subtitle {
      margin: 4px 0 0;
      font-size: 12px;
      color: var(--ink-soft);
      overflow-wrap: anywhere;
    }

    .kpi-grid {
      padding: 14px;
      display: grid;
      grid-template-columns: repeat(7, minmax(0, 1fr));
      gap: 12px;
    }

    .kpi-card {
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 12px;
      min-height: 122px;
      overflow: hidden;
      background: linear-gradient(180deg, var(--surface-elev) 0%, var(--surface) 100%);
    }

    .kpi-card.kpi-primary { border-top: 3px solid #0f3b66; }
    .kpi-card.kpi-danger { border-top: 3px solid #b42318; }
    .kpi-card.kpi-warning { border-top: 3px solid #b45309; }
    .kpi-card.kpi-success { border-top: 3px solid #0f766e; }

    .kpi-label {
      font-size: 10px;
      text-transform: uppercase;
      letter-spacing: 0.55px;
      color: var(--ink-soft);
      font-weight: 700;
      overflow-wrap: anywhere;
    }

    .kpi-value {
      margin-top: 5px;
      font-size: clamp(22px, 2.2vw, 28px);
      line-height: 1.05;
      color: var(--brand);
      font-weight: 750;
      overflow-wrap: anywhere;
    }

    .kpi-note {
      margin-top: 4px;
      font-size: 12px;
      color: var(--ink-soft);
      overflow-wrap: anywhere;
    }

    .kpi-delta {
      margin-top: 4px;
      font-size: 11px;
      font-weight: 700;
      overflow-wrap: anywhere;
    }

    .delta-up { color: #b91c1c; }
    .delta-down { color: #166534; }
    .delta-flat { color: #667085; }

    .chart-grid-2,
    .chart-grid-4,
    .chart-grid-risk {
      padding: 14px;
      display: grid;
      gap: 12px;
      min-width: 0;
    }

    .chart-grid-2 { grid-template-columns: repeat(2, minmax(0, 1fr)); }
    .chart-grid-4 { grid-template-columns: repeat(4, minmax(0, 1fr)); }
    .chart-grid-risk { grid-template-columns: 1.1fr 0.9fr; }

    .chart-card {
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 12px;
      background: var(--surface-elev);
      min-width: 0;
      overflow: hidden;
    }

    .chart-title {
      margin: 0 0 10px;
      font-size: 13px;
      line-height: 1.25;
      color: var(--ink);
      font-weight: 700;
      overflow-wrap: anywhere;
      letter-spacing: 0.15px;
    }

    .canvas-wrap {
      position: relative;
      width: 100%;
      height: 310px;
      min-width: 0;
      min-height: 310px;
    }

    .canvas-wrap.tall {
      height: 360px;
      min-height: 360px;
    }

    .diagnostic-metrics {
      padding: 0 14px 14px;
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 12px;
    }

    .metric-card {
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 11px;
      min-height: 88px;
      overflow: hidden;
      background: var(--surface-soft);
    }

    .metric-card h5 {
      margin: 0;
      font-size: 11px;
      text-transform: uppercase;
      letter-spacing: 0.35px;
      color: var(--ink-soft);
    }

    .metric-card p {
      margin: 6px 0 0;
      font-size: 13px;
      color: var(--ink);
      font-weight: 700;
      overflow-wrap: anywhere;
    }

    .risk-layout {
      padding: 14px;
      display: grid;
      gap: 12px;
      grid-template-columns: minmax(0, 1.45fr) minmax(0, 1fr);
      min-width: 0;
    }

    .table-card,
    .mini-card {
      border: 1px solid var(--border);
      border-radius: 11px;
      background: var(--surface-elev);
      min-width: 0;
      overflow: hidden;
    }

    .table-head {
      padding: 12px 13px;
      border-bottom: 1px solid var(--border);
      font-size: 13px;
      font-weight: 700;
      color: var(--ink);
      overflow-wrap: anywhere;
      background: var(--surface-soft);
    }

    .table-wrap {
      overflow: auto;
      max-height: 460px;
      width: 100%;
      min-width: 0;
    }

    table {
      border-collapse: collapse;
      width: 100%;
      min-width: 860px;
      font-size: 12px;
    }

    thead th {
      position: sticky;
      top: 0;
      z-index: 2;
      background: var(--thead-bg);
      border-bottom: 1px solid var(--border);
      color: var(--ink-soft);
      text-align: left;
      padding: 8px;
      cursor: pointer;
      white-space: nowrap;
    }

    tbody td {
      border-bottom: 1px solid var(--table-row-border);
      padding: 8px;
      color: var(--ink);
      white-space: nowrap;
    }

    tbody tr:nth-child(even) { background: var(--row-alt); }

    .risk-side {
      display: grid;
      gap: 9px;
      min-width: 0;
      grid-template-rows: auto auto 1fr;
    }

    .stacked-tier {
      width: 100%;
      height: 14px;
      border-radius: 999px;
      overflow: hidden;
      display: flex;
      border: 1px solid var(--stack-border);
      margin-top: 8px;
    }

    .stacked-tier > div { height: 100%; min-width: 2px; }

    .tier-legend {
      margin-top: 8px;
      display: grid;
      gap: 6px;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      font-size: 12px;
      color: var(--ink-soft);
    }

    .pill {
      display: inline-block;
      border-radius: 999px;
      padding: 2px 7px;
      font-size: 11px;
      font-weight: 700;
      color: #fff;
      margin-right: 6px;
    }

    .tier-critical { background: #8e1b1b; }
    .tier-high { background: #c0392b; }
    .tier-medium { background: #d4a20f; color: #0f172a; }
    .tier-low { background: #1f78b4; }
    .tier-churned { background: #475467; }

    .actions-grid {
      padding: 14px;
      display: grid;
      gap: 12px;
      grid-template-columns: repeat(5, minmax(0, 1fr));
    }

    .action-card {
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 11px;
      min-height: 126px;
      overflow: hidden;
      background: var(--surface-soft);
    }

    .action-card h5 {
      margin: 0 0 6px;
      font-size: 13px;
      color: var(--ink);
      text-transform: capitalize;
      overflow-wrap: anywhere;
    }

    .action-card .meta {
      text-align: left;
      white-space: normal;
      color: var(--ink-soft);
      display: grid;
      gap: 4px;
      font-size: 12px;
    }

    .footer {
      padding: 11px 13px;
      font-size: 12px;
      line-height: 1.5;
      color: var(--ink-soft);
      overflow-wrap: anywhere;
    }

    .mono { font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; }

    @media (max-width: 1580px) {
      .filters { grid-template-columns: repeat(5, minmax(0, 1fr)); }
      .summary-strip { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .kpi-grid { grid-template-columns: repeat(4, minmax(0, 1fr)); }
      .chart-grid-4 { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .actions-grid { grid-template-columns: repeat(3, minmax(0, 1fr)); }
    }

    @media (max-width: 1200px) {
      .header-top { grid-template-columns: 1fr; }
      .header-meta { text-align: left; white-space: normal; }
      .theme-toggle-wrap { justify-content: stretch; }
      .chart-grid-2 { grid-template-columns: 1fr; }
      .chart-grid-risk { grid-template-columns: 1fr; }
      .risk-layout { grid-template-columns: 1fr; }
      .risk-side { grid-template-rows: auto; }
      .actions-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .diagnostic-metrics { grid-template-columns: repeat(2, minmax(0, 1fr)); }
    }

    @media (max-width: 760px) {
      .container { width: calc(100% - 16px); margin: 10px auto 16px; }
      .filters { grid-template-columns: 1fr 1fr; }
      .summary-strip,
      .kpi-grid,
      .chart-grid-4,
      .diagnostic-metrics,
      .actions-grid { grid-template-columns: 1fr; }
      .canvas-wrap { height: 270px; min-height: 270px; }
      .canvas-wrap.tall { height: 320px; min-height: 320px; }
      table { min-width: 760px; }
    }

    @media print {
      body {
        background: #ffffff !important;
        color: #0b1320;
      }

      .container {
        width: 100%;
        margin: 0;
        gap: 12px;
      }

      .panel {
        box-shadow: none;
        border-color: #cbd5e1;
        page-break-inside: avoid;
      }

      .filters,
      .theme-toggle-wrap,
      .scope-note { display: none !important; }

      .header {
        padding: 16px;
        border: 1px solid #cbd5e1;
      }

      .summary-strip,
      .kpi-grid,
      .chart-grid-2,
      .chart-grid-4,
      .chart-grid-risk,
      .risk-layout,
      .actions-grid,
      .diagnostic-metrics {
        gap: 10px;
      }

      .chart-card,
      .kpi-card,
      .summary-card,
      .metric-card,
      .action-card,
      .table-card,
      .mini-card {
        box-shadow: none;
        border-color: #cbd5e1;
      }

      .canvas-wrap,
      .canvas-wrap.tall {
        height: 260px;
        min-height: 260px;
      }

      table {
        min-width: 0;
        font-size: 10px;
      }

      thead th,
      tbody td { padding: 6px; }
    }
  </style>
</head>
<body>
  <div class="container">
    <section class="panel header">
      <div class="header-top">
        <div>
          <h1 class="title">Churn & Retention Command Center</h1>
          <p class="subtitle">Core business question: Where is the company losing future revenue, which customers are most at risk, and what actions should be prioritized?</p>
        </div>
        <div class="header-meta">
          <div><strong>Available period:</strong> <span id="coverageText"></span></div>
          <div><strong>Selected period:</strong> <span id="selectedPeriodText"></span></div>
        </div>
      </div>

      <div class="filters">
        <div class="filter"><label for="filterStartMonth">Date Start</label><input type="date" id="filterStartMonth" /></div>
        <div class="filter"><label for="filterEndMonth">Date End</label><input type="date" id="filterEndMonth" /></div>
        <div class="filter">
          <label for="filterPeriodPreset">Period Preset</label>
          <select id="filterPeriodPreset">
            <option value="all">All period</option>
            <option value="12m">Last 12 months</option>
            <option value="6m">Last 6 months</option>
            <option value="3m">Last 3 months</option>
            <option value="custom">Custom range</option>
          </select>
        </div>
        <div class="filter"><label for="filterSegment">Segment</label><select id="filterSegment"></select></div>
        <div class="filter"><label for="filterRegion">Region</label><select id="filterRegion"></select></div>
        <div class="filter"><label for="filterChannel">Acquisition Channel</label><select id="filterChannel"></select></div>
        <div class="filter"><label for="filterPlan">Plan Type</label><select id="filterPlan"></select></div>
        <div class="filter"><label for="filterRiskTier">Risk Tier</label><select id="filterRiskTier"></select></div>
        <div class="theme-toggle-wrap">
          <div class="header-actions">
            <button id="themeToggle" class="action-btn" type="button">Dark Mode</button>
            <button id="printBtn" class="action-btn" type="button">Print</button>
          </div>
        </div>
      </div>
      <div class="scope-note" id="scopeNote"></div>
    </section>

    <section class="summary-strip" id="summaryStrip"></section>

    <section class="panel">
      <div class="section-header">
        <h3 class="section-title">KPI Snapshot</h3>
        <p class="section-subtitle">Official KPI values from governed cubes. Trend KPIs are anchored to selected period end month.</p>
      </div>
      <div class="kpi-grid">
        <div class="kpi-card kpi-primary"><div class="kpi-label">Active Customers</div><div class="kpi-value" id="kpiActive"></div><div class="kpi-note" id="kpiActiveNote"></div><div class="kpi-delta" id="kpiActiveDelta"></div></div>
        <div class="kpi-card kpi-danger"><div class="kpi-label">Customer Churn Rate</div><div class="kpi-value" id="kpiCustChurn"></div><div class="kpi-note" id="kpiCustChurnNote"></div><div class="kpi-delta" id="kpiCustChurnDelta"></div></div>
        <div class="kpi-card kpi-danger"><div class="kpi-label">Revenue Churn Rate</div><div class="kpi-value" id="kpiRevChurn"></div><div class="kpi-note" id="kpiRevChurnNote"></div><div class="kpi-delta" id="kpiRevChurnDelta"></div></div>
        <div class="kpi-card kpi-warning"><div class="kpi-label">Revenue at Risk</div><div class="kpi-value" id="kpiRevRisk"></div><div class="kpi-note" id="kpiRevRiskNote"></div><div class="kpi-delta" id="kpiRevRiskDelta"></div></div>
        <div class="kpi-card kpi-warning"><div class="kpi-label">High-Risk Customers</div><div class="kpi-value" id="kpiHighRisk"></div><div class="kpi-note" id="kpiHighRiskNote"></div><div class="kpi-delta" id="kpiHighRiskDelta"></div></div>
        <div class="kpi-card kpi-danger"><div class="kpi-label">Critical-Risk Customers</div><div class="kpi-value" id="kpiCriticalRisk"></div><div class="kpi-note" id="kpiCriticalRiskNote"></div><div class="kpi-delta" id="kpiCriticalRiskDelta"></div></div>
        <div class="kpi-card kpi-primary"><div class="kpi-label">Avg Priority Score</div><div class="kpi-value" id="kpiAvgPriority"></div><div class="kpi-note" id="kpiAvgPriorityNote"></div><div class="kpi-delta" id="kpiAvgPriorityDelta"></div></div>
      </div>
    </section>

    <section class="panel">
      <div class="section-header">
        <h3 class="section-title">Primary Retention</h3>
        <p class="section-subtitle">Trend signals for volume loss, value loss, and cohort durability.</p>
      </div>
      <div class="chart-grid-2">
        <div class="chart-card"><h4 class="chart-title">Customer Churn Trend (Monthly)</h4><div class="canvas-wrap"><canvas id="chartCustomerChurnTrend"></canvas></div></div>
        <div class="chart-card"><h4 class="chart-title">Revenue Churn Trend (Monthly)</h4><div class="canvas-wrap"><canvas id="chartRevenueChurnTrend"></canvas></div></div>
        <div class="chart-card"><h4 class="chart-title">Cohort Retention Curves (Recent Cohorts)</h4><div class="canvas-wrap tall"><canvas id="chartCohortRetention"></canvas></div></div>
        <div class="chart-card"><h4 class="chart-title">Cohort Revenue Retention (Latest Observation)</h4><div class="canvas-wrap tall"><canvas id="chartRevenueRetentionCohort"></canvas></div></div>
      </div>
    </section>

    <section class="panel">
      <div class="section-header">
        <h3 class="section-title">Diagnostic View</h3>
        <p class="section-subtitle">Commercial concentration and behavioral deterioration signals.</p>
      </div>
      <div class="chart-grid-4">
        <div class="chart-card"><h4 class="chart-title">Where Churn Is Highest: Segment</h4><div class="canvas-wrap"><canvas id="chartChurnSegment"></canvas></div></div>
        <div class="chart-card"><h4 class="chart-title">Where Churn Is Highest: Region</h4><div class="canvas-wrap"><canvas id="chartChurnRegion"></canvas></div></div>
        <div class="chart-card"><h4 class="chart-title">Where Churn Is Highest: Acquisition Channel</h4><div class="canvas-wrap"><canvas id="chartChurnChannel"></canvas></div></div>
        <div class="chart-card"><h4 class="chart-title">Where Churn Is Highest: Plan Type</h4><div class="canvas-wrap"><canvas id="chartChurnPlan"></canvas></div></div>
      </div>
      <div class="diagnostic-metrics">
        <div class="metric-card"><h5>Usage Trend</h5><p id="diagUsage"></p></div>
        <div class="metric-card"><h5>Support Burden</h5><p id="diagSupport"></p></div>
        <div class="metric-card"><h5>Failed Payment Rate</h5><p id="diagFailed"></p></div>
        <div class="metric-card"><h5>NPS Gap</h5><p id="diagNps"></p></div>
      </div>
    </section>

    <section class="panel">
      <div class="section-header">
        <h3 class="section-title">Risk Prioritization</h3>
        <p class="section-subtitle">Intervention queue ranked by risk likelihood and revenue importance.</p>
      </div>
      <div class="risk-layout">
        <div class="table-card">
          <div class="table-head">Ranked High-Priority Customers</div>
          <div class="table-wrap">
            <table id="priorityTable">
              <thead>
                <tr>
                  <th data-sort="customer_id">customer_id</th>
                  <th data-sort="segment">segment</th>
                  <th data-sort="current_mrr">current_mrr</th>
                  <th data-sort="churn_risk_score">churn_risk_score</th>
                  <th data-sort="revenue_risk_score">revenue_risk_score</th>
                  <th data-sort="retention_priority_score">retention_priority_score</th>
                  <th data-sort="main_risk_driver">main_risk_driver</th>
                  <th data-sort="recommended_action">recommended_action</th>
                </tr>
              </thead>
              <tbody id="priorityTableBody"></tbody>
            </table>
          </div>
        </div>
        <div class="risk-side">
          <div class="mini-card" style="padding:10px;">
            <h4 class="chart-title" style="margin-bottom:4px;">Risk Tier Distribution</h4>
            <div class="stacked-tier" id="riskTierStack"></div>
            <div class="tier-legend" id="riskTierLegend"></div>
          </div>
          <div class="chart-grid-risk" style="padding:0;">
            <div class="chart-card"><h4 class="chart-title">Top Risk Drivers in Scope</h4><div class="canvas-wrap"><canvas id="chartRiskDrivers"></canvas></div></div>
            <div class="chart-card"><h4 class="chart-title">Revenue at Risk Concentration by Segment</h4><div class="canvas-wrap"><canvas id="chartRevenueRiskSegment"></canvas></div></div>
          </div>
        </div>
      </div>
    </section>

    <section class="panel">
      <div class="section-header">
        <h3 class="section-title">Action Orientation</h3>
        <p class="section-subtitle">Execution playbooks grouped by customer volume and MRR coverage.</p>
      </div>
      <div class="actions-grid" id="actionsGrid"></div>
    </section>

    <section class="panel footer">
      <strong>Presentation note:</strong> Use this view to align leadership on retention health, risk concentration, and intervention priorities.
      <div style="margin-top:6px;"><strong>Caveat:</strong> Cohort charts are portfolio-level and date-filtered; commercial and risk filters apply to trends, diagnostics, and prioritization views.</div>
    </section>
  </div>

  <script>
__CHART_JS__
  </script>
  <script>
const DATA = __DATA_JSON__;
const ALL = '__all__';
const THEME_KEY = 'cri_dashboard_theme';
const charts = {};
const tableSortState = { key: 'retention_priority_score', dir: 'desc' };
const MONTHS = DATA.months || [];
const DIM_INDEX = {
  segment: new Map((DATA.domains.segment || []).map((v, i) => [v, i])),
  region: new Map((DATA.domains.region || []).map((v, i) => [v, i])),
  acquisition_channel: new Map((DATA.domains.acquisition_channel || []).map((v, i) => [v, i])),
  plan_type: new Map((DATA.domains.plan_type || []).map((v, i) => [v, i])),
};

const numFmt = new Intl.NumberFormat('en-US');
const curFmt = new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 0 });
const cur2Fmt = new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD', minimumFractionDigits: 2, maximumFractionDigits: 2 });
const pctFmt = new Intl.NumberFormat('en-US', { style: 'percent', minimumFractionDigits: 1, maximumFractionDigits: 1 });

function pct(v) { return Number.isFinite(v) ? pctFmt.format(v) : 'n/a'; }
function money(v) { return Number.isFinite(v) ? curFmt.format(v) : 'n/a'; }
function money2(v) { return Number.isFinite(v) ? cur2Fmt.format(v) : 'n/a'; }
function num(v, d = 1) { return Number.isFinite(v) ? Number(v).toFixed(d) : 'n/a'; }

function currentTheme() {
  return document.body.dataset.theme === 'dark' ? 'dark' : 'light';
}

function resolveInitialTheme() {
  try {
    const saved = localStorage.getItem(THEME_KEY);
    if (saved === 'dark' || saved === 'light') return saved;
  } catch (e) {}
  return (window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches) ? 'dark' : 'light';
}

function applyTheme(theme) {
  const next = theme === 'dark' ? 'dark' : 'light';
  document.body.dataset.theme = next;
  const btn = document.getElementById('themeToggle');
  if (btn) btn.textContent = next === 'dark' ? 'Light Mode' : 'Dark Mode';
  try { localStorage.setItem(THEME_KEY, next); } catch (e) {}
}

function toggleTheme() {
  applyTheme(currentTheme() === 'dark' ? 'light' : 'dark');
  renderDashboard();
}

function chartPalette() {
  if (currentTheme() === 'dark') {
    return {
      text: '#dbe9f7',
      muted: '#a6bdd6',
      grid: 'rgba(163, 186, 211, 0.18)',
      tooltipBg: '#0f1e30',
      tooltipText: '#eaf3ff',
      lineCustomer: '#f48d86',
      areaCustomer: 'rgba(244, 141, 134, 0.22)',
      lineRevenue: '#78c3f1',
      areaRevenue: 'rgba(120, 195, 241, 0.22)',
      cohort: ['#78c3f1', '#5ba8de', '#8bd3ff', '#6ec6d3', '#65b0ff', '#8daff0', '#a7d7ff', '#94e7d2'],
      churnBars: ['#7cbde7', '#69acd9', '#5a9ccc', '#4d90c3'],
      driverBar: '#6cb6e3',
      driverBorder: '#7fc4ed',
      riskBar: '#f48d86',
      riskBorder: '#f5a49f',
      retentionBar: 'rgba(104, 203, 186, 0.78)',
      retentionBorder: '#67ceb6',
    };
  }
  return {
    text: '#334155',
    muted: '#475569',
    grid: 'rgba(51, 65, 85, 0.16)',
    tooltipBg: '#1f2937',
    tooltipText: '#f8fafc',
    lineCustomer: '#b42318',
    areaCustomer: 'rgba(180,35,24,0.16)',
    lineRevenue: '#174f77',
    areaRevenue: 'rgba(23,79,119,0.15)',
    cohort: ['#0b3c5d', '#174f77', '#2f6f97', '#2e86c1', '#4fa0d1', '#70b5de', '#9bcced', '#bddff6'],
    churnBars: ['#0b3c5d', '#174f77', '#2e86c1', '#4fa0d1'],
    driverBar: '#174f77cc',
    driverBorder: '#174f77',
    riskBar: '#b42318cc',
    riskBorder: '#b42318',
    retentionBar: 'rgba(15,118,110,0.70)',
    retentionBorder: '#0f766e',
  };
}

function deltaClass(delta, invert = false) {
  if (!Number.isFinite(delta) || Math.abs(delta) < 1e-9) return 'delta-flat';
  if (!invert) return delta > 0 ? 'delta-up' : 'delta-down';
  return delta > 0 ? 'delta-down' : 'delta-up';
}

function setDelta(id, text, delta = 0, invert = false) {
  const el = document.getElementById(id);
  el.textContent = text;
  el.className = `kpi-delta ${deltaClass(delta, invert)}`;
}

function addOption(select, value, label) {
  const opt = document.createElement('option');
  opt.value = value;
  opt.textContent = label;
  select.appendChild(opt);
}

function monthToStartDate(month) {
  return `${month}-01`;
}

function monthToEndDate(month) {
  const [y, m] = month.split('-').map(Number);
  const end = new Date(Date.UTC(y, m, 0));
  return end.toISOString().slice(0, 10);
}

function dateToMonth(dateStr, fallbackMonth) {
  if (!dateStr || dateStr.length < 7) return fallbackMonth;
  return dateStr.slice(0, 7);
}

function setSelectedPeriodLabel(startDate, endDate) {
  document.getElementById('selectedPeriodText').textContent = `${startDate} to ${endDate}`;
}

function applyPeriodPreset(preset) {
  const startEl = document.getElementById('filterStartMonth');
  const endEl = document.getElementById('filterEndMonth');
  const coverageStart = DATA.meta.coverage_start_month;
  const coverageEnd = DATA.meta.coverage_end_month;
  const endIdx = Math.max(0, MONTHS.indexOf(coverageEnd));

  let start = coverageStart;
  if (preset === '12m') start = MONTHS[Math.max(0, endIdx - 11)] || coverageStart;
  else if (preset === '6m') start = MONTHS[Math.max(0, endIdx - 5)] || coverageStart;
  else if (preset === '3m') start = MONTHS[Math.max(0, endIdx - 2)] || coverageStart;
  else if (preset === 'all') start = coverageStart;
  else return;

  startEl.value = monthToStartDate(start);
  endEl.value = monthToEndDate(coverageEnd);
  setSelectedPeriodLabel(startEl.value, endEl.value);
}

function populateFilters() {
  const domains = DATA.domains;
  const map = [
    ['filterSegment', domains.segment],
    ['filterRegion', domains.region],
    ['filterChannel', domains.acquisition_channel],
    ['filterPlan', domains.plan_type],
    ['filterRiskTier', domains.risk_tier],
  ];

  map.forEach(([id, values]) => {
    const el = document.getElementById(id);
    el.innerHTML = '';
    addOption(el, 'all', 'All');
    values.forEach(v => addOption(el, v, v));
  });

  document.getElementById('filterStartMonth').value = monthToStartDate(DATA.meta.coverage_start_month);
  document.getElementById('filterEndMonth').value = monthToEndDate(DATA.meta.coverage_end_month);
  document.getElementById('filterPeriodPreset').value = 'all';
  document.getElementById('coverageText').textContent = `${DATA.meta.coverage_start_month} to ${DATA.meta.coverage_end_month}`;
  setSelectedPeriodLabel(
    document.getElementById('filterStartMonth').value,
    document.getElementById('filterEndMonth').value,
  );

  document.getElementById('filterStartMonth').min = monthToStartDate(DATA.meta.coverage_start_month);
  document.getElementById('filterStartMonth').max = monthToEndDate(DATA.meta.coverage_end_month);
  document.getElementById('filterEndMonth').min = monthToStartDate(DATA.meta.coverage_start_month);
  document.getElementById('filterEndMonth').max = monthToEndDate(DATA.meta.coverage_end_month);

  ['filterSegment','filterRegion','filterChannel','filterPlan','filterRiskTier'].forEach(id => {
    document.getElementById(id).addEventListener('change', renderDashboard);
  });
  document.getElementById('filterPeriodPreset').addEventListener('change', (e) => {
    applyPeriodPreset(e.target.value);
    renderDashboard();
  });
  document.getElementById('filterStartMonth').addEventListener('change', () => {
    document.getElementById('filterPeriodPreset').value = 'custom';
    renderDashboard();
  });
  document.getElementById('filterEndMonth').addEventListener('change', () => {
    document.getElementById('filterPeriodPreset').value = 'custom';
    renderDashboard();
  });
  document.getElementById('themeToggle').addEventListener('click', toggleTheme);
  document.getElementById('printBtn').addEventListener('click', () => window.print());

  document.querySelectorAll('#priorityTable thead th').forEach(th => {
    th.addEventListener('click', () => {
      const key = th.getAttribute('data-sort');
      if (!key) return;
      if (tableSortState.key === key) {
        tableSortState.dir = tableSortState.dir === 'asc' ? 'desc' : 'asc';
      } else {
        tableSortState.key = key;
        tableSortState.dir = ['customer_id', 'segment', 'main_risk_driver', 'recommended_action'].includes(key) ? 'asc' : 'desc';
      }
      renderDashboard();
    });
  });
}

function getFilters() {
  const startDate = document.getElementById('filterStartMonth').value;
  const endDate = document.getElementById('filterEndMonth').value;
  let startMonth = dateToMonth(startDate, DATA.meta.coverage_start_month);
  let endMonth = dateToMonth(endDate, DATA.meta.coverage_end_month);
  const f = {
    startDate,
    endDate,
    startMonth,
    endMonth,
    periodPreset: document.getElementById('filterPeriodPreset').value,
    segment: document.getElementById('filterSegment').value,
    region: document.getElementById('filterRegion').value,
    channel: document.getElementById('filterChannel').value,
    plan: document.getElementById('filterPlan').value,
    riskTier: document.getElementById('filterRiskTier').value,
  };

  if (f.startDate > f.endDate) {
    f.endDate = f.startDate;
    document.getElementById('filterEndMonth').value = f.endDate;
    endMonth = dateToMonth(f.endDate, DATA.meta.coverage_end_month);
    f.endMonth = endMonth;
  }
  setSelectedPeriodLabel(f.startDate, f.endDate);
  return f;
}

function filterLabel(v) {
  return v === 'all' ? ALL : v;
}

function monthInRange(month, startMonth, endMonth) {
  return month >= startMonth && month <= endMonth;
}

function matchDims(row, filters) {
  if (filters.segment !== 'all' && row.segment !== filters.segment) return false;
  if (filters.region !== 'all' && row.region !== filters.region) return false;
  if (filters.channel !== 'all' && row.acquisition_channel !== filters.channel) return false;
  if (filters.plan !== 'all' && row.plan_type !== filters.plan) return false;
  return true;
}

function getTrendRows(filters) {
  const segIdx = filters.segment === 'all' ? null : DIM_INDEX.segment.get(filters.segment);
  const regIdx = filters.region === 'all' ? null : DIM_INDEX.region.get(filters.region);
  const chnIdx = filters.channel === 'all' ? null : DIM_INDEX.acquisition_channel.get(filters.channel);
  const planIdx = filters.plan === 'all' ? null : DIM_INDEX.plan_type.get(filters.plan);

  const monthAgg = new Map();
  (DATA.monthly_fact_rows || []).forEach(row => {
    const month = MONTHS[Number(row[0])];
    if (!month || !monthInRange(month, filters.startMonth, filters.endMonth)) return;

    if (segIdx !== null && Number(row[1]) !== segIdx) return;
    if (regIdx !== null && Number(row[2]) !== regIdx) return;
    if (chnIdx !== null && Number(row[3]) !== chnIdx) return;
    if (planIdx !== null && Number(row[4]) !== planIdx) return;

    if (!monthAgg.has(month)) {
      monthAgg.set(month, {
        month,
        active_customers_start: 0,
        active_mrr_start: 0,
        churned_customers: 0,
        churned_mrr: 0,
      });
    }
    const cur = monthAgg.get(month);
    cur.active_customers_start += Number(row[5] || 0);
    cur.active_mrr_start += Number(row[6] || 0);
    cur.churned_customers += Number(row[7] || 0);
    cur.churned_mrr += Number(row[8] || 0);
  });

  return Array.from(monthAgg.values())
    .sort((a, b) => a.month.localeCompare(b.month))
    .map(r => ({
      ...r,
      customer_churn_rate: r.active_customers_start > 0 ? r.churned_customers / r.active_customers_start : 0,
      revenue_churn_rate: r.active_mrr_start > 0 ? r.churned_mrr / r.active_mrr_start : 0,
      retention_rate: r.active_customers_start > 0 ? 1 - (r.churned_customers / r.active_customers_start) : 0,
    }));
}

function getRiskKpi(filters) {
  const seg = filterLabel(filters.segment);
  const reg = filterLabel(filters.region);
  const chn = filterLabel(filters.channel);
  const plan = filterLabel(filters.plan);
  const tier = filterLabel(filters.riskTier);

  const row = DATA.risk_kpi_cube.find(r =>
    r.segment === seg &&
    r.region === reg &&
    r.acquisition_channel === chn &&
    r.plan_type === plan &&
    r.risk_tier_filter === tier
  );

  return row || {
    scored_customers: 0,
    total_current_mrr: 0,
    revenue_at_risk: 0,
    high_risk_customers: 0,
    critical_customers: 0,
    avg_priority_score: 0,
  };
}

function getFilteredSnapshot(filters) {
  return DATA.snapshot_agg.filter(r => {
    if (!matchDims(r, filters)) return false;
    if (filters.riskTier !== 'all' && r.risk_tier !== filters.riskTier) return false;
    return true;
  });
}

function getFilteredScored(filters) {
  return DATA.scored_customers.filter(r => {
    if (!matchDims(r, filters)) return false;
    if (filters.riskTier !== 'all' && r.risk_tier !== filters.riskTier) return false;
    return true;
  });
}

function getCohortRows(filters) {
  return DATA.cohort_rows
    .filter(r => monthInRange(r.observation_month, filters.startMonth, filters.endMonth))
    .map(r => ({
      cohort_month: r.cohort_month,
      age: Number(r.cohort_age_months || 0),
      retention_rate: Number(r.retention_rate || 0),
      revenue_retention: Number(r.revenue_retention || 0),
      observation_month: r.observation_month,
    }));
}

function upsertChart(id, config) {
  if (charts[id]) charts[id].destroy();
  const el = document.getElementById(id);
  charts[id] = new Chart(el, config);
}

function commonOptions(yPercent = false, horizontal = false, maxTicks = 10) {
  const pal = chartPalette();
  return {
    responsive: true,
    maintainAspectRatio: false,
    animation: false,
    indexAxis: horizontal ? 'y' : 'x',
    plugins: {
      legend: {
        position: 'bottom',
        labels: { boxWidth: 12, boxHeight: 12, usePointStyle: true, pointStyle: 'rectRounded', font: { size: 12 }, color: pal.text },
      },
      tooltip: {
        mode: 'index',
        intersect: false,
        backgroundColor: pal.tooltipBg,
        titleColor: pal.tooltipText,
        bodyColor: pal.tooltipText,
        borderColor: pal.grid,
        borderWidth: 1,
      },
    },
    scales: {
      x: {
        ticks: {
          autoSkip: true,
          maxTicksLimit: maxTicks,
          minRotation: horizontal ? 0 : 0,
          maxRotation: horizontal ? 0 : 35,
          font: { size: 12 },
          color: pal.text,
        },
        grid: { color: pal.grid, drawTicks: false },
      },
      y: {
        beginAtZero: true,
        grid: { color: pal.grid },
        ticks: {
          maxTicksLimit: 6,
          font: { size: 12 },
          color: pal.text,
          callback: yPercent ? (v => `${(Number(v) * 100).toFixed(0)}%`) : undefined,
        },
      },
    },
  };
}

function groupByDimension(snapshotRows, dimension) {
  const map = new Map();
  snapshotRows.forEach(r => {
    const k = String(r[dimension] || 'Unknown');
    if (!map.has(k)) map.set(k, { label: k, total: 0, churned: 0 });
    const cur = map.get(k);
    const cnt = Number(r.customer_count || 0);
    cur.total += cnt;
    if (Number(r.churn_flag) === 1) cur.churned += cnt;
  });

  return Array.from(map.values())
    .map(r => ({ label: r.label, churnRate: r.total > 0 ? r.churned / r.total : 0, count: r.total }))
    .sort((a, b) => b.churnRate - a.churnRate);
}

function compactCategories(rows, limit = 10) {
  if (rows.length <= limit) return rows;
  const top = rows.slice(0, limit - 1);
  const rest = rows.slice(limit - 1);
  const other = {
    label: 'Other',
    count: rest.reduce((a, r) => a + Number(r.count || 0), 0),
    churnRate: 0,
  };
  const total = rest.reduce((a, r) => a + Number(r.count || 0), 0);
  if (total > 0) {
    const churnWeighted = rest.reduce((a, r) => a + Number(r.count || 0) * Number(r.churnRate || 0), 0);
    other.churnRate = churnWeighted / total;
  }
  return [...top, other];
}

function renderScopeNote(filters) {
  const scope = document.getElementById('scopeNote');
  const tierTxt = filters.riskTier === 'all' ? 'all tiers' : filters.riskTier;
  scope.textContent = `Date + commercial filters drive trend, diagnostics, risk table, and action sections. Cohort charts are date-filtered at portfolio level. Current risk scope: ${tierTxt}.`;
}

function renderSummary(filters, trendRows, riskKpi, scoredRows, snapshotRows) {
  const strip = document.getElementById('summaryStrip');
  const last = trendRows[trendRows.length - 1] || { customer_churn_rate: 0, revenue_churn_rate: 0 };

  const segRows = compactCategories(groupByDimension(snapshotRows, 'segment'), 6);
  const topSeg = segRows[0] || { label: 'n/a', churnRate: 0 };

  const highCritical = scoredRows.filter(r => ['high', 'critical'].includes(r.risk_tier));
  const hcShare = scoredRows.length ? highCritical.length / scoredRows.length : 0;
  const hcMrr = highCritical.reduce((a, r) => a + Number(r.current_mrr || 0), 0);
  const baseMrr = scoredRows.reduce((a, r) => a + Number(r.current_mrr || 0), 0);

  const usageDeclineShare = highCritical.length
    ? highCritical.filter(r => Number(r.usage_trend || 0) < 0).length / highCritical.length
    : 0;

  const insights = [
    { title: 'Churn Concentration', body: `${topSeg.label} has highest churn in current scope (${pct(topSeg.churnRate)}).` },
    { title: 'Value Leakage', body: `Latest month customer churn ${pct(Number(last.customer_churn_rate || 0))} vs revenue churn ${pct(Number(last.revenue_churn_rate || 0))}.` },
    { title: 'Risk Concentration', body: `${pct(hcShare)} of scoped customers are high/critical, covering ${pct(baseMrr > 0 ? hcMrr / baseMrr : 0)} of scoped MRR.` },
    { title: 'Behavior + Surface', body: `${pct(usageDeclineShare)} of high/critical accounts show usage decline; ${numFmt.format(Number(riskKpi.scored_customers || 0))} scored accounts are in current intervention scope.` },
  ];

  strip.innerHTML = insights.map((i, idx) => `
    <div class="summary-card summary-card-${idx + 1}">
      <p class="summary-title">${i.title}</p>
      <p class="summary-body">${i.body}</p>
    </div>
  `).join('');
}

function updateKpis(trendRows, riskKpi) {
  const last = trendRows[trendRows.length - 1] || {
    active_customers_start: 0,
    churned_customers: 0,
    customer_churn_rate: 0,
    revenue_churn_rate: 0,
    month: 'n/a',
  };
  const prev = trendRows.length > 1 ? trendRows[trendRows.length - 2] : null;

  const active = Number(last.active_customers_start || 0);
  const customerChurn = Number(last.customer_churn_rate || 0);
  const revenueChurn = Number(last.revenue_churn_rate || 0);

  document.getElementById('kpiActive').textContent = numFmt.format(active);
  document.getElementById('kpiActiveNote').textContent = `Active customers at start of ${last.month}.`;
  setDelta(
    'kpiActiveDelta',
    prev ? `vs prior month: ${(active - Number(prev.active_customers_start || 0)) >= 0 ? '+' : ''}${numFmt.format(active - Number(prev.active_customers_start || 0))}` : 'No prior month in selected range',
    prev ? active - Number(prev.active_customers_start || 0) : 0,
    true
  );

  document.getElementById('kpiCustChurn').textContent = pct(customerChurn);
  document.getElementById('kpiCustChurnNote').textContent = `${numFmt.format(Number(last.churned_customers || 0))} churn events in latest month.`;
  setDelta(
    'kpiCustChurnDelta',
    prev ? `vs prior month: ${((customerChurn - Number(prev.customer_churn_rate || 0)) * 100 >= 0 ? '+' : '')}${((customerChurn - Number(prev.customer_churn_rate || 0)) * 100).toFixed(1)} pp` : 'No prior month in selected range',
    prev ? customerChurn - Number(prev.customer_churn_rate || 0) : 0
  );

  document.getElementById('kpiRevChurn').textContent = pct(revenueChurn);
  document.getElementById('kpiRevChurnNote').textContent = 'Computed from governed monthly revenue cube.';
  setDelta(
    'kpiRevChurnDelta',
    prev ? `vs prior month: ${((revenueChurn - Number(prev.revenue_churn_rate || 0)) * 100 >= 0 ? '+' : '')}${((revenueChurn - Number(prev.revenue_churn_rate || 0)) * 100).toFixed(1)} pp` : 'No prior month in selected range',
    prev ? revenueChurn - Number(prev.revenue_churn_rate || 0) : 0
  );

  const revAtRisk = Number(riskKpi.revenue_at_risk || 0);
  const totalMrr = Number(riskKpi.total_current_mrr || 0);
  const shareRisk = totalMrr > 0 ? revAtRisk / totalMrr : 0;

  document.getElementById('kpiRevRisk').textContent = money(revAtRisk);
  document.getElementById('kpiRevRiskNote').textContent = `${pct(shareRisk)} of scoped MRR under risk criteria.`;
  setDelta('kpiRevRiskDelta', `${numFmt.format(Number(riskKpi.scored_customers || 0))} scored customers in scope`, 0);

  document.getElementById('kpiHighRisk').textContent = numFmt.format(Number(riskKpi.high_risk_customers || 0));
  document.getElementById('kpiHighRiskNote').textContent = 'Tier = high + critical.';
  setDelta('kpiHighRiskDelta', 'From governed risk KPI cube', 0);

  document.getElementById('kpiCriticalRisk').textContent = numFmt.format(Number(riskKpi.critical_customers || 0));
  document.getElementById('kpiCriticalRiskNote').textContent = 'Immediate executive-save candidates.';
  setDelta('kpiCriticalRiskDelta', 'Critical tier only', 0);

  document.getElementById('kpiAvgPriority').textContent = num(Number(riskKpi.avg_priority_score || 0), 1);
  document.getElementById('kpiAvgPriorityNote').textContent = 'Average retention priority in current scope.';
  setDelta('kpiAvgPriorityDelta', 'Weighted score from risk methodology', 0);
}

function renderPrimaryCharts(trendRows, cohortRows) {
  const pal = chartPalette();
  const labels = trendRows.map(r => r.month);

  upsertChart('chartCustomerChurnTrend', {
    type: 'line',
    data: {
      labels,
      datasets: [{
        label: 'Customer churn rate',
        data: trendRows.map(r => Number(r.customer_churn_rate || 0)),
        borderColor: pal.lineCustomer,
        backgroundColor: pal.areaCustomer,
        borderWidth: 2.1,
        pointRadius: labels.length > 24 ? 0 : 2,
        fill: true,
        tension: 0.25,
      }],
    },
    options: commonOptions(true, false, 12),
  });

  upsertChart('chartRevenueChurnTrend', {
    type: 'line',
    data: {
      labels,
      datasets: [{
        label: 'Revenue churn rate',
        data: trendRows.map(r => Number(r.revenue_churn_rate || 0)),
        borderColor: pal.lineRevenue,
        backgroundColor: pal.areaRevenue,
        borderWidth: 2.1,
        pointRadius: labels.length > 24 ? 0 : 2,
        fill: true,
        tension: 0.25,
      }],
    },
    options: commonOptions(true, false, 12),
  });

  const cohortMap = new Map();
  cohortRows.forEach(r => {
    if (!cohortMap.has(r.cohort_month)) cohortMap.set(r.cohort_month, []);
    cohortMap.get(r.cohort_month).push(r);
  });

  const cohortMonths = Array.from(cohortMap.keys()).sort().slice(-8);
  const lineSets = cohortMonths.map((cohort, idx) => {
    const rows = (cohortMap.get(cohort) || []).slice().sort((a, b) => a.age - b.age);
    return {
      label: cohort,
      data: rows.map(r => ({ x: Number(r.age), y: Number(r.retention_rate || 0) })),
      borderColor: pal.cohort[idx % pal.cohort.length],
      backgroundColor: pal.cohort[idx % pal.cohort.length],
      borderWidth: 2,
      pointRadius: 1.5,
      tension: 0.2,
    };
  });

  upsertChart('chartCohortRetention', {
    type: 'line',
    data: { datasets: lineSets },
    options: {
      ...commonOptions(true, false, 12),
      parsing: false,
      scales: {
        x: {
          type: 'linear',
          title: { display: true, text: 'Cohort age (months)', color: pal.muted },
          ticks: { maxTicksLimit: 10, font: { size: 11 }, color: pal.text },
          grid: { color: pal.grid, drawTicks: false },
        },
        y: {
          beginAtZero: true,
          max: 1.0,
          grid: { color: pal.grid },
          ticks: { callback: v => `${(Number(v) * 100).toFixed(0)}%`, font: { size: 11 }, color: pal.text },
        },
      },
    },
  });

  const latestByCohort = new Map();
  cohortRows.forEach(r => {
    const cur = latestByCohort.get(r.cohort_month);
    if (!cur || r.observation_month > cur.observation_month) latestByCohort.set(r.cohort_month, r);
  });

  const latestRows = Array.from(latestByCohort.values())
    .sort((a, b) => a.cohort_month.localeCompare(b.cohort_month))
    .slice(-12);

  upsertChart('chartRevenueRetentionCohort', {
    type: 'bar',
    data: {
      labels: latestRows.map(r => r.cohort_month),
      datasets: [{
        label: 'Revenue retention',
        data: latestRows.map(r => Number(r.revenue_retention || 0)),
        borderColor: pal.retentionBorder,
        backgroundColor: pal.retentionBar,
        borderWidth: 1,
      }],
    },
    options: commonOptions(true, false, 12),
  });
}

function renderDiagnosticCharts(snapshotRows) {
  const pal = chartPalette();
  const build = (dimension, chartId, color) => {
    const rows = compactCategories(groupByDimension(snapshotRows, dimension), 10);
    upsertChart(chartId, {
      type: 'bar',
      data: {
        labels: rows.map(r => r.label),
        datasets: [{
          label: 'Churn rate',
          data: rows.map(r => r.churnRate),
          borderColor: color,
          backgroundColor: `${color}bb`,
          borderWidth: 1,
        }],
      },
      options: commonOptions(true, false, Math.min(10, rows.length + 1)),
    });
  };

  build('segment', 'chartChurnSegment', pal.churnBars[0]);
  build('region', 'chartChurnRegion', pal.churnBars[1]);
  build('acquisition_channel', 'chartChurnChannel', pal.churnBars[2]);
  build('plan_type', 'chartChurnPlan', pal.churnBars[3]);

  const retained = { count: 0, usage: 0, support: 0, nps: 0, failed: 0 };
  const churned = { count: 0, usage: 0, support: 0, nps: 0, failed: 0 };

  snapshotRows.forEach(r => {
    const target = Number(r.churn_flag) === 1 ? churned : retained;
    const c = Number(r.customer_count || 0);
    target.count += c;
    target.usage += Number(r.usage_sum || 0);
    target.support += Number(r.support_sum || 0);
    target.nps += Number(r.nps_sum || 0);
    target.failed += Number(r.payment_failure_sum || 0);
  });

  const retUsage = retained.count > 0 ? retained.usage / retained.count : 0;
  const chUsage = churned.count > 0 ? churned.usage / churned.count : 0;
  const retSupport = retained.count > 0 ? retained.support / retained.count : 0;
  const chSupport = churned.count > 0 ? churned.support / churned.count : 0;
  const retNps = retained.count > 0 ? retained.nps / retained.count : 0;
  const chNps = churned.count > 0 ? churned.nps / churned.count : 0;
  const retFailed = retained.count > 0 ? retained.failed / retained.count : 0;
  const chFailed = churned.count > 0 ? churned.failed / churned.count : 0;

  document.getElementById('diagUsage').textContent = `Retained ${num(retUsage,2)} vs churned ${num(chUsage,2)} (delta ${num(chUsage - retUsage,2)}).`;
  document.getElementById('diagSupport').textContent = `Retained ${num(retSupport,2)} vs churned ${num(chSupport,2)} tickets/90d.`;
  document.getElementById('diagFailed').textContent = `Retained ${pct(retFailed)} vs churned ${pct(chFailed)} failed-payment incidence.`;
  document.getElementById('diagNps').textContent = `Retained ${num(retNps,1)} vs churned ${num(chNps,1)} (gap ${num(retNps - chNps,1)}).`;
}

function renderRiskSection(scoredRows) {
  const pal = chartPalette();
  const sorted = scoredRows.slice().sort((a, b) => {
    const key = tableSortState.key;
    const av = a[key];
    const bv = b[key];

    let cmp = 0;
    if (typeof av === 'number' || typeof bv === 'number') cmp = Number(av || 0) - Number(bv || 0);
    else cmp = String(av ?? '').localeCompare(String(bv ?? ''));

    return tableSortState.dir === 'asc' ? cmp : -cmp;
  });

  const topRows = sorted.slice(0, 300);
  document.getElementById('priorityTableBody').innerHTML = topRows.map(r => `
    <tr>
      <td>${r.customer_id}</td>
      <td>${r.segment}</td>
      <td>${money2(Number(r.current_mrr || 0))}</td>
      <td>${num(Number(r.churn_risk_score || 0), 1)}</td>
      <td>${num(Number(r.revenue_risk_score || 0), 1)}</td>
      <td>${num(Number(r.retention_priority_score || 0), 1)}</td>
      <td>${r.main_risk_driver}</td>
      <td>${r.recommended_action}</td>
    </tr>
  `).join('');

  const tierOrder = ['critical', 'high', 'medium', 'low', 'churned'];
  const tierColors = {
    critical: '#8e1b1b',
    high: '#c0392b',
    medium: '#d4a20f',
    low: '#1f78b4',
    churned: '#475467',
  };

  const tierCounts = new Map();
  scoredRows.forEach(r => {
    const t = String(r.risk_tier || 'low');
    tierCounts.set(t, (tierCounts.get(t) || 0) + 1);
  });

  const total = scoredRows.length || 1;
  const stack = document.getElementById('riskTierStack');
  stack.innerHTML = tierOrder
    .filter(t => (tierCounts.get(t) || 0) > 0)
    .map(t => `<div title="${t}: ${tierCounts.get(t)}" style="background:${tierColors[t]};width:${((tierCounts.get(t) || 0) / total) * 100}%"></div>`)
    .join('');

  const legend = document.getElementById('riskTierLegend');
  legend.innerHTML = tierOrder
    .filter(t => (tierCounts.get(t) || 0) > 0)
    .map(t => `<div><span class="pill" style="background:${tierColors[t]};${t==='medium' ? 'color:#111827;' : ''}">${t}</span>${numFmt.format(tierCounts.get(t) || 0)}</div>`)
    .join('');

  const driverMap = new Map();
  scoredRows.forEach(r => {
    const k = String(r.main_risk_driver || 'unknown');
    driverMap.set(k, (driverMap.get(k) || 0) + 1);
  });

  const driverRows = Array.from(driverMap.entries())
    .map(([label, count]) => ({ label, count }))
    .sort((a, b) => b.count - a.count)
    .slice(0, 8);

  upsertChart('chartRiskDrivers', {
    type: 'bar',
    data: {
      labels: driverRows.map(r => r.label),
      datasets: [{
        label: 'Customers',
        data: driverRows.map(r => r.count),
        backgroundColor: pal.driverBar,
        borderColor: pal.driverBorder,
        borderWidth: 1,
      }],
    },
    options: commonOptions(false, true, 8),
  });

  const riskSegMap = new Map();
  scoredRows.forEach(r => {
    const seg = String(r.segment || 'Unknown');
    const rev = ((Number(r.at_risk_flag) === 1) || ['high', 'critical'].includes(String(r.risk_tier || '')))
      ? Number(r.current_mrr || 0)
      : 0;
    riskSegMap.set(seg, (riskSegMap.get(seg) || 0) + rev);
  });

  const riskSegRows = Array.from(riskSegMap.entries())
    .map(([label, value]) => ({ label, value }))
    .sort((a, b) => b.value - a.value);

  upsertChart('chartRevenueRiskSegment', {
    type: 'bar',
    data: {
      labels: riskSegRows.map(r => r.label),
      datasets: [{
        label: 'Revenue at risk (MRR)',
        data: riskSegRows.map(r => r.value),
        backgroundColor: pal.riskBar,
        borderColor: pal.riskBorder,
        borderWidth: 1,
      }],
    },
    options: {
      ...commonOptions(false, false, 8),
      scales: {
        x: { ticks: { maxTicksLimit: 8, maxRotation: 20, minRotation: 0, font: { size: 11 }, color: pal.text }, grid: { color: pal.grid, drawTicks: false } },
        y: { beginAtZero: true, grid: { color: pal.grid }, ticks: { callback: v => curFmt.format(v), font: { size: 11 }, color: pal.text, maxTicksLimit: 6 } },
      },
    },
  });
}

function renderActions(scoredRows) {
  const actions = [
    'billing intervention',
    'customer success outreach',
    'product adoption campaign',
    'renewal conversation',
    'executive save motion',
    'monitor only',
  ];

  const groups = new Map();
  scoredRows.forEach(r => {
    const a = String(r.recommended_action || 'monitor only');
    if (!groups.has(a)) groups.set(a, []);
    groups.get(a).push(r);
  });

  const cards = actions.map(action => {
    const rows = groups.get(action) || [];
    const count = rows.length;
    const mrr = rows.reduce((a, r) => a + Number(r.current_mrr || 0), 0);
    const avgPr = count ? rows.reduce((a, r) => a + Number(r.retention_priority_score || 0), 0) / count : 0;

    const segMap = new Map();
    rows.forEach(r => {
      const seg = String(r.segment || 'Unknown');
      segMap.set(seg, (segMap.get(seg) || 0) + Number(r.current_mrr || 0));
    });

    let topSeg = 'n/a';
    let topSegMrr = 0;
    segMap.forEach((val, key) => {
      if (val > topSegMrr) {
        topSeg = key;
        topSegMrr = val;
      }
    });

    return `
      <div class="action-card">
        <h5>${action}</h5>
        <div class="meta">
          <div><strong>Customers:</strong> ${numFmt.format(count)}</div>
          <div><strong>MRR coverage:</strong> ${money(mrr)}</div>
          <div><strong>Avg priority:</strong> ${num(avgPr, 1)}</div>
          <div><strong>Top segment:</strong> ${topSeg} (${money(topSegMrr)})</div>
        </div>
      </div>
    `;
  });

  document.getElementById('actionsGrid').innerHTML = cards.join('');
}

function renderDashboard() {
  const filters = getFilters();
  renderScopeNote(filters);

  const trendRows = getTrendRows(filters);
  const riskKpi = getRiskKpi(filters);
  const snapshotRows = getFilteredSnapshot(filters);
  const scoredRows = getFilteredScored(filters);
  const cohortRows = getCohortRows(filters);

  renderSummary(filters, trendRows, riskKpi, scoredRows, snapshotRows);
  updateKpis(trendRows, riskKpi);
  renderPrimaryCharts(trendRows, cohortRows);
  renderDiagnosticCharts(snapshotRows);
  renderRiskSection(scoredRows);
  renderActions(scoredRows);
}

applyTheme(resolveInitialTheme());
populateFilters();
renderDashboard();
  </script>
</body>
</html>
'''

    html = template.replace("__CHART_JS__", safe_chart_js)
    html = html.replace("__DATA_JSON__", safe_data_json)
    return html


def _enforce_single_official_html(dashboard_dir: Path, official_filename: str) -> None:
    for html_path in dashboard_dir.glob("*.html"):
        if html_path.name != official_filename:
            html_path.unlink()


def main() -> None:
    project_root = Path(__file__).resolve().parents[2]
    dashboard_dir = project_root / "outputs" / "dashboard"
    dashboard_dir.mkdir(parents=True, exist_ok=True)
    pages_dir = project_root / "docs"
    pages_dir.mkdir(parents=True, exist_ok=True)

    data = load_data(project_root)
    data_json = json.dumps(data, separators=(",", ":"), ensure_ascii=False)

    vendor_chart = project_root / "assets" / "vendor" / "chart.umd.min.js"
    chart_js = vendor_chart.read_text(encoding="utf-8")

    _enforce_single_official_html(dashboard_dir, OFFICIAL_DASHBOARD_FILENAME)
    html = build_html(data_json, chart_js)
    output_file = dashboard_dir / OFFICIAL_DASHBOARD_FILENAME
    output_file.write_text(html, encoding="utf-8")
    pages_file = pages_dir / "index.html"
    pages_file.write_text(html, encoding="utf-8")

    print("Executive dashboard generated:", output_file)
    print("GitHub Pages dashboard:", pages_file)
    print("Dashboard version:", data["meta"]["dashboard_version"])
    print("Builder version:", data["meta"]["builder_version"])
    print("Payload bytes:", len(html.encode("utf-8")))


if __name__ == "__main__":
    main()
