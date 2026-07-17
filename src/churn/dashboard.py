from __future__ import annotations

import hashlib
import json
from itertools import product
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from churn.risk import (
    CRITICAL_OVERRIDE_CHURN,
    CRITICAL_OVERRIDE_VALUE,
    PRIORITY_BASE,
    PRIORITY_VALUE_WEIGHT,
    TIER_CUTOFFS,
)

ALL_FILTER_VALUE = "__all__"
RISK_ORDER = ["critical", "high", "medium", "low", "churned"]
OFFICIAL_DASHBOARD_FILENAME = "executive-retention-command-center.html"


def _json_records(df: pd.DataFrame) -> list[dict]:
    clean = df.replace({np.nan: None})
    return clean.to_dict(orient="records")


def _build_version(payload: dict, static_inputs: list[Path]) -> str:
    h = hashlib.sha256()
    h.update(
        json.dumps(
            payload,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    )
    for p in sorted(static_inputs):
        h.update(p.name.encode("utf-8"))
        h.update(p.read_bytes())
    return h.hexdigest()[:12]


def _month_str(value: pd.Series) -> pd.Series:
    return pd.to_datetime(value).dt.strftime("%Y-%m")


def _expand_dims_with_all(df: pd.DataFrame, dims: list[str]) -> pd.DataFrame:
    expanded_frames: list[pd.DataFrame] = []
    for use_all_mask in product([0, 1], repeat=len(dims)):
        part = df.copy()
        for idx, dim in enumerate(dims):
            if use_all_mask[idx] == 1:
                part[dim] = ALL_FILTER_VALUE
        expanded_frames.append(part)
    return pd.concat(expanded_frames, ignore_index=True)


def _load_base_tables(project_root: Path) -> dict[str, pd.DataFrame]:
    processed = project_root / "data" / "processed"
    outputs = project_root / "outputs" / "tables"

    def optional(path: Path, columns: list[str]) -> pd.DataFrame:
        """Validation tables are produced after the dashboard in a fresh build."""
        if path.exists():
            return pd.read_csv(path)
        return pd.DataFrame(columns=columns)

    return {
        "features": pd.read_csv(processed / "customer_retention_features.csv"),
        "risk": pd.read_csv(processed / "customer_risk_scores.csv"),
        "monthly_dim": pd.read_csv(
            outputs / "monthly_dimensional_trend.csv", parse_dates=["month"]
        ),
        "cohort": pd.read_csv(
            processed / "cohort_retention_table.csv",
            parse_dates=["cohort_month", "observation_month"],
        ),
        "validation_checks": optional(
            outputs / "final_validation_checks.csv",
            [
                "category",
                "check_name",
                "status",
                "severity",
                "gate_level",
                "is_blocker",
                "evidence",
            ],
        ),
        "validation_issues": optional(
            outputs / "final_validation_issues.csv",
            [
                "category",
                "check_name",
                "severity",
                "gate_level",
                "is_blocker",
                "status",
                "evidence",
                "fix_applied",
            ],
        ),
    }


def _build_snapshot_tables(
    features: pd.DataFrame, risk: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    dims = ["segment", "region", "acquisition_channel", "plan_type"]
    risk_cols = [
        "customer_id",
        "churn_risk_score",
        "customer_value_score",
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
    snapshot["at_risk_mrr_component"] = np.where(
        snapshot["at_risk_flag"] == 1, snapshot["current_mrr"], 0.0
    )
    snapshot["churned_revenue_component"] = np.where(
        snapshot["churn_flag"] == 1, snapshot["avg_monthly_revenue"], 0.0
    )

    snapshot_agg = (
        snapshot.groupby([*dims, "risk_tier", "churn_flag"], as_index=False)
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
            "customer_value_score",
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
        "customer_value_score": 2,
        "retention_priority_score": 2,
    }
    for col, digits in numeric_round.items():
        scored[col] = scored[col].round(digits)

    return snapshot_agg, scored


def _encode_monthly_fact(
    monthly_dim: pd.DataFrame, domains: dict[str, list[str]]
) -> tuple[list[str], list[list[float]]]:
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
    base["priority_accounts"] = (
        (base["at_risk_flag"] == 1) | base["risk_tier"].isin(["high", "critical"])
    ).astype(int)
    base["priority_sum"] = base["retention_priority_score"]
    base["priority_mrr_component"] = np.where(
        base["priority_accounts"] == 1,
        base["current_mrr"],
        0.0,
    )

    expanded_dims = _expand_dims_with_all(base, dims)

    all_rows = expanded_dims.copy()
    all_rows["risk_tier_filter"] = ALL_FILTER_VALUE

    tier_rows = expanded_dims.copy()
    tier_rows["risk_tier_filter"] = tier_rows["risk_tier"]

    cube_input = pd.concat([all_rows, tier_rows], ignore_index=True)

    cube = (
        cube_input.groupby([*dims, "risk_tier_filter"], as_index=False)
        .agg(
            scored_customers=("scored_customers", "sum"),
            total_current_mrr=("current_mrr", "sum"),
            priority_mrr_exposure=("priority_mrr_component", "sum"),
            priority_accounts=("priority_accounts", "sum"),
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

    for col in ["total_current_mrr", "priority_mrr_exposure", "avg_priority_score"]:
        cube[col] = cube[col].round(2)

    return cube


def _prepare_cohort_rows(cohort: pd.DataFrame) -> pd.DataFrame:
    rows = cohort.copy()
    rows["cohort_age_months"] = (
        rows["observation_month"].dt.year - rows["cohort_month"].dt.year
    ) * 12 + (rows["observation_month"].dt.month - rows["cohort_month"].dt.month)
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


def load_data(project_root: Path) -> dict:
    base = _load_base_tables(project_root)

    features = base["features"]
    risk = base["risk"]
    monthly_dim = base["monthly_dim"]
    cohort = base["cohort"]
    snapshot_agg, scored_all = _build_snapshot_tables(features, risk)
    risk_kpi_cube = _build_risk_kpi_cube(scored_all)
    cohort_rows = _prepare_cohort_rows(cohort)
    validation_checks = base["validation_checks"]
    validation_issues = base["validation_issues"]

    validation_summary = {
        "total_checks": len(validation_checks),
        "pass_checks": int((validation_checks["status"] == "PASS").sum()),
        "warn_checks": int((validation_checks["status"] == "WARN").sum()),
        "fail_checks": int((validation_checks["status"] == "FAIL").sum()),
        "blocker_fails": int(
            (
                (validation_checks["status"] == "FAIL")
                & (validation_checks["is_blocker"].astype(str).str.lower() == "true")
            ).sum()
        ),
        "top_issue": (
            str(validation_issues.iloc[0]["evidence"])
            if len(validation_issues)
            else "No validation issues in the current release gate."
        ),
    }

    dims = ["segment", "region", "acquisition_channel", "plan_type"]
    domains = {dim: sorted(features[dim].dropna().astype(str).unique().tolist()) for dim in dims}
    domains["risk_tier"] = ["critical", "high", "medium", "low", "churned"]
    months, monthly_fact_rows = _encode_monthly_fact(monthly_dim, domains)
    monthly_base = (
        monthly_dim.assign(month_key=_month_str(monthly_dim["month"]))
        .groupby("month_key")["active_customers_start"]
        .sum()
    )
    mature_months = monthly_base[monthly_base >= 100].index.tolist()
    coverage_start_month = mature_months[0] if mature_months else months[0]

    # Keep the complete open-account population so every filtered ranking is exact.
    # The browser still renders only the first 300 matching rows.
    scored = scored_all.sort_values(
        ["retention_priority_score", "current_mrr"], ascending=[False, False]
    ).copy()

    data: dict[str, Any] = {
        "meta": {
            "project": "Churn & Retention Intelligence System",
            "coverage_start_month": coverage_start_month,
            "coverage_end_month": months[-1],
            "data_snapshot_month": months[-1],
        },
        "domains": domains,
        "months": months,
        # The dashboard draws the tiering policy as curves over churn risk ×
        # customer value, so it needs the same constants risk.py scores with.
        # Emitting them keeps risk.py the single source of truth.
        "policy": {
            "base": PRIORITY_BASE,
            "value_weight": PRIORITY_VALUE_WEIGHT,
            "tiers": [
                {"key": key, "min": cutoff}
                for key, cutoff in sorted(TIER_CUTOFFS.items(), key=lambda kv: kv[1], reverse=True)
            ],
            "override": {
                "risk": CRITICAL_OVERRIDE_CHURN,
                "value": CRITICAL_OVERRIDE_VALUE,
            },
        },
        "validation_summary": validation_summary,
        "monthly_fact_rows": monthly_fact_rows,
        "risk_kpi_cube": _json_records(risk_kpi_cube),
        "snapshot_agg": _json_records(snapshot_agg),
        "scored_customers": _json_records(scored),
        "cohort_rows": _json_records(cohort_rows),
    }
    data["meta"]["dashboard_version"] = _build_version(
        data,
        [
            TEMPLATE_PATH,
            project_root / "assets" / "vendor" / "chart.umd.min.js",
        ],
    )
    return data


TEMPLATE_PATH = Path(__file__).parent / "dashboard_template.html"


def build_html(data_json: str, chart_js: str) -> str:
    """Render the dashboard HTML by injecting the data JSON and Chart.js into the template."""
    # Defensive escaping for inline <script> boundaries.
    safe_chart_js = chart_js.replace("</script", "<\\/script")
    safe_data_json = (
        data_json.replace("&", "\\u0026")
        .replace("<", "\\u003c")
        .replace(">", "\\u003e")
        .replace("\u2028", "\\u2028")
        .replace("\u2029", "\\u2029")
    )
    template = TEMPLATE_PATH.read_text(encoding="utf-8")
    return template.replace("__CHART_JS__", safe_chart_js).replace("__DATA_JSON__", safe_data_json)


def build_redirect_html(target_href: str) -> str:
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <meta http-equiv="refresh" content="0; url={target_href}" />
  <title>Dashboard Redirect</title>
  <style>
    body {{
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      margin: 0;
      min-height: 100vh;
      display: grid;
      place-items: center;
      background: #f5f7fb;
      color: #15212b;
    }}
    .box {{
      text-align: center;
      max-width: 560px;
      padding: 24px;
    }}
    a {{
      color: #0b5fcc;
      text-decoration: none;
      font-weight: 600;
    }}
  </style>
</head>
<body>
  <div class="box">
    <h1>Churn & Retention Dashboard</h1>
    <p>Redirecting to the live dashboard...</p>
    <p><a href="{target_href}">Open dashboard</a></p>
  </div>
</body>
</html>
"""


def _enforce_single_official_html(dashboard_dir: Path, official_filename: str) -> None:
    for html_path in dashboard_dir.glob("*.html"):
        if html_path.name != official_filename:
            html_path.unlink()


def main() -> None:
    from churn.common import outputs_dashboard_dir
    from churn.common import project_root as _root

    root = _root()
    dashboard_dir = outputs_dashboard_dir()
    dashboard_dir.mkdir(parents=True, exist_ok=True)
    (root / "docs").mkdir(parents=True, exist_ok=True)

    data = load_data(root)
    data_json = json.dumps(data, separators=(",", ":"), ensure_ascii=False)
    chart_js = (root / "assets" / "vendor" / "chart.umd.min.js").read_text(encoding="utf-8")

    _enforce_single_official_html(dashboard_dir, OFFICIAL_DASHBOARD_FILENAME)
    html = build_html(data_json, chart_js)
    output_file = dashboard_dir / OFFICIAL_DASHBOARD_FILENAME
    output_file.write_text(html, encoding="utf-8")
    (root / "index.html").write_text(
        build_redirect_html(f"./outputs/dashboard/{OFFICIAL_DASHBOARD_FILENAME}"),
        encoding="utf-8",
    )
    (root / "docs" / "index.html").write_text(
        build_redirect_html(f"../outputs/dashboard/{OFFICIAL_DASHBOARD_FILENAME}"),
        encoding="utf-8",
    )

    print(
        f"Dashboard built: {output_file.relative_to(root)} "
        f"({len(html.encode('utf-8')):,} bytes, version {data['meta']['dashboard_version']})."
    )


if __name__ == "__main__":
    main()
