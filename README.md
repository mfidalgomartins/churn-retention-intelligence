# Churn & Retention Intelligence System

An executive‑grade retention command center that turns churn signals into ranked intervention priorities. It is built to answer a simple, high‑stakes question: where future revenue is leaking and which customers should be saved first.

Most churn programs fail not on measurement, but on prioritization. This project translates usage, support, payment, and commercial signals into a clear action queue and a compact executive view.

### What it delivers
A full retention pipeline from simulated raw data to risk scoring, diagnostics, and an executive dashboard. Outputs are governed, reproducible, and tied to explicit decision use.

### Decisions it supports
- Where churn is concentrated by segment, channel, plan, and region.
- Which accounts combine high risk with high revenue exposure.
- Which intervention plays should be sequenced now.

### Architecture (at a glance)
Data generation → profiling → feature engineering → churn analysis → risk scoring → visualization → dashboard → QA gates.

### Repository layout
```text
src/        pipeline logic
data/       raw + processed tables
outputs/    governed outputs, charts, final dashboard
docs/       decision memo, QA summary, methodology
sql/        staging + marts (warehouse equivalents)
config/     data contracts + QA policy
tests/      integrity + bounds checks
```

### Core outputs
- Decision memo: `docs/reports/executive_decision_memo.md`
- QA summary: `docs/governance/qa_release_summary.md`
- Dashboard: `outputs/dashboard/churn_retention_command_center.html`
- Key tables: `outputs/tables/main_analysis_structured_findings.csv`, `outputs/tables/risk_tier_summary.csv`

### Why it stands out
It is not a chart gallery. It is an end‑to‑end decision system with QA gates, interpretable scoring, and a dashboard generated strictly from governed outputs. SQL equivalents are included for warehouse translation.

### Run
```bash
make install
make all
make test
```

### Limits
Synthetic data; decision‑support only. Revenue churn uses a monthly‑value proxy. Behavioral drivers are correlational.

Tools: Python, SQL, pandas, DuckDB, Chart.js.
