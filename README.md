# Churn & Retention Intelligence System

**One‑line:** Executive retention intelligence system that turns churn signals into prioritized revenue‑protection actions.

## Business problem
Churn is measurable, but action is usually unprioritized. This project focuses on where future revenue is leaking and which accounts should be saved first.

## What the system does
- Simulates realistic subscription data and customer health signals.
- Builds retention features, churn diagnostics, and interpretable risk scores.
- Produces a decision memo, QA summary, and an executive HTML dashboard.

## Decisions supported
- Which segments and channels are driving churn concentration.
- Which accounts are high‑risk and high‑value, and should be prioritized first.
- Which intervention plays should be sequenced now.

## Project architecture
Data generation → profiling → feature engineering → churn analysis → risk scoring → visualization → dashboard → QA gates.

## Repository structure
```text
src/        core pipeline (generation → features → scoring → dashboard)
data/       raw + processed tables
outputs/    governed outputs, charts, final dashboard
docs/       decision memo, QA summary, methodology
sql/        staging + marts (warehouse equivalents)
config/     data contracts + QA policy
tests/      integrity + bounds checks
```

## Core outputs
- Decision memo: `docs/reports/executive_decision_memo.md`
- QA summary: `docs/governance/qa_release_summary.md`
- Dashboard: `outputs/dashboard/churn_retention_command_center.html`
- Key tables: `outputs/tables/main_analysis_structured_findings.csv`, `outputs/tables/risk_tier_summary.csv`

## Why this project is strong
- End‑to‑end pipeline with governance and QA gates, not just charts.
- Interpretable scoring with explicit drivers and action mapping.
- Executive‑ready dashboard generated from governed outputs.
- SQL equivalents included for warehouse translation.

## How to run
```bash
make install
make all
make test
```

## Limitations
- Synthetic data; decision‑support only.
- Revenue churn uses monthly‑value proxy, not contractual ARR.
- Behavioral signals are correlational.

## Tools
Tools: Python, SQL, pandas, DuckDB, Chart.js.
