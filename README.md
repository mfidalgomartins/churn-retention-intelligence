# Churn & Retention Intelligence

An end-to-end retention command center built on synthetic SaaS data. It scores
recoverable customers, ranks them by risk-weighted revenue exposure, and
publishes a single self-contained dashboard for executive review.

The deliverable is a **prioritised action queue**, not a chart gallery: which
accounts to save first, why, and with what play.

**Live dashboard:** <https://mfidalgomartins.github.io/churn-retention-intelligence/>

## What it does

```
generate → profile → features → analyze → risk → dashboard → validate
```

Every step is deterministic (`seed = 42`), governed by data contracts, and gated
by automated quality checks. The output is one HTML file with embedded data and
charts — no server, no external dependencies.

## Quickstart

```bash
make install
make all
make test
```

That's it. `make all` produces every artifact from scratch; `make test` runs
52 tests covering both unit logic and end-to-end integrity.

## What the pipeline produces

| Layer | Output | Purpose |
|---|---|---|
| Raw | `data/raw/*.csv` | Simulated customers, subscriptions, weekly usage, payments |
| Features | `data/processed/customer_retention_features.csv` | Per-customer snapshot with usage, billing, and health signals |
| Analysis | `outputs/tables/main_analysis_*.csv` | Trends, drivers, revenue at risk, intervention plays |
| Risk | `data/processed/customer_risk_scores.csv` | Tiered priority queue with recommended actions |
| Dashboard | `outputs/dashboard/executive-retention-command-center.html` | Self-contained UI for executive review |
| Governance | `outputs/tables/*validation*.csv`, `release_readiness_matrix.csv` | Release gates and audit log |

## Decisions it supports

- Where churn concentrates (segment, region, channel, plan).
- Which accounts combine high risk and high revenue exposure.
- Which intervention plays return the highest near-term ROI.

## Architecture

```
src/churn/        pipeline modules (generate, profile, features, analyze, risk, dashboard, contracts, validate)
src/churn/common  shared constants and helpers (REFERENCE_DATE, SEED, snapshot inference, paths)
config/           data contracts and release policy
sql/              warehouse equivalents of the staging and mart logic
docs/             methodology, governance, decision memo
tests/unit/       business-logic tests (38)
tests/test_integration.py   end-to-end artifact and gate tests (14)
```

Each module is invocable via `python -m churn.<name>` and reads its inputs from
governed locations only — no module reaches outside its declared contract.

## Limits

Synthetic data, decision-support only. Revenue churn uses a monthly-value proxy
rather than full contract ARR accounting. Behavioural drivers are correlational.

## Tech

Python 3.12, NumPy, pandas, vanilla JS + Chart.js for the dashboard.
