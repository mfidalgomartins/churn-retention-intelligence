# Dashboard Design Note

## Objective
Build an executive retention command center that turns churn analytics into intervention decisions, with clear prioritization across revenue leadership, Customer Success, finance, and operations.

## Design Choices
- **Self-contained HTML**: The dashboard is fully offline-capable by embedding both data and Chart.js directly in one file.
- **Executive hierarchy**: Header and filters first, then insight strip, KPI row, diagnostics, risk prioritization, and action playbooks.
- **Decision-first titles**: Chart titles communicate implications, not only metric names.
- **Interactivity**: Date + commercial + risk filters drive trend, diagnostics, risk, and action sections; cohort charts are portfolio-level and date-filtered.
- **Operational readiness**: Includes sortable priority table with required intervention fields and an action grouping section.
- **Traceability**: Dashboard metadata (`dashboard_version`, `builder_version`) remains embedded in payload for auditability without cluttering executive presentation.

## Decision Surfaces
- **Executive Summary**: Rapid alignment on where churn concentrates and which signals are most predictive.
- **KPI Row**: Quick gating on current health and the size of the risk pool.
- **Retention Trends**: Identify trend breaks that require immediate leadership attention.
- **Cohort Retention**: Validate whether recent acquisition quality is improving or decaying.
- **Diagnostics**: Pinpoint operational and product drivers to prioritize interventions.
- **Risk Prioritization**: Rank accounts for action based on risk-weighted revenue impact.
- **Action Section**: Translate scores into concrete plays for CS, billing, and renewal teams.

## Performance & Maintainability
- Uses governed pre-aggregated outputs (`monthly_fact_rows` + dimensions index, `risk_kpi_cube`, `snapshot_agg`) to avoid critical KPI logic in frontend runtime.
- Built with readable vanilla JS + Chart.js and generated via a Python builder script for repeatable refreshes.

## Output
- Main file: `outputs/dashboard/churn_retention_command_center.html`
- Builder script: `src/dashboard_builder/build_executive_dashboard.py`
