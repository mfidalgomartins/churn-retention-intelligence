# Dashboard Design

## Goal
One screen an executive can land on and act from. Not a chart gallery — a
decision surface.

## Layout
Filters first → insight strip → KPI row → retention trends → cohort curves →
behavioural diagnostics → priority queue → action grouping.

Chart titles state the implication, not just the metric. Filters drive every
section except the cohort curves, which are intentionally portfolio-level.

## Performance
The HTML embeds three pre-aggregated cubes (monthly trend, risk KPI, snapshot
aggregates) plus a row-level scored customers table. Frontend code never
re-computes KPIs from raw data — it picks the right pre-aggregated row.

Chart.js and the data payload are inlined, so the dashboard runs offline and on
GitHub Pages with no extra setup.

## Versioning
A 12-char hash derived from the input artifact mtimes is embedded in the
payload as `dashboard_version`. The builder version (`builder_version`) is
bumped manually when the HTML or JS templates change.

## Files
- Builder: `src/churn/dashboard.py`
- Output: `outputs/dashboard/executive-retention-command-center.html`
- Pages entrypoints: `index.html`, `docs/index.html` (both redirect to the output)
