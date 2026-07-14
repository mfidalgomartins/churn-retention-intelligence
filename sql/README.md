# PostgreSQL Reference Models

These PostgreSQL 15+ queries implement the pipeline's core temporal definitions
for warehouse use:

- `staging/subscriptions_clean.sql` — typed subscription staging model.
- `marts/customer_retention_features.sql` — customer features measured at churn
  date for closed accounts and snapshot date for open accounts, including the
  contract-renewal flag used by the Python risk score.
- `marts/churn_kpis.sql` — monthly customer and revenue churn over fully observed
  calendar months.
- `marts/revenue_movement_bridge.sql` — monthly new, expansion, contraction,
  reactivation, churn, NRR/GRR, margin proxy, and reconciliation.

Assumptions: one subscription per customer. Feature and KPI examples use the
`raw_*` staging names; the revenue bridge reads the production
`analytics.revenue_movements` and `analytics.service_costs` mappings. Supply the
explicit `:observation_date` parameter in orchestration; no mart depends on the
database clock.
