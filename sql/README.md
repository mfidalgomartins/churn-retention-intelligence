# PostgreSQL Reference Models

These PostgreSQL 15+ queries implement the pipeline's core temporal definitions
for warehouse use:

- `staging/subscriptions_clean.sql` — typed subscription staging model.
- `marts/customer_retention_features.sql` — customer features measured at churn
  date for closed accounts and snapshot date for open accounts.
- `marts/churn_kpis.sql` — monthly customer and revenue churn over fully observed
  calendar months.

Assumptions: one subscription per customer and raw tables named
`raw_customers`, `raw_subscriptions`, `raw_product_usage`, and `raw_payments`.
Set the snapshot date in each mart's `params` CTE before execution.
