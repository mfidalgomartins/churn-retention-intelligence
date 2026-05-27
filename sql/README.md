# SQL Equivalents

The Python pipeline is the source of truth. These SQL files exist to show how
the same staging and mart logic translates to a warehouse — useful for porting
the design to dbt or a direct SQL implementation.

- `staging/subscriptions_clean.sql` — typed cast of `raw_subscriptions`
- `marts/customer_retention_features.sql` — simplified per-customer feature mart
- `marts/churn_kpis.sql` — monthly customer and revenue churn rates

The SQL is illustrative; match the Python first when they drift.
