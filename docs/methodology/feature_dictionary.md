# Feature Dictionary

## customer_retention_features (one row per customer at snapshot)

| Column | Definition |
|---|---|
| `customer_id` | Stable customer key. |
| `observation_date` | Churn date for closed accounts; portfolio snapshot for open accounts. |
| `segment`, `region`, `acquisition_channel`, `plan_type` | Commercial dimensions from customer master. |
| `tenure_days` | Days from subscription start to churn date (if churned) or snapshot. |
| `current_mrr` | Monthly recurring revenue proxy; zero for churned accounts. |
| `avg_monthly_revenue` | Mean of paid monthly-equivalent payments; falls back to `monthly_revenue`. |
| `lifetime_revenue` | Sum of successful payments. |
| `recent_sessions_30d`, `recent_sessions_90d` | Sessions in trailing 30 / 90 days. |
| `usage_trend` | Avg sessions in last 30d minus avg sessions in days 31-60. |
| `feature_adoption_score_recent` | Mean adoption score in last 30 days. |
| `support_tickets_30d`, `support_tickets_90d` | Tickets in trailing 30 / 90 days. |
| `nps_score_recent` | Mean NPS in last 90 days. |
| `failed_payments_90d` | Failed payments in last 90 days. |
| `payment_failure_flag` | `1` if any failed payment in last 90 days. |
| `renewal_near_flag` | `1` if an open account's contract anniversary falls within 45 days. Contract term, not billing cadence, determines renewal. |
| `churn_flag` | `1` if `status == "churned"`. |
| `at_risk_flag` | `1` if `status == "at_risk"`. |

## cohort_retention_table (one row per cohort × observation month)

`active_customers` is the **initial cohort size** (denominator), not currently active.
`retained_customers` is the number not yet churned by the observation month-end.
`retention_rate = retained_customers / active_customers`.
`revenue_retention = sum(retained MRR) / sum(initial MRR)`.
Only fully observed calendar months are included.

## segment_retention_summary (one row per segment)

`current_mrr_at_risk` and `churned_monthly_value_proxy` are reported separately
to avoid mixing future exposure with realised churn.
