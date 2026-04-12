# Feature Dictionary

## Transparent Flag Definitions
- `churn_flag`: 1 when `subscriptions.status == 'churned'`, else 0.
- `at_risk_flag`: 1 when `subscriptions.status == 'at_risk'`, else 0.

## Table: customer_retention_features
- `customer_id`: Stable customer key.
- `segment`: Commercial segment from customer master data.
- `region`: Geographic region of account.
- `acquisition_channel`: Source channel used to acquire the account.
- `plan_type`: Product plan tier.
- `tenure_days`: Days from subscription start to churn date (if churned) or snapshot date.
- `current_mrr`: Current monthly recurring revenue proxy; zeroed for churned accounts.
- `avg_monthly_revenue`: Mean of paid monthly-equivalent payment amounts (`amount / billing_cycle_months`), with fallback to `monthly_revenue` when needed.
- `lifetime_revenue`: Sum of successful (`payment_status='paid'`) payment amounts.
- `recent_sessions_30d`: Sum of sessions in the last 30 days from snapshot date.
- `recent_sessions_90d`: Sum of sessions in the last 90 days from snapshot date.
- `usage_trend`: Difference between average sessions in recent 30 days and prior 30-day window (days 31-60).
- `feature_adoption_score_recent`: Mean feature adoption score in last 30 days.
- `support_tickets_30d`: Sum of support tickets in last 30 days.
- `support_tickets_90d`: Sum of support tickets in last 90 days.
- `nps_score_recent`: Mean NPS in last 90 days.
- `failed_payments_90d`: Count of failed payments in last 90 days.
- `payment_failure_flag`: 1 if `failed_payments_90d > 0`, else 0.
- `renewal_near_flag`: 1 when non-churned account has next billing-cycle renewal in next 45 days.
- `contraction_flag`: 1 when recent 90-day paid monthly-equivalent amount is <85% of prior 90-day paid monthly-equivalent amount.

## Table: cohort_retention_table
- `cohort_month`: Month of subscription start (`subscription_start_date`, month grain).
- `observation_month`: Month where retention is evaluated.
- `active_customers`: Initial cohort size (denominator) for that cohort.
- `retained_customers`: Customers in cohort not churned by observation month end.
- `retention_rate`: `retained_customers / active_customers`.
- `revenue_retention`: Sum of retained-customer `monthly_revenue` divided by initial cohort `monthly_revenue`.

## Table: segment_retention_summary
- `segment`: Customer segment.
- `active_customers`: Non-churned customers (`churn_flag=0`) in segment.
- `churned_customers`: Churned customers (`churn_flag=1`) in segment.
- `churn_rate`: `churned_customers / total_customers_in_segment`.
- `revenue_at_risk`: `sum(current_mrr for at_risk)` + `sum(avg_monthly_revenue for churned)`.
- `avg_tenure`: Mean `tenure_days` in segment.
- `avg_nps`: Mean `nps_score_recent` in segment.
- `avg_usage_trend`: Mean `usage_trend` in segment.
