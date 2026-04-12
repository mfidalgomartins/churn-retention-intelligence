# Data Quality + Profiling Report

## Dataset-Level Overview

| table | row_count | column_count | candidate_pk | pk_valid | duplicate_rows |
|---|---:|---:|---|:---:|---:|
| customers | 3500 | 6 | customer_id | yes | 0 |
| subscriptions | 3500 | 8 | subscription_id | yes | 0 |
| product_usage | 367054 | 7 | usage_id | yes | 0 |
| payments | 56445 | 5 | payment_id | yes | 0 |

## customers

- Grain: One row per customer account
- Candidate primary key: `customer_id`
- Nulls in candidate key: 0
- Duplicates in candidate key: 0
- Useful dimensions: segment, region, acquisition_channel, plan_type
- Useful metrics: None
- Date coverage:
  - `signup_date`: 2022-01-01 to 2026-02-28 (1519 days)
- Cardinality by relevant dimensions:
  - `segment`: 4
  - `region`: 4
  - `acquisition_channel`: 6
  - `plan_type`: 4
- Column classification:
  - identifier: customer_id
  - dimension: segment, region, acquisition_channel, plan_type
  - metric: None
  - temporal: signup_date
  - boolean: None
  - text: None
- Null profile: no nulls detected.

## subscriptions

- Grain: One row per subscription record (current simulation has one subscription per customer)
- Candidate primary key: `subscription_id`
- Nulls in candidate key: 0
- Duplicates in candidate key: 0
- Useful dimensions: contract_type, billing_cycle, status
- Useful metrics: monthly_revenue
- Date coverage:
  - `subscription_start_date`: 2022-01-02 to 2026-03-01 (1519 days)
  - `subscription_end_date`: 2024-08-18 to 2026-02-28 (559 days)
- Cardinality by relevant dimensions:
  - `contract_type`: 2
  - `billing_cycle`: 3
  - `status`: 3
- Column classification:
  - identifier: subscription_id, customer_id
  - dimension: contract_type, billing_cycle, status
  - metric: monthly_revenue
  - temporal: subscription_start_date, subscription_end_date
  - boolean: None
  - text: None
- Null profile (columns with nulls):
  - `subscription_end_date`: 2547 (72.77%)

## product_usage

- Grain: One row per customer usage event date (weekly cadence)
- Candidate primary key: `usage_id`
- Nulls in candidate key: 0
- Duplicates in candidate key: 0
- Useful dimensions: customer_id
- Useful metrics: sessions, feature_adoption_score, support_tickets, nps_score
- Date coverage:
  - `usage_date`: 2022-01-02 to 2026-03-01 (1519 days)
- Cardinality by relevant dimensions:
  - `customer_id`: 3500
- Column classification:
  - identifier: usage_id, customer_id
  - dimension: None
  - metric: sessions, feature_adoption_score, support_tickets, nps_score
  - temporal: usage_date
  - boolean: None
  - text: None
- Null profile: no nulls detected.

## payments

- Grain: One row per payment attempt
- Candidate primary key: `payment_id`
- Nulls in candidate key: 0
- Duplicates in candidate key: 0
- Useful dimensions: payment_status, customer_id
- Useful metrics: amount
- Date coverage:
  - `payment_date`: 2022-01-02 to 2026-03-01 (1519 days)
- Cardinality by relevant dimensions:
  - `payment_status`: 2
  - `customer_id`: 3500
- Column classification:
  - identifier: payment_id, customer_id
  - dimension: payment_status
  - metric: amount
  - temporal: payment_date
  - boolean: None
  - text: None
- Null profile: no nulls detected.

## Data Quality Checks
- Total checks: 14
- Passed checks: 14
- Failed checks: 0

### Explicit Required Checks

| check_name | table | failed_rows | failure_rate | status |
|---|---|---:|---:|:---:|
| overlapping_subscriptions | subscriptions | 0 | 0.0000% | PASS |
| impossible_revenue_values | subscriptions | 0 | 0.0000% | PASS |
| invalid_subscription_status | subscriptions | 0 | 0.0000% | PASS |
| usage_outside_subscription_period | product_usage | 0 | 0.0000% | PASS |
| payment_inconsistencies | payments | 0 | 0.0000% | PASS |

### Failed Checks

No failed quality checks detected.

## Suspicious Values

- `subscriptions.extreme_high_monthly_revenue`: 0 rows (`monthly_revenue > 7247.52`). Likely rare tail accounts; validate if enterprise pricing assumptions are intentional.
- `product_usage.zero_session_events`: 669 rows (`sessions == 0`). Not impossible; can represent dormant weeks and is analytically useful for risk signals.
- `payments.failed_payment_events`: 2017 rows (`payment_status == 'failed'`). Expected risk signal; monitor concentration by segment/channel before modeling.