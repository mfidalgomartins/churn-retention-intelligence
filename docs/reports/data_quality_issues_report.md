# Data Quality Issues Report

Generated at: `2026-04-02 13:56:05`

- Total checks: 14
- Passed checks: 14
- Failed checks: 0

## Explicit Required Checks

| check_name | table | failed_rows | failure_rate | status |
|---|---|---:|---:|:---:|
| overlapping_subscriptions | subscriptions | 0 | 0.0000% | PASS |
| impossible_revenue_values | subscriptions | 0 | 0.0000% | PASS |
| invalid_subscription_status | subscriptions | 0 | 0.0000% | PASS |
| usage_outside_subscription_period | product_usage | 0 | 0.0000% | PASS |
| payment_inconsistencies | payments | 0 | 0.0000% | PASS |

## Failed Checks

No failed quality checks detected.

## Suspicious Values

- `subscriptions.extreme_high_monthly_revenue`: 0 rows (`monthly_revenue > 7247.52`). Likely rare tail accounts; validate if enterprise pricing assumptions are intentional.
- `product_usage.zero_session_events`: 669 rows (`sessions == 0`). Not impossible; can represent dormant weeks and is analytically useful for risk signals.
- `payments.failed_payment_events`: 2017 rows (`payment_status == 'failed'`). Expected risk signal; monitor concentration by segment/channel before modeling.