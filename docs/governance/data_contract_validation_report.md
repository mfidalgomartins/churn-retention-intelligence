# Data Contract Validation Report

Generated at: `2026-04-06 00:08:48`

- Total checks: **20**
- PASS: **20**
- FAIL: **0**

## Contract Checks

| Dataset | Check | Status | Severity | Evidence |
|---|---|---|---|---|
| raw_customers | dataset_exists | PASS | info | path=data/raw/customers.csv; exists=True |
| raw_customers | required_columns_present | PASS | info | missing_columns=[] |
| raw_customers | row_count_nonzero | PASS | info | row_count=3500 |
| raw_customers | primary_key_not_null | PASS | info | primary_key=customer_id; null_rows=0 |
| raw_customers | primary_key_unique | PASS | info | primary_key=customer_id; duplicate_rows=0 |
| raw_subscriptions | dataset_exists | PASS | info | path=data/raw/subscriptions.csv; exists=True |
| raw_subscriptions | required_columns_present | PASS | info | missing_columns=[] |
| raw_subscriptions | row_count_nonzero | PASS | info | row_count=3500 |
| raw_subscriptions | primary_key_not_null | PASS | info | primary_key=subscription_id; null_rows=0 |
| raw_subscriptions | primary_key_unique | PASS | info | primary_key=subscription_id; duplicate_rows=0 |
| processed_customer_features | dataset_exists | PASS | info | path=data/processed/customer_retention_features.csv; exists=True |
| processed_customer_features | required_columns_present | PASS | info | missing_columns=[] |
| processed_customer_features | row_count_nonzero | PASS | info | row_count=3500 |
| processed_customer_features | primary_key_not_null | PASS | info | primary_key=customer_id; null_rows=0 |
| processed_customer_features | primary_key_unique | PASS | info | primary_key=customer_id; duplicate_rows=0 |
| processed_risk_scores | dataset_exists | PASS | info | path=data/processed/customer_risk_scores.csv; exists=True |
| processed_risk_scores | required_columns_present | PASS | info | missing_columns=[] |
| processed_risk_scores | row_count_nonzero | PASS | info | row_count=2547 |
| processed_risk_scores | primary_key_not_null | PASS | info | primary_key=customer_id; null_rows=0 |
| processed_risk_scores | primary_key_unique | PASS | info | primary_key=customer_id; duplicate_rows=0 |