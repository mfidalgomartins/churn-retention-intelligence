# Synthetic Data Generation Note

## Purpose
This synthetic dataset is designed to support churn and retention analytics workflows by embedding realistic commercial and behavioral patterns tied to revenue risk.

## Fixed Reproducibility
- Random seed: `42`
- Reference date used for account status simulation: `2026-03-01`

## Business-Oriented Simulation Assumptions
1. Customer mix reflects B2B SaaS heterogeneity across `Startup`, `SMB`, `Mid-Market`, and `Enterprise` segments.
2. Churn risk is structurally higher for `Startup` and `SMB` cohorts, and lower for `Enterprise`.
3. Acquisition quality differs by channel, with lower retention from `Paid Search` and `Affiliate`, and stronger retention from `Referral` and `Partner`.
4. Regional variance is explicit: `LATAM` and `APAC` have higher baseline churn pressure than `North America`.
5. Revenue is plan-driven and right-skewed (lognormal), producing realistic concentration of account value in higher tiers.
6. Subscription status includes a practical mix of `active`, `at_risk`, and `churned` customers.
7. Product usage degrades prior to churn and, to a lesser extent, for at-risk accounts near the reference date.
8. Support tickets rise before churn, with a stronger spike in `Startup` and `SMB` profiles.
9. NPS drops as churn proximity increases and service friction rises.
10. Failed payments are more likely near churn and are forcibly introduced for a subset of churned accounts to emulate delinquency-led attrition.

## Files Produced
- `data/raw/customers.csv`
- `data/raw/subscriptions.csv`
- `data/raw/product_usage.csv`
- `data/raw/payments.csv`
