# QA + Release Summary

## Validation Scope
- Data quality checks (raw and processed tables)
- Metric correctness (flags, churn/revenue metrics, cohort logic)
- Analytical integrity (joins, denominators, overclaiming risk)
- Visualization review (chart pack structure/readability)
- Dashboard review (KPI/chart/filter/table consistency)

## Summary
- Total checks: **48**
- PASS: **47**
- WARN: **1**
- FAIL: **0**
- Blocker FAILs: **0**

## Issues (Required Disclosure)

| Category | Check | Severity | Evidence |
|---|---|---|---|
| Analytical Integrity | Incomplete period comparison risk | major | Months with active_customers_start < 100: 2 (2022-01-01, 2022-02-01). |

## Release State

| State | Active |
|---|---|
| technically valid | True |
| analytically acceptable | True |
| decision-support only | True |
| screening-grade only | False |
| not committee-grade | True |
| publish-blocked | False |

## Required Caveats
- Early months have low active-customer denominators; trend interpretation should emphasize mature periods.