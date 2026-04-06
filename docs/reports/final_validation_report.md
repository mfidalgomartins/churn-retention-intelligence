# Final Validation Report

Generated at: `2026-04-06 00:08:51`

## Validation Scope
- Data quality checks (raw and processed tables)
- Metric correctness checks (flags, churn/revenue metrics, cohort logic)
- Analytical integrity checks (joins, denominator risks, overclaiming risks)
- Visualization review (chart pack structure/readability)
- Dashboard review (KPI/chart/filter/table consistency)

## Validation Summary
- Total checks: **47**
- PASS: **46**
- WARN: **1**
- FAIL: **0**
- Blocker FAILs: **0**
- Major WARNs: **1**

## Issues Found

| Category | Check | Severity | Gate | Blocker | Status | Evidence |
|---|---|---|---|---|---|---|
| Analytical Integrity | Incomplete period comparison risk | major | analytical_validity | False | WARN | Months with active_customers_start < 100: 2 (2022-01-01, 2022-02-01). |

## Readiness Matrix

| State | Active | Criterion | Evidence |
|---|---|---|---|
| technically valid | True | No blocker failures in technical and product-quality gates. | blocker_fails=0, technical_failures=0 |
| analytically acceptable | True | Technically valid and no analytical failures with controlled major caveats. | analytical_failures=0, major_warns=1 |
| decision-support only | True | Analytically acceptable but still caveated (simulation/proxy/correlation limits). | synthetic_data=True, total_warns=1 |
| screening-grade only | False | Technically stable but analytically below decision-support threshold. | technically_valid=True, analytically_acceptable=True |
| not committee-grade | True | Any unresolved caveat or synthetic-data limitation prevents committee-grade claims. | synthetic_data=True, warns=1 |
| publish-blocked | False | Any FAIL or blocker fail blocks publication-ready claim. | fail_count=0, blocker_fails=0 |

## Fixes Applied
- Validation step only. No direct remediation changes are applied inside this report.

## Required Stakeholder Caveats
- Early months have low active-customer denominators; trend interpretation should emphasize mature periods.

## Final Confidence Assessment
**Share with caveats**

## Release Readiness Recommendation
**decision-support only**

## Generated Artifacts
- `outputs/tables/final_validation_checks.csv`
- `outputs/tables/final_validation_issues.csv`
- `outputs/tables/release_readiness_matrix.csv`
- `docs/reports/final_validation_report.md`