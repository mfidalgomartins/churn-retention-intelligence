# Release Runbook

## Objective
Produce a governed release of analytics outputs and dashboard artifacts with deterministic execution and QA gates.

## Prerequisites
- Python 3.12+
- Clean working tree
- Dependencies installed via `make install`

## Release Procedure
1. Regenerate all governed artifacts:
   - `make all`
2. Execute governance checks:
   - `make validate`
3. Run integrity tests:
   - `make test`
4. Review release outputs:
   - `outputs/tables/final_validation_checks.csv`
   - `outputs/tables/final_validation_issues.csv`
   - `outputs/tables/release_readiness_matrix.csv`
5. Confirm dashboard packaging:
   - `outputs/dashboard/executive-retention-command-center.html` is the only full dashboard payload.
   - `index.html` and `docs/index.html` are redirect entrypoints.

## Blockers
Do not release when any `FAIL` exists in:
- `outputs/tables/data_contract_checks.csv`
- `outputs/tables/final_validation_checks.csv`

## Rollback
If a release artifact is invalid after publish:
1. Revert to previous git commit.
2. Re-run `make validate` and `make test` on the reverted state.
3. Re-publish only after blocker checks pass.

## Ownership
- Data contracts and QA policy: Analytics Engineering
- Dashboard artifact generation: BI / Analytics
- Final release approval: Project owner
