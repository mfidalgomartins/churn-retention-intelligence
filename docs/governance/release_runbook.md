# Release Runbook

## Release verification

```bash
make install     # one-time setup
make release     # regenerate dashboard, chart pack, report, and validation
make quality     # lint, coverage, and security/dependency checks
```

`make release` rebuilds data, features, analysis tables, risk scores, the
dashboard, validation outputs, chart pack, and PDF report. `make quality` runs
Ruff, coverage, Bandit, and dependency audit.

## Inspect before publish

- `outputs/tables/release_readiness_matrix.csv` — readiness state
- `outputs/tables/final_validation_issues.csv` — open warnings or failures
- `outputs/dashboard/executive-retention-command-center.html` — the deliverable
- `outputs/reports/churn-retention-intelligence-report.pdf` — narrative report
- CI result for the final commit — release and quality gates passed

## Block release when

- `make release` or `make quality` fails.
- Any FAIL exists in `data_contract_checks.csv` or `final_validation_checks.csv`.
- `release_readiness_matrix.csv` reports `publish-blocked`.
- The generated artifacts differ from the committed artifacts after CI rebuild.
- A security finding is unresolved or accepted without owner approval.

## Evidence

Record the final commit SHA, CI run URL, readiness state, accepted WARN rows,
and published artifact paths. Keep this evidence with the release notes or
ticket.

## Rollback

```bash
git revert <bad-commit>
make release && make quality
```

After rollback, verify the dashboard and report open from the reverted commit
and that CI is green.

## Ownership

| Area | Owner |
|---|---|
| Data contracts, QA policy, security triage | Analytics Engineering |
| Dashboard and report artifacts | BI / Analytics |
| Final release approval | Project owner |
