# Release Runbook

## Cut a release

```bash
make install     # one-time setup
make release     # regenerate dashboard, chart pack, report, and validation
make test        # unit and integration tests
make lint        # static code checks
```

Inspect:
- `outputs/tables/release_readiness_matrix.csv` — readiness state
- `outputs/tables/final_validation_issues.csv` — open warnings or failures
- `outputs/dashboard/executive-retention-command-center.html` — the deliverable

## Block release when

Any FAIL in `data_contract_checks.csv` or `final_validation_checks.csv`.

## Rollback

```bash
git revert <bad-commit>
make release && make test && make lint
```

## Ownership

| Area | Owner |
|---|---|
| Data contracts, QA policy | Analytics Engineering |
| Dashboard artifact | BI / Analytics |
| Final release approval | Project owner |
