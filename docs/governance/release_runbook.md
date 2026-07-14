# Release Runbook

## Build and verify

```bash
make install
make release
make quality
git diff --exit-code -- docs/index.html docs/decision_memo.md index.html outputs/dashboard outputs/graphs outputs/models outputs/reports
```

`make release` rebuilds the synthetic reference, economics, model, experiment,
monitoring, dashboard, report, release gates, and immutable snapshot. `make
quality` runs lint, formatting, typing, branch coverage, static security, and
dependency vulnerability checks.

## Required evidence

- `data_contract_checks.csv`: no FAIL rows.
- `final_validation_checks.csv`: no FAIL or WARN rows.
- `release_readiness_matrix.csv`: `publish-blocked=False`.
- `model_performance.csv`: all configured out-of-time gates pass.
- `intervention_balance.csv`: randomization balance passes.
- `monitoring_alerts.csv`: alerts reviewed and owned.
- Local release and quality commands pass from a clean checkout.
- Pull-request CI and subsequent `main` CI both pass.

## Publish

1. Commit the final generated artifacts and source changes on a release branch.
2. Open a pull request and wait for every required check.
3. Merge without bypassing CI.
4. Confirm the `main` workflow passes on the merge commit.
5. Create an annotated semantic-version tag on that merge commit.
6. Publish a GitHub release with the PDF, dashboard, snapshot archive, and
   snapshot manifest attached.
7. Record tag, commit SHA, CI URL, readiness state, and accepted alerts in the
   release notes.

## Block release when

- Any build, contract, validation, quality, or security command fails.
- Generated tracked artifacts differ after a second build.
- Model gates, probability bounds, experiment assignment integrity, baseline
  balance, saved-MRR identity, or complete-period transition checks fail.
- A security finding or monitoring alert lacks an explicit owner and decision.

## Rollback

```bash
git revert <bad-commit>
make release
make quality
```

Publish a patch release from the reverted state. Keep the failed release and
its snapshot available for audit; never move or reuse an existing tag.

## Ownership

| Area | Owner |
|---|---|
| Source contracts and ingestion | Analytics Engineering |
| Revenue definitions and model governance | Analytics / Data Science |
| Experiment delivery and holdout integrity | Customer Success Operations |
| Dashboard and report | BI / Analytics |
| Release approval and security acceptance | Project owner |
