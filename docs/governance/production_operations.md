# Production Operations

## Source configuration

`config/pipeline.yml` maps the seven required source tables: customers,
subscriptions, product usage, payments, revenue movements, acquisition spend,
and direct service costs.

CSV ingestion reads `CHURN_SOURCE_DIR` or `--source-directory`. PostgreSQL
ingestion reads `CHURN_DATABASE_URL`; credentials are never written to logs or
configuration. SQL schema and table identifiers are allow-listed before query
construction, and each query selects the contracted columns explicitly.
`CHURN_REFERENCE_DATE` must equal the batch cutoff date. Production owners must
also roll the train, calibration, and test windows in `config/modeling.yml` as
new labelled history becomes available.

The adapter loads the complete batch before publication. Validation covers
required columns, non-empty tables, primary keys, allowed domains, numeric
ranges, date order, and foreign keys. File publication uses an atomic replace
and writes `data/raw/_ingestion_manifest.json` with row counts and SHA-256
checksums. The manifest is published last and acts as the batch commit marker;
profiling refuses to run if any raw file differs from it.

`make production` never invokes the synthetic data or outcome generators. On a
new experiment it persists the assignment and explicit holdout. If
`CHURN_INTERVENTION_OUTCOMES_FILE` is absent, the outcome ledger remains
`pending` and incremental effects are `not_estimable`; a later run can merge an
observed file with `customer_id`, `assigned_action_delivered`, `churned_90d`,
and `lost_mrr_90d`. Existing production assignments are immutable for the same
experiment ID.
When the processed layer is ephemeral, set
`CHURN_INTERVENTION_ASSIGNMENTS_FILE` to a durable copy of the first run's
assignment ledger. A configured but missing file blocks execution, preventing
silent re-randomization across stateless jobs.

## Scheduled execution

`.github/workflows/scheduled-snapshot.yml` runs the reproducible reference
release at 06:00 UTC every Monday and supports manual dispatch. The workflow:

1. installs from the resolved constraints;
2. runs the complete release or production pipeline;
3. blocks on source contracts and analytical release gates;
4. creates a content-addressed snapshot; and
5. retains the GitHub Actions artifact for 90 days.

Static tests, type checks, security scanning, and dependency audit run in the
required pull-request CI workflow before code reaches the scheduled branch.

Production scheduling belongs in an environment with a secret manager and a
durable assignment store. That orchestrator should install the `warehouse`
extra, set `CHURN_DATABASE_URL`, `CHURN_REFERENCE_DATE`, and
`CHURN_INTERVENTION_ASSIGNMENTS_FILE` after the first assignment, then run
`make production` followed by `make snapshot`.

## Immutable snapshots

`make snapshot` packages configured raw, processed, model, and analytical files.
The archive name contains the as-of date and the first 12 characters of a digest
over every member path and checksum. ZIP metadata is fixed, so identical inputs
produce identical bytes. A checksum collision at an existing path is rejected.

Each archive contains `manifest.json` with source adapter, as-of date, file
count, bytes, content digest, and per-file SHA-256. Snapshot archives are
operational artifacts rather than source-controlled files.

## Alert triage

Inspect `outputs/tables/monitoring_alerts.csv` after every run.

| Category | First response |
|---|---|
| Portfolio risk | Confirm critical accounts and intervention capacity |
| Risk migration | Segment new critical entries and inspect leading signals |
| Calibration | Verify outcome completeness, then recalibrate if persistent |
| Feature drift | Validate source definitions before retraining |

Migration alerts use complete monthly intervals. The latest partial snapshot is
shown in portfolio totals but cannot trigger a monthly-transition alert.

## Recovery

- A failed adapter does not publish any validated frames.
- A failed analytical gate stops the release before the final dashboard render.
- Re-run with the same source batch to reproduce the same analytical artifacts.
- Restore a prior content-addressed snapshot to inspect or replay a released
  state; do not overwrite an existing snapshot.
