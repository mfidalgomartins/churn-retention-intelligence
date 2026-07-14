# Changelog

Notable changes follow [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [2.0.0] - 2026-07-14

### Added

- Contract-validated CSV and PostgreSQL source adapters with atomic publication
  and an ingestion checksum manifest.
- Contract-MRR movements, monthly revenue bridge, direct-cost margin, CAC,
  payback, cohort economics, and capped 24-month LTV.
- Point-in-time customer-month snapshots and a regularized 90-day churn model
  with independent temporal calibration, out-of-time gates, executable bundle,
  outcome monitoring, and feature PSI.
- Stratified intervention assignment, explicit holdout, observed/pending outcome
  ledger, synthetic reference simulation, intention-to-treat inference, balance
  checks, and incremental saved-MRR measurement.
- Risk-tier transition history, latest complete-month matrix, portfolio trend,
  and threshold-based operational alerts.
- Deterministic content-addressed snapshots and a weekly GitHub Actions workflow
  with 90-day artifact retention.
- Production architecture, model, economics, experimentation, and operations
  documentation.

### Changed

- Expanded the release pipeline and data contracts to cover economics, model,
  intervention, and monitoring outputs.
- Tuned synthetic pre-churn signals to avoid implausibly perfect model
  performance while retaining decision-useful separation.
- Upgraded the project version and resolved runtime to scikit-learn 1.9.0.
- Expanded validation to 57 passing analytical checks and the full test suite
  with at least 80% branch coverage.

### Fixed

- Excluded incomplete periods from operational economics and monthly transition
  alerts.
- Added explicit simulation provenance so synthetic intervention results cannot
  be mistaken for observed causal evidence.
- Restricted outcome simulation to the synthetic adapter; CSV and PostgreSQL
  runs preserve immutable assignments and monotonic pending/observed outcomes.
- Added current model, assignment, and outcome contracts with foreign-key and
  probability-bound checks.

## [1.0.0] - 2026-06-23

### Added

- Reproducible SaaS churn pipeline with deterministic synthetic data.
- Governed risk scoring, self-contained executive dashboard, chart pack, and PDF.
- Data contracts, release-readiness gates, CI, security checks, and governance
  documentation.
