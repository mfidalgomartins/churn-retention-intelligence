# Outputs

Everything in this directory is generated. Change the producing module, not an
artifact.

- `dashboard/executive-retention-command-center.html` — self-contained
  dashboard with embedded governed data and chart runtime.
- `graphs/` — 18-chart static analytical pack.
- `models/churn_probability_model.joblib` — executable preprocessing,
  classifier, calibrator, features, horizon, and tier policy.
- `models/model_metadata.json` — model version, split dates, test metrics,
  limitations, library version, and artifact SHA-256.
- `reports/churn-retention-intelligence-report.pdf` — narrative report.
- `snapshots/` — local content-addressed ZIP and manifest outputs; ignored by
  Git and retained as workflow/release artifacts.
- `tables/` — analytical marts and release evidence; ignored by Git and rebuilt
  in every release.

The dashboard, graphs, model, and report are tracked. Run `make release` to
rebuild the full artifact set.

Live dashboard: <https://mfidalgomartins.github.io/churn-retention-intelligence/>
