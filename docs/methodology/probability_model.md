# Churn Probability Model

## Estimation target

- Unit: an active customer observed at calendar month-end.
- Outcome: subscription termination within the next 90 days.
- Eligibility: at least 60 days of observed subscription history.
- Label availability: the full 90-day horizon must end on or before the data snapshot.
- Intended use: prioritize retention review and define eligible populations for controlled intervention tests.

The label is never used in the feature calculation. Usage and payment windows
end at the observation timestamp; subscription status after that timestamp is
used only to construct the outcome.

## Temporal design

| Split | Observation dates | Purpose |
|---|---|---|
| Train | 2022-04-30 to 2024-06-30 | Fit model coefficients |
| Calibration | 2024-10-31 to 2025-03-31 | Fit Platt probability calibration |
| Test | 2025-07-31 to 2025-11-30 | Final out-of-time evaluation |

Each gap exceeds the 90-day label horizon. This prevents the outcome window of
one split from overlapping the observation window of the next.

## Model

The release model is a class-weighted, L2-regularized logistic regression.
Categorical inputs are one-hot encoded with unseen-category handling; numeric
inputs are standardized. Platt scaling is fitted only on the calibration split.
This design keeps the bundle interpretable and operationally stable while still
returning calibrated probabilities.

Inputs include commercial context, tenure, current MRR, recent sessions,
usage trend, adoption, support, NPS, failed payments, and renewal proximity.
The synthetic generator does not expose its status field to the model.

## Release evidence

| Test metric | Value | Gate |
|---|---:|---:|
| ROC AUC | 0.8227 | ≥ 0.70 |
| Average precision | 0.3122 | ≥ 0.15 |
| Brier score | 0.0344 | ≤ 0.06 |
| Calibration intercept | 0.1179 | absolute value ≤ 0.50 |
| Calibration slope | 1.0691 | 0.60-1.40 |

The model also publishes decile calibration, coefficient, monthly outcome, and
PSI drift tables. `outputs/models/model_metadata.json` records model version,
library version, split dates, intended use, limitations, and artifact SHA-256.

## Monitoring and retraining

- Outcome monitoring compares mean predicted probability with observed event
  rate and calculates Brier score and ROC AUC by observation month.
- Numeric and categorical PSI compare train with out-of-time and current data.
- Tenure drift is expected as the portfolio ages and is excluded from actionable
  alerts; it remains visible in the drift table.
- Alert thresholds are in `config/monitoring.yml`.
- Retraining requires complete outcomes, confirmed source semantics, a new
  independent calibration window, and the same out-of-time release gates.

## Limits

All current outcomes are synthetic. Repeated customer-month rows make the
reported metrics descriptive out-of-time evidence rather than confidence
intervals from independent observations. Live data requires a fresh calibration
study before probabilities support financial decisions.
