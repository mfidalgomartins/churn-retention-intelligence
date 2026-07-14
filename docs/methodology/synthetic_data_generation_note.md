# Synthetic Data Generation

The reference data is deterministic under `CHURN_SEED=42` and
`CHURN_REFERENCE_DATE=2026-03-01`. Both values can be overridden explicitly.
The report date is deterministic by default and accepts `CHURN_REPORT_DATE`.

## Simulation design

- Segment and plan mix resemble a B2B SaaS portfolio weighted toward SMB and
  Mid-Market accounts.
- Subscription lifetime follows a right-censored Weibull event-time process.
  Segment, acquisition channel, region, plan, and recurring value influence the
  hazard without reading the final portfolio snapshot.
- Account MRR is plan-dependent and lognormal, creating a realistic value tail.
- Pre-churn usage, adoption, support, NPS, and payment signals deteriorate with
  noise and overlap. Their strength is intentionally limited so the out-of-time
  probability model is useful rather than implausibly perfect.
- Open at-risk accounts show a softer late-window deterioration pattern.
- The MRR event ledger contains new, expansion, contraction, reactivation, and
  churn movements that reconcile to the subscription snapshot.
- Acquisition spend varies by month and channel. Direct service cost varies by
  plan, usage intensity, support load, and active MRR.
- Forward intervention outcomes are generated separately from historical churn
  and carry explicit `simulated` status and source fields.

## Canonical source batch

| File | Grain | Reference rows |
|---|---|---:|
| `customers.csv` | customer | 3,500 |
| `subscriptions.csv` | current subscription | 3,500 |
| `product_usage.csv` | customer-week | 304,547 |
| `payments.csv` | billing event | 46,213 |
| `revenue_movements.csv` | customer MRR event | approximately 7,800 |
| `acquisition_spend.csv` | channel-month | 306 |
| `service_costs.csv` | customer-month | 74,361 |

`data/raw/_ingestion_manifest.json` records the synthetic adapter, contract
version, row counts, and file checksums. Exact movement count can change when
simulation logic changes; contracts and reconciliation gates define validity.

## Use boundary

The data exists to make the pipeline, metric definitions, governance, model
workflow, and experiment design inspectable without exposing customer data. It
cannot establish external validity, production treatment efficacy, audited
economics, or expected ROI.
