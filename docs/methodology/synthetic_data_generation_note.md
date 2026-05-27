# Synthetic Data Generation

Deterministic given `(SEED, REFERENCE_DATE)`. Both are read from environment
variables (`CHURN_SEED`, `CHURN_REFERENCE_DATE`) with defaults of `42` and
`2026-03-01`.

## What the simulator embeds

- **Segment mix** weighted toward SMB and Mid-Market, with a long tail of Startup and Enterprise.
- **Churn pressure** is higher for Startup/SMB and lower for Enterprise; logit
  combines segment, channel, region, plan, tenure, and revenue effects.
- **Acquisition quality** differs by channel: Paid Search and Affiliate retain
  worse, Referral and Partner retain better.
- **Revenue** is plan-driven and right-skewed (lognormal). Account value is
  concentrated in higher tiers, as it tends to be in real B2B SaaS.
- **Pre-churn deterioration** is observable in the usage stream up to 180 days
  before churn: sessions decay, NPS drops, support tickets spike (more for
  Startup/SMB), feature adoption falls.
- **At-risk accounts** show a softer version of the same pattern in the last
  90 days before the snapshot.
- **Failed payments** spike near churn; a configurable share of churned
  accounts gets a forced delinquency event in the final 60 days.

## Files produced

- `data/raw/customers.csv` (3,500 rows)
- `data/raw/subscriptions.csv` (3,500 rows)
- `data/raw/product_usage.csv` (~367k rows, weekly grain)
- `data/raw/payments.csv` (~56k rows, billing-cycle grain)

## Why synthetic
Real customer-level retention data is usually too sensitive to share. Synthetic
data lets the analytics and governance design be inspected publicly while
keeping the patterns realistic enough to behave like a real retention problem.
