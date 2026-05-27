# Executive Decision Memo

## Where revenue is leaking

- 3,500 customers in the base; 2,547 active, 953 churned.
- Customer churn **27.2%**, revenue churn **13.8%** — value loss is concentrated, not uniform.
- Explicit at-risk MRR: **$142,193**. Adding hidden risk signals brings the future exposure to **$214,300**.

## What to do first

1. **Protect high-value accounts.** A small number of accounts carry a disproportionate share of exposed revenue.
2. **Separate volume from value.** Use scaled plays for high-volume churn segments; reserve dedicated save motions for high-tier accounts.
3. **Act on the strongest behavioural signals.** Failed payments, low NPS, and low adoption have the highest churn lift in this run.

## What the analysis shows

| Question | Result |
|---|---|
| Strongest segments at risk | Startup churn highest; Mid-Market and SMB carry the most absolute revenue exposure |
| Strongest channel quality issues | Paid Search and Affiliate retain materially worse than Referral or Partner |
| Cohort trend | 6-month retention has weakened in recent cohorts — acquisition or onboarding drift |
| Top behavioural lift | Failed payments and low NPS show the largest in-vs-out churn ratios |

## Priority interventions

| Play | Target | When |
|---|---|---|
| Renewal Save Desk | Renewal-near accounts with any risk signal | This quarter |
| Service Recovery | Low NPS or high ticket load + usage decline | Now (7-day SLA) |
| Payment Rescue | Failed payments in last 90 days | Always-on |
| Adoption Rescue | Basic/Startup, low adoption + usage decline | Onboarding pivot |
| Channel Quality | Tighten Paid Search and Affiliate gates | Procurement review |

## Caveats

Synthetic data. Revenue uses monthly-value proxies, not full ARR.
Behavioural drivers indicate correlation, not causation. Cohort comparisons at
long horizons are unfair while recent cohorts remain right-censored.
