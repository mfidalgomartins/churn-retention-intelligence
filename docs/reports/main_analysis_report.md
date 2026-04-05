# Main Analysis Narrative

Generated at: `2026-04-02 13:56:07`

## Concise Executive Summary

The current retention profile shows 2,547 active customers out of 3,500, with customer churn at 27.2% and revenue churn at 13.8%. Future revenue at risk is estimated at $142,193 from explicitly at-risk accounts, with additional hidden risk in behaviorally deteriorating accounts.
Churn pressure is strongest in specific cohorts and commercial slices, and the top-ranked drivers indicate a mix of acquisition-quality effects and preventable operational friction (usage decay, support burden, and payment failures).

## 1. Overall Retention Health

Question being answered: What is the current health of the customer base and how much revenue is being lost?
Metrics used: active customer base, customer churn rate, revenue churn rate, customer churn vs revenue churn gap, and monthly retention trend.
Result: active customers = 2,547; churned customers = 953; customer churn = 27.2%; revenue churn = 13.8%; delta (revenue - customer churn) = -13.5%; last-12-month average customer churn = 2.7% and trend = deteriorating.
Business interpretation: revenue churn close to or above customer churn indicates meaningful value loss per churn event, not just volume loss.
Caveat: monthly-value proxies are used for revenue churn, not full contractual ARR waterfalls.

## 2. Cohort Retention

Question being answered: Are newer signup cohorts retaining better than older cohorts?
Metrics used: cohort retention rate, cohort revenue retention, and 6-month mature-cohort comparison.
Result: average 6-month retention = 97.8%; average 6-month revenue retention = 98.8%; mature cohort trend = deteriorating.
Business interpretation: onboarding and early lifecycle effectiveness can be inferred from fixed-age cohort performance.
Caveat: recent cohorts are incomplete and should not be over-interpreted at longer ages.

## 3. Churn Drivers

Question being answered: Which segments and behaviors are most associated with churn?
Metrics used: churn rates by segment/region/channel/plan and behavioral churn-rate lifts.
Result: top churn drivers by estimated excess MRR loss are listed below, combining prevalence, churn lift, and account value.
Business interpretation: churn is not random; it clusters in specific commercial and behavioral profiles that can be targeted.
Caveat: these are correlations and should guide hypotheses for intervention testing.

Behavior relationships (required checks):

| Behavior | Churn Rate In Group | Churn Rate Out Group | Lift |
|---|---:|---:|---:|
| Usage decline | 28.8% | 26.1% | 1.11x |
| High support ticket load | 53.4% | 18.1% | 2.95x |
| Failed payments | 72.7% | 21.3% | 3.42x |
| Low NPS | 99.7% | 3.0% | 33.09x |

## 4. Revenue at Risk

Question being answered: Where is future revenue most exposed, and is loss concentrated in high-value accounts?
Metrics used: at-risk MRR, high-value at-risk concentration, segment contribution to revenue loss, value-tier churn distribution.
Result: future revenue at risk = $214,300; high-value at-risk accounts = 120 (MRR $97,256); largest segment loss concentration = SMB ($137,223).
Business interpretation: concentration of risk in high-value cohorts requires precision retention motions, not broad campaigns.
Caveat: value concentration is quantile-based and should be revalidated on production revenue definitions.

## 5. Retention Opportunities

Question being answered: Which interventions are most actionable for near-term retention impact?
Metrics used: recoverable customers, recoverable MRR, benchmark churn rate, and priority score.
Result: intervention priorities are ranked below by estimated retention ROI proxy.
Business interpretation: top opportunities combine high MRR coverage with high observed churn propensity.
Caveat: opportunity priority should be validated with capacity constraints and conversion benchmarks.

## Ranked Main Churn Drivers

| Rank | Driver | Impacted Customers | Churn Lift | Estimated Excess MRR Loss |
|---:|---|---:|---:|---:|
| 1 | low_nps_flag | 877 | 3.66x | $105,048 |
| 2 | low_feature_adoption_flag | 875 | 3.37x | $97,205 |
| 3 | high_support_ticket_flag | 905 | 1.96x | $61,651 |

## Prioritized Intervention Opportunities

| Rank | Opportunity | Recoverable Customers | Recoverable MRR | Benchmark Churn Rate |
|---:|---|---:|---:|---:|
| 1 | Renewal Save Desk | 897 | $290,553 | 42.0% |
| 2 | Service Recovery | 248 | $71,092 | 68.6% |
| 3 | Payment Rescue | 111 | $47,549 | 72.7% |