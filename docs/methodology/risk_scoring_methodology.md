# Risk Scoring Methodology Note

## Scope
- This scoring layer targets **recoverable customers only** (`churn_flag = 0`).
- Goal: prioritize intervention effort by combining behavioral churn risk and economic importance.

## Score 1: `churn_risk_score` (0-100)
`churn_risk_score = 100 * weighted_signal_sum + adjustments`

Where weighted signal sum is:
- `0.24 * usage_decline_signal`
- `0.22 * failed_payment_signal`
- `0.16 * support_burden_signal`
- `0.18 * low_nps_signal`
- `0.14 * low_adoption_signal`
- `0.06 * contract_renewal_risk_signal`

Signal definitions (all clipped to `[0,1]`):
- `usage_decline_signal = clip((-usage_trend) / 4, 0, 1)`
- `failed_payment_signal = clip(failed_payments_90d / 2, 0, 1)`
- `support_burden_signal = clip(support_tickets_90d / 6, 0, 1)`
- `low_nps_signal = clip((25 - nps_score_recent) / 35, 0, 1)`
- `low_adoption_signal = clip((55 - feature_adoption_score_recent) / 35, 0, 1)`
- `contract_renewal_risk_signal = renewal_near_flag * (0.60 + 0.40 * max(usage_decline_signal, failed_payment_signal, low_nps_signal, low_adoption_signal))`

Adjustments:
- `+12` if `at_risk_flag = 1`
- `+8` if `contraction_flag = 1`
- `+6` if `recent_sessions_30d = 0`

Final score is capped to `[0,100]`.

## Score 2: `revenue_risk_score` (0-100)
Percentile-based value importance:
- `revenue_risk_score = 100 * (0.60 * rank_pct(current_mrr) + 0.30 * rank_pct(avg_monthly_revenue) + 0.10 * rank_pct(lifetime_revenue))`

## Score 3: `retention_priority_score` (0-100)
Combined intervention priority:
- `retention_priority_score = 0.65 * churn_risk_score + 0.35 * revenue_risk_score`

## Score 4: `risk_tier`
- `critical`: priority >= 75 OR (`churn_risk_score >= 85` AND `revenue_risk_score >= 70`)
- `high`: priority >= 60 and not critical
- `medium`: priority >= 40 and not high/critical
- `low`: priority < 40

## Score 5: `main_risk_driver`
Assigned as the largest weighted component among:
- usage decline
- failed payments
- support burden
- low NPS
- low adoption
- contract renewal risk

## Score 6: `recommended_action`
Rules:
- `executive save motion`: critical tier and high revenue importance
- `billing intervention`: failed payments is main driver with meaningful churn risk
- `renewal conversation`: contract renewal risk is main driver for medium+ risk accounts, or high-value renewal-near accounts
- `product adoption campaign`: usage decline or low adoption main driver in elevated risk
- `customer success outreach`: all high/critical accounts, plus medium-tier support/NPS-led risk
- `monitor only`: low-priority low-risk accounts

## Output Tables
- `data/processed/customer_risk_scores.csv`
- `data/processed/customer_risk_priority_ranked.csv`
- `outputs/tables/risk_tier_summary.csv`
