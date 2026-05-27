# Risk Scoring Methodology

Recoverable customers only (`churn_flag = 0`). `at_risk_flag` is excluded from the
score to avoid label leakage from the simulator's own status field.

## Signals (0-1, higher = more risk)
- `usage_decline   = clip(-usage_trend / 5, 0, 1)`
- `failed_payments = clip(failed_payments_90d / 1.5, 0, 1)`
- `support_burden  = clip((support_tickets_90d - 3) / 6, 0, 1)`
- `low_nps         = clip((20 - nps_score_recent) / 25, 0, 1)`
- `low_adoption    = clip((50 - feature_adoption_score_recent) / 25, 0, 1)`

Thresholds are anchored on the active-customer distribution: each signal hits 1.0
where the value enters the bottom decile of the active population.

## Churn risk score (0-100)
```
churn_risk = 100 * sum(signal_i * weight_i)
           + 4 * max(0, count(signals >= 0.5) - 2)        # concentration bonus
           + renewal_near_flag * min(base, 60) * 0.10     # renewal amplifier
           + contraction_flag * 6
           + (recent_sessions_30d == 0) * 5
clip to [0, 100]
```

Weights:
  - `usage_decline` → 0.26
  - `failed_payments` → 0.22
  - `low_nps` → 0.20
  - `low_adoption` → 0.16
  - `support_burden` → 0.16

The concentration bonus reflects that co-firing signals are stronger churn
predictors than isolated ones; it adds 4 points per signal beyond the second.

## Revenue risk score (0-100)
```
revenue_risk = 100 * (0.60 * rank_pct(current_mrr)
                    + 0.30 * rank_pct(avg_monthly_revenue)
                    + 0.10 * rank_pct(lifetime_revenue))
```

## Retention priority (0-100)
```
retention_priority = 0.65 * churn_risk + 0.35 * revenue_risk
```

## Tiering
  - `critical` → priority ≥ 50.0
  - `high` → priority ≥ 40.0
  - `medium` → priority ≥ 30.0
  - `critical` override: `churn_risk >= 55 AND revenue_risk >= 60`

## Main risk driver
The signal with the largest weighted contribution.

## Recommended actions
First match wins:
- `executive save motion` — critical tier with revenue_risk ≥ 70
- `billing intervention` — failed payments driver, churn_risk ≥ 45
- `product adoption campaign` — usage / adoption driver, churn_risk ≥ 35
- `renewal conversation` — renewal_near_flag + tier ≥ medium
- `customer success outreach` — any remaining high/critical, plus support/NPS medium
- `monitor only` — otherwise

## Outputs
- `data/processed/customer_risk_scores.csv` — one row per scored customer
- `outputs/tables/risk_tier_summary.csv` — tier-level rollup
