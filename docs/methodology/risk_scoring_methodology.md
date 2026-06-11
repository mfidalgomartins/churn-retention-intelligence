# Risk Scoring Methodology

Open accounts only (`churn_flag = 0`). `at_risk_flag` is excluded from the
score to avoid label leakage from the simulator's own status field.

## Signals (0-1, higher = more risk)
- `usage_decline   = clip(-usage_trend / 5, 0, 1)`
- `failed_payments = clip(failed_payments_90d / 1.5, 0, 1)`
- `support_burden  = clip((support_tickets_90d - 3) / 6, 0, 1)`
- `low_nps         = clip((20 - nps_score_recent) / 25, 0, 1)`
- `low_adoption    = clip((50 - feature_adoption_score_recent) / 25, 0, 1)`

The thresholds are policy choices for this synthetic case. The score is an
interpretable prioritisation index, not a calibrated churn probability.

## Churn risk score (0-100)
```
churn_risk = 100 * sum(signal_i * weight_i)
           + 4 * max(0, count(signals >= 0.5) - 2)        # concentration bonus
           + renewal_near_flag * min(base, 60) * 0.10     # renewal amplifier
           + (recent_sessions_30d == 0) * 5
clip to [0, 100]
```

Weights:
  - `usage_decline` → 0.26
  - `failed_payments` → 0.22
  - `low_nps` → 0.20
  - `low_adoption` → 0.16
  - `support_burden` → 0.16

The concentration bonus encodes the policy assumption that co-firing signals
warrant more attention than isolated ones; it adds 4 points per signal beyond
the second.

## Customer value score (0-100)
```
customer_value = 100 * (0.60 * rank_pct(current_mrr)
                      + 0.30 * rank_pct(avg_monthly_revenue)
                      + 0.10 * rank_pct(lifetime_revenue))
```

## Retention priority (0-100)
```
retention_priority = churn_risk * (0.65 + 0.35 * customer_value / 100)
```
Customer value ranks risky accounts; it cannot create a priority signal when
behavioural churn risk is zero.

## Tiering
  - `critical` → priority ≥ 35.0
  - `high` → priority ≥ 25.0
  - `medium` → priority ≥ 15.0
  - `critical` override: `churn_risk >= 45 AND customer_value >= 70`

## Main risk driver
The signal with the largest weighted contribution. Accounts with no positive
signal are labelled `no material signal`.

## Recommended actions
First match wins:
- `executive save motion` — critical tier with customer_value ≥ 70
- `billing intervention` — failed payments driver, churn_risk ≥ 45
- `product adoption campaign` — usage / adoption driver, churn_risk ≥ 35
- `renewal conversation` — renewal_near_flag + tier ≥ medium
- `customer success outreach` — any remaining high/critical, plus support/NPS medium
- `monitor only` — otherwise

## Outputs
- `data/processed/customer_risk_scores.csv` — one row per scored customer
- `outputs/tables/risk_tier_summary.csv` — tier-level rollup
