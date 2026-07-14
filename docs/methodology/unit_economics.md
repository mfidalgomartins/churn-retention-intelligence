# Revenue and Unit Economics

All monetary outputs use USD. They are recurring-value and direct-cost proxies,
not audited revenue recognition or full cost accounting.

## Contract-MRR bridge

The event ledger records five mutually exclusive movements: new, expansion,
contraction, reactivation, and churn. For every calendar month:

```text
closing MRR = opening MRR + new + expansion + contraction + reactivation + churn
```

Contraction and churn are negative values. The release gate requires the bridge
to reconcile within $0.01 at every month.

```text
NRR = (opening + expansion + contraction + reactivation + churn) / opening
GRR = max(opening + contraction + churn, 0) / opening
```

New MRR is excluded from both measures. Reactivation is included in NRR and
excluded from GRR, as specified in `config/economics.yml`.

## Revenue and margin basis

Monthly recurring-value proxy is average opening and closing MRR. Direct service
cost is the cost ledger at customer-month grain.

```text
gross profit proxy = recurring-value proxy - direct service cost
gross margin rate = gross profit proxy / recurring-value proxy
```

The current partial month is excluded from NRR, GRR, churn, margin, payback, and
LTV inputs. Current MRR remains a snapshot measure and therefore includes the
snapshot date.

## Acquisition and lifetime economics

```text
CAC = (marketing spend + sales spend) / acquired customers
payback months = CAC / monthly gross profit per active account
24m LTV = ARPA × gross margin × sum(monthly survival through month 24)
LTV:CAC = 24m LTV / CAC
```

Spend is allocated by acquisition channel. Realized gross-margin LTV is also
reported, but cohorts have unequal follow-up and must not be compared without
that caveat. The 24-month survival model caps the horizon to avoid an unstable
perpetuity when observed churn is low.

## Canonical outputs

- `revenue_movement_bridge.csv`
- `unit_economics_by_channel.csv`
- `unit_economics_by_segment.csv`
- `cohort_unit_economics.csv`
- `unit_economics_summary.csv`
- `unit_economics_metadata.json`
