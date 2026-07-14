# Retention Experimentation and Incrementality

## Design

- Experiment: `retention-outreach-v1`.
- Unit of assignment: customer.
- Eligible population: open accounts in the calibrated high or critical
  90-day probability tier.
- Treatment: retention outreach.
- Counterfactual: business-as-usual holdout.
- Allocation: 50/50 within probability tier and segment.
- Estimand: intention-to-treat effect among eligible customers.
- Outcomes: 90-day churn and lost monthly recurring value.

The deterministic reference assignment contains 172 treatment and 171 control
accounts. Baseline balance is checked on probability, current MRR, tenure,
sessions, NPS, and failed payments. The maximum allowed absolute standardized
mean difference is 0.20.

## Effect calculation

Within each assignment stratum, the treatment mean is compared with the control
mean. Stratum effects are weighted by their eligible-population share.

```text
ITT effect = weighted mean(treatment outcome - control outcome)
incremental saved MRR = - ITT lost-MRR effect × treated customers
```

Neyman variance combines treatment and control sample variances within strata.
The output reports standard error, normal-approximation 95% confidence interval,
and two-sided p-value. Outcome completeness and action contamination are
monitored separately from the effect estimate.

## Reference result

The bundled forward outcomes are explicitly simulated with a treatment odds
ratio of 0.70. The realized deterministic sample does not show an effect:
90-day churn differs by 0.4 percentage points and incremental saved MRR is
-$131, with a 95% interval from -$10,907 to $10,645. The null result is retained
rather than selecting a favourable seed; it demonstrates that the estimator can
return insufficient evidence.

This result proves the assignment and measurement workflow; it does not prove
that outreach works in production. A live deployment must replace
`synthetic_counterfactual_simulation` outcomes with observed data before making
causal or ROI claims.

For CSV and PostgreSQL runs, simulation is disabled by construction. Missing
forward outcomes produce a pending ledger and no effect estimate. Estimation
starts only when every assigned customer has a complete observed outcome and
both arms remain represented within every assignment stratum.

## Operational safeguards

- Assignment is persisted before treatment delivery.
- Holdouts are explicit and foreign-keyed to the scored population.
- Outcomes retain assignment, due date, source, status, delivery, and MRR basis.
- Release checks reconcile assignment/outcome IDs, holdout flags, balance, and
  the saved-MRR identity.
