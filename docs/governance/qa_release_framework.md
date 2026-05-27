# QA & Release Framework

## Readiness states

| State | Means |
|---|---|
| `technically valid` | No blocker failures in data quality or product gates |
| `analytically acceptable` | Technically valid + no analytical failures + ≤1 major warning |
| `decision-support only` | Analytically acceptable but with synthetic-data or proxy caveats |
| `screening-grade only` | Technically stable but analytically below decision-support threshold |
| `not committee-grade` | Any unresolved caveat or the synthetic-data flag is on |
| `publish-blocked` | Any FAIL anywhere |

## Severity

`blocker` → release stop. `critical` → correctness failure. `major` → caveat
requiring acknowledgement. `minor` → tracked but non-blocking. `info` → trace.

## Gate inputs

- `config/contracts/data_contracts.json` — schema and primary-key contracts
- `config/governance/release_policy.yml` — release policy
- `config/governance/score_stability_baseline.json` — score drift baseline

## Gate outputs

- `outputs/tables/data_contract_checks.csv` and `data_contract_issues.csv`
- `outputs/tables/final_validation_checks.csv` and `final_validation_issues.csv`
- `outputs/tables/release_readiness_matrix.csv`

## How to run

```bash
make validate    # contract gate then final validation
make test        # unit + integration tests
```

Both run in CI on every push to `main` and every pull request.
