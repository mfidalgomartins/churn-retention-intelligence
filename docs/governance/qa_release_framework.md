# QA & Release Framework

The release framework has two independent gate groups:

| Group | Command | Purpose |
|---|---|---|
| Analytical release gates | `make validate` | Data contracts, metric integrity, model quality, experiment integrity, monitoring, dashboard readiness, and release state |
| Engineering quality gates | `make quality` | Lint, tests with coverage, Bandit, and dependency audit |

Both gate groups must pass before publishing dashboard, chart, or report
artifacts.

## Readiness states

| State | Means |
|---|---|
| `technically valid` | No blocker failures in data quality or product gates |
| `analytically acceptable` | Technically valid + no analytical failures + at most 1 major warning |
| `decision-support only` | Analytically acceptable but with synthetic-data or proxy caveats |
| `screening-grade only` | Technically stable but analytically below decision-support threshold |
| `not committee-grade` | Any unresolved caveat or the synthetic-data flag is on |
| `publish-blocked` | Any FAIL anywhere |

## Severity

| Severity | Release handling |
|---|---|
| `blocker` | Stop release. |
| `critical` | Correctness failure; stop release unless explicitly downgraded in policy. |
| `major` | Requires acknowledgement and may lower analytical readiness. |
| `minor` | Track but do not block release. |
| `info` | Trace only. |

## Gate inputs

- `config/contracts/data_contracts.json` — schema, primary-key, value-domain, numeric-range, date-order, and referential-integrity contracts
- `config/governance/release_policy.yml` — release policy
- `config/governance/score_stability_baseline.json` — score drift baseline
- `config/modeling.yml` — temporal split, calibration, and model thresholds
- `config/experiments.yml` — eligibility, allocation, estimand, and balance thresholds
- `config/monitoring.yml` — transition, outcome, and drift alert thresholds

## Gate outputs

- `outputs/tables/data_contract_checks.csv` and `data_contract_issues.csv`
- `outputs/tables/final_validation_checks.csv` and `final_validation_issues.csv`
- `outputs/tables/release_readiness_matrix.csv`

## Release decision

Publish only when all of the following are true:

- `make release` completed from a clean dependency environment.
- `make quality` completed successfully.
- `release_readiness_matrix.csv` does not contain `publish-blocked`.
- `data_contract_checks.csv` and `final_validation_checks.csv` contain no FAIL rows.
- Any WARN rows in `final_validation_issues.csv` are accepted by the project owner.

Synthetic-data releases remain decision-support only. They are not
committee-grade, regardless of technical quality.

## How to run

```bash
make validate    # contract gate then final validation
make quality     # lint, coverage, and security/dependency checks
```

Both run in CI on every push to `main` and every pull request.
