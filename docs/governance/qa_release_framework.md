# QA & Release Framework

## Readiness States
This project now tracks readiness using explicit states:
- technically valid
- analytically acceptable
- decision-support only
- screening-grade only
- not committee-grade
- publish-blocked

## Gate Logic
- **Contract gate**: required datasets/columns/primary keys must pass before analytical validation
- **Blocker FAIL**: immediate `publish-blocked`
- **Technical validity**: no blocker failures in data quality/metric/dashboard core gates
- **Analytical acceptance**: technically valid + no analytical FAIL + bounded major WARNs
- **Decision-support only**: analytically acceptable but still caveated (synthetic/proxy/correlation)
- **Screening-grade only**: technically stable but analytically insufficient
- **Not committee-grade**: any unresolved caveat or synthetic-data limitation

## Severity Levels
- `blocker`: release stop
- `critical`: major correctness failure
- `major`: material caveat requiring explicit stakeholder acknowledgement
- `minor`: non-blocking but trackable issue
- `info`: pass/trace information

## Policy Files
- `config/governance/release_policy.yml`
- `config/governance/score_stability_baseline.json`
- `config/contracts/data_contracts.json`

## Enforcement Outputs
- `outputs/tables/data_contract_checks.csv`
- `outputs/tables/data_contract_issues.csv`
- `outputs/tables/final_validation_checks.csv`
- `outputs/tables/final_validation_issues.csv`
- `outputs/tables/release_readiness_matrix.csv`
- `docs/governance/data_contract_validation_report.md`
- `docs/governance/qa_release_summary.md`

## Operational Runbook
- `docs/governance/release_runbook.md`
