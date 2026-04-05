# Churn & Retention Intelligence System

## 1) Project Title
**Churn & Retention Intelligence System**  
Portfolio project simulating an internal analytics workflow for churn diagnostics, revenue-risk prioritization, and executive decision support.

## 2) Business Problem
B2B subscription businesses rarely fail because they cannot measure churn; they fail because they cannot convert churn signals into prioritized action.  
The core question in this project is:

**Where is the company losing future revenue, which customers are most at risk, and what actions should be prioritized first?**

## 3) Objective
Build a complete retention analytics system that:
- Quantifies customer and revenue churn
- Identifies concentration of risk across segments, channels, plans, and regions
- Produces interpretable customer-level risk scores
- Surfaces intervention priorities for Revenue, Customer Success, and Operations
- Delivers an executive HTML command center for decision-making

## 4) Repository Structure
```text
churn-retention-intelligence-system/
├── data/
│   ├── raw/                         # Synthetic source tables
│   └── processed/                   # Engineered feature tables + risk outputs
├── notebooks/                       # Notebook workspace (kept lightweight for portfolio hygiene)
├── src/
│   ├── data_generation/
│   ├── data_profiling/
│   ├── feature_engineering/
│   ├── churn_analysis/
│   ├── retention_analysis/
│   ├── risk_scoring/
│   ├── visualization/
│   ├── dashboard_builder/
│   └── validation/
├── tests/                           # Smoke/integrity checks
├── outputs/
│   ├── tables/                      # Analysis outputs, summaries, validation logs
│   └── charts/                      # Publication-quality chart pack
├── config/
│   ├── contracts/                   # Data contracts and required schema/keys
│   └── governance/                  # Release policy + score stability baseline
├── dashboard/
│   └── churn_retention_command_center.html
├── docs/                            # Methodology notes, analysis report, QA report
│   ├── architecture/                # Dashboard product/design docs
│   ├── methodology/                 # Feature/risk/scoring references
│   ├── reports/                     # Generated analytical and QA reports
│   └── governance/                  # QA framework and release readiness outputs
├── requirements.txt
├── Makefile
└── LICENSE
```

## 5) Methodology
The project follows a production-style analytics lifecycle:
1. Synthetic data simulation with business behavior assumptions
2. Formal profiling and data quality checks
3. Feature engineering for retention and risk signals
4. Main churn/revenue analysis (cohorts, drivers, concentration)
5. Interpretable risk scoring and action mapping
6. Visualization pack for narrative support
7. Executive dashboard build
8. End-to-end QA validation before stakeholder delivery

## 6) Data Generation or Data Source
This project uses **synthetic data** designed to mimic subscription business dynamics:
- `customers`
- `subscriptions`
- `product_usage`
- `payments`

Simulation logic includes:
- Segment/channel/region differences in churn propensity
- Usage decline and support burden patterns before churn
- Failed payment signals in some churn pathways
- Revenue skew and account value distribution

## 7) Key Engineered Features
Core engineered outputs are in `data/processed/customer_retention_features.csv`, including:
- Lifecycle: `tenure_days`, `renewal_near_flag`
- Revenue: `current_mrr`, `avg_monthly_revenue`, `lifetime_revenue`
- Engagement: `recent_sessions_30d`, `recent_sessions_90d`, `usage_trend`
- Product health: `feature_adoption_score_recent`, `nps_score_recent`
- Support/billing: `support_tickets_30d`, `support_tickets_90d`, `failed_payments_90d`, `payment_failure_flag`
- Status flags: `churn_flag`, `at_risk_flag`, `contraction_flag`

Additional analytical tables:
- `cohort_retention_table.csv`
- `segment_retention_summary.csv`

## 8) Risk Scoring Framework
The scoring layer is intentionally interpretable (no black-box model):
- **`churn_risk_score`**: weighted behavioral + operational risk signals
- **`revenue_risk_score`**: value importance using percentile-ranked revenue features
- **`retention_priority_score`**: combined intervention priority (`0.65 * churn_risk + 0.35 * revenue_risk`)
- **`risk_tier`**: `low`, `medium`, `high`, `critical`
- **`main_risk_driver`** + **`recommended_action`** for operational routing

Outputs:
- `data/processed/customer_risk_scores.csv`
- `data/processed/customer_risk_priority_ranked.csv`
- `outputs/tables/risk_tier_summary.csv`

## 9) Key Findings
From the current analytical run:
- Customer churn: **27.2%** (953 / 3,500)
- Revenue churn (monthly-value proxy): **13.8%**
- Explicit at-risk MRR: **$142,193**
- Estimated future revenue at risk (explicit + hidden): **$214,300**
- Highest churn concentrations:
  - Segment: **Startup (44.8%)**
  - Region: **LATAM (35.0%)**
  - Acquisition channel: **Affiliate (42.1%)**
  - Plan: **Basic (44.7%)**
- Largest revenue loss concentration: **SMB**

## 10) Business Recommendations
1. Stand up a **Renewal Save Desk** for renewal-near high-risk accounts (CS + Sales).
2. Run **Service Recovery** motion for high-support / low-NPS accounts (Support + CS Ops).
3. Launch **adoption reactivation campaigns** for usage/adoption deterioration (Product + CS).
4. Apply **retention-weighted CAC governance** to Affiliate and Paid Search (Marketing + RevOps).
5. Maintain a **high-value account protection list** with executive escalation for critical-risk accounts.

## 11) Dashboard Overview
The executive dashboard is available at:
- `dashboard/churn_retention_command_center.html`

It includes:
- KPI strip (active base, churn, revenue at risk, high/critical counts)
- Retention trends and cohort views
- Diagnostic breakdowns (segment/region/channel/plan + behavioral indicators)
- Customer prioritization table with recommended actions
- Action-oriented intervention grouping
- Version stamping (`dashboard_version`, `builder_version`) and offline packaging for traceable executive exports

## 12) How to Run
From project root:

```bash
python -m venv .venv
./.venv/bin/python -m pip install --upgrade pip
./.venv/bin/python -m pip install -r requirements.txt

./.venv/bin/python src/data_generation/generate_synthetic_data.py
./.venv/bin/python src/data_profiling/profile_data_quality.py
./.venv/bin/python src/feature_engineering/create_retention_features.py
./.venv/bin/python src/churn_analysis/run_main_analysis.py
./.venv/bin/python src/risk_scoring/build_risk_scores.py
./.venv/bin/python src/visualization/build_chart_pack.py
./.venv/bin/python src/dashboard_builder/build_executive_dashboard.py
./.venv/bin/python src/validation/validate_data_contracts.py
./.venv/bin/python src/validation/run_final_validation.py
./.venv/bin/python -m unittest discover -s tests -p "test_*.py" -v
```

Then open:
- `dashboard/churn_retention_command_center.html`

Shortcut:

```bash
make install
make all
make test
```

## 13) Limitations
- Data is synthetic and does not represent live production behavior.
- Revenue churn uses monthly-value proxy logic, not full ARR contract accounting.
- Behavioral relationships are associative, not causal.
- Synthetic-data governance policy marks outputs as **not committee-grade** even when technically strong.
- Current release recommendation should be read from:
  - `outputs/tables/data_contract_checks.csv`
  - `docs/governance/data_contract_validation_report.md`
  - `outputs/tables/release_readiness_matrix.csv`
  - `docs/reports/final_validation_report.md`
  - `docs/governance/release_readiness_summary.md`

## 14) Future Improvements
- Add causal testing layer (A/B intervention lift measurement).
- Add probabilistic calibration and backtesting for risk scoring.
- Integrate real CRM/billing/support sources with data contracts.
- Add CI checks for metric definitions and dashboard consistency.
- Add orchestration (scheduled pipeline runs + alerting on risk spikes).
