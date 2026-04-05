# Churn & Retention Intelligence System: Executive Business Report

## 1. Executive Summary
The company is losing revenue through a mix of realized churn and near-term exposure in recoverable accounts.

- **Insight:** The current base is 3,500 customers, with 2,547 active and 953 churned. Customer churn is 27.2%, while revenue churn is 13.8%. Explicit at-risk MRR is $142,193, and estimated future revenue at risk (explicit + hidden) is $214,300.
- **Inference:** Churn volume is high, but value loss is concentrated. This is not a broad “all-customers” problem; it is a concentrated commercial risk problem that can be managed with targeted interventions.

## 2. Business Context
The business question is where future revenue loss is emerging, which customers are most at risk, and which interventions should be prioritized now.

- **Insight:** Churn concentration is uneven across segment, plan, channel, and region. Startup and SMB drive most customer churn, while SMB and Mid-Market contribute most total revenue loss proxy.
- **Inference:** A generic retention program will be inefficient. Commercial strategy should separate volume control (high-churn cohorts) from value protection (high-MRR accounts).

## 3. Methodology
The analysis used synthetic but behaviorally realistic customer, subscription, usage, and payment data from January 2022 through March 2026, with engineered retention features, cohort tables, churn diagnostics, and interpretable risk scores.

- **Insight:** Flags and metrics were defined transparently (for example, `churn_flag`, `at_risk_flag`, cohort retention, and weighted risk scoring outputs). Risk tiers and recommended actions were derived from interpretable rules, not black-box models.
- **Inference:** The framework is operationally usable for prioritization and decision support, provided caveats are respected.

## 4. Retention Health Overview
- **Insight:** Active base: 2,547 customers. Customer churn: 27.2%. Revenue churn: 13.8%. Last-12-month average churn remains materially lower than cumulative churn, indicating lifetime accumulation and recent-period flow tell different stories.
- **Inference:** Leadership should track both stock and flow metrics. Lifetime churn indicates cumulative loss burden; monthly churn indicates current operational velocity.

## 5. Cohort Findings
- **Insight:** Average 6-month retention is 97.8%; 6-month revenue retention is 98.8%; mature cohort trend is deteriorating.
- **Inference:** Early lifecycle monetization remains relatively durable, but newer cohorts are weakening in retention performance. Acquisition and onboarding quality should be reviewed by source and plan.

## 6. Churn Driver Analysis
- **Insight:** Highest churn slices are Startup (44.8%), LATAM (35.0%), Affiliate (42.1%), and Basic plan (44.7%). Top ranked drivers by estimated excess MRR loss include low NPS, low feature adoption, Paid Search channel, and SMB segment concentration.
- **Insight:** Behavioral signals in this final run show strong lift for low NPS, low adoption, failed payments, and elevated support burden.
- **Inference:** The most reliable near-term targeting variables are commercial mix (segment/plan/channel) and customer-health variables (NPS, adoption, payment reliability).

## 7. Revenue at Risk Analysis
- **Insight:** Estimated future revenue at risk is $214,300. High-value at-risk customers account for $97,256 MRR. The largest revenue loss concentration is in SMB ($137,227 total loss proxy).
- **Insight:** Churned customer share is concentrated in lower-value tiers, but churned revenue is concentrated in higher-value tiers (high tier contributes ~40.0% of churned revenue with only ~9.8% of churned customers).
- **Inference:** Revenue protection should prioritize fewer, higher-value accounts while separately reducing high-volume churn in lower tiers through scaled plays.

## 8. Priority Intervention Opportunities
- **Insight:** Highest priority interventions by score are:
  - Renewal Save Desk: 897 recoverable customers, $290,553 recoverable MRR, 42.0% benchmark churn.
  - Service Recovery: 248 customers, $71,092 recoverable MRR, 68.6% benchmark churn.
  - Payment Rescue: 111 customers, $47,549 recoverable MRR.
- **Insight:** Risk tiers among scored (recoverable) customers are concentrated in medium/low count, but high/critical tiers represent immediate operational urgency.
- **Inference:** Intervention sequencing should favor high recoverable MRR multiplied by observed churn propensity, not customer count alone.

## 9. Recommended Actions
1. **Launch a Renewal Save Desk motion for renewal-near, risk-flagged accounts (owner: Customer Success + Sales, start immediately).**  
   Trigger on renewal window plus risk signals; require save offers, executive outreach path, and weekly conversion tracking.
2. **Deploy Service Recovery for high-support/low-NPS accounts (owner: Support + CS Operations, within 30 days).**  
   Route priority queue, define 7-day recovery SLA, and measure post-intervention usage/NPS recovery.
3. **Run targeted adoption campaigns for low-adoption and usage-decline accounts (owner: Product + CS, within 30 days).**  
   Focus on Basic and Startup cohorts with milestone-based onboarding reactivation.
4. **Tighten acquisition quality controls for Affiliate and Paid Search (owner: Marketing + RevOps, within 45 days).**  
   Add retention-weighted CAC gates and channel-level quality scorecards.
5. **Create a high-value account protection list (owner: Revenue Leadership, immediate).**  
   Maintain top-priority account watchlist with executive save motions and weekly forecast impact review.

## 10. Limitations and Caveats
- **Insight:** This project is based on synthetic data and monthly-value proxies rather than full contractual ARR waterfalls.
- **Insight:** Final QA status is `Ready to share` with zero failed validation checks.
- **Inference:** The analytical direction is decision-useful and presentation-ready for stakeholder review in this simulation environment.
