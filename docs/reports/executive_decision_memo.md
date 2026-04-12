# Executive Decision Memo: Churn & Retention Intelligence System

## Purpose
Provide a concise, decision‑oriented view of where revenue loss is occurring, which accounts are most at risk, and which interventions should be prioritized first.

## Executive Summary
- **Current base:** 3,500 customers (2,547 active; 953 churned).
- **Churn profile:** customer churn **27.2%** vs revenue churn **13.8%**.
- **Revenue at risk:** explicit at‑risk MRR **$142,193**; estimated future revenue at risk **$214,300**.
- **Concentration:** churn volume is high in smaller tiers, but value loss is concentrated in higher‑value accounts.

## What Leadership Should Do Next
1. **Protect high‑value accounts first.** High‑value at‑risk customers represent a disproportionate share of exposed revenue.
2. **Separate volume control from value protection.** Use scaled plays for high‑volume churn segments and dedicated save motions for high‑value tiers.
3. **Target behavioral triggers immediately.** Failed payments, low NPS, and low adoption are the strongest warning signals in this run.

## Retention Health Snapshot
- **Active base:** 2,547 customers.
- **Churn dynamics:** revenue churn materially lower than customer churn, indicating value leakage is concentrated rather than uniform.
- **Trend:** recent period churn shows deterioration relative to mature cohort benchmarks.

## Cohort Retention
- **Average 6‑month retention:** 97.8%.
- **Average 6‑month revenue retention:** 98.8%.
- **Signal:** mature cohorts are weakening, suggesting acquisition and onboarding quality drift.

## Churn Drivers (Highest Signal)
Top commercial and behavioral drivers in this simulation:
- **Commercial:** Startup segment, Affiliate channel, Basic plan.
- **Behavioral:** Low NPS, low feature adoption, failed payments, high support burden.

These signals are correlational and should guide targeted experiments, not be treated as causal proof.

## Revenue at Risk
Estimated future revenue at risk is **$214,300**, with the largest loss concentration in **SMB** and **Mid‑Market**.  

## Priority Interventions
1. **Renewal Save Desk:** Focus on renewal‑near accounts with risk signals; weekly save rate tracking.
2. **Service Recovery:** High‑support and low‑NPS accounts; 7‑day recovery SLA and follow‑up usage monitoring.
3. **Payment Rescue:** Failed‑payment accounts; proactive billing intervention and retry sequencing.
4. **Adoption Rescue:** Low‑adoption Basic/Startup accounts; milestone‑based onboarding reactivation.
5. **Channel Quality Controls:** Tighten Paid Search and Affiliate acquisition gates using retention‑weighted CAC.

## Caveats (Decision‑Support Only)
- Synthetic data; revenue uses monthly‑value proxies rather than full contract ARR.
- Behavioral drivers indicate correlation, not causation.
- Early months have lower denominators; prioritize mature periods for trend interpretation.
