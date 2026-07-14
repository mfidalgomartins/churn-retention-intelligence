-- PostgreSQL 15+
-- Contract-MRR bridge. :observation_date is supplied by the orchestrator.
WITH params AS (
    SELECT CAST(:observation_date AS date) AS observation_date
),
monthly_movements AS (
    SELECT
        date_trunc('month', rm.effective_date)::date AS month_start,
        SUM(rm.mrr_delta) FILTER (WHERE rm.movement_type = 'new') AS new_mrr,
        SUM(rm.mrr_delta) FILTER (WHERE rm.movement_type = 'expansion') AS expansion_mrr,
        SUM(rm.mrr_delta) FILTER (WHERE rm.movement_type = 'contraction') AS contraction_mrr,
        SUM(rm.mrr_delta) FILTER (WHERE rm.movement_type = 'reactivation') AS reactivation_mrr,
        SUM(rm.mrr_delta) FILTER (WHERE rm.movement_type = 'churn') AS churn_mrr,
        SUM(rm.mrr_delta) AS net_mrr_movement
    FROM analytics.revenue_movements AS rm
    CROSS JOIN params AS p
    WHERE rm.effective_date <= p.observation_date
    GROUP BY 1
),
monthly_costs AS (
    SELECT
        date_trunc('month', sc.cost_month)::date AS month_start,
        SUM(sc.direct_service_cost) AS direct_service_cost
    FROM analytics.service_costs AS sc
    CROSS JOIN params AS p
    WHERE sc.cost_month <= p.observation_date
    GROUP BY 1
),
balances AS (
    SELECT
        mm.*,
        SUM(mm.net_mrr_movement) OVER (
            ORDER BY mm.month_start
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS closing_mrr
    FROM monthly_movements AS mm
),
bridge AS (
    SELECT
        b.month_start,
        b.closing_mrr - b.net_mrr_movement AS opening_mrr,
        COALESCE(b.new_mrr, 0) AS new_mrr,
        COALESCE(b.expansion_mrr, 0) AS expansion_mrr,
        COALESCE(b.contraction_mrr, 0) AS contraction_mrr,
        COALESCE(b.reactivation_mrr, 0) AS reactivation_mrr,
        COALESCE(b.churn_mrr, 0) AS churn_mrr,
        b.closing_mrr,
        COALESCE(mc.direct_service_cost, 0) AS direct_service_cost
    FROM balances AS b
    LEFT JOIN monthly_costs AS mc USING (month_start)
)
SELECT
    br.*,
    (br.opening_mrr + br.closing_mrr) / 2.0 AS mrr_revenue_proxy,
    (br.opening_mrr + br.closing_mrr) / 2.0 - br.direct_service_cost AS gross_profit_proxy,
    (
        br.opening_mrr + br.expansion_mrr + br.contraction_mrr
        + br.reactivation_mrr + br.churn_mrr
    ) / NULLIF(br.opening_mrr, 0) AS nrr,
    GREATEST(br.opening_mrr + br.contraction_mrr + br.churn_mrr, 0)
        / NULLIF(br.opening_mrr, 0) AS grr,
    br.closing_mrr - (
        br.opening_mrr + br.new_mrr + br.expansion_mrr + br.contraction_mrr
        + br.reactivation_mrr + br.churn_mrr
    ) AS reconciliation_diff,
    br.month_start < date_trunc('month', p.observation_date)::date AS is_complete_month
FROM bridge AS br
CROSS JOIN params AS p
ORDER BY br.month_start;
