-- PostgreSQL mart: core customer retention features.
-- Windows are anchored to churn date for closed accounts and snapshot date for open accounts.

with params as (
  select date '2026-03-01' as snapshot_date
),
subscription_base as (
  select
    c.customer_id,
    c.segment,
    c.region,
    c.acquisition_channel,
    c.plan_type,
    s.subscription_start_date,
    least(coalesce(s.subscription_end_date, p.snapshot_date), p.snapshot_date) as observation_date,
    s.monthly_revenue,
    s.contract_type,
    s.billing_cycle,
    (s.status = 'churned')::integer as churn_flag,
    (s.status = 'at_risk')::integer as at_risk_flag,
    case when s.status = 'churned' then 0.0 else s.monthly_revenue end as current_mrr
  from raw_customers c
  join subscriptions_clean s using (customer_id)
  cross join params p
),
base as (
  select
    b.*,
    renewal.next_renewal_date
  from subscription_base b
  left join lateral (
    select min(renewal_date)::date as next_renewal_date
    from generate_series(
      b.subscription_start_date::timestamp,
      b.observation_date::timestamp + interval '1 year',
      case
        when b.contract_type = 'Annual' then interval '1 year'
        else interval '1 month'
      end
    ) as series(renewal_date)
    where renewal_date > b.observation_date
  ) renewal on true
),
usage_observed as (
  select
    b.customer_id,
    b.observation_date - cast(u.usage_date as date) as days_before_observation,
    u.sessions,
    u.feature_adoption_score,
    u.support_tickets,
    u.nps_score
  from base b
  join raw_product_usage u
    on u.customer_id = b.customer_id
   and cast(u.usage_date as date) <= b.observation_date
),
usage_features as (
  select
    customer_id,
    coalesce(sum(sessions) filter (where days_before_observation between 0 and 29), 0) as recent_sessions_30d,
    coalesce(sum(sessions) filter (where days_before_observation between 0 and 89), 0) as recent_sessions_90d,
    coalesce(
      avg(sessions) filter (where days_before_observation between 0 and 29)
      - avg(sessions) filter (where days_before_observation between 30 and 59),
      0.0
    ) as usage_trend,
    coalesce(avg(feature_adoption_score) filter (where days_before_observation between 0 and 29), 0.0) as feature_adoption_score_recent,
    coalesce(sum(support_tickets) filter (where days_before_observation between 0 and 29), 0) as support_tickets_30d,
    coalesce(sum(support_tickets) filter (where days_before_observation between 0 and 89), 0) as support_tickets_90d,
    coalesce(avg(nps_score) filter (where days_before_observation between 0 and 89), 0.0) as nps_score_recent
  from usage_observed
  group by customer_id
),
payment_observed as (
  select
    b.customer_id,
    b.observation_date - cast(p.payment_date as date) as days_before_observation,
    p.payment_status,
    p.amount::numeric,
    case b.billing_cycle
      when 'Annual' then 12
      when 'Quarterly' then 3
      else 1
    end as cycle_months
  from base b
  join raw_payments p
    on p.customer_id = b.customer_id
   and cast(p.payment_date as date) <= b.observation_date
),
payment_features as (
  select
    customer_id,
    coalesce(sum(amount) filter (where payment_status = 'paid'), 0.0) as lifetime_revenue,
    avg(amount / cycle_months) filter (where payment_status = 'paid') as avg_monthly_revenue_calc,
    count(*) filter (
      where payment_status = 'failed' and days_before_observation between 0 and 89
    ) as failed_payments_90d
  from payment_observed
  group by customer_id
)
select
  b.customer_id,
  b.observation_date,
  b.segment,
  b.region,
  b.acquisition_channel,
  b.plan_type,
  b.observation_date - b.subscription_start_date as tenure_days,
  b.current_mrr,
  coalesce(p.avg_monthly_revenue_calc, b.monthly_revenue) as avg_monthly_revenue,
  coalesce(p.lifetime_revenue, 0.0) as lifetime_revenue,
  coalesce(u.recent_sessions_30d, 0) as recent_sessions_30d,
  coalesce(u.recent_sessions_90d, 0) as recent_sessions_90d,
  coalesce(u.usage_trend, 0.0) as usage_trend,
  coalesce(u.feature_adoption_score_recent, 0.0) as feature_adoption_score_recent,
  coalesce(u.support_tickets_30d, 0) as support_tickets_30d,
  coalesce(u.support_tickets_90d, 0) as support_tickets_90d,
  coalesce(u.nps_score_recent, 0.0) as nps_score_recent,
  coalesce(p.failed_payments_90d, 0) as failed_payments_90d,
  (coalesce(p.failed_payments_90d, 0) > 0)::integer as payment_failure_flag,
  (
    b.churn_flag = 0
    and b.next_renewal_date - b.observation_date between 0 and 45
  )::integer as renewal_near_flag,
  b.churn_flag,
  b.at_risk_flag
from base b
left join usage_features u using (customer_id)
left join payment_features p using (customer_id);
