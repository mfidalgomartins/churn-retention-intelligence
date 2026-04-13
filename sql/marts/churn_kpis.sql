-- KPI model: monthly churn and revenue churn rates
-- aligned with src/churn_analysis/run_main_analysis.py monthly_retention_trend().

with month_bounds as (
  select
    generate_series(
      date_trunc('month', (select min(subscription_start_date) from subscriptions_clean)),
      date_trunc(
        'month',
        greatest(
          (select max(subscription_start_date) from subscriptions_clean),
          coalesce((select max(subscription_end_date) from subscriptions_clean), (select max(subscription_start_date) from subscriptions_clean))
        )
      ),
      interval '1 month'
    )::date as month_start
),
active_base as (
  select
    m.month_start,
    count(*) as active_customers_start,
    sum(s.monthly_revenue) as active_mrr_start
  from month_bounds m
  join subscriptions_clean s
    on s.subscription_start_date <= m.month_start
   and (
     s.subscription_end_date is null
     or s.subscription_end_date >= m.month_start
   )
  group by 1
),
churn_in_month as (
  select
    m.month_start,
    count(*) as churned_customers,
    sum(s.monthly_revenue) as churned_mrr
  from month_bounds m
  join subscriptions_clean s
    on s.subscription_end_date is not null
   and s.subscription_end_date >= m.month_start
   and s.subscription_end_date <= (m.month_start + interval '1 month' - interval '1 day')
  group by 1
)
select
  m.month_start as month,
  coalesce(a.active_customers_start, 0) as active_customers_start,
  coalesce(a.active_mrr_start, 0.0) as active_mrr_start,
  coalesce(c.churned_customers, 0) as churned_customers,
  coalesce(c.churned_mrr, 0.0) as churned_mrr,
  case
    when coalesce(a.active_customers_start, 0) = 0 then 0.0
    else coalesce(c.churned_customers, 0)::double precision / a.active_customers_start
  end as customer_churn_rate,
  case
    when coalesce(a.active_mrr_start, 0.0) = 0 then 0.0
    else coalesce(c.churned_mrr, 0.0) / a.active_mrr_start
  end as revenue_churn_rate
from month_bounds m
left join active_base a on a.month_start = m.month_start
left join churn_in_month c on c.month_start = m.month_start
order by m.month_start;
