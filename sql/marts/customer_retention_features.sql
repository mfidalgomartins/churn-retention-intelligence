-- Mart model: customer_retention_features
-- Simplified illustration of core feature table.

with customers as (
  select
    customer_id,
    segment,
    region,
    acquisition_channel,
    plan_type,
    cast(signup_date as date) as signup_date
  from raw_customers
),
subscriptions as (
  select
    customer_id,
    min(subscription_start_date) as first_start_date,
    max(subscription_end_date) as last_end_date,
    max(case when status = 'active' then monthly_revenue else 0 end) as current_mrr,
    avg(monthly_revenue) as avg_monthly_revenue
  from subscriptions_clean
  group by customer_id
),
payments as (
  select
    customer_id,
    sum(case when payment_status = 'paid' then amount else 0 end) as lifetime_revenue,
    sum(case when payment_status = 'failed' and payment_date >= current_date - interval '90 day' then 1 else 0 end) as failed_payments_90d
  from raw_payments
  group by customer_id
)
select
  c.customer_id,
  c.segment,
  c.region,
  c.acquisition_channel,
  c.plan_type,
  datediff('day', s.first_start_date, coalesce(s.last_end_date, current_date)) as tenure_days,
  s.current_mrr,
  s.avg_monthly_revenue,
  p.lifetime_revenue,
  p.failed_payments_90d,
  case when p.failed_payments_90d > 0 then 1 else 0 end as payment_failure_flag
from customers c
left join subscriptions s using (customer_id)
left join payments p using (customer_id);
