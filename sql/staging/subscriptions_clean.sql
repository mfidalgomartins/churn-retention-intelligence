-- PostgreSQL staging model: subscriptions_clean.

select
  subscription_id::text as subscription_id,
  customer_id::text as customer_id,
  cast(subscription_start_date as date) as subscription_start_date,
  nullif(subscription_end_date::text, '')::date as subscription_end_date,
  monthly_revenue::numeric(18, 2) as monthly_revenue,
  contract_type::text as contract_type,
  billing_cycle::text as billing_cycle,
  status::text as status
from raw_subscriptions
where subscription_start_date is not null;
