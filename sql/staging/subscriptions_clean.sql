-- Staging model: subscriptions_clean
-- Assumes raw tables are available as `raw_subscriptions`.

select
  subscription_id,
  customer_id,
  cast(subscription_start_date as date) as subscription_start_date,
  nullif(subscription_end_date, '')::date as subscription_end_date,
  monthly_revenue,
  contract_type,
  billing_cycle,
  status
from raw_subscriptions
where subscription_start_date is not null;
