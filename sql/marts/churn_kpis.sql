-- KPI model: monthly churn and revenue churn rates.

with monthly as (
  select
    date_trunc('month', subscription_start_date) as month,
    count(*) filter (where status = 'active') as active_customers_start,
    count(*) filter (where status = 'churned') as churned_customers,
    sum(case when status = 'active' then monthly_revenue else 0 end) as active_mrr_start,
    sum(case when status = 'churned' then monthly_revenue else 0 end) as churned_mrr
  from subscriptions_clean
  group by 1
)
select
  month,
  active_customers_start,
  churned_customers,
  case when active_customers_start = 0 then 0 else churned_customers::double / active_customers_start end as customer_churn_rate,
  active_mrr_start,
  churned_mrr,
  case when active_mrr_start = 0 then 0 else churned_mrr / active_mrr_start end as revenue_churn_rate
from monthly
order by month;
