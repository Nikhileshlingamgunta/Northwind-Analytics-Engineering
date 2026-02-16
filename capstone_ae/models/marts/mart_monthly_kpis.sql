with f as (
  select
    order_id,
    customer_id,
    order_date,
    net_line_amount
  from {{ ref('fct_order_items') }}
  where order_date is not null
),

order_level as (
  -- one row per order, per customer, per month
  select
    order_id,
    customer_id,
    date_trunc('month', order_date) as order_month,
    sum(net_line_amount) as order_revenue
  from f
  group by 1, 2, 3
),

customer_orders_by_month as (
  select
    customer_id,
    order_month,
    count(distinct order_id) as orders_in_month
  from order_level
  group by 1, 2
),

repeat_customers_by_month as (
  select
    order_month,
    count(*) as repeat_customers
  from customer_orders_by_month
  where orders_in_month >= 2
  group by 1
),

monthly as (
  select
    order_month,
    count(distinct order_id) as total_orders,
    sum(order_revenue) as total_revenue,
    count(distinct customer_id) as active_customers
  from order_level
  group by 1
)

select
  m.order_month,
  m.total_orders,
  round(m.total_revenue, 2) as total_revenue,
  round(m.total_revenue / nullif(m.total_orders, 0), 2) as avg_order_value,
  m.active_customers,
  coalesce(r.repeat_customers, 0) as repeat_customers,
  round(coalesce(r.repeat_customers, 0) * 1.0 / nullif(m.active_customers, 0), 4) as repeat_rate
from monthly m
left join repeat_customers_by_month r
  on m.order_month = r.order_month
order by 1
