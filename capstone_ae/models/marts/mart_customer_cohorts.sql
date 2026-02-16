with orders as (
  select distinct
    customer_id,
    order_id,
    order_date
  from {{ ref('fct_order_items') }}
  where order_date is not null
),

first_purchase as (
  select
    customer_id,
    min(order_date) as first_order_date
  from orders
  group by 1
),

orders_with_cohort as (
  select
    o.customer_id,
    o.order_id,
    o.order_date,
    f.first_order_date,
    date_trunc('month', f.first_order_date) as cohort_month,
    date_trunc('month', o.order_date) as order_month,
    date_diff('month', date_trunc('month', f.first_order_date), date_trunc('month', o.order_date)) as months_since_first
  from orders o
  join first_purchase f
    on o.customer_id = f.customer_id
),

cohort_counts as (
  select
    cohort_month,
    months_since_first,
    count(distinct customer_id) as active_customers
  from orders_with_cohort
  group by 1, 2
),

cohort_sizes as (
  select
    cohort_month,
    max(case when months_since_first = 0 then active_customers else null end) as cohort_size
  from cohort_counts
  group by 1
)

select
  c.cohort_month,
  c.months_since_first,
  c.active_customers,
  s.cohort_size,
  case
    when s.cohort_size = 0 or s.cohort_size is null then 0
    else round((c.active_customers * 1.0) / s.cohort_size, 4)
  end as retention_rate
from cohort_counts c
join cohort_sizes s
  on c.cohort_month = s.cohort_month
order by 1, 2
