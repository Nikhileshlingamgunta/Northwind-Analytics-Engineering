with customers as (
  select
    customer_id,
    companyName,
    contactName,
    city,
    country,
    region,
    postalCode
  from {{ ref('dim_customer') }}
),

orders_rollup as (
  select
    customer_id,
    min(order_date) as first_order_date,
    max(order_date) as last_order_date,
    count(distinct order_id) as orders_count,
    sum(net_line_amount) as lifetime_value
  from {{ ref('fct_order_items') }}
  group by 1
),

final as (
  select
    c.customer_id,
    c.companyName,
    c.contactName,
    c.city,
    c.country,
    c.region,
    c.postalCode,

    o.first_order_date,
    o.last_order_date,

    -- if customer has never ordered, these become null; we handle flags safely
    case
      when o.last_order_date is null then null
      else date_diff('day', o.last_order_date, current_date)
    end as days_since_last_order,

    case
      when o.last_order_date is null then 1
      when date_diff('day', o.last_order_date, current_date) > 30 then 1 else 0
    end as inactive_30,

    case
      when o.last_order_date is null then 1
      when date_diff('day', o.last_order_date, current_date) > 90 then 1 else 0
    end as inactive_90,

    case
      when o.last_order_date is null then 1
      when date_diff('day', o.last_order_date, current_date) > 180 then 1 else 0
    end as inactive_180,

    case
      when o.last_order_date is null then 1
      when date_diff('day', o.last_order_date, current_date) > 365 then 1 else 0
    end as inactive_365,

    coalesce(o.orders_count, 0) as orders_count,
    coalesce(o.lifetime_value, 0) as lifetime_value,

    case
      when coalesce(o.orders_count, 0) = 0 then 0
      else coalesce(o.lifetime_value, 0) / o.orders_count
    end as avg_order_value

  from customers c
  left join orders_rollup o
    on c.customer_id = o.customer_id
)

select * from final
