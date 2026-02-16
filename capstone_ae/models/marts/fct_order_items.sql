with orders as (
  select * from {{ ref('stg_order_updated') }}
),
items as (
  select * from {{ ref('stg_order_detail') }}
),
products as (
  select * from {{ ref('stg_product') }}
)

select
  o.order_id,
  o.customer_id,
  o.employee_id,
  o.order_date,
  o.shipped_date,
  o.ship_country,
  
  i.product_id,
  i.unit_price as line_unit_price,
  i.quantity,
  i.discount,

  -- metrics
  (i.unit_price * i.quantity) as gross_line_amount,
  (i.unit_price * i.quantity) * (1 - coalesce(i.discount, 0)) as net_line_amount,

  -- optional product attributes (handy for slicing)
  p.category_id,
  p.supplier_id
from orders o
join items i
  on o.order_id = i.order_id
left join products p
  on i.product_id = p.product_id
