with dq as (

  select
    'stg_order_updated' as table_name,
    count(*) as row_count,
    sum(case when order_id is null then 1 else 0 end) as null_primary_keys
  from {{ ref('stg_order_updated') }}

  union all

  select
    'stg_customer' as table_name,
    count(*) as row_count,
    sum(case when customer_id is null then 1 else 0 end) as null_primary_keys
  from {{ ref('stg_customer') }}

  union all

  select
    'stg_product' as table_name,
    count(*) as row_count,
    sum(case when product_id is null then 1 else 0 end) as null_primary_keys
  from {{ ref('stg_product') }}

  union all

  select
    'fct_order_items' as table_name,
    count(*) as row_count,
    sum(case when order_id is null then 1 else 0 end) as null_primary_keys
  from {{ ref('fct_order_items') }}

)

select
  table_name,
  row_count,
  null_primary_keys,
  round((row_count - null_primary_keys) * 1.0 / nullif(row_count, 0), 4) as pk_completeness_rate
from dq
order by table_name
