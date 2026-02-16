select
  cast(orderId as integer) as order_id,
  cast(productId as integer) as product_id,
  cast(unitPrice as double) as unit_price,
  cast(quantity as integer) as quantity,
  cast(discount as double) as discount
from {{ ref('order_detail') }}