select
  product_id,
  productName,
  supplier_id,
  category_id,
  quantityPerUnit,
  unit_price,
  units_in_stock,
  discontinued
from {{ ref('stg_product') }}
