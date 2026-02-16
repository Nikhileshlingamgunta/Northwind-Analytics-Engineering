select
  cast(productId as integer) as product_id,
  productName,
  cast(supplierId as integer) as supplier_id,
  cast(categoryId as integer) as category_id,
  quantityPerUnit,
  cast(unitPrice as double) as unit_price,
  cast(unitsInStock as integer) as units_in_stock,
  cast(discontinued as integer) as discontinued
from {{ ref('product') }}