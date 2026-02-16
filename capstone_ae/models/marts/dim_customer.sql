select
  customer_id,
  companyName,
  contactName,
  city,
  country,
  region,
  postalCode
from {{ ref('stg_customer') }}
