select
  cast(customerID as varchar) as customer_id,
  companyName,
  contactName,
  city,
  country,
  region,
  postalCode
from {{ ref('customer') }}
