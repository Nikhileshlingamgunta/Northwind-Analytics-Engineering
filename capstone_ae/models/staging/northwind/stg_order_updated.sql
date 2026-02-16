select
  cast(OrderID as integer) as order_id,
  cast(CustomerID as varchar) as customer_id,
  cast(EmployeeID as integer) as employee_id,

  -- Northwind dates can look like '04-Jul-1996'
  coalesce(
    try_cast(OrderDate as date),
    try_cast(strptime(OrderDate, '%d-%b-%Y') as date)
  ) as order_date,

  coalesce(
    try_cast(RequiredDate as date),
    try_cast(strptime(RequiredDate, '%d-%b-%Y') as date)
  ) as required_date,

  coalesce(
    try_cast(ShippedDate as date),
    try_cast(strptime(ShippedDate, '%d-%b-%Y') as date)
  ) as shipped_date,

  cast(ShipVia as integer) as ship_via,
  ShipName as ship_name,
  ShipCity as ship_city,
  ShipCountry as ship_country,
  cast(Freight as double) as freight
from {{ ref('order_updated') }}
where OrderID is not null
