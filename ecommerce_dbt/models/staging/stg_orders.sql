select
  order_id,
  customer_id,
  order_status,
  order_purchase_timestamp,
  order_approved_at,
  order_delivered_customer_date,
  order_estimated_delivery_date,
  ingested_at
from {{ source('ecommerce', 'orders') }}

