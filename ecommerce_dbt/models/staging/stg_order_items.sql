select
  order_id,
  order_item_id,
  product_id,
  seller_id,
  price,
  freight_value,
  ingested_at
from {{ source('ecommerce', 'order_items') }}

