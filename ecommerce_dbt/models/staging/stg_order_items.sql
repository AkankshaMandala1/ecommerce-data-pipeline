select
  order_id,
  order_item_id,
  product_id,
  seller_id,
  cast(nullif(price, '') as numeric) as price,
  cast(nullif(freight_value, '') as numeric) as freight_value,
  ingested_at
from {{ source('ecommerce', 'order_items') }}

