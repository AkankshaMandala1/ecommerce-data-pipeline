select
  customer_id,
  customer_unique_id,
  customer_city,
  customer_state,
  ingested_at
from {{ source('ecommerce', 'customers') }}

