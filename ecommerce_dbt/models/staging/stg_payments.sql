select
  order_id,
  payment_sequential,
  payment_type,
  payment_installments,
  payment_value,
  ingested_at
from {{ source('ecommerce', 'payments') }}

