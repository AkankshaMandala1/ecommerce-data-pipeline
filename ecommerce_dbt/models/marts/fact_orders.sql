select
  o.order_id,
  o.customer_id,
  o.order_status,
  o.order_purchase_timestamp,
  o.order_approved_at,
  o.order_delivered_customer_date,
  o.order_estimated_delivery_date,

  -- metrics
  coalesce(p.payment_value, 0) as payment_value,
  coalesce(oi.items_count, 0) as items_count,
  coalesce(oi.items_revenue, 0) as items_revenue,
  coalesce(oi.freight_total, 0) as freight_total

from {{ ref('stg_orders') }} o

left join (
  select
    order_id,
    sum(payment_value) as payment_value
  from {{ ref('stg_payments') }}
  group by 1
) p
  on o.order_id = p.order_id

left join (
  select
    order_id,
    count(*) as items_count,
    sum(price) as items_revenue,
    sum(freight_value) as freight_total
  from {{ ref('stg_order_items') }}
  group by 1
) oi
  on o.order_id = oi.order_id

