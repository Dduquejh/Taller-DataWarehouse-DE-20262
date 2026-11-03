{{ config(materialized='view', bind=false) }}

-- Vista para QuickSight: una fila por línea de venta con los atributos de
-- las dimensiones vigentes en la fecha del pedido (SCD2 point-in-time).
-- refreshed_at y last_batch_id permiten validar el indicador contra la misma
-- actualización que muestra el dashboard.
with last_batch as (
    select max(batch_id) as last_batch_id, max(processed_at) as refreshed_at
    from {{ source('control', 'batch_entities') }}
    where status = 'processed'
)

select
    sales.sale_sk,
    dates.date as sale_date,
    dates.year,
    dates.month,
    dates.day_of_week,
    dates.is_holiday,
    products.product_id,
    products.name as product_name,
    products.category,
    customers.customer_id,
    geography.department,
    geography.city,
    channels.sales_channel,
    sales.quantity,
    sales.unit_price,
    sales.subtotal as sales_amount,
    last_batch.last_batch_id,
    last_batch.refreshed_at
from {{ ref('fct_sales') }} as sales
inner join {{ ref('dim_date') }} as dates on dates.date_sk = sales.date_sk
left join {{ ref('dim_product') }} as products on products.product_sk = sales.product_sk
left join {{ ref('dim_customer') }} as customers on customers.customer_sk = sales.customer_sk
left join {{ ref('dim_geography') }} as geography on geography.geography_sk = sales.geography_sk
left join {{ ref('dim_sales_channel') }} as channels on channels.sales_channel_sk = sales.sales_channel_sk
cross join last_batch
