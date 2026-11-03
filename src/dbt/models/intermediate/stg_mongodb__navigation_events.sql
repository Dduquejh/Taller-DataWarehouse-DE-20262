-- occurred_at queda en UTC (como lo guarda DocumentDB); los hechos lo convierten a hora de Bogotá.
with source as (
    {{ latest_batch_rows('mongodb_raw', 'mongo_navigation_events', 'event_id') }}
)

select
    cast(event_id as bigint) as event_id,
    nullif(trim(event_type), '') as event_type,
    cast(occurred_at as timestamp) as occurred_at,
    cast(customer_id as bigint) as customer_id,
    cast(product_id as bigint) as product_id,
    nullif(trim(sales_channel), '') as sales_channel,
    document_json,
    _batch_id,
    _extracted_at,
    _source
from source
