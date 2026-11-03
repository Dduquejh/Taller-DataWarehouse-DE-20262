-- reviewed_at queda en UTC (como lo guarda DocumentDB); los hechos lo convierten a hora de Bogotá.
with source as (
    {{ latest_batch_rows('mongodb_raw', 'mongo_reviews', 'review_id') }}
),

typed as (
    select
        *,
        lower(json_extract_path_text(document_json, 'verification', 'purchase_confirmed', true)) as purchase_confirmed_text
    from source
)

select
    cast(review_id as bigint) as review_id,
    cast(customer_id as bigint) as customer_id,
    cast(product_id as bigint) as product_id,
    cast(rating as integer) as rating,
    cast(reviewed_at as timestamp) as reviewed_at,
    case purchase_confirmed_text when 'true' then true when 'false' then false end as is_verified_purchase,
    document_json,
    _batch_id,
    _extracted_at,
    _source
from typed
