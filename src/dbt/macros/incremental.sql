{# Filtro de cargas incrementales para la fila candidata (alias candidate). Carga las llaves nuevas, las filas cuya fecha de negocio cae en la ventana de reproceso (desde incremental_lookback_days antes de la fecha máxima ya cargada) y las filas cuyas llaves de dimensión resueltas cambiaron (tracked_columns), por ejemplo cuando una versión SCD2 nueva reemplaza a la anterior. #}
{% macro incremental_candidate_filter(key_column, business_date_column, tracked_columns=[]) %}
{% if is_incremental() %}
where candidate.{{ key_column }} not in (
    select existing.{{ key_column }} from {{ this }} as existing
)
or candidate.{{ business_date_column }} >= (
    select {{ dbt.dateadd('day', -1 * var('incremental_lookback_days'), 'max(dates.date)') }}
    from {{ this }} as existing
    inner join {{ ref('dim_date') }} as dates on dates.date_sk = existing.date_sk
)
{% if tracked_columns %}
{# Redshift rechaza aquí un EXISTS correlacionado ("correlated subquery pattern is not supported"), así que los cambios se detectan comparando una huella de la llave y las columnas seguidas. La huella trata los nulos como un valor. #}
{% set compared_columns = [key_column] + tracked_columns %}
or {{ row_fingerprint('candidate', compared_columns) }} not in (
    select {{ row_fingerprint('existing', compared_columns) }} from {{ this }} as existing
)
{% endif %}
{% endif %}
{% endmacro %}

{# Huella de las columnas indicadas de una fila, calificadas con el alias de la tabla. #}
{% macro row_fingerprint(table_alias, columns) %}
{%- set qualified_columns = [] -%}
{%- for column in columns -%}
{%- do qualified_columns.append(table_alias ~ '.' ~ column) -%}
{%- endfor -%}
{{ dbt_utils.generate_surrogate_key(qualified_columns) }}
{%- endmacro %}

{# Elimina de la tabla de hechos las filas cuya llave de origen ya no existe en la capa validada. #}
{% macro delete_orphaned_facts(sk_column, source_relation, source_key) %}
delete from {{ this }}
where {{ sk_column }} not in (
    select {{ dbt_utils.generate_surrogate_key(['cast(' ~ source_key ~ ' as varchar)']) }}
    from {{ source_relation }}
)
{% endmacro %}
