{#
  Filas vigentes de una tabla raw de solo anexado:
  - descarta lo anterior a la última carga completa (full) de la entidad, que
    redefine el estado de la fuente (por ejemplo, tras regenerar los datos);
  - de lo que queda, toma la versión del lote más reciente de cada llave.
  Solo considera lotes con COPY confirmado (loaded o processed).
#}
{% macro latest_batch_rows(source_name, table_name, key_column) %}
select *
from (
    select
        raw_rows.*,
        row_number() over (
            partition by raw_rows.{{ key_column }}
            order by raw_rows._batch_id desc
        ) as _version_rank
    from {{ source(source_name, table_name) }} as raw_rows
    inner join {{ source('control', 'batch_entities') }} as batches
        on batches.batch_id = raw_rows._batch_id
        and batches.raw_table = '{{ table_name }}'
        and batches.status in ('loaded', 'processed')
    where raw_rows._batch_id >= (
        select coalesce(max(full_batches.batch_id), '')
        from {{ source('control', 'batch_entities') }} as full_batches
        where full_batches.raw_table = '{{ table_name }}'
        and full_batches.mode = 'full'
        and full_batches.status in ('loaded', 'processed')
    )
) as ranked
where _version_rank = 1
{% endmacro %}
