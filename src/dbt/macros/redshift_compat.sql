{# Conversión de UTC a la hora de negocio (America/Bogota). En raw, las fechas de DocumentDB son TIMESTAMP en UTC. #}
{% macro to_business_time(column) -%}
convert_timezone('UTC', '{{ var("business_timezone") }}', {{ column }})
{%- endmacro %}

{# Redshift no admite IS DISTINCT FROM: comparación que trata dos nulos como iguales. #}
{% macro is_distinct_from(left, right) -%}
(({{ left }}) <> ({{ right }}) or (({{ left }}) is null) <> (({{ right }}) is null))
{%- endmacro %}

{# Día de la semana ISO (1 = lunes ... 7 = domingo); Redshift solo trae dow (0 = domingo). #}
{% macro iso_day_of_week(column) -%}
case when extract(dow from {{ column }}) = 0 then 7 else cast(extract(dow from {{ column }}) as integer) end
{%- endmacro %}
