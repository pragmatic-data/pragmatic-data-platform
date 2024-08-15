{% macro save_history(
    input_rel, 
    key_column,
    diff_column,

    load_ts_column          = var('pdp.load_ts_column', 'INGESTION_TS_UTC'),
    high_watermark_column   = var('pdp.high_watermark_column', 'INGESTION_TS_UTC'),
    high_watermark_test     = var('pdp.high_watermark_test', '>'),
    input_filter_expr       = 'true',
    history_filter_expr     = 'true',
    order_by_expr           = none,
    history_rel = this
) -%}

{{ config(materialized='incremental') }}
{% if execute and not flags.FULL_REFRESH %}
    {% set ext_input = (history_rel != this) %}    {#% print(existing)%#}
{% endif %}

{% set hist_load_ts_column = var('pdp.hist_load_ts_column', 'HIST_LOAD_TS_UTC') %}

WITH
{%- if is_incremental() or ext_input %}
current_from_history as (
    {{current_from_history(
        history_rel = history_rel, 
        key_column = key_column,
        selection_expr = diff_column,
        load_ts_column = load_ts_column,
        history_filter_expr = history_filter_expr
    ) }}
),

load_from_input as (
    SELECT i.*
        , '{{ run_started_at }}'::timestamp as {{hist_load_ts_column}}
    FROM {{input_rel}} as i
    LEFT OUTER JOIN current_from_history as h ON h.{{diff_column}} = i.{{diff_column}}
    WHERE h.{{diff_column}} is null
        and {{input_filter_expr}}
    {%- if high_watermark_column %}        
        and {{high_watermark_column}} {{high_watermark_test}} (select max({{high_watermark_column}}) from {{ history_rel }}) 
    {%- endif %}
)

{%- else %}
load_from_input as (
    SELECT *
        , '{{ run_started_at }}'::timestamp as {{hist_load_ts_column}}
    FROM {{input_rel}} 
    WHERE {{input_filter_expr}}
)    
{%- endif %}

SELECT * FROM load_from_input
{%- if order_by_expr %}
ORDER BY {{order_by_expr}}
{%- endif %}

{%- endmacro %}