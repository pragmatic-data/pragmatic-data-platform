{% macro save_history_with_multiple_versions(
    input_rel, 
    key_column,
    diff_column,
    history_rel = this,
    sort_expr               = var('pdp.sort_expr', var('pdp.effectivity_column', 'INGESTION_TS_UTC')), 
    load_ts_column          = var('pdp.load_ts_column', 'INGESTION_TS_UTC'),
    high_watermark_column   = var('pdp.high_watermark_column', 'INGESTION_TS_UTC'),
    high_watermark_test     = var('pdp.high_watermark_test', '>'),
    input_filter_expr = 'true',
    history_filter_expr = 'true'
) -%}

{{- config(materialized='incremental') }}
{%- if execute and not flags.FULL_REFRESH %}
    {% set incremental_w_external_input = (history_rel != this) %}
{% endif -%}

{% set hist_load_ts_column = var('pdp.hist_load_ts_column', 'HIST_LOAD_TS_UTC') %}
{% set high_watermark_column_expr = ', ' ~ high_watermark_column if high_watermark_column else '' %}

WITH 
{% if is_incremental() or incremental_w_external_input %}

current_from_history as (
    {{ pragmatic_data.current_from_history_with_multiple_versions(
        history_rel = history_rel, 
        key_column = key_column,
        sort_expr = sort_expr,
        selection_expr = key_column ~ ', ' ~ diff_column ~ high_watermark_column_expr,
        load_ts_column = load_ts_column,
        history_filter_expr = history_filter_expr
    ) }}
),

load_from_input as (
    SELECT 
        i.*
        , '{{ run_started_at }}'::timestamp as {{hist_load_ts_column}}
        , LAG(i.{{diff_column}}) OVER(PARTITION BY i.{{key_column}} ORDER BY i.{{sort_expr}}) as PREV_HDIFF
        , CASE 
            WHEN PREV_HDIFF is null THEN COALESCE(i.{{diff_column}} != h.{{diff_column}}, true)
            ELSE (i.{{diff_column}} != PREV_HDIFF) 
          END as TO_BE_STORED
    FROM {{input_rel}} as i
    LEFT OUTER JOIN current_from_history as h ON h.{{key_column}} = i.{{key_column}}
    WHERE h.{{key_column}} is null  -- new key
      and {{input_filter_expr}}
    {%- if high_watermark_column %}        
       or i.{{high_watermark_column}} {{high_watermark_test}} h.{{high_watermark_column}}     -- Key specific High Watermark
    {%- endif %}
)

{%- else %}
load_from_input as (
    SELECT 
        i.*
        , '{{ run_started_at }}'::timestamp as {{hist_load_ts_column}}
        , LAG(i.{{diff_column}}) OVER(PARTITION BY i.{{key_column}} ORDER BY i.{{sort_expr}}) as PREV_HDIFF
        , CASE 
            WHEN PREV_HDIFF is null THEN true
            ELSE (i.{{diff_column}} != PREV_HDIFF) 
          END as TO_BE_STORED
    FROM {{input_rel}} as i
    WHERE {{input_filter_expr}}
)    
{%- endif %}

SELECT * EXCLUDE(PREV_HDIFF, TO_BE_STORED)
FROM load_from_input
WHERE TO_BE_STORED
ORDER BY {{key_column}}, {{sort_expr}}

{%- endmacro %}