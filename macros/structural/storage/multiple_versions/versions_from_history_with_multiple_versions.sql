{%- macro versions_from_history_with_multiple_versions(
    history_rel, 
    key_column,
    diff_column,
    version_sort_column = var('pdp.sort_expr', 'INGESTION_TS_UTC'),
    load_ts_column      = var('pdp.load_ts_column', 'INGESTION_TS_UTC'),
    hist_load_ts_column = var('pdp.hist_load_ts_column', 'HIST_LOAD_TS_UTC'),
    selection_expr      = '*',
    history_filter_expr = 'true'
) -%}

{#%- set version_sort_expr = [hist_load_ts_column, load_ts_column, version_sort_column]|join(', ') %#}
{%- set version_sort_expr = [version_sort_column, load_ts_column, hist_load_ts_column]|join(', ') %}
{%- set ingest_sort_expr = load_ts_column %}
{%- set hist_load_sort_expr = hist_load_ts_column %}
{% set end_of_time = var('pdp.end_of_time', '9999-09-09') %}
{% set end_of_time_type = var('pdp.end_of_time_type', 'DATE') %}

SELECT {{selection_expr}}
    , count(*) OVER( PARTITION BY {{key_column}}) as version_count
    , row_number() OVER( PARTITION BY {{key_column}} ORDER BY {{version_sort_expr}}) as version_number
    , dense_rank() OVER( PARTITION BY {{key_column}} ORDER BY {{ingest_sort_expr}}) as ingestion_batch
    , dense_rank() OVER( PARTITION BY {{key_column}} ORDER BY {{hist_load_sort_expr}}) as load_batch
    , {{ pragmatic_data.pdp_hash([diff_column, version_sort_column]) }} as DIM_SCD_HKEY
    , {{ version_sort_column }} as VALID_FROM
    , coalesce( LEAD({{ version_sort_column }}) OVER(PARTITION BY {{key_column}} ORDER BY {{version_sort_expr}})
               , '{{end_of_time}}'::{{end_of_time_type}}
        ) as VALID_TO
    , (version_number = version_count) as IS_CURRENT

FROM {{history_rel}}
WHERE {{history_filter_expr}}
    
{%- endmacro %}