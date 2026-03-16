{%- macro clean_landing_table_sql(landing_table_dict) -%}

{%- set cleanup = landing_table_dict.get('cleanup') -%}
{%- if not cleanup -%}
    {%- do return('') -%}
{%- endif -%}

{%- set fqn          = pragmatic_data.landing_table_fqn(landing_table_dict) -%}
{%- set keep_batches = cleanup.get('keep_n_batches') -%}
{%- set keep_days    = cleanup.get('keep_days') -%}
{%- set from_date    = cleanup.get('from_date') -%}
{%- set ts_column    = cleanup.get('ts_column', 'INGESTION_TS_UTC') -%}

{%- if keep_batches is none and keep_days is none -%}
    {{- exceptions.raise_compiler_error(
        "clean_landing_table_sql: specify 'keep_n_batches' or 'keep_days' in the cleanup dict."
    ) -}}
{%- elif keep_batches is not none and keep_days is not none -%}
    {{- exceptions.raise_compiler_error(
        "clean_landing_table_sql: 'keep_n_batches' and 'keep_days' are mutually exclusive — specify only one."
    ) -}}
{%- elif keep_batches is not none -%}

DELETE FROM {{ fqn }}
WHERE {{ ts_column }} < (
    SELECT MIN({{ ts_column }})
    FROM (
        SELECT DISTINCT {{ ts_column }}
        FROM {{ fqn }}
        ORDER BY {{ ts_column }} DESC
        LIMIT {{ keep_batches }}
    )
)

{%- elif keep_days is not none -%}

{%- set ref_date = "'" ~ from_date ~ "'::date" if from_date else 'CURRENT_DATE()' -%}

DELETE FROM {{ fqn }}
WHERE {{ ts_column }} < DATEADD(DAY, -{{ keep_days }}, {{ ref_date }})

{%- endif -%}

{%- endmacro -%}
