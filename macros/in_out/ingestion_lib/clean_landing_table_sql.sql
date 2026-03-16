{%- macro clean_landing_table_sql(landing_table_dict) -%}

{%- set cleanup      = landing_table_dict.get('cleanup', {}) -%}
{%- set fqn          = pragmatic_data.landing_table_fqn(landing_table_dict) -%}
{%- set keep_batches = cleanup.get('keep_n_batches') -%}
{%- set keep_days    = cleanup.get('keep_days') -%}
{%- set from_date    = cleanup.get('from_date') -%}
{%- set ts_column    = cleanup.get('ts_column', 'INGESTION_TS_UTC') -%}

{%- if keep_batches is none and keep_days is none -%}
    {{- exceptions.raise_compiler_error(
        "clean_landing_table_sql: specifica 'keep_n_batches' oppure 'keep_days' nel dict cleanup."
    ) -}}
{%- elif keep_batches is not none and keep_days is not none -%}
    {{- exceptions.raise_compiler_error(
        "clean_landing_table_sql: 'keep_n_batches' e 'keep_days' sono mutuamente esclusivi — specificane solo uno."
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
