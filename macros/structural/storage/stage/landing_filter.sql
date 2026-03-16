{%- macro landing_filter(
    source_rel,
    n_batches   = none,
    since_days  = none,
    since_hours = none,
    ts_column   = 'INGESTION_TS_UTC'
) -%}

{%- if flags.FULL_REFRESH -%}

true

{%- else -%}

{%- set conditions = [] -%}

{%- if n_batches is not none -%}
    {%- set n_batches_filter -%}
        {{ ts_column }} >= (
            SELECT MIN({{ ts_column }})
            FROM (
                SELECT DISTINCT {{ ts_column }}
                FROM {{ source_rel }}
                ORDER BY {{ ts_column }} DESC
                LIMIT {{ n_batches }}
            )
        )
    {%- endset -%}
    {%- do conditions.append(n_batches_filter) -%}
{%- endif -%}

{%- if since_days is not none -%}
    {%- do conditions.append(ts_column ~ ' >= DATEADD(DAY, -' ~ since_days ~ ', CURRENT_TIMESTAMP())') -%}
{%- endif -%}

{%- if since_hours is not none -%}
    {%- do conditions.append(ts_column ~ ' >= DATEADD(HOUR, -' ~ since_hours ~ ', CURRENT_TIMESTAMP())') -%}
{%- endif -%}

{%- if conditions | length == 0 -%}
    true
{%- else -%}
    {{ conditions | join('\n    AND ') }}
{%- endif -%}

{%- endif -%}

{%- endmacro -%}
