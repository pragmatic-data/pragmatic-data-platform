{% macro copy_into__sql(
        full_table_name, 
        field_definitions, 
        full_stage_name, 
        stage_path = '/', 
        file_pattern = none, 
        full_format_name = none,
        ingestion_ts_utc = run_started_at,
        add_file_content_key = false
    ) %}

COPY INTO {{ full_table_name }}
FROM (
    SELECT
        {{ field_definitions }},

        METADATA$FILENAME                                                   as FROM_FILE,
        METADATA$FILE_ROW_NUMBER                                            as FILE_ROW_NUMBER,
        METADATA$FILE_LAST_MODIFIED                                         as FILE_LAST_MODIFIED_TS_UTC,
        {%- if ingestion_ts_utc %}
        '{{ ingestion_ts_utc }}'::TIMESTAMP_NTZ                             as INGESTION_TS_UTC
        {%- else %}
        CONVERT_TIMEZONE('UTC', METADATA$START_SCAN_TIME)::TIMESTAMP_NTZ    as INGESTION_TS_UTC
        {%- endif %}
        {%- if add_file_content_key %}
        , METADATA$FILE_CONTENT_KEY                                           as FILE_CONTENT_KEY
        {%- endif %}

    FROM @{{ full_stage_name }}{{ stage_path }}
)
{%- if file_pattern %}
PATTERN = '{{ file_pattern }}'
{%- endif %}
{%- if file_pattern and full_format_name %}, {% endif %}
{%- if full_format_name %}
FILE_FORMAT = (FORMAT_NAME = '{{ full_format_name }}')
{%- endif %}
;

{%- endmacro %}
