{% macro ingest_files_into_landing_sql(
    full_table_name,
    field_definitions, 
    full_stage_name,
    stage_path, 
    file_pattern, 
    full_format_name,
    ingestion_ts_utc,
    add_file_content_key
) %}

{{ pragmatic_data.refresh_stage_sql(full_stage_name) }}

BEGIN TRANSACTION;

{{ pragmatic_data.copy_into__sql(
    full_table_name, 
    field_definitions, 
    full_stage_name, 
    stage_path, 
    file_pattern, 
    full_format_name,
    ingestion_ts_utc,
    add_file_content_key
)}}

COMMIT;

{%- endmacro %}
