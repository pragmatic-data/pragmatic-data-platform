{% macro create_pipe__sql(
    full_pipe_name,
    notification_integration,
    full_table_name, 
    field_definitions, 
    full_stage_name, 
    stage_path = '/', 
    file_pattern = none, 
    full_format_name = none,
    ingestion_ts_utc = none,
    add_file_content_key = true,
    recreate_pipe = false
) %}

CREATE {% if recreate_pipe %}OR REPLACE {% endif -%}
PIPE {% if not recreate_pipe %}IF NOT EXISTS {% endif %}
{{full_pipe_name}}

{%- if notification_integration %}
    AUTO_INGEST = TRUE
    INTEGRATION = '{{ notification_integration }}'
{%- endif %}
    AS
{{- pragmatic_data.copy_into__sql(
        full_table_name = full_table_name,
        field_definitions = field_definitions, 
        full_stage_name = full_stage_name,
        stage_path = stage_path, 
        file_pattern = file_pattern, 
        full_format_name = full_format_name,
        ingestion_ts_utc = ingestion_ts_utc,
        add_file_content_key = add_file_content_key
    ) }}
{% endmacro %}