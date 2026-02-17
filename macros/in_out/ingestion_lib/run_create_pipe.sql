{% macro run_create_pipe(
    landing_table_dict,
    pipe_dict,
    recreate_table = false
) %}
{% if execute %}
    
{% set full_table_name = landing_table_dict.db_name 
                 ~ '.' ~ landing_table_dict.schema_name 
                 ~ '.' ~ landing_table_dict.table_name %}

{% set full_pipe_name = full_table_name ~ var('pdp.pipe_suffix', '__PIPE') %}
{% if pipe_dict.pipe_name %}
    {% set full_pipe_name = landing_table_dict.db_name 
                    ~ '.' ~ landing_table_dict.schema_name 
                    ~ '.' ~ pipe_dict.pipe_name %}
{% endif %}

{% set field_definitions = pragmatic_data.field_definitions(pipe_dict, landing_table_dict.columns|length) %}

{% set add_file_content_key = pipe_dict.add_file_content_key | default(True) %} {# True if not set. Otherwise, stays truthy/flasey as set. #}

{{ log('Starting PIPE creation for Landing Table ' ~ full_table_name , info=True) }}
{{ log(' Creating Landing Table ' ~ full_table_name , info=True) }}
{% set results = run_query(
    pragmatic_data.create_landing_table_sql(
        landing_table_dict = landing_table_dict,
        recreate_table = recreate_table,
        add_file_content_key = add_file_content_key
) ) %}
{{ log(' *** Status: ' ~ results.columns[0].values()[0] , info=True) }}    -- Status column


{{ log(' Creating PIPE ' ~ full_pipe_name , info=True) }}
{% set results = run_query(
    pragmatic_data.create_pipe__sql(
        full_pipe_name = full_pipe_name,
        notification_integration = pipe_dict.integration,
        full_table_name = full_table_name,
        field_definitions = field_definitions, 
        full_stage_name = pipe_dict.stage_name,
        stage_path = pipe_dict.stage_path or '/', 
        file_pattern = pipe_dict.pattern, 
        full_format_name = pipe_dict.format_name,
        ingestion_ts_utc = none,
        recreate_pipe = recreate_table,
        add_file_content_key = add_file_content_key,
) ) %}

{% set ingestion_result_str = 'Status: ' ~ results.columns[0].values()[0] %}

{{ log(' *** ' ~ ingestion_result_str , info=True) }}
{{ log('DONE PIPE creation for Landing Table ' ~ full_table_name , info=True) }}

{% endif %} {# if execute #}
{% endmacro %}
