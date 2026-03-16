{% macro run_file_ingestion(
    landing_table_dict,
    ingestion_dict,
    recreate_table = false
) %}
{% if execute %}
    
{% set full_table_name = pragmatic_data.landing_table_fqn(landing_table_dict) %}

{% set field_definitions = pragmatic_data.field_definitions(ingestion_dict, landing_table_dict.columns|length) %}

{% set add_file_content_key = ingestion_dict.add_file_content_key | default(True) %} {# True if not set. Otherwise, stays truthy/flasey as set. #}

{{ log('Starting ingestion into Landing Table ' ~ full_table_name , info=True) }}
{{ log(' Creating Landing Table ' ~ full_table_name , info=True) }}
{% set results = run_query(
    pragmatic_data.create_landing_table_sql(
        landing_table_dict = landing_table_dict,
        recreate_table = recreate_table,
        add_file_content_key = add_file_content_key
) ) %}
{{ log(' *** Status: ' ~ results.columns[0].values()[0] , info=True) }}    -- Status column


{{ log(' Ingesting data into Landing Table ' ~ full_table_name , info=True) }}
{% set results = run_query(
    pragmatic_data.ingest_files_into_landing_sql(
        full_table_name = full_table_name,
        field_definitions = field_definitions, 
        full_stage_name = ingestion_dict.stage_name,
        stage_path = ingestion_dict.stage_path or '/', 
        file_pattern = ingestion_dict.pattern, 
        full_format_name = ingestion_dict.format_name,
        ingestion_ts_utc = run_started_at,
        add_file_content_key = add_file_content_key,
) ) %}

{% set ingestion_result_str %}
{%- if results.column_names|length > 1 -%}
Loaded {{ results.columns[0].values() | length }} files
{%- else -%}
Status: {{ results.columns[0].values()[0]  }}
{%- endif %}
{% endset %}
{{ log(' *** ' ~ ingestion_result_str , info=True) }}
{{ log('DONE ingestion into Landing Table ' ~ full_table_name , info=True) }}

{% if landing_table_dict.get('cleanup') %}
    {{ log(' Cleaning Landing Table ' ~ full_table_name, info=True) }}
    {% set cleanup_result = run_query(
        pragmatic_data.clean_landing_table_sql(landing_table_dict)
    ) %}
    {{ log(' *** Deleted ' ~ cleanup_result.columns[0].values()[0] ~ ' rows from ' ~ full_table_name, info=True) }}
{% endif %}

{% endif %} {# if execute #}
{% endmacro %}
