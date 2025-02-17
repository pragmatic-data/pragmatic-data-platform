{% macro run_semi_structured_ingestion(
    landing_table_dict,
    ingestion_dict,
    recreate_table = false
) %}
{% if execute %}
    
{% set full_table_name = landing_table_dict.db_name
                 ~ '.' ~ landing_table_dict.schema_name
                 ~ '.' ~ landing_table_dict.table_name %}

{% set field_definitions %}
    {%- for expression in ingestion_dict.field_expressions %}
        {%- if expression is mapping %}
            {%- for field, field_def in expression.items() %}
        {{field_def}} as {{field}}
            {%- endfor %}
        {%- if not loop.last %}, {% endif %}
        {%- endif %}
    {%- endfor %}
{% endset %}


{{ log('Starting ingestion into Landing Table ' ~ full_table_name , info=True) }}
{{ log(' Creating Landing Table ' ~ full_table_name , info=True) }}
{% set results = run_query(
    pragmatic_data.create_landing_table_sql(
        landing_table_dict = landing_table_dict,
        recreate_table = recreate_table
) ) %}
{{ log(' *** Status: ' ~ results.columns[0].values()[0] , info=True) }}    -- Status column

{{ log(' Ingesting data into Landing Table ' ~ full_table_name , info=True) }}
{% set results = run_query(
    pragmatic_data.ingest_semi_structured_into_landing_sql(
        full_table_name     = full_table_name,
        field_definitions   = field_definitions,
        file_pattern        = ingestion_dict.pattern,
        full_stage_name     = ingestion_dict.stage_name,
        full_format_name    = ingestion_dict.format_name
    )
) %}

{% set ingestion_result_str %}
{%- if results.column_names|length > 1 -%}
Loaded {{ results.columns[0].values() | length }} files
{%- else -%}
Status: {{ results.columns[0].values()[0]  }}
{%- endif %}
{% endset %}
{{ log(' *** ' ~ ingestion_result_str , info=True) }}
{{ log('DONE ingestion into Landing Table ' ~ full_table_name , info=True) }}

{% endif %}
{% endmacro %}
