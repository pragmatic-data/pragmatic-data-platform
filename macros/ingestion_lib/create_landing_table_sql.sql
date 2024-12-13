{% macro create_landing_table_sql(
    landing_table_dict,
    recreate_table = false    
) %}

{% set full_table_name = landing_table_dict.db_name ~ '.' ~ landing_table_dict.schema_name ~ '.' ~ landing_table_dict.table_name %}

    CREATE {% if recreate_table %} OR REPLACE {% endif -%}
    TRANSIENT TABLE {{ full_table_name }}
    {%- if not recreate_table %} IF NOT EXISTS {% endif %}    
    (
    {%- for definition in landing_table_dict.columns %}
        {%- if definition is mapping %}
            {%- for col, col_def in definition.items() %}
        {{col}} {{col_def or 'TEXT'}}, 
            {%- endfor %}                    
        {%- else %}
        {{definition}} TEXT, 
        {%- endif %}
    {%- endfor %}
        -- metadata
        FROM_FILE                   string,
        FILE_ROW_NUMBER             integer,
        FILE_LAST_MODIFIED_TS_UTC   TIMESTAMP_NTZ(9),
        INGESTION_TS_UTC            TIMESTAMP_NTZ(9)
    )
    COMMENT = '{{ landing_table_dict.comment or ('Landing table ' ~ full_table_name ) }}';
    
{% endmacro %}