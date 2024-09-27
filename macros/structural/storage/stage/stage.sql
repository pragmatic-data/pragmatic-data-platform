{%- macro stage( 
    source_model,
    source = none,
    calculated_columns = none,
    hashed_columns = none,
    default_records = none,
    remove_duplicates = none
) -%}


WITH
src_data as (
    SELECT 
    {%- if source is mapping and source.columns.include_all %} *
        {% if source.columns.exclude_columns %} EXCLUDE (
            {%- for col in source.columns.exclude_columns -%}
               {{col}}{% if not loop.last %}, {% endif -%}
            {%- endfor -%}
        )
        {%- endif %}
        {% if source.columns.replace_columns %} REPLACE (
            {{- pragmatic_data.column_expressions(source.columns.replace_columns)}}
        )
        {%- endif %}
        {% if source.columns.rename_columns %} RENAME (
            {{- pragmatic_data.column_expressions(source.columns.rename_columns)}}
        )
        {%- endif %}
    {%- endif %}

    {%- if source is mapping and source.columns.include_all and calculated_columns %},{% endif %}
    {% if calculated_columns %}
        {{- pragmatic_data.column_expressions(calculated_columns)}}
    {% endif %}        

    FROM {{ source_model }}
    WHERE {{ source.where or 'true' }}   
)

{%- if default_records %}
, default_record_inputs as (
    {%- for default_record in default_records -%}
        {%- for default_record_name, columns in default_record.items() %}
    SELECT '{{default_record_name}}' as default_record_name, 
    {{- pragmatic_data.column_expressions(columns)}}
        {%- endfor %}
    {% if not loop.last %}UNION ALL {% endif -%}
    {%- endfor -%}
)
, default_records as (
    SELECT r.*
        REPLACE(
    {%- for default_record_name, column_dicts in default_records[0].items() %}
        {%- for column_dict in column_dicts %}
            {%- for column_name, sql_expression in column_dict.items() %}
            d.{{column_name}} as {{column_name}}
            {%- endfor -%}{%- if not loop.last %}, {% endif %}
        {%- endfor -%}
    {%- endfor -%}
      )
    FROM default_record_inputs as d
    LEFT OUTER JOIN (SELECT * FROM src_data WHERE false) as r ON(false)
) 
, with_default_record as(
    SELECT * FROM src_data
    UNION ALL
    SELECT * FROM default_records
)

{% else %}
, with_default_record as(
    SELECT * FROM src_data
)
{% endif %}

{% if hashed_columns %}
, hashed as (
    SELECT *,
        {{- pragmatic_data.hash_columns_sql(hashed_columns) }}
    FROM with_default_record
)
SELECT * FROM hashed

{% else %}
SELECT * FROM with_default_record
{% endif %}

{%- if remove_duplicates and remove_duplicates['partition_by'] and remove_duplicates['order_by'] %}
{% set qualify_function = remove_duplicates['qualify_function'] if remove_duplicates['qualify_function'] else 'row_number()' %}
{% set qualify_value = remove_duplicates['qualify_value'] if remove_duplicates['qualify_value'] else '1' %}
QUALIFY {{qualify_function}} OVER( 
        PARTITION BY {%- for c in remove_duplicates['partition_by'] %} {{c}}{%- if not loop.last %}, {% endif %}{% endfor %}
        ORDER BY{%- for c in remove_duplicates['order_by'] %} {{c}}{%- if not loop.last %}, {% endif %}{% endfor %}
    ) = {{qualify_value}}
{%- endif -%}

{%- endmacro %}