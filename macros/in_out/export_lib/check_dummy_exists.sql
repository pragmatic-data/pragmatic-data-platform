{% macro check_dummy_exists(stage_name, dummy_full_name) %}
 
    {% set query_sql %}
        ALTER STAGE IF EXISTS {{stage_name}} REFRESH;
        SELECT relative_path FROM DIRECTORY( @{{stage_name}} )
        where relative_path = '{{dummy_full_name}}'
    {% endset %}

    
    {%- set dummy_exists = dbt_utils.get_single_value(query_sql) -%} 


    {% if dummy_exists %}
        {{ return(true) }}
    {% endif %}

    {{ return(false) }}   

{% endmacro %}