{% macro check_dummy_exists(stage_name, dummy_base_name) %}
 
    {% if execute %}
        {% set query_sql %}
            ALTER STAGE IF EXISTS {{stage_name}} REFRESH;
            SELECT relative_path FROM DIRECTORY( @{{stage_name}} )
            where relative_path ilike '{{dummy_base_name}}%'
        {% endset %}

        {%- set dummy_exists = dbt_utils.get_single_value(query_sql, false) -%}
        {% if dummy_exists %}
            {% do return(true) %}
        {% endif %}
    {% endif %}

    {% do return(false) %}

{% endmacro %}