{% macro refresh_stage_sql(stage_name) %}
    {% do return('ALTER STAGE IF EXISTS ' ~ stage_name ~ ' REFRESH;') %}
{% endmacro %}

{% macro run_refresh_stage(stage_name) %}
    {% if execute %}
        {% set result = run_query(pragmatic_data.refresh_stage_sql(stage_name)) %}
        {{ print( result | pprint )}}
    {% endif %}
    {% do return(result) %}
{% endmacro %}
