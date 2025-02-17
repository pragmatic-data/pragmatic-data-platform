{% macro select_from_table(table = None, column_expression = None) %}
    SELECT {{column_expression or '*'}}
    {% if table %}FROM {{table}}{% endif %}
{% endmacro %}