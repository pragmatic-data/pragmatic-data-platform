{% macro select_from_table(table, column_expression) %}
    SELECT {{column_expression}}
    FROM {{table}}
{% endmacro %}