{%- macro column_expressions(column_definitions, alias = none) %}
    {%- if column_definitions is mapping %}
        {%-for column_name, sql_expression in column_definitions.items() %}
            {%- if sql_expression %}
                {{ pragmatic_data.column_expression(sql_expression, column_name, alias)}}
            {%- else %}    
                {{ pragmatic_data.column_expression(column_name, alias = alias ) }}
            {%- endif %}
            {%- if not loop.last %}, {% endif -%}
        {%- endfor %}
    {%- else %}    {#-- we have a list with column names or dictionaries with column definitions inside #}
        {%- for col_def in column_definitions %}
            {%- if col_def is mapping %}{# recursive call to navigate the mapping that is single or multiple columns #}
                {{- pragmatic_data.column_expressions(col_def, alias = alias) -}}
            {%- else %}                 {# handle the item from the list, that should be just a column name#}
                {{ pragmatic_data.column_expression(col_def, alias = alias ) }}
            {%- endif %}
            {%- if not loop.last %}, {% endif -%}
        {%- endfor %}
    {%- endif %}
{%- endmacro %}

{% macro column_expression(sql_expression, column_name = none, alias = none) %}
    {%- if sql_expression.startswith('!') -%}
    '{{ sql_expression[1:] }}'
    {%- else -%}
    {%- if alias %}{{alias}}.{% endif -%}
    {{ sql_expression }}
    {%- endif %}    
    {%- if column_name %} as {{ column_name }}{%- endif -%}
{% endmacro %}