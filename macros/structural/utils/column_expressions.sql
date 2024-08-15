{%- macro column_expressions(calculated_columns, alias = none) %}
    {%- for calculated_column_dict in calculated_columns %}
        {%- if calculated_column_dict is mapping %}            
            {%- for calculated_column, sql_expression in calculated_column_dict.items() %}
    {{ column_expression(sql_expression, calculated_column, alias) }}
            {%- endfor %}
        {%- else %}
    {{ column_expression(calculated_column_dict, alias = alias ) }}
        {%- endif %}
        {%- if not loop.last %}, {% endif -%}
    {%- endfor %}
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