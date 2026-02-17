{% macro field_definitions(pipe_dict, payload_column_count) %}
    
{%- set field_definitions %}
    {%- if pipe_dict and pipe_dict.field_expressions %}
        {%- for expression in pipe_dict.field_expressions %}
            {%- if expression is string %}
        {{ expression }}
            {%- else %}
                {%- for col, def in expression.items() %}
        {{ def }} as {{ col }}
                {%- endfor %}
            {%- endif %}
            {%- if not loop.last %}, {% endif %}
        {%- endfor %}
    {%- else %}
        {% for i in range(1, payload_column_count + 1) %}${{i}}{% if not loop.last %}, {% endif %}{% endfor %}
    {%- endif %}
{%- endset %}

{{ return(field_definitions) }}

{% endmacro %}