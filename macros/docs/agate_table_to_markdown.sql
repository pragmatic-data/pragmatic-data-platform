{% macro agate_table_to_markdown(agate_table) %}
    {%- set t_header = ['| '] %}
    {%- set t_separator = ['| '] %}

    {%- for cname in agate_table.column_names %}
        {%- do t_header.append( cname ~ ' | ') %}
        {%- do t_separator.append( '--- | ') %}
    {%- endfor %}

 {{ t_header|join("") }}
 {{ t_separator|join("") }}

    {%- for row in agate_table.rows %}
 |  {{ row.values()|join(" | ") }} |
    {%- endfor %}

{%- endmacro %}