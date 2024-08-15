{% macro make_hash_diff(relation, except=[], include=[]) %}
    {%- if execute and relation.identifier != 'request' -%}
        {% set col_list = dbt_utils.get_filtered_columns_in_relation(relation, except) %}
        {%- for inc in include %}
            {%- do col_list.append(inc)  %}
        {%- endfor %}

        {%- if col_list|length > 0 %}
            {{ log('HASH DIFF col_list = ' ~ col_list, true) }}
            {{ dbt_utils.surrogate_key(col_list) }}
        {%- else %}
            {% do exceptions.raise_compiler_error("Relation does not exist yet, no columns. HDIFF not calculated.") %}
        {%- endif %}

    {%- else %}    -- preview or compilation
        {{ return("'Hash SQL generated only during model execution, not in preview or compile. ' ") }}
    {%- endif %}
{%- endmacro %}