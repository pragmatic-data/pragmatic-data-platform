{%- macro hash_columns_sql(hashed_columns) %}

{%- if hashed_columns -%}

    {%- if hashed_columns is mapping -%}
        {%- set hashed_columns = [hashed_columns] -%}
    {%- endif -%}

    {%- for hash_definition in hashed_columns -%}
        {%- if hash_definition is mapping -%}
        {%- set outer_loop = loop -%}
        {%- for hash_name, definition in hash_definition.items() -%}
            {%- if definition is mapping and definition.columns %}
                {{ pragmatic_data.pdp_hash(definition.columns) }} as {{ hash_name }}
            {%- else %}
                {{ pragmatic_data.pdp_hash(definition) }} as {{ hash_name }}
            {%- endif %}
            {%- if not outer_loop.last or not loop.last %}, {% endif %}
        {%- endfor %}
        {%- endif %}
    {%- endfor %}

{%- endif %}
{% endmacro %}