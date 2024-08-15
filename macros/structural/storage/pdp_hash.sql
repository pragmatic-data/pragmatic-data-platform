{%- macro pdp_hash(
    columns_to_hash,
    hash_col_separator = '-|-',
    hash_null_string = '-***-'
) %}
MD5_BINARY( concat_ws( '{{hash_col_separator}}', 
{%- for col in columns_to_hash %}
    coalesce( {{col}}::text, '{{hash_null_string}}' ) 
{%- if not loop.last %}, {% endif %}
{%- endfor %}
) )
{%- endmacro -%}