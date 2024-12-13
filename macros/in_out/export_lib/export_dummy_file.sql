{% macro export_dummy_file_sql(stage_with_path) %}
BEGIN TRANSACTION;

COPY INTO @{{ pragmatic_data.get_dummy_base_name(stage_with_path) }}
FROM (
    SELECT current_timestamp() as dummy_ts
)
HEADER = TRUE
OVERWRITE = TRUE
;

COMMIT;

{%- endmacro %}

{% macro get_dummy_file_name_prefix() %}
    {% do return('dummy') %}
{% endmacro %}

{% macro get_dummy_base_name(stage_with_path) %}
    {% do return( stage_with_path ~ pragmatic_data.get_dummy_file_name_prefix()) %}
{% endmacro %}
