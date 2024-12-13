{% macro export_dummy_file(stage_with_path) %}
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
    {{ return('dummy') }}
{% endmacro %}

{% macro get_dummy_base_name(stage_with_path) %}
    {{ return( stage_with_path ~ pragmatic_data.get_dummy_file_name_prefix()) }}
{% endmacro %}
