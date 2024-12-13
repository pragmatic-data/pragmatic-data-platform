{% macro export_to_stage_sql(table_name, stage_with_path, format_name = none) %}
BEGIN TRANSACTION;

COPY INTO @{{ stage_with_path }}
FROM (
    SELECT * FROM {{ table_name }}
)

{%- if format_name %}
FILE_FORMAT = (FORMAT_NAME = '{{ format_name }}')
{%- endif %}

HEADER = TRUE
OVERWRITE = TRUE
;

COMMIT;

{%- endmacro %}