{% macro export_to_stage_sql(full_table_name, stage_with_path, full_format_name = none) %}
BEGIN TRANSACTION;

COPY INTO @{{ stage_with_path }}
FROM (
    SELECT * FROM {{ full_table_name }}
)

{%- if full_format_name %}
FILE_FORMAT = (FORMAT_NAME = '{{ full_format_name }}')
{%- endif %}

HEADER = TRUE
OVERWRITE = TRUE
;

COMMIT;

{%- endmacro %}