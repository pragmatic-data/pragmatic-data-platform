{% macro export_dummy_file(stage_with_path) %}
BEGIN TRANSACTION;

COPY INTO @{{ stage_with_path }}dummy
FROM (
    SELECT current_timestamp() as dummy_ts
)


HEADER = TRUE
OVERWRITE = TRUE
;

COMMIT;

{%- endmacro %}