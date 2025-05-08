{% test all_files_from_stage( model, column_name, 
    pattern,
    stage_name,
    min_file_size = None,
    relative_path_prefix = None
) -%}

{% set query %}
    ALTER STAGE {{ stage_name }} REFRESH;
{% endset %}

{% do run_query(query) %}

WITH
all_files_in_stg as (
  --  $1 as name, $2 as size, $3 as last_modified, $4 as md5, $5 as etag, $6 as file_url
  -- RELATIVE_PATH, SIZE, LAST_MODIFIED, MD5, ETAG, FILE_URL
  SELECT 
    {% if relative_path_prefix %}'{{relative_path_prefix}}'||{% endif %}RELATIVE_PATH as file_path
  FROM DIRECTORY ( @{{stage_name}} )
  WHERE REGEXP_LIKE(file_path, '{{pattern}}' )
  {% if min_file_size %}AND $2 > {{ min_file_size }}{% endif %}
)
, all_files_in_lt as (
  SELECT distinct {{ column_name }} as file_path -- FROM_FILE
  FROM {{ model }}
)

, validation_errors as (
    SELECT file_path from all_files_in_stg
    EXCEPT 
    SELECT file_path from all_files_in_lt
    order by 1
)

SELECT * FROM validation_errors
{%- endtest %}
