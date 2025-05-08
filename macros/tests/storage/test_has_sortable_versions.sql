{% test has_sortable_versions( model, 
    key_column,
    diff_column,
    sort_columns,
    multiple_equal_versions_is_ok = true
) -%}

WITH
validation_errors as (
    SELECT {{ key_column }}, {{ sort_columns }}
        , count({{ diff_column }}) as cnt_versions
        , count(distinct {{ diff_column }}) as cnt_distinct_versions
    FROM {{ model }}
    GROUP BY {{ key_column }}, {{ sort_columns }}
    {%- if multiple_equal_versions_is_ok %}
    HAVING cnt_distinct_versions > 1
    {%- else %}
    HAVING cnt_versions > 1
    {%- endif %}
)

SELECT * FROM validation_errors
{%- endtest %}
