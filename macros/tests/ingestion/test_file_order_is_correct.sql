{% test file_order_is_correct( 
    model, 
    column_name = 'FROM_FILE',
    time_column_name = 'INGESTION_TS_UTC' 
) -%}

WITH
loading_info as (
    SELECT distinct {{ time_column_name }}, {{ column_name }} as LOADED_FILE
    FROM {{ model }}
)

, validation_errors as (
SELECT 
    {{ time_column_name }}, 
    LOADED_FILE, 
    LAG(LOADED_FILE) OVER(order by {{ time_column_name }}, LOADED_FILE) as PREV_FILE,
    CASE 
        WHEN PREV_FILE is null THEN true
        ELSE (LOADED_FILE >= PREV_FILE)
    END as IN_CORRECT_ORDER
FROM loading_info 
order by 1, 2
)

SELECT * FROM validation_errors
WHERE NOT IN_CORRECT_ORDER

{%- endtest %}