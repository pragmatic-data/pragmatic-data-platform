-- depends_on: {{ ref('TEST_RUN_SETUP_SYSTEM_A') }}     <- the model running the setup
-- depends_on: {{ ref('TEST_EXPORT_GENERIC_JINJA') }}   <- the model writing out the data

{#% set full_table_name = source('SYSTEM_A', 'GENERIC_TWO_COLUMN_TABLE') %#}
{%- set full_table_name = get_SYSTEM_A_inout_db_name() 
                 ~ '.' ~ get_SYSTEM_A_inout_schema_name() 
                 ~ '.' ~ 'GENERIC_TWO_COLUMN_TABLE' %}

{%- do ingest__generic_two_column_table__YAML() %}

SELECT
    INGESTION_TS_UTC,
    FROM_FILE,
    FILE_LAST_MODIFIED_TS_UTC,
    count(*) as rows_from_file
FROM {{ full_table_name }}
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
