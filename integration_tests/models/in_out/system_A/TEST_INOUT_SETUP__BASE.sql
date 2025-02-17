
WITH
use_case_01 as (
    {% set inout = {'database': target.database, 'schema': 'LAND_SYSTEM_A'} %}
    SELECT
        '{{ inout.database }}' as inout_database,
        '{{ inout.schema }}' as inout_schema,
        '{{ pragmatic_data.get_inout_db(inout) }}' as actual_db,
        '{{ target.database }}' as expected_db,
        '{{ pragmatic_data.get_inout_schema(inout) }}' as actual_schema,
        'LAND_SYSTEM_A' as expected_schema,
        '{{ pragmatic_data.get_inout_fq_schema(inout) }}' as actual_fq_schema,
        '{{ target.database }}.LAND_SYSTEM_A' as expected_fq_schema
)
, use_case_02 as (
    {% set inout = {'database': 'PROJECT_X', 'schema': 'LAND_SYSTEM_A'} %}
    SELECT
        '{{ inout.database }}' as inout_database,
        '{{ inout.schema }}' as inout_schema,
        '{{ pragmatic_data.get_inout_db(inout) }}' as actual_db,
        'PROJECT_X' as expected_db,
        '{{ pragmatic_data.get_inout_schema(inout) }}' as actual_schema,
        'LAND_SYSTEM_A' as expected_schema,
        '{{ pragmatic_data.get_inout_fq_schema(inout) }}' as actual_fq_schema,
        'PROJECT_X.LAND_SYSTEM_A' as expected_fq_schema
)
, use_case_03 as (
    {% set inout = {'database': none, 'schema': 'LAND_SYSTEM_A'} %}
    SELECT
        '{{ inout.database }}' as inout_database,
        '{{ inout.schema }}' as inout_schema,
        '{{ pragmatic_data.get_inout_db(inout) }}' as actual_db,
        '{{ target.database }}' as expected_db,
        '{{ pragmatic_data.get_inout_schema(inout) }}' as actual_schema,
        'LAND_SYSTEM_A' as expected_schema,
        '{{ pragmatic_data.get_inout_fq_schema(inout) }}' as actual_fq_schema,
        '{{ target.database }}.LAND_SYSTEM_A' as expected_fq_schema
)
, use_case_04 as (
    {% set inout = {'database': none, 'schema': none} %}
    SELECT
        '{{ inout.database }}' as inout_database,
        '{{ inout.schema }}' as inout_schema,
        '{{ pragmatic_data.get_inout_db(inout) }}' as actual_db,
        '{{ target.database }}' as expected_db,
        '{{ pragmatic_data.get_inout_schema(inout) }}' as actual_schema,
        '{{ target.schema }}_LANDING' as expected_schema,
        '{{ pragmatic_data.get_inout_fq_schema(inout) }}' as actual_fq_schema,
        '{{ target.database }}.{{ target.schema }}_LANDING' as expected_fq_schema
)
, use_case_05 as (
    {% set inout = {'database': '', 'schema': ''} %}
    SELECT
        '{{ inout.database }}' as inout_database,
        '{{ inout.schema }}' as inout_schema,
        '{{ pragmatic_data.get_inout_db(inout) }}' as actual_db,
        '{{ target.database }}' as expected_db,
        '{{ pragmatic_data.get_inout_schema(inout) }}' as actual_schema,
        '{{ target.schema }}_LANDING' as expected_schema,
        '{{ pragmatic_data.get_inout_fq_schema(inout) }}' as actual_fq_schema,
        '{{ target.database }}.{{ target.schema }}_LANDING' as expected_fq_schema
)
SELECT * FROM use_case_01
UNION ALL
SELECT * FROM use_case_02
UNION ALL
SELECT * FROM use_case_03
UNION ALL
SELECT * FROM use_case_04
UNION ALL
SELECT * FROM use_case_05
