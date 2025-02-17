{% set inout = {'database': 'PROJECT_X', 'schema': 'LAND_SYSTEM_A'} %}
{% set file_format = {'name': 'SYSTEM_A_CSV__FF', 'definition': { "TYPE": "'CSV'", "SKIP_HEADER": "1" }} %}

WITH
use_case_01 as (
    -- Passing a stage name => get FQ stage name using inout
    -- NOT passing a FILE FORMAT nor a FF name in the stage =>  omit the FF from the stage
    {% set stage = {'name': 'SYSTEM_A__STAGE', 
        'definition': { "DIRECTORY": "( ENABLE = true )", "FILE_FORMAT": none }} %}
    SELECT
        $${{ inout }}$$ as inout_dict,
        $${{ file_format }}$$ as file_format_dict,
        $${{ stage }}$$ as stage_dict,

        '{{ pragmatic_data.get_inout_fq_stage_name(stage.name, inout) }}' as actual_fq_STAGE_name,
        'PROJECT_X.LAND_SYSTEM_A.SYSTEM_A__STAGE' as expected_fq_STAGE_name,

        $${{- pragmatic_data.create_stage(stage, none, inout) -}}$$ as actual_stage_sql,
        'CREATE STAGE IF NOT EXISTS PROJECT_X.LAND_SYSTEM_A.SYSTEM_A__STAGE' as expected_stage__creation,
        'DIRECTORY = ( ENABLE = true )' as expected_stage__directory,
        '' as expected_stage__file_format,          -- NO FF, as no name in stage nor passed
        'FILE_FORMAT' as expected_NOT_IN_stage_sql
)
, use_case_02 as (
    -- Passing a FQ stage name => use that FQ stage name as is
    -- NOT passing a FILE FORMAT, but providing a FF name (not FQ) in the stage => FF fully qualified (by inout def) in the stage
    {% set stage = {'name': 'SOME_DB.SOME_SCHEMA.SOME_STAGE', 
        'definition': { "DIRECTORY": "( ENABLE = true )", "FILE_FORMAT": 'SOME_FILE_FORMAT' }} %}
    SELECT
        $${{ inout }}$$ as inout_dict,
        $${{ file_format }}$$ as file_format_dict,
        $${{ stage }}$$ as stage_dict,

        '{{ pragmatic_data.get_inout_fq_stage_name(stage.name, inout) }}' as actual_fq_STAGE_name,
        'SOME_DB.SOME_SCHEMA.SOME_STAGE' as expected_fq_STAGE_name,

        $${{- pragmatic_data.create_stage(stage, none, inout) -}}$$ as actual_stage_sql,
        'CREATE STAGE IF NOT EXISTS SOME_DB.SOME_SCHEMA.SOME_STAGE' as expected_stage__creation,
        'DIRECTORY = ( ENABLE = true )' as expected_stage__directory,
        'FILE_FORMAT = PROJECT_X.LAND_SYSTEM_A.SOME_FILE_FORMAT' as expected_stage__file_format,    -- as stage defined FF name, FQ by inout
        'XXXX' as expected_NOT_IN_stage_sql
)
, use_case_03 as (
    -- Passing NO stage name, but fq_name => use the fq_name without changes in CREATE. NOTE that the generated FQ name will differ as it looks after stage.name
    -- NOT passing in the FILE FORMAT, but providing a FQ FF name in the stage => FQ FF 'as is' in the stage
    {% set stage = {'fq_name': 'MY_SPECIAL_STAGE', 
        'definition': { "DIRECTORY": "( ENABLE = true )", "FILE_FORMAT": 'SOME_DB.SOME_SCHEMA.SOME_FILE_FORMAT' }} %}
    SELECT
        $${{ inout }}$$ as inout_dict,
        $${{ file_format }}$$ as file_format_dict,
        $${{ stage }}$$ as stage_dict,

        '{{ pragmatic_data.get_inout_fq_stage_name(stage.name, inout) }}' as actual_fq_STAGE_name,
        'PROJECT_X.LAND_SYSTEM_A.{{target.schema}}_STAGE' as expected_fq_STAGE_name,

        $${{- pragmatic_data.create_stage(stage, none, inout) -}}$$ as actual_stage_sql,
        'CREATE STAGE IF NOT EXISTS MY_SPECIAL_STAGE' as expected_stage__creation,
        'DIRECTORY = ( ENABLE = true )' as expected_stage__directory,
        'FILE_FORMAT = SOME_DB.SOME_SCHEMA.SOME_FILE_FORMAT' as expected_stage__file_format,  -- as stage defined FQ FF
        'XXXX' as expected_NOT_IN_stage_sql
)
, use_case_04 as (
    -- passing a FILE FORMAT, but no FF name in the stage =>  use the passed FILE FORMAT as FF of the stage
    {% set stage = {'name': 'SYSTEM_A__STAGE', 
        'definition': { "DIRECTORY": "( ENABLE = true )", "FILE_FORMAT": none }} %}
    SELECT
        $${{ inout }}$$ as inout_dict,
        $${{ file_format }}$$ as file_format_dict,
        $${{ stage }}$$ as stage_dict,

        '{{ pragmatic_data.get_inout_fq_stage_name(stage.name, inout) }}' as actual_fq_STAGE_name,
        'PROJECT_X.LAND_SYSTEM_A.SYSTEM_A__STAGE' as expected_fq_STAGE_name,

        $${{- pragmatic_data.create_stage(stage, file_format, inout) -}}$$ as actual_stage_sql,
        'CREATE STAGE IF NOT EXISTS PROJECT_X.LAND_SYSTEM_A.SYSTEM_A__STAGE' as expected_stage__creation,
        'DIRECTORY = ( ENABLE = true )' as expected_stage__directory,
        'FILE_FORMAT = PROJECT_X.LAND_SYSTEM_A.SYSTEM_A_CSV__FF' as expected_stage__file_format,    -- from passed FILE FORMAT
        'XXXX' as expected_NOT_IN_stage_sql
)
, use_case_05 as (
    -- passing a FILE FORMAT, and a FF name in the stage =>  use the FF name (FQ with inout) as FF of the stage
    {% set stage = {'name': 'SYSTEM_A__STAGE', 
        'definition': { "DIRECTORY": "( ENABLE = true )", "FILE_FORMAT": 'SOME_FILE_FORMAT' }} %}
    SELECT
        $${{ inout }}$$ as inout_dict,
        $${{ file_format }}$$ as file_format_dict,
        $${{ stage }}$$ as stage_dict,

        '{{ pragmatic_data.get_inout_fq_stage_name(stage.name, inout) }}' as actual_fq_STAGE_name,
        'PROJECT_X.LAND_SYSTEM_A.SYSTEM_A__STAGE' as expected_fq_STAGE_name,

        $${{- pragmatic_data.create_stage(stage, file_format, inout) -}}$$ as actual_stage_sql,
        'CREATE STAGE IF NOT EXISTS PROJECT_X.LAND_SYSTEM_A.SYSTEM_A__STAGE' as expected_stage__creation,
        'DIRECTORY = ( ENABLE = true )' as expected_stage__directory,
        'FILE_FORMAT = PROJECT_X.LAND_SYSTEM_A.SOME_FILE_FORMAT' as expected_stage__file_format,    -- as stage defined FF name, FQ by inout
        'XXXX' as expected_NOT_IN_stage_sql
)
, use_case_06 as (
    -- passing a FILE FORMAT, and a FQ FF name in the stage =>  use the FF name (as is) as FF of the stage
    {% set stage = {'name': 'SYSTEM_A__STAGE', 
        'definition': { "DIRECTORY": "( ENABLE = true )", "FILE_FORMAT": 'SOME_DB.SOME_SCHEMA.SOME_FILE_FORMAT' }} %}
    SELECT
        $${{ inout }}$$ as inout_dict,
        $${{ file_format }}$$ as file_format_dict,
        $${{ stage }}$$ as stage_dict,

        '{{ pragmatic_data.get_inout_fq_stage_name(stage.name, inout) }}' as actual_fq_STAGE_name,
        'PROJECT_X.LAND_SYSTEM_A.SYSTEM_A__STAGE' as expected_fq_STAGE_name,

        $${{- pragmatic_data.create_stage(stage, file_format, inout) -}}$$ as actual_stage_sql,
        'CREATE STAGE IF NOT EXISTS PROJECT_X.LAND_SYSTEM_A.SYSTEM_A__STAGE' as expected_stage__creation,
        'DIRECTORY = ( ENABLE = true )' as expected_stage__directory,
        'FILE_FORMAT = SOME_DB.SOME_SCHEMA.SOME_FILE_FORMAT' as expected_stage__file_format,    -- as stage defined FQ FF name
        'XXXX' as expected_NOT_IN_stage_sql
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
UNION ALL
SELECT * FROM use_case_06
{#
#}
