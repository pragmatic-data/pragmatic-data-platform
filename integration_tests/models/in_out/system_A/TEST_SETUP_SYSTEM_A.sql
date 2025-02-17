{% set cfg = get_SYSTEM_A_inout_cfg() %}

WITH
use_case_01 as (
    SELECT
        $${{ cfg }}$$ as cfg,
        $${{ get_SYSTEM_A_inout_setup_sql()}}$$ as result,
        
        '{{ cfg.inout.schema }}' as inout_schema,
        '{{ cfg.file_format.name }}' as file_format_name,
        $${{ cfg.file_format.definition.TYPE }}$$ as file_format_definition_TYPE,
        '{{ cfg.stage.name }}' as stage_name,
        $${{ cfg.stage.definition.DIRECTORY }}$$ as stage_definition_DIRECTORY
)
SELECT * FROM use_case_01
