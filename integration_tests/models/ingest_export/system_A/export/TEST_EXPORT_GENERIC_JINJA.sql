-- depends_on: {{ ref('TEST_RUN_SETUP_SYSTEM_A') }} 

{%- set stage_name = get_SYSTEM_A_inout_stage_name() %}
{%- do export__generic_two_column_table__JINJA() %}
{%- do pragmatic_data.run_refresh_stage(stage_name) %}

SELECT * FROM DIRECTORY( @{{ stage_name }} )
