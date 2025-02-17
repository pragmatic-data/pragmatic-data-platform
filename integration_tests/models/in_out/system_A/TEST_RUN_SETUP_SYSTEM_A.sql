{% do run_SYSTEM_A_inout_setup() %}
SELECT * FROM DIRECTORY( @{{get_SYSTEM_A_inout_stage_name()}} )