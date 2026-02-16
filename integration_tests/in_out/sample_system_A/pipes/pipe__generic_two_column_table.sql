{% macro pipe__generic_two_column_table(recreate_table = false) %}

{%- set yaml_str -%}
landing_table:
    db_name:     "{{ get_SYSTEM_A_inout_db_name() }}"
    schema_name: "{{ get_SYSTEM_A_inout_schema_name() }}"
    table_name:  GENERIC_TWO_COLUMN_TABLE
    columns: #-- Define the landing table columns and eventually its type (& constraints - same syntax as in Create Table).
        - COLUMN1                       # -- NO data type => TEXT
        - COLUMN2: NUMBER NOT NULL      # -- defines column2 of type Number with NOT NULL constraint

pipe:
    integration: NOTIFICATION_INTEGRATION_NAME              # -- Name of the notification integration to be used by the pipe
    stage_name: "{{ get_SYSTEM_A_inout_stage_name() }}"     # -- "PDP_TEST.LAND_SYSTEM_A.SYSTEM_A__STAGE"
    # format_name:                                          # -- No format to use the format defined in the stage - put format to override
    # pattern: '.*.csv.gz'      #-- optional

# -- Sample full FILE PATH of file to be ingested
# -- '@"PDP_TEST"."LAND_SYSTEM_A"."SYSTEM_A__STAGE"/SYSTEM_A/generic/2024_12_15/GENERIC_TWO_COLUMN_TABLE___0_0_0.csv.gz'

{%- endset -%}

{%- set metadata_dict = fromyaml(yaml_str) -%}

{% do pragmatic_data.run_CSV_ingestion(
        landing_table_dict = metadata_dict['landing_table'],
        ingestion_dict  = metadata_dict['ingestion'],
        recreate_table = recreate_table
) %}

{% endmacro %}
