{% macro pipe__generic_two_column_table(recreate_table = true) %}

{%- set yaml_str -%}
landing_table:
    db_name:     "{{ get_SYSTEM_A_inout_db_name() }}"
    schema_name: "{{ get_SYSTEM_A_inout_schema_name() }}"
    table_name:  PIPED_TWO_COLUMN_TABLE
    columns: #-- Define the landing table columns and eventually its type (& constraints - same syntax as in Create Table).
        - COLUMN1                       # -- NO data type => TEXT
        - COLUMN2: NUMBER NOT NULL      # -- defines column2 of type Number with NOT NULL constraint

pipe:                                                       # -- We always create pipes in the same schema as the LT they load data into
    stage_name: "{{ get_SYSTEM_A_inout_stage_name() }}"     # -- Fully qualified name "PDP_TEST.LAND_SYSTEM_A.SYSTEM_A__STAGE", always needed.
    # integration: NOTIFICATION_INTEGRATION_NAME            # -- Name of the notification integration to be used by the pipe, needed for AUTO ingestion!

    # pipe_name: my_pipe            # -- Optional, base name (not FQ) of pipe; default is the Landing Table name, with _PIPE appended
    # format_name:                  # -- Optional, if empty use the format defined in the stage - provide value to override
    # stage_path:                   # -- Optional, subpath in the stage - to filter the files loaded by the pipe (string path, quick)
    # pattern: '.*.csv.gz'          # -- Optional, to filter the files loaded by the pipe (regexp, slow)
    # add_file_content_key: True    # -- Optional, add the content key metadata; true by default, false if explicitly set to falsey (false, null, empty).

    # -- Field definition NEEDED FOR SEMI STRUCTURED inputs - Map the variant internal columns to desired explicit columns
    # field_expressions:         # -- NO expressions needed for CSV; the number of fields for $1, ...$n is taken from the list of columns.
    #   - COLUMN1: $1::TEXT
    #   - COLUMN2: $2::NUMBER
    #   - src_data: $1          # -- the full record as a Variant column (note - the LT would need an extra VARIANT column)

# -- Sample full FILE PATH of file to be ingested
# -- '@"PDP_TEST"."LAND_SYSTEM_A"."SYSTEM_A__STAGE"/SYSTEM_A/generic/2024_12_15/GENERIC_TWO_COLUMN_TABLE___0_0_0.csv.gz'
# -- Run 'ALTER PIPE PDP_TEST.LAND_SYSTEM_A.PIPED_TWO_COLUMN_TABLE__PIPE REFRESH;' to trigger the ingestion of the files in the LT.

{%- endset -%}

{%- set cfg = fromyaml(yaml_str) -%}

{% do pragmatic_data.run_create_pipe(
        landing_table_dict  = cfg['landing_table'],
        pipe_dict           = cfg['pipe'],
        recreate_table      = recreate_table
) %}

{% endmacro %}
