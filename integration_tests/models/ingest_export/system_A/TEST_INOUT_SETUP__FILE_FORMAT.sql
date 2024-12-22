{% set inout = {'database': 'PROJECT_X', 'schema': 'LAND_SYSTEM_A'} %}

WITH
use_case_01 as (
    {% set file_format = {'name': 'SYSTEM_A_CSV__FF', 
        'definition': { "TYPE": "'CSV'", "SKIP_HEADER": "1" }} %}
    SELECT
        $${{ inout }}$$ as inout_dict,
        $${{ file_format }}$$ as file_format_dict,

        '{{ pragmatic_data.get_inout_fq_file_format_name(file_format.name, inout) }}' as actual_fq_FF_name,
        'PROJECT_X.LAND_SYSTEM_A.SYSTEM_A_CSV__FF' as expected_fq_FF_name,

        $${{- pragmatic_data.create_file_format(file_format, inout) -}}$$ as actual_file_format,
        'TYPE = ''CSV''' as expected_file_format__type,
        'SKIP_HEADER = 1' as expected_file_format__skip_header
)
, use_case_02 as (
    {% set file_format = {'name': 'AAA.BBBB.SYSTEM_A_CSV__FF', 
        'definition': { "TYPE": "'Parquet'"}} %}
    SELECT
        $${{ inout }}$$ as inout_dict,
        $${{ file_format }}$$ as file_format_dict,

        '{{ pragmatic_data.get_inout_fq_file_format_name(file_format.name, inout) }}' as actual_fq_FF_name,
        'AAA.BBBB.SYSTEM_A_CSV__FF' as expected_fq_FF_name,

        $${{- pragmatic_data.create_file_format(file_format, inout) -}}$$ as actual_file_format,
        'TYPE = ''Parquet''' as expected_file_format__type,
        '' as expected_file_format__skip_header
)
, use_case_03 as (
    {% set file_format = {'name': none, 
        'definition': { "TYPE": "'CSV'", "SKIP_HEADER": none }} %}
    SELECT
        $${{ inout }}$$ as inout_dict,
        $${{ file_format }}$$ as file_format_dict,

        '{{ pragmatic_data.get_inout_fq_file_format_name(file_format.name, inout) }}' as actual_fq_FF_name,
        'PROJECT_X.LAND_SYSTEM_A.{{ target.schema }}_FF' as expected_fq_FF_name,

        $${{- pragmatic_data.create_file_format(file_format, inout) -}}$$ as actual_file_format,
        'TYPE = ''CSV''' as expected_file_format__type,
        '' as expected_file_format__skip_header
)
, use_case_04 as ( -- Testing the fq_name attribute used to override fq name generation
    {% set file_format = {'fq_name': 'XXXX.YYYYY.SYSTEM_A_CSV__FF', 
        'definition': { "TYPE": "'Parquet'"}} %}
    SELECT
        $${{ inout }}$$ as inout_dict,
        $${{ file_format }}$$ as file_format_dict,

        '{{ pragmatic_data.get_inout_fq_file_format_name(file_format.name, inout) }}' as actual_fq_FF_name,
        'PROJECT_X.LAND_SYSTEM_A.{{ target.schema }}_FF' as expected_fq_FF_name,    -- NO file_format.name

        $${{- pragmatic_data.create_file_format(file_format, inout) -}}$$ as actual_file_format,
        'TYPE = ''Parquet''' as expected_file_format__type,
        'CREATE FILE FORMAT IF NOT EXISTS XXXX.YYYYY.SYSTEM_A_CSV__FF' as expected_file_format__skip_header    -- re-purposed to test FQ Name override
)

SELECT * FROM use_case_01
UNION ALL
SELECT * FROM use_case_02
UNION ALL
SELECT * FROM use_case_03
UNION ALL
SELECT * FROM use_case_04
{#
UNION ALL
SELECT * FROM use_case_05
#}
