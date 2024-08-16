
{%- set local_yaml_config -%}
source:
    columns: 
        include_all: false          #-- True enables using eclude / replace / rename lists // false does not include any source col
    where: "Column1 != 'xxx'"

calculated_columns:
    - COLUMN_1: Column1
    - COLUMN_2: UPPER(Column2)
    - COLUMN_3: '!COL_4_CONSTANT'
    - COLUMN_4: current_timestamp()::TIMESTAMP_NTZ

default_records: 
    - not_provided:    #-- def_record_1
        - COLUMN_1: "'-1'"                              #-- String literals either as "'string'" or '!string'
        - COLUMN_2: '!NOT Provided (optional)'          #-- String literals either as "'string'" or '!string'
        - COLUMN_3: '!System.DefaultRecord'
        - COLUMN_4: "'{{ run_started_at }}'::TIMESTAMP_NTZ"
    - missing:          #-- def_record_2
        - COLUMN_1: "'-2'"                              #-- String literals either as "'string'" or '!string'
        - COLUMN_2: '!Missing (required data)'          #-- String literals either as "'string'" or '!string'
        - COLUMN_3: '!System.DefaultRecord'
        - COLUMN_4: "'{{ run_started_at }}'::TIMESTAMP_NTZ"

hashed_columns: 
    - MODEL_HKEY:
        - COLUMN_1
    - OTHER_HKEY:
        - COLUMN_1
        - COLUMN_2

    - MODEL_HDIFF:
        - COLUMN_1
        - COLUMN_2
        - COLUMN_3

{%- endset -%}

{%- set metadata_dict = fromyaml(local_yaml_config) -%}

{{- pragmatic_data.stage(
    source_model            = ref('GENERIC_TWO_COLUMN_TABLE'),
    source                  = metadata_dict['source'],
    calculated_columns      = metadata_dict['calculated_columns'],
    hashed_columns          = metadata_dict['hashed_columns'],
    default_records         = metadata_dict['default_records'],
    remove_duplicates       = metadata_dict['remove_duplicates'],
) }}
