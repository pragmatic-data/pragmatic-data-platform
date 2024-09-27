
{%- set local_yaml_config -%}
source: {{ config.require('source') }}
calculated_columns: {{ config.require('calculated_columns') }}
hashed_columns: {{ config.require('hashed_columns') }}
default_records: {{ config.require('default_records') }}
remove_duplicates: {{ config.require('remove_duplicates') }}

flat_dict_calculated_columns:                                   #-- Merging dictionaries => YES ADD & REDEFINE Keys
  <<: {{ config.get('calculated_columns') }}                    #-- Importing the FLATTENED dict from the CFG
  EFFECTIVITY_DATE: "'{{ run_started_at }}'::TIMESTAMP_NTZ"     #-- Added column EFFECTIVITY_DATE
  COLUMN_4: "'9999-09-09'::timestamp"                           #-- Redefined COLUMN_4

{%- endset -%}

{%- set metadata_dict = fromyaml(local_yaml_config) -%}

{{- pragmatic_data.stage(
    source_model            = ref('GENERIC_TWO_COLUMN_TABLE'),
    source                  = metadata_dict['source'],
    calculated_columns      = metadata_dict['flat_dict_calculated_columns'],
    hashed_columns          = metadata_dict['hashed_columns'],
    default_records         = metadata_dict['default_records'],
    remove_duplicates       = metadata_dict['remove_duplicates']
) }}

