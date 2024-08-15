
{%- set local_yaml_config -%}
source:
    model: "{{ ref('GENERIC_TWO_COLUMN_TABLE') }}"
    columns: 
        include_all: false          #-- True enables using eclude / replace / rename lists // false does not include any source col
    where: "Column1 != 'xxx'"
{%- endset -%}

{{ pragmatic_data.stage(
    source                  = fromyaml(local_yaml_config)['source'],
    calculated_columns      = config.require('calculated_columns'),
    hashed_columns          = config.require('hashed_columns'),
    default_records         = config.require('default_records'),
    remove_duplicates       = config.require('remove_duplicates')
) }}

