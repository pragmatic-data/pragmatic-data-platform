
{%- set local_yaml_config -%}
source:
    model: "PORTFOLIO_WH.LAND_IB.OPEN_POSITIONS"
    columns: 
        include_all: false          #-- True enables using eclude / replace / rename lists // false does not include any source col
    where: "ASSET_CLASS != 'AssetClass'"

{%- endset -%}

{#%- set metadata_dict = fromyaml(local_yaml_config) -%#}

src: {{ fromyaml(local_yaml_config)['source'] }}
cc: {{ config.require('calculated_columns') }}
hc: {{ config.require('hashed_columns') }}
dr: {{ config.require('default_records') }}
rd: {{ config.require('remove_duplicates') }}

{{ pragmatic_data.stage(
    source                  = fromyaml(local_yaml_config)['source'],
    calculated_columns      = config.require('calculated_columns'),
    hashed_columns          = config.require('hashed_columns'),
    default_records         = config.require('default_records'),
    remove_duplicates       = config.require('remove_duplicates')
) }}
{#
#}

