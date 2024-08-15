
{%- set yaml_str -%}
source:
    model: "PORTFOLIO_WH.LAND_IB.OPEN_POSITIONS"     #-- "{#{ source('IB', 'OPEN_POSITIONS') }#}"
    columns: 
        include_all: false          #-- True enables using eclude / replace / rename lists // false does not include any source col
    where: "ASSET_CLASS != 'AssetClass'"

Z_calcl_cols:
    - COL1: EXPR1
    - COL2
    - COL3: EXP3

Y_calcl_cols:
    COL1: EXPR1
    COL2:
    COL3: EXP3

{%- endset -%}

{%- set metadata_dict = fromyaml(yaml_str) -%}

{{ metadata_dict['Z_calcl_cols'] }}
{{ metadata_dict['Y_calcl_cols'] }}
{{ config.require('calculated_columns')  }}
{{ config.require('x_calculated_columns') }}
--


    {#% set calculated_columns  = config.require('x_calculated_columns') %#}
    {% set calculated_columns  = metadata_dict['Y_calcl_cols'] %}
    {%- if calculated_columns is mapping %}
        {% for column_name, sql_expression in calculated_columns.items() %}
            {% if sql_expression %}
                {{ column_expression(sql_expression, column_name, alias) }}
            {%- else %}    
                {{ column_expression(column_name, alias = alias ) }}            
            {% endif %}
            {%- if not loop.last %}, {% endif -%}
        {% endfor %}
    {%- else %}    
    {%- for col_def in calculated_columns %}
        {%- if col_def is mapping %}            
            {%- for column_name, sql_expression in col_def.items() %}
                {{ column_expression(sql_expression, column_name, alias) }}
            {%- endfor %}
        {%- else %}
            {{ column_expression(col_def, alias = alias ) }}
        {%- endif %}
        {%- if not loop.last %}, {% endif -%}
    {%- endfor %}
    {%- endif %}
----

{#{ config.require('y_calculated_columns') }#}