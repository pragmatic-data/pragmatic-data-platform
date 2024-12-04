{% macro time_join(
    base_table_dict,
    joined_tables_dict,
    calculated_columns = [],

    high_watermark_column   = var('pdp.high_watermark_column', var('pdp.hist_load_ts_column', 'HIST_LOAD_TS_UTC')),
    high_watermark_test     = var('pdp.high_watermark_test', '>')
) %}

SELECT
    {%- if base_table_dict['include_all_columns'] %}bt.* {% endif %}
    {%- if base_table_dict['include_all_columns'] and base_table_dict['exclude_column_list'] -%}
        EXCLUDE( {{ base_table_dict['exclude_column_list'] | join(', ') }} ){%- endif -%}
    {%- if base_table_dict['include_all_columns'] and base_table_dict['columns'] %},{% endif %}

    {{- pragmatic_data.column_expressions(base_table_dict['columns'], 'bt') }}
    {%- if base_table_dict['columns'] %}, {% endif -%}

    {%- for joined_table_name in joined_tables_dict %}
    {%- set alias = 't'~loop.index %}
    {{ pragmatic_data.column_expressions(joined_tables_dict[joined_table_name]['columns'], alias ) }}
    {%- if not loop.last %}, {% endif -%}
    {%- endfor -%}

    {%- if calculated_columns %}, 
    {{ pragmatic_data.column_expressions(calculated_columns) }}
    {%- endif %}

FROM {{ base_table_dict['name'] }} as bt

{%- for joined_table_name in joined_tables_dict %}
{%- set alias = 't'~loop.index %}
{%- set time_operator = joined_tables_dict[joined_table_name]['time_operator'] or '>=' %}
ASOF JOIN {{joined_table_name}} as {{alias}}
    MATCH_CONDITION(
    {%- for t_col, bt_col in joined_tables_dict[joined_table_name]['time_column'].items() -%}
        bt.{{bt_col}} {{time_operator}} {{alias}}.{{t_col}}
    {%- endfor -%} )    
    ON(
    {%- for t_col, bt_col in joined_tables_dict[joined_table_name]['join_columns'].items() -%}
        bt.{{bt_col}} = {{alias}}.{{t_col}}
        {%- if not loop.last %} and {% endif -%}
    {%- endfor -%} )    
{% endfor %}

WHERE true
{%- if base_table_dict['filter'] %}
  and {{ base_table_dict['filter'] }}
{%- endif %}

{%- for joined_table_name in joined_tables_dict %}
    {%- for filter in joined_tables_dict[joined_table_name]['filters'] %}
  and {{filter}}        
    {%- endfor %}
{%- endfor %}

{%- if is_incremental() and high_watermark_column %}    -- Incremental load based on High Water Mark on {{high_watermark_column}}
  and bt.{{high_watermark_column}} {{high_watermark_test}} (select max({{high_watermark_column}}) from {{ this }}) 
{% endif %}

{%- endmacro %}