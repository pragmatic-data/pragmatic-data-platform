{% macro self_completing_dimension(
    dim_rel,
    dim_key_column,
    dim_default_key_value = '-1',
    ref_columns_to_exclude = [],

    fact_defs = []
) -%}

/* ** Usage notes **
 * - The primary key has to be the first field in the underlying reference for the dimension
 */

{% set ref_columns_to_exclude_w_key = ref_columns_to_exclude.copy() %}
{% do ref_columns_to_exclude_w_key.append(dim_key_column) %}

/* Force model dependencies */
-- depends_on: {{ dim_rel }}
{%- for fact_model_key in fact_defs %}
-- depends_on: {{ ref(fact_model_key['model']) }}
{%- endfor %}

WITH
dim_base as (
    SELECT
          {{ dim_key_column }}
        , d.* EXCLUDE( {{ ref_columns_to_exclude_w_key | join(', ') }} )
    FROM {{ dim_rel }} as d
),

fact_key_list as ( 
    {% if fact_defs|length > 0 %}   -- If a FACT reference is passed, then check for orphans and add them in the dimension

        {%- for fact_model_key in fact_defs %}
        select distinct {{fact_model_key['key']}} as FOREIGN_KEY
        FROM {{ ref(fact_model_key['model']) }}
        WHERE {{fact_model_key['key']}} is not null
            {% if not loop.last %} union {% endif %}
        {%- endfor -%}

    {%- else %}          -- If NO FACT reference is passed, the list of fact keys is just empty.
    select null as FOREIGN_KEY WHERE false
    {%- endif%}
),
missing_keys as (
    SELECT fkl.FOREIGN_KEY 
    from fact_key_list fkl 
    left outer join dim_base on dim_base.{{dim_key_column}} = fkl.FOREIGN_KEY
    where dim_base.{{dim_key_column}} is null
),
default_key as (
    SELECT *
    FROM dim_base
    WHERE {{dim_key_column}} = '{{dim_default_key_value}}'
),
dim_missing_entries as (
    SELECT 
        mk.FOREIGN_KEY,
        dk.* EXCLUDE( {{ ref_columns_to_exclude_w_key | join(', ') }} )
        -- {#{ dbt_utils.star(dim_rel, relation_alias='dk', except=ref_columns_to_exclude + [dim_key_column]) }#}
    FROM missing_keys as mk 
    join default_key dk -- on dk.{{dim_key_column}} = '{{dim_default_key_value}}'
),

dim as (
    SELECT * FROM dim_base 
    UNION ALL
    SELECT * FROM dim_missing_entries 
)


SELECT * FROM dim

{% endmacro %}