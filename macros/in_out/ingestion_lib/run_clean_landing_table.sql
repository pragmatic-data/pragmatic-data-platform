{% macro run_clean_landing_table(landing_table_dict) %}
{% if execute %}

{%- if not landing_table_dict.get('cleanup') -%}
    {# no cleanup configured — nothing to do #}
{%- else -%}

{%- set fqn = pragmatic_data.landing_table_fqn(landing_table_dict) -%}
{{ log(' Cleaning landing table ' ~ fqn, info=True) }}
{% set result = run_query(pragmatic_data.clean_landing_table_sql(landing_table_dict)) %}
{{ log(' *** Deleted ' ~ result.columns[0].values()[0] ~ ' rows from ' ~ fqn, info=True) }}

{%- endif %}

{% endif %}
{% endmacro %}
