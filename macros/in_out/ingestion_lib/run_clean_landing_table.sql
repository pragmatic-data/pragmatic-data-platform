{% macro run_clean_landing_table(landing_table_dict) %}
{% if execute %}

{%- set fqn = pragmatic_data.landing_table_fqn(landing_table_dict) -%}

{%- if landing_table_dict.get('cleanup') -%}

{{ log(' Cleaning landing table ' ~ fqn, info=True) }}
{% set result = run_query(pragmatic_data.clean_landing_table_sql(landing_table_dict)) %}
{{ log(' *** Deleted ' ~ result.columns[0].values()[0] ~ ' rows from ' ~ fqn, info=True) }}

{%- else -%}

{{ log(' No cleanup configured for landing table ' ~ fqn ~ ' — skipping.', info=True) }}

{%- endif %}

{% endif %}
{% endmacro %}
