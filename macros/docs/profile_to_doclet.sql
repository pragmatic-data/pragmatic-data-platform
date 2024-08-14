{% macro profile_to_doclet(relation) %}

{{- '{% docs dbt_profiler__' + relation.identifier + '  %}' }}
## Profile of {{ relation.identifier }}

Rendered on DB as {{ relation }}
{%- if execute -%}
{{ agate_table_to_markdown( dbt_profiler.get_profile_table(relation=relation) ) }}
{%- endif %}
{{ '{% enddocs %}' }}

{%- endmacro %}