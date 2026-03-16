{%- macro landing_table_fqn(landing_table_dict) -%}
{{- landing_table_dict.db_name -}}.{{- landing_table_dict.schema_name -}}.{{- landing_table_dict.table_name -}}
{%- endmacro -%}
