{%- macro landing_table_fqn(landing_table_dict) -%}

{%- set db     = landing_table_dict.get('db_name')     or target.database -%}
{%- set schema = landing_table_dict.get('schema_name') -%}
{%- set table  = landing_table_dict.get('table_name')  -%}

{%- if not schema -%}
    {{- exceptions.raise_compiler_error("landing_table_fqn: 'schema_name' is required in landing_table_dict") -}}
{%- endif -%}
{%- if not table -%}
    {{- exceptions.raise_compiler_error("landing_table_fqn: 'table_name' is required in landing_table_dict") -}}
{%- endif -%}

{%- do return(db ~ '.' ~ schema ~ '.' ~ table) -%}

{%- endmacro -%}
