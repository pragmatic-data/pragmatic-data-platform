{%  macro inout_setup_sql(cfg) %}

{%- set db = cfg.inout.database or target.database %}
{%- set schema = cfg.inout.schema or (target.schema ~ '_LANDING') %}

-- 1. Creation of the schema for the Landing Tables
CREATE SCHEMA IF NOT EXISTS {{ db }}.{{ schema }}
COMMENT = {{ cfg.inout.comment or 'Schema for Landing Tables.'}};


-- 2. Creation of the File Format to read the files for the Landing Tables
{%- if cfg.file_format and cfg.file_format.definition %}
CREATE FILE FORMAT IF NOT EXISTS {{ db }}.{{ schema }}.{{ cfg.file_format.name }}
    {%- for option, value in cfg.file_format.definition.items() %}
    {{option}} = {{value}}
    {%- endfor %}
;
{%- else %}
-- FILE FORMAT or its definition not specified in the Config object provided
{%- endif %}

-- 3. Creation of the Stage holding the files for the Landing Tables
{%- if cfg.stage and cfg.stage.definition %}
CREATE STAGE IF NOT EXISTS {{ db }}.{{ schema }}.{{ cfg.stage.name }}
    {%- if 'FILE_FORMAT' not in cfg.stage.definition or not cfg.stage.definition['FILE_FORMAT'] %}
    FILE_FORMAT = {{ db }}.{{ schema }}.{{ cfg.file_format.name }}
    {%- do cfg.stage.definition.pop('FILE_FORMAT', None) %}
    {%- endif %}
    {%- for option, value in cfg.stage.definition.items() %}
    {{option}} = {{value}}
    {%- endfor %}
;
{%- else %}
-- STAGE or its definition not specified in the Config object provided
{%- endif %}

{%- endmacro %}

{% macro create_file_format(file_format) %}
{%- if file_format and file_format.definition %}
CREATE FILE FORMAT IF NOT EXISTS {{ db }}.{{ schema }}.{{ cfg.file_format.name }}
    {%- for option, value in cfg.file_format.definition.items() %}
    {{option}} = {{value}}
    {%- endfor %}
;
{%- else %}
-- FILE FORMAT or its definition not specified in the Config object provided
{%- endif %}

{% endmacro %}
