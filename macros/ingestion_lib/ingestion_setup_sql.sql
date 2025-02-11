{%  macro ingestion_setup_sql(cfg) %}

{%- set db = cfg.landing.database or target.database %}
{%- set schema = cfg.landing.schema or (target.schema ~ '_LANDING') %}

-- 1. Creation of the schema for the Landing Tables
CREATE SCHEMA IF NOT EXISTS {{ db }}.{{ schema }}
COMMENT = {{ cfg.landing.comment or 'Schema for Landing Tables.'}};


-- 2. Creation of the File Format(s) to read the files for the Landing Tables
{%- if cfg.file_format and cfg.file_format.definition %}
CREATE FILE FORMAT IF NOT EXISTS {{ db }}.{{ schema }}.{{ cfg.file_format.name }}
    {%- for option, value in cfg.file_format.definition.items() %}
    {{option}} = {{value}}
    {%- endfor %}
;
{% elif cfg.file_formats %}
    {%- for file_format in cfg.file_formats.values() %}
        CREATE FILE FORMAT IF NOT EXISTS {{ db }}.{{ schema }}.{{ file_format.name }}
            {%- for option, value in file_format.definition.items() %}
            {{option}} = {{value}}
            {%- endfor %}
        ;
    {% endfor %}
{%- else %}
-- FILE FORMAT or its definition not specified in the Config object provided
{%- endif %}

-- 3. Creation of the Stage(s) holding the files for the Landing Tables
{%- if cfg.stage and cfg.stage.definition %}
CREATE STAGE IF NOT EXISTS {{ db }}.{{ schema }}.{{ cfg.stage.name }}
    {%- if 'file_format' in cfg and ('FILE_FORMAT' not in cfg.stage.definition or not cfg.stage.definition['FILE_FORMAT']) %}
    FILE_FORMAT = {{ db }}.{{ schema }}.{{ cfg.file_format.name }}
    {%- do cfg.stage.definition.pop('FILE_FORMAT', None) %}
    {%- endif %}
    {%- for option, value in cfg.stage.definition.items() %}
    {{option}} = {{value}}
    {%- endfor %}
;
{% elif  cfg.stages %}
    {% for stage in cfg.stages.values() %}
        CREATE STAGE IF NOT EXISTS {{ db }}.{{ schema }}.{{ stage.name }}
            {%- if 'file_format' in cfg and ('FILE_FORMAT' not in stage.definition or not stage.definition['FILE_FORMAT']) %}
                FILE_FORMAT = {{ db }}.{{ schema }}.{{ cfg.file_format.name }}
                {%- do stage.definition.pop('FILE_FORMAT', None) %}
            {% elif 'FILE_FORMAT' in stage.definition %}
                FILE_FORMAT = {{ db }}.{{ schema }}.{{ stage.definition['FILE_FORMAT'] }}
                {%- do stage.definition.pop('FILE_FORMAT', None) %}
            {%- endif %}
            {%- for option, value in stage.definition.items() %}
            {{option}} = {{value}}
            {%- endfor %}
        ;
    {% endfor %}
{%- else %}
-- STAGE or its definition not specified in the Config object provided
{%- endif %}

{%- endmacro %}
