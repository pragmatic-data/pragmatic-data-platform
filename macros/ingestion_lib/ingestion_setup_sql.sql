{%  macro ingestion_setup_sql(cfg) %}

{%- set db = cfg.landing.database or target.database %}
{%- set schema = cfg.landing.schema or (target.schema ~ '_LANDING') %}

-- 1. Creation of the schema for the Landing Tables
    CREATE SCHEMA IF NOT EXISTS {{ db }}.{{ schema }}
    COMMENT = {{ cfg.landing.comment or 'Schema for Landing Tables.'}};


-- 2. Creation of the File Format(s) to read the files for the Landing Tables
{%- if cfg.file_format and cfg.file_format.definition %}
    {{- pragmatic_data.create_file_format(cfg.file_format, db, schema) }}
{%- elif cfg.file_formats %}
    {%- for file_format in cfg.file_formats.values() %}
        {{- pragmatic_data.create_file_format(file_format, db, schema) }}
    {% endfor %}
{%- else %}
-- FILE FORMAT or its definition not specified in the Config object provided
{%- endif %}

-- 3. Creation of the Stage(s) holding the files for the Landing Tables
{%- if cfg.stage and cfg.stage.definition %}
{{- pragmatic_data.create_stage(cfg.stage, db, schema, cfg.file_format.name if cfg.file_format) }}
{%- elif  cfg.stages %}
    {% for stage in cfg.stages.values() %}
        {{- pragmatic_data.create_stage(stage, db, schema, cfg.file_format.name if cfg.file_format) }}
    {% endfor %}
{%- else %}
-- STAGE or its definition not specified in the Config object provided
{%- endif %}

{%- endmacro %}


{%- macro create_file_format(
    file_format_cfg, 
    db = target.database, 
    schema = (target.schema ~ '_LANDING')
) %}
    CREATE FILE FORMAT IF NOT EXISTS {{ db }}.{{ schema }}.{{ file_format_cfg.name }}
        {%- for option, value in file_format_cfg.definition.items() %}
        {{option}} = {{value}}
        {%- endfor %}
    ;
{%- endmacro %}

{%- macro create_stage(
    stage_cfg,
    db = target.database, 
    schema = (target.schema ~ '_LANDING'),
    default_file_format_name = none
) %}
    CREATE STAGE IF NOT EXISTS {{ db }}.{{ schema }}.{{ stage_cfg.name }}
        {%- if default_file_format_name and ('FILE_FORMAT' not in stage_cfg.definition or not stage_cfg.definition['FILE_FORMAT']) %}
            FILE_FORMAT = {{ db }}.{{ schema }}.{{ default_file_format_name }}
            {%- do stage_cfg.definition.pop('FILE_FORMAT', None) %}
        {%- endif %}
        {%- for option, value in stage_cfg.definition.items() %}
        {{option}} = {{value}}
        {%- endfor %}
    ;
{%- endmacro %}
