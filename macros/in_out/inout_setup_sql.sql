{%  macro inout_setup_sql(cfg) %}

-- 1. Creation of the schema for the Landing Tables
CREATE SCHEMA IF NOT EXISTS {{ pragmatic_data.get_inout_db(cfg.inout) }}.{{ pragmatic_data.get_inout_schema(cfg.inout) }}
COMMENT = {{ cfg.inout.comment or 'Schema for Landing Tables.'}};


-- 2. Creation of the File Format to read the files for the Landing Tables
{{ pragmatic_data.create_file_format(cfg.file_format, cfg.inout) }}

-- 3. Creation of the Stage holding the files for the Landing Tables
{{ pragmatic_data.create_stage(cfg.stage, cfg.file_format, cfg.inout) }}

{%- endmacro %}


{% macro get_inout_db(inout = none) %}
    {% do return(inout.database or target.database) %}
{% endmacro %}

{% macro get_inout_schema(inout = none) %}
    {% do return(inout.schema or (target.schema ~ '_LANDING')) %}
{% endmacro %}

{% macro get_inout_fq_schema(inout = none) %}
    {% do return(pragmatic_data.get_inout_db(inout) ~'.'~ pragmatic_data.get_inout_schema(inout)) %}
{% endmacro %}

{% macro get_inout_fq_file_format_name(file_format_name, inout = none) %}
    {% set file_format_name = file_format_name or (target.schema ~ '_FF') %}
    {% if '.' in file_format_name %}
        {% do return(file_format_name ) %}
    {% else %}
        {% do return( pragmatic_data.get_inout_fq_schema(inout) ~'.'~ file_format_name ) %}
    {% endif %}
{% endmacro %}

{% macro get_inout_fq_stage_name(stage_name, inout = none) %}
    {% set stage_name = stage_name or (target.schema ~ '_STAGE') %}
    {% if '.' in stage_name %}
        {% do return(stage_name ) %}
    {% else %}
        {% do return( pragmatic_data.get_inout_fq_schema(inout) ~'.'~ stage_name ) %}
    {% endif %}
{% endmacro %}


{% macro create_file_format(file_format, inout = none) -%}
    {%- if file_format and file_format.definition %}
        {%- set fq_file_format_name = file_format.fq_name or pragmatic_data.get_inout_fq_file_format_name(file_format.name, inout) %}
CREATE FILE FORMAT IF NOT EXISTS {{ fq_file_format_name }}
        {%- for option, value in file_format.definition.items() %}
            {%- if option and value %}
    {{option}} = {{value}}        
            {%- endif %}
        {%- endfor %}
;
    {%- else %}
        -- FILE FORMAT or its definition not specified in the Config object provided
        {{ exceptions.raise_compiler_error("Missing or invalid file format configuration. Got: " ~ file_format | pprint) }}
    {%- endif %}
{% endmacro %}

{% macro create_stage(stage, file_format = none, inout = none) %}

    {%- if stage and stage.definition %}
        {%- set fq_stage_name = stage.fq_name or pragmatic_data.get_inout_fq_stage_name(stage.name, inout) %}

        {%- set stage_file_format = stage.definition.FILE_FORMAT or stage.definition['FILE_FORMAT'] %}
        {%- if stage_file_format %}
            {%- set stage_fq_file_format = stage_file_format if '.' in stage_file_format 
                                                            else pragmatic_data.get_inout_fq_file_format_name(stage_file_format, inout)  %}
            {%- do stage.definition.update({'FILE_FORMAT': stage_fq_file_format}) %}
        {%- elif file_format %}
            {%- do stage.definition.update({'FILE_FORMAT': file_format.fq_name or pragmatic_data.get_inout_fq_file_format_name(file_format.name, inout)}) %}
        {%- endif %}
CREATE STAGE IF NOT EXISTS {{ fq_stage_name }}
        {%- for option, value in stage.definition.items() %}
            {%- if option and value %}
        {{option}} = {{value}}        
            {%- endif %}
        {%- endfor %}
;
    {%- else %}
        -- STAGE or its definition not specified in the Config object provided
        {{ exceptions.raise_compiler_error("Missing or invalid stage configuration. Got: " ~ stage | pprint) }}
    {%- endif %}

{% endmacro %}
