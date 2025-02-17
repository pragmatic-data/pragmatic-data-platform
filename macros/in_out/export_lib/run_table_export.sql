{#  ** Macro to run a default table export script **
    *  This macro receives as input a ref (dbt Relation) and three configuration dictionaries:
    *  - table_ref:         a dbt Relation to the table with the data to export 
    *  - export_path_cfg:   the configuration related to the path and name of the exported files
    *  - stage_cfg:         the configuration related to the stage and file format to write the files
    *  - flags:             the process flags to enable (true) or disable (false or absent) the script's functionalities

    * Sample configuration for this script:
        {% set table_ref = ref('GENERIC_TWO_COLUMN_TABLE') %}    
        {% set yaml_config %}
        export_path_cfg: 
            export_path_base:           SYSTEM_A/generic/
            export_path_date_part:
            export_file_name_prefix:

        stage_cfg:
            format_name: "{{ get_SYSTEM_A_inout_csv_ff_name() }}"
            stage_name:  "{{ get_SYSTEM_A_inout_stage_name() }}"

        flags:
            only_one_export:                true
            remove_folder_before_export:    true
            create_dummy_file:              true
        {% endset %}
        {%- set cfg_dict = fromyaml(yaml_config) -%}
#} 

{% macro run_table_export(
    table_ref,
    export_path_cfg,
    stage_cfg,
    flags
) %}
{% if execute %}

    {% if export_path_cfg.export_path_date_part %}
        {% set export_path_date_part = export_path_cfg.export_path_date_part | replace("-", "_") -%}
    {% else%}
        {% set current_date = modules.datetime.datetime.now() %}
        {% set export_path_date_part = current_date.strftime('%Y-%m-%d') | replace("-", "_") -%}
    {% endif %}

    {% if export_path_cfg.export_path_base %}
        {% set export_path = export_path_cfg.export_path_base ~ export_path_date_part ~ '/' %}
    {% else%}
        {% set export_path_base = table_ref | replace(".", "/") %}
        {% set export_path = export_path_base ~ '/' ~ export_path_date_part ~ '/' %}        
    {% endif %}

    {% set stage_with_export_path = stage_cfg.stage_name ~ '/' ~ export_path %}

    {% if export_path.export_file_name_prefix %}
        {% set export_file_name_prefix = export_path.export_file_name_prefix %}
    {% else%}
        {% set export_file_name_prefix = table_ref.identifier ~ '__' %}
    {% endif %}

    {% if flags.only_one_export and pragmatic_data.check_dummy_exists(stage_cfg.stage_name, export_path) %}
        {{ print('***** dummy file found in '~ stage_with_export_path) }}
        {{ print('***** not doing the export again.') }}
        {% do return(false) %}
    {% endif %}

    {% if flags.remove_folder_before_export %}
        {{ print('* Removing folder ' ~ stage_with_export_path) }}
        {% set results = run_query('REMOVE @' ~ stage_with_export_path) %}
        {{ log('*** Status - Removed: ' ~ results.columns[0].values() | length ~ ' files.', info=True) }}
    {% endif %}

    {{ log('* Exporting data to stage from table ' ~ table_ref ~ '.', true) }}
    {% set results = run_query(pragmatic_data.export_to_stage_sql(
        table_name          = table_ref, 
        stage_with_path     = stage_with_export_path ~ export_file_name_prefix, 
        format_name         = stage_cfg.format_name
    ) ) %}
    {{ log('*** Exported data to stage from table ' ~ table_ref ~ '.', true) }}
    {{ pragmatic_data.log_export_results(results) }}

    {% if flags.create_dummy_file %}
        {% set results = run_query(pragmatic_data.export_dummy_file_sql(stage_with_export_path)) %}
        {{ print('*** Dummy file exported as : ' ~ pragmatic_data.get_dummy_base_name(stage_with_export_path) ~ '...') }}
    {% endif %}

    {{ print('***** DONE Extraction of Table ' ~ table_ref) }}
    {% do return(true) %}

{% endif %}
{% endmacro %}

{% macro log_export_results(results) %}
    {% set export_result_str %}
        {%- if results.column_names|length > 1 -%}
        Exported: {{ results.columns[0].values()[0] }} rows || {{ results.columns[1].values()[0] }}/{{ results.columns[2].values()[0] }} : input/output bytes
        {%- else -%}
        Status: {{ results.columns[0].values()[0]  }}
        {%- endif %}
    {% endset %}
    {{ print('*** ' ~ export_result_str) }}
{% endmacro %}
