{% set ingestion_cfg %}
landing:                         #-- The DB and Schema config can be also the 'inout:' attribute
    database:  PDP_TARGET_DB     #-- Leave empty or remove to use the DB for the env (target.database)
    schema:     LAND_SAMPLE_XXX
    comment:    "'Landing table schema for CSV files from SYSTEM SAMPLE_XXX.'"

file_formats:
    CSV_files:
        name: SAMPLE_XXX_CSV__FF
        definition:
            TYPE: "'CSV'" 
            SKIP_HEADER: 1              #-- Set to 0 when we have more than one in each file
            FIELD_DELIMITER: "','"
            FIELD_OPTIONALLY_ENCLOSED_BY: "'\\042'"      #-- '\042' double quote
            COMPRESSION: "'AUTO'" 
            ERROR_ON_COLUMN_COUNT_MISMATCH: TRUE 

    JSON_file_format:
        name: MY_JSON__FF
        definition:
            TYPE: "'JSON'"
            REPLACE_INVALID_CHARACTERS: true

    SAMPLE_PARQUET__FF:
        name: MY_PARQUET__FF
        definition:
            TYPE: "'Parquet'"
            REPLACE_INVALID_CHARACTERS: true


stages:
    stage_1:
        name: SAMPLE_XXX_CSV__STAGE
        definition:
            DIRECTORY: ( ENABLE = true )
            COMMENT: "'Stage to read/write CSV files from SAMPLE_XXX.'"
            FILE_FORMAT: SAMPLE_XXX_CSV__FF                   #-- FF name needed when using multiple FF
            STORAGE_INTEGRATION: default_storage_integration
            URL: "'<provider>://<root_url>/<folder_path>'"
    stage_2:
        name: JSON__STAGE
        definition:
            DIRECTORY: ( ENABLE = true )
            COMMENT: "'Stage to read/write JSON files.'"
            FILE_FORMAT: MY_JSON__FF
            STORAGE_INTEGRATION: default_storage_integration
            URL: "'<provider>://<root_url>/<folder_path>'"

    stage_3:
        name: Parquet_01__STAGE
        definition:
            DIRECTORY: ( ENABLE = true )
            COMMENT: "'Stage to read/write Parquet files.'"
            FILE_FORMAT: SAMPLE_PARQUET__FF
            STORAGE_INTEGRATION: default_storage_integration
            URL: "'<provider>://<root_url>/<folder_path>'"

    stage_4:
        name: Parquet_02__STAGE
        definition:
            DIRECTORY: ( ENABLE = true )
            COMMENT: "'Another stage to read/write Parquet files.'"
            STORAGE_INTEGRATION: default_storage_integration
            URL: "'<provider>://<root_url>/<another_folder_path>'"
            # -- Stage without FF - No default FF when there are multiple FF

{% endset %}

SELECT 
$${{ pragmatic_data.inout_setup_sql( cfg = fromyaml(ingestion_cfg) ) }}$$ as generated_sql
