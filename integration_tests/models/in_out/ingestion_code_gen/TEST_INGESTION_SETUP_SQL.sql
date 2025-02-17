{% set ingestion_cfg %}
landing:                         #-- The DB and Schema config can be also the 'inout:' attribute
    database:  PDP_TARGET_DB     #-- Leave empty or remove to use the DB for the env (target.database)
    schema:     LAND_SAMPLE_XXX
    comment:    "'Landing table schema for CSV files from SYSTEM SAMPLE_XXX.'"

file_format:
    name: SAMPLE_XXX_CSV__FF
    definition:
        TYPE: "'CSV'" 
        SKIP_HEADER: 1              #-- Set to 0 when we have more than one in each file
        FIELD_DELIMITER: "','"
        FIELD_OPTIONALLY_ENCLOSED_BY: "'\\042'"      #-- '\042' double quote
        COMPRESSION: "'AUTO'" 
        ERROR_ON_COLUMN_COUNT_MISMATCH: TRUE 

stage:
    name: SAMPLE_XXX_CSV__STAGE
    definition:
        DIRECTORY: ( ENABLE = true )
        COMMENT: "'Stage for CSV files from SAMPLE_XXX.'"
        # FILE_FORMAT:                    #-- leave empty (or remove) to use the FF from the stage
{% endset %}

SELECT 
$${{ pragmatic_data.inout_setup_sql( cfg = fromyaml(ingestion_cfg) ) }}$$ as generated_sql
