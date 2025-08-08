# Ingestion and Export layer
Welcome to the **ingestion layer** of the Pragmatic Data Platform (PDP) package.

**Table of Contents**
- [General ingestion and export process](#general-ingestion-and-export-process)
- [Ingestion and Export Playbook](#ingestion-and-export-playbook)
- [Ingestion and Export Setup](#ingestion-and-export-setup)
- Ingestion >> [Landing Tables Macros](ingestion_lib/README.md#landing-tables-macros)
- Export >> [Export Macros](export_lib/README.md)

## General ingestion and export process
Ingeston and Export are specular operations on files, one loading data from files into tables and 
the other writing data from tables into files.

By working on files they both need to setup the DB objects for File Formats and Stages into a DB and Schema.
Also the operations to read or write the data are similar, both using the COPY INTO command.

For this reason the library macros are bundeld together under a common `in_out` folder in this package 
and we suggest to keep together the ingestion/export macros in your projects, grouped by source system, 
under a common folder named `ingestion`, `export` or `in_out` depending if you have only ingestion, export or both.


The Pragmatic Data Platform can easily ingest from and export to CSV and SEMI-STRUCUTRED files in internal or external stages, 
allowing you to ingest and export files from anywhere your Snowflake account is authorized to read.

## Ingestion and export playbook
The playbook to ingest data from files or export data to files is the following:
1. create a setup file to define names and the shared DB objects (schema, file format, stage)  
   This is explained in the [Ingestion Setup](#ingestion-and-export-setup) section later inthis file.

2. create a .sql file with the YAML config or SQL code for each table to be ingested or exported  
   A. for ingestion read the [Landing Tables Macros](ingestion_lib/README.md#landing-tables-macros) section  
   B. for export read the [Export Macros](export_lib/README.md) section

We suggest to create a separate folder in your dbt project for each source system or domain that you want
to ingest data from or export data to. This allows you to create a single setup file for each system and 
separate folders for ingestion and export, even if it is not common to have both for the same system/domain.  

This is consistent with the fact that usually all files exported from one system 
are extracted to a single location, with the same file format and you generally 
want to put all the landing tables in the same DB schema.

The suggested file organization in your project looks like this:
```
/in_out/                        - the base folder for ingestion and export macros, to be added to the macro path
  /system_xxx/                  - the folder to hold everything about a system / domain
    /system_xxx__setup.sql              - the file with the setup and naming macros for in and out
    /ingest/                            - a folder for ingestion macros from the system or domain
        /system_xxx__table_xyz.sql      - a file with the macro to ingest the individual Landing Table
    /export/                            - a folder for export macros for the system or domain
        /export_abc.sql                 - a file with the macro to export the individual table/view to a set of files
    ...
```


For the general process to start ingesting data into landing tables, look at the 
[Ingestion Macros](#ingestion-macros) section in this page.

For details on the parameters, the format of the dictionaries and basic examples of use
of the macros in this section, please look at the individual macro definition below in this file.

For extended examples of use you can look in the [ingestion](integration_tests/models/ingestion) folder 
in the Integration Tests part of this repository.

## Ingestion and Export Setup
To do the one time setup needed to start ingesting files we use the `ingestion_setup_sql()` macro,
coupled with a YAML block to easily write the configuration to be passed to the macro.

This is encapsulated into a small setup macro, that is run before the ingestion macros to make sure that
the correct setup is in place before we go through processing each Landing Table.

The same setup file usually contains also other one time setup needs like names, 
so that you can define it here once and reuse them everywhere.

All these macros and names usually contain the name of the source system where the data originates,
in this case SOURCE_XXX, to keep the names unique while allowing to ingest files from multiple systems
with different setup needs and putting the LTs in different places. 

The setup file looks like this:
```
/* **Provide the configuration to set up the schema, file format and stage**
 * This macro also converts the YAML into a set of nested Pythion dictionaries.
 */
{% macro get_SOURCE_XXX_ingestion_cfg() %}
{% set ingestion_cfg %}
landing:
    #database:   "{{ target.database }}"     #-- Leave empty or remove to use the DB for the env (target.database)
    schema:     LAND_SOURCE_XXX
    comment:    "'Landing table schema for CSV files from SYSTEM SOURCE_XXX.'"

file_format:
    name: SOURCE_XXX_CSV__FF
    definition:
        TYPE: "'CSV'"                               #-- note the double quotes (for YAML) around the single quotes needed for Snowflake
        SKIP_HEADER: 1                              #-- Set to 0 and handle afterwards, when we have more than one in each file
        FIELD_DELIMITER: "','"                      
        FIELD_OPTIONALLY_ENCLOSED_BY: "'\\042'"     #-- '\042' double quote
        COMPRESSION: "'AUTO'" 
        ERROR_ON_COLUMN_COUNT_MISMATCH: TRUE
        #-- EMPTY_FIELD_AS_NULL: TRUE               #-- sometimes you need this
        #--NULL_IF: ('', '\\N')                     #-- sometimes you need this too
        #-- ENCODING: "'ISO-8859-1'"                #-- For nordic languages

stage:
    name: SOURCE_XXX_CSV__STAGE
    definition:
        DIRECTORY: ( ENABLE = true )
        COMMENT: "'Stage for CSV files from SOURCE_XXX.'"
        # FILE_FORMAT:                    #-- leave empty (or remove) to use the FF from the stage
{% endset %}

{{ return(fromyaml(ingestion_cfg)) }}
{% endmacro %}

/* **Generate the SQL to set up the schema, file format and stage**
 * We keep in a separated macro so it's easy to inspect :)
 */
{%  macro get_SOURCE_XXX_ingestion_setup_sql() %}
  {% do return(ingestion_setup_sql(cfg = get_SOURCE_XXX_ingestion_cfg())) %}
{%- endmacro %}

/* ** Run the SQL to set up the schema, file format and stage** */
{%  macro run_SOURCE_XXX_ingestion_setup() %}
    {{ log('Setting up landing table schema, file format and stage for schema: '  ~ get_SOURCE_XXX_ingestion_schema_name() ~ ' .', true) }}
    {% do run_query(get_SOURCE_XXX_ingestion_setup_sql()) %}
    {{ log('Setup completed for schema: '  ~ get_SOURCE_XXX_ingestion_schema_name() ~ ' .', true) }} 
{%- endmacro %}


/* DEFINE Names or get them from the running environment (target.xxx)  */ 
{%  macro get_SOURCE_XXX_ingestion_db_name( cfg = get_SOURCE_XXX_ingestion_cfg() ) %}
  {% do return( cfg.landing.database  or target.database ) %}
{%- endmacro %}

{%  macro get_SOURCE_XXX_ingestion_schema_name( cfg = get_SOURCE_XXX_ingestion_cfg() ) %}
  {% do return( cfg.landing.schema or target.schema) %}
{%- endmacro %}

{%  macro get_SOURCE_XXX_ingestion_csv_ff_name( cfg = get_SOURCE_XXX_ingestion_cfg() ) %}  -- return fully qualified name
  {% do return( get_SOURCE_XXX_ingestion_db_name() ~ '.' ~ get_SOURCE_XXX_ingestion_schema_name() ~  '.' ~ cfg.file_format.name ) %}
{%- endmacro %}

{%  macro get_SOURCE_XXX_ingestion_stage_name( cfg = get_SOURCE_XXX_ingestion_cfg() ) %}    -- return fully qualified name
  {% do return( get_SOURCE_XXX_ingestion_db_name() ~ '.' ~ get_SOURCE_XXX_ingestion_schema_name() ~  '.' ~ cfg.stage.name ) %}
{%- endmacro %}
```
As you can see the setup is quite straightforward, feeding into the YAML what you need to pass on to Snwqflake

The alternative is to directly implement your own macro with code similar to the one in the `ingestion_setup_sql()` macro.
