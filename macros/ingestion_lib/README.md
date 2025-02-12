# PDP Ingestion layer 
Welcome to the **ingestion layer** of the Pragmatic Data Platform (PDP) package.

**Table of Contents**
- [General ingestion process](#general-ingestion-process)
- [Ingestion Setup](#ingestion-setup)
- [Landing Tables Macros](#landing-tables-macros)
  - [Current, YAML based Landing Table ingestion](#current-yaml-based-landing-table-ingestion)
  - [Original (legacy), SQL based Landing Table ingestion](#original-legacy-sql-based-landing-table-ingestion)
- [Ingestion Process Macros](#ingestion-process-macros)
- [Ingestion Base Macros](#ingestion-base-macros)

## General ingestion process
The Pragmatic Data Platform can easily ingest CSV and SEMI-STRUCUTRED files located in internal or external stages, 
allowing you to ingest files from anywhere your Snowflake account is authorized to read.

Ingestion of files into Landing Tables in the PDP is based on three operations:
1. creation fo the shared DB objects (schema, file format and stage), if they not exists
2. creation of the landing table, if not exists
3. ingestion of all the new files since the last ingestion

The playbook to ingest files is therefore the following:
1. create a setup file to define names and the shared DB objects (schema, file format, stage)  
   This is explained in the [Ingestion Setup](#ingestion-setup) section
2. create an ingestion file for each landing table  
   This is explained in the [Landing Tables Macros](#landing-tables-macros) section

We suggest to create a separate folder in your dbt project for each source system you want
to ingest data from, creating one setup file for each. 
This is consistent with the fact that usually all files exported from one system 
are extracted to a single location, with the same file format and you generally 
want to put all the landing tables in the same DB schema.

For the general process to start ingesting data into landing tables, look at the 
[Ingestion Macros](#ingestion-macros) section in this page.

For details on the parameters, the format of the dictionaries and basic examples of use
of the macros in this section, please look at the individual macro definition below in this file.

For extended examples of use you can look in the [ingestion](integration_tests/models/ingestion) folder 
in the Integration Tests part of this repository.

## Ingestion Setup
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


## Landing Tables Macros
The Base Macros found in this package are the evolution of the 
original macros presented in my first book "Data Engineering with dbt".

They generate the SQL code to perform the two operations required during ingestion: 
1. Landing Table creation, using a CREATE TABLE command
2. Ingestion of data from files, using the COPY INTO command

Originally, to automate the ingestion operations, two base macros (the create table 
plus one ingestion) were used to create an ingestion macro for evey desired Landing Table.
The process was simple and quick, as all the ingestion macro had the same code, 
with the only differences being in the parameter definition. 
The downside being the creation of a lod of copy pasted code. Not DRY at all.

With the evolution of the PDP the process macros have been introduced to solve this issue.
They implement the common way of doing data ingestion and remove pretty much all code duplication.
Now we can create the ingestion macro for each LT with very DRY code that only defines 
the required metadata in YAML format and then calls one of the process macros.

The base macros are still used inside the process macros and they are available 
to allow you to reshape the ingestion to cater for any special use case.


### Current, YAML based Landing Table ingestion
The current way to create a RAW_ORDERS Landing Table is exemplified in the following piece of code.  

First we create one .sql file to create and ingest data in one LT.
I suggest to name it like the LT (`RAW_ORDERS.sql`) and to place it inside some subfolder 
of the `macro` folder or -better- inside an `ingestion` folder added to the macro paths.

Then in that file we define a macro where we first enter the configuration as YAML 
and call the process macro passing the configuration (`run_CSV_ingestion`in this case).
The suggestion is to call the macro after the LT, like `load_RAW_ORDERS` in the example.

The example shows that we define two blocks inside the YAML, one to define what files to ingest
and one to define the landing table.
We then convert the YAML string into a Python dictionary and finally 
we pass the two parts of the YAML config to the `run_CSV_ingestion()` macro.

```
{% macro load_RAW_ORDERS(recreate_table = false) %}

{%- set yaml_str -%}
ingestion:
    pattern: '.*/raw_orders/.*/RAW_ORDERS.*[.]csv.gz'
    stage_name: "{{ get_SOURCE_XXX_stage_fq_name() }}"
    format_name:

landing_table:
    db_name:     "{{ get_SOURCE_XXX_ingestion_db_name() }}"
    schema_name: "{{ get_SOURCE_XXX_ingestion_schema_name() }}"
    table_name:  RAW_ORDERS
    columns: #-- Define the landing table columns. Same syntax as in Create Table
        - ORDERKEY: NUMBER NOT NULL
        - CUSTOMERKEY: NUMBER
        - ORDERSTATUS: TEXT
        - TOTALPRICE: NUMBER
        - ORDERDATE: DATE
        - ORDERPRIORITY: TEXT
        - CLERK: TEXT
        ...
{%- endset -%}

{%- set metadata_dict = fromyaml(yaml_str) -%}

{% do run_CSV_ingestion(
        landing_table_dict = metadata_dict['landing_table'],
        ingestion_dict  = metadata_dict['ingestion'],
        recreate_table = recreate_table
) %}

{% endmacro %}
```

Please note that to keep the config DRY we have used some simple getter macros 
to abstract away the definition of the database, schema and stage names.

### Original (legacy), SQL based Landing Table ingestion
The original way to perform the ingestion was similar in principle, 
but based on two manually written (copy-pasted) macros in a single file for each Landing Table.

As the example below clarly shows, the configuration metadata is the same, 
but instead of being entered as YAML it is entered as the macro parameters.

```
{%  macro load_RAW_ORDERS(

        db_name     = get_landing_db_name(),
        schema_name = get_landing_schema_name(),
        stage_name  = get_stage_fq_name(),
        format_name = get_csv_file_format_fq_name(),
        table_name  = 'RAW_ORDERS',
        pattern     = '.*/raw_orders/.*/RAW_ORDERS.*[.]csv.gz',
        field_count = 7

) %}

    {% set full_table_name = db_name ~ '.' ~ schema_name ~ '.' ~ table_name %}

    {{ log('Creating table ' ~ full_table_name ~ ' if not exists', true) }}
    {% do run_query(RAW_ORDERS_create_table_sql(full_table_name)) %}
    {{ log('Created table '  ~ full_table_name ~ ' if not exists', true) }}


    {{ log('Ingesting data in table ' ~ full_table_name ~ '.', true) }}
    {% do run_query(ingest_into_landing_sql(
            full_table_name, 
            field_count         = field_count, 
            file_pattern        = pattern,
            full_stage_name     = stage_name, 
            full_format_name    = format_name 
        ) ) %}
    {{ log('Ingested data in table ' ~ full_table_name ~ '.', true) }}

{%- endmacro %}

{% macro RAW_ORDERS_create_table_sql(full_table_name) %}

    -- ** Create table if not exists
    CREATE TRANSIENT TABLE {{ full_table_name }} IF NOT EXISTS
    (
        ORDERKEY NUMBER NOT NULL,
        CUSTOMERKEY NUMBER,
        ORDERSTATUS TEXT,
        TOTALPRICE NUMBER,
        ORDERDATE DATE,
        ORDERPRIORITY TEXT,
        CLERK TEXT,

        -- metadata
        FROM_FILE string,
        FILE_ROW_NUMBER integer,
        INGESTION_TS_UTC TIMESTAMP_NTZ(9)
    )
    COMMENT = '...';

{%- endmacro %}
```

## Ingestion Process Macros
The original Pragmatic Data Platform playbook was to use the above base macros to create one macro
that went through the process to ingest the desired data for each Landing Table, as explained in 
[the SQL based Landing Table ingestion section above](#original-legacy-sql-based-landing-table-ingestion).

All these macros went through the same motions to ingest the data, creating a lot of repetitions.
To reduce repetitions from copy-paste and to simplyfy even more the usage of the ingestion in the Pragmatic Data Platform
we have crystallized the most common way to use the base macro in the following two macros (one for CSV,
one for SEMI-STRUCTURED formats) that take the input needed for the base macros and automate the ingestion process.
The input is passed in two dictionaries that describe the Landing Table and the files to load.

#### run_CSV_ingestion(...)
Creates one landing table -if not exists- and ingests the data from the designated files.
This macro is a wrapper that executes the Base Macros and logs their output.

#### run_SEMI_STRUCT_ingestion(...)
Creates one landing table -if not exists- and ingests the data from the designated files.
This macro is a wrapper that executes the Base Macros and logs their output.


## Ingestion Base Macros
The following macros are the evolution of the original macros from my book,
and they are still used to generate the actual code to perform the ingestion.

#### create_landing_table_sql(...)
By passing a dictionary with the table name components (db, schema and name)
and the column definition (name and eventual type & not null contraint)
the macro produces the SQL code to create -if not exists already-
or recreate -if forced by the `recreate_table` parameter-
the Landing Table in the desired position and shape.

#### ingest_into_landing_sql(...)
Creates the COPY INTO statement to ingest the desired CSV files into the designated Landing Table.
The CSV specific feature is that we just need the number of columns, but not their names.

#### ingest_semi_structured_into_landing_sql(...)
Creates the COPY INTO statement to ingest the desired SEMI-STRUCTURED files into the designated Landing Table.
The SEMI-STRUCTURED specific feature is that in the `field_definitions`parameter
we need the name of the target columns and the expression to extrat each from the $1 variant column.

