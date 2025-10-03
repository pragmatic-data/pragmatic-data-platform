# Ingestion and Export
Welcome to the **ingestion and export** macros of the Pragmatic Data Platform (PDP) package.

You can see ingestion as an optional part of the Storage layer, or as its own layer before Storage.
Similarly, export can be considered part of the Delivery layer or an independent step after it.
In any case they are built using YAML and dbt macros, and they are activated with `dbt run-operation`
commands, so they work a little different from the other layers built on usual dbt models.

## **Table of Contents**
- [Macros](#macros)
   - [`inout_setup_sql()`](#inout_setup_sql)
   - [`run_csv_ingestion()`](#run_csv_ingestion)
   - [`run_semi_structured_ingestion()`](#run_semi_structured_ingestion)
   - [`run_table_export()`](#run_table_export)

- [General ingestion and export process](#general-ingestion-and-export-process)
- [Ingestion and Export Playbook](#ingestion-and-export-playbook)
- [Ingestion and Export Setup File](#ingestion-and-export-setup-file)
- [Landing Table ingestion file](#landing-table-ingestion-file)
  - [Load All tables macro](#load-all-tables-macro)

<!-- 
- Export >> [Export Macros](export_lib/README.md)
-->

## Macros

### `inout_setup_sql()`

This macro generates the SQL statements required to create the necessary database objects 
for ingestion or export operations:
- **schema**: the schema that will host the file format, stage and landing tables. 
  A key part of the design is to decide in what database to put it for each environment.
- **file format**(s): one or more file formats used to read the files to be ingested.
- **stage**(s): a stage to provide access to the files to be ingested.

It is used in the [Ingestion and Export Setup File](#ingestion-and-export-setup-file) to convert
the declarative configuration in YAML into SQL that can be executed to create the required database objects.

**Arguments:**  
It gets a single `cfg` (dict) argument: a dictionary, usually expressed in YAML, containing the 
configuration for the database objects to be created.
It expects keys for: 
- `landing` (or `export` or `inout`): the `database` and `schema` names, as well as an optional `comment`.  
  The `database` is optional, defaulting to the environment's db.
- `file_format`(s): a `name` and a `definition` as required by Snowflake.
- `stage`(s): a `name` and a `definition` as required by Snowflake.   
  Usually defers to a STORAGE INTEGRATION to provide authentication and authorization.

**Usage:**
```YAML
{% macro get_SOURCE_XXX_ingestion_cfg() %}  #-- Example CONFIG, directly as a dict.
  {% set config_dict = {
    'landing': {'schema': 'LAND_SOURCE_XXX', 'comment': 'Landing schema'},
    'file_format': {'name': 'MY_CSV_FF', 'definition': {'TYPE': "'CSV'", 'SKIP_HEADER': 1}},
    'stage': {'name': 'MY_CSV_STAGE', 'definition': {'DIRECTORY': '( ENABLE = true )'}}
  } %}
  {% do return(config_dict) %}
{%- endmacro %}

{% macro run_SOURCE_XXX_ingestion_setup_sql() %}  #-- using the **inout_setup_sql** macro
  {% do run_query(pragmatic_data.inout_setup_sql(cfg = get_SOURCE_XXX_ingestion_cfg())) %}
{%- endmacro %}
```
The above example uses the **`inout_setup_sql`** macro to generate the SQL for the setup and 
runs it passing its output to the `run_query()` macro. This is fine for exemplification. 

In our setup we prefer to be able to easily inspect the SQL and we do it in two steps:
one macro generates the SQL (`get_...sql()`) and another one runs its output (`run_...sql()`)

For more detailed examples, including the YAML format of the configuration and the helper macros for 
the full setup, see the [Ingestion and Export Setup File](#ingestion-and-export-setup-file) section,
later in this file.

---

### `run_csv_ingestion()`

This macro orchestrates the entire process of ingesting data from CSV files into a specified landing table.  
It handles table creation and the `COPY INTO` operation.

**Arguments:**

- `landing_table_dict` (dict): A dictionary describing the target landing table, 
  including its database, schema, name, and column definitions.

- `ingestion_dict` (dict): A dictionary containing ingestion parameters like 
  the file `pattern`, `stage_name`, and `format_name`.

- `recreate_table` (bool, optional): If `true`, the landing table will be dropped 
  and recreated before ingestion. Defaults to `false`.


**Usage:**
```YAML
{%- set ingestion_cfg -%}
ingestion:
   pattern: 'cash_transactions/.*CashTransactions.*[.]csv.gz'   # ** 1 ** 
   stage_name: "{{ get_SOURCE_XXX_ingestion_stage_name() }}"
   format_name:

landing_table:
   db_name:     "{{ get_SOURCE_XXX_ingestion_db_name() }}"
   schema_name: "{{ get_SOURCE_XXX_ingestion_schema_name() }}"
   table_name:  Cash_Transactions   # ** 2 **
   columns:                         # ** 3 **
      - ClientAccountID                       #-- No data type specification means TEXT
      - AccountAlias
      - FXRateToBase: Number(38, 5)           #-- with data type specification 
      . . . more columns
{%- endset -%}

{%- set cfg = fromyaml(ingestion_cfg) -%}

{{ pragmatic_data.run_csv_ingestion(
    landing_table_dict = cfg.landing_table,
    ingestion_dict     = cfg.ingestion,
    recreate_table     = false
) }}
```
The above example illustrates how you can easily provide the required parameter to ingest data in a landing
table in a declarative way using YAML.

The example also highlights that most values can be set once and for all the Landing Tables of a source system,
using the macros created in the setup, with **only THREE parameters** having to be **specific for each LT**:
- the name of the landing table itself
- the pattern to select the files to be loaded in the LT
- the list of columns in the LT.  
  This is generally generated by SQL or using the header of the CSV file.

It is -in fact- always possible to use the alternative representation as a dictionary.
The following example also illustrates how you can pass columns with and without data type specification. 
```YAML
{% set landing_config = {
    'db_name': 'my_db',
    'schema_name': 'my_schema',
    'table_name': 'my_landing_table',
    'columns': [ 'col1', 'col2', {'col3': 'NUMBER(38, 5)'}]    
    #-- columns is a list of column names (data type = TEXT) or dicts {col_name: data_type} 
} %}
```

For other examples, including the legacy SQL based ingestion macros, 
refer to the [Landing Tables Macros documentation](ingestion_lib/README.md#landing-tables-macros).

---

### `run_semi_structured_ingestion()`

This macro is similar to `run_csv_ingestion` but is designed for semi-structured data like JSON or Parquet.  
It handles table creation and the `COPY INTO` operation, but unlike the CSV version, it requires you to 
provide the expressions to handle field extraction during the `COPY INTO` process.

**Arguments:**

- `landing_table_dict` (dict): A dictionary describing the target landing table.  
  Exactly the same as in [`run_csv_ingestion()`](#run_csv_ingestion).

- `ingestion_dict` (dict): A dictionary containing ingestion parameters. Similar to its CSV equivalent, 
  but must include `field_expressions` to map data from the semi-structured files to the table columns.

- `recreate_table` (bool, optional): If `true`, the landing table will be dropped and recreated.   
  Defaults to `false`. Exactly the same as in [`run_csv_ingestion()`](#run_csv_ingestion).


**Usage:**

```YAML
{%- set ingestion_cfg -%}
ingestion:
   pattern: 'cash_transactions/.*CashTransactions.*[.]csv.gz'   # ** 1 ** 
   stage_name: "{{ get_SOURCE_XXX_ingestion_stage_name() }}"
   format_name:
   field_expressions:   # ** 4 **
      # - src_data: $1     #-- This would bring the full source record as a Variant column
      - ClientAccountID: $1:ClientAccountID::string      #-- Get the ClientAccountID as a string
      - AccountAlias: $1:AccountAlias::string
      - FXRateToBase: $1:FXRateToBase::Number(38, 5)     #-- Get the FXRateToBase as a Number
      
landing_table:
      . . . same as in run_csv_ingestion() example above
{%- endset -%}

{%- set cfg = fromyaml(ingestion_cfg) -%}

{{ pragmatic_data.run_semi_structured_ingestion(
    landing_table_dict  = cfg.landing_table,
    ingestion_dict      = cfg.ingestion,
    recreate_table      = false
) }}
```
The above example shows how ingesting semi-structured files is almost as simple as ingesting CSVs,
with only four parameters being Landing Table specific. The fourth is the list of column expressions. 

The extra (little) effort of having to list the desired columns and the expressions to get them,
is balanced by the great benefit -at least for Parquet and Avro- of stable data types and validated content.

The expressions to define the column can be generated by a SQL query using the INFER_SCHEMA function:
```SQL
-- Produce the list of columns for LT ingestion macro in the form " - col1: expression"
select '- ' || column_name || ': ' || expression  AS sql_text 
from table( INFER_SCHEMA(
   LOCATION => '@database.schema.stage_name/folder/'    -- stage and path
   , FILE_FORMAT => 'database.schema.file_format_name'  -- file format 
   , FILES => 'file_name.parquet'                       -- exact file name (use LS @stage to find one)
   , IGNORE_CASE => FALSE
) );
```

---

### `run_table_export()`

This macro handles the process of exporting data from a dbt model (table or view) into files 
in a Snowflake stage.

**Arguments:**

- `table_ref` (Relation): A dbt relation object (created using `ref()` or `source()`) 
  pointing to the data to be exported.

- `export_path_cfg` (dict): Configuration for the output file path and file naming within the output stage.

- `stage_cfg` (dict): Configuration specifying the `stage_name` and `format_name` to be used for the export.

- `flags` (dict): A dictionary of boolean flags to control the export behavior, 
  such as `only_one_export` and `remove_folder_before_export` and `create_dummy_file`.

**Usage:**

```YAML
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

{%- set cfg = fromyaml(yaml_config) -%}

{{ pragmatic_data.run_table_export(
    table_ref       = table_ref,
    export_path_cfg = cfg.export_path_cfg,
    stage_cfg       = cfg.stage_cfg,
    flags           = cfg.flags
) }}
```

The process flags are used to enable (true) or disable (false or absent) the script's functionalities:
- **create_dummy_file**: create a dummy file when the export process if finished
- **only_one_export**: if an export (dummy file) is already in the folder targeted for export (date based)
- **remove_folder_before_export**: if the content of the target folder must be deleted before starting the export. 
  The content is deleted only if the export is being written. Nothing is deleted if no export is scheduled
  because of the `only_one_export` falg.

## General ingestion and export process
Ingestion and Export are specular operations on files, one loading data from files into tables and 
the other writing data from tables into files.

To work on files they both need to setup the DB objects for File Formats and Stages in a Schema of a Database.
The operations to read or write the data are also similar, both using the `COPY INTO` command.

For this reason the library macros are bundled together under a common `in_out` folder in this package. 
We suggest to keep together the ingestion/export macros in your projects, grouped by source/target system, 
under a common folder named `ingestion` and `export` depending on the operation.
If you have both ingestion and export for a single system consider an `in_out` folder to keep both close.

The Pragmatic Data Platform can easily ingest from and export to CSV and SEMI-STRUCUTRED file formats 
in internal or external stages, allowing you to ingest and export files from anywhere your 
Snowflake account is authorized to read.

## Ingestion and export playbook
The playbook to ingest data from files or export data to files is the following:
1. create a setup file to define names and the shared DB objects (schema, file format, stage)  
   This is explained in the [Ingestion and Export Setup File](#ingestion-and-export-setup-file) section,
   later in this file.

2. create a .sql file with the YAML config or SQL code for each table to be ingested or exported  
   A. for ingestion read the [Landing Table ingestion file](#landing-table-ingestion-file) section,
   later in this file.  
   B. for export read the [Export Macros](export_lib/README.md) section, later in this file.

We suggest creating a separate folder in your dbt project ingestion and export, with sub-folders 
for each source system or domain that you want to ingest data from or export data to. 

This is consistent with the fact that usually all files exported from one system 
are extracted to a single location (often a Data Lake), with the same file format, 
and you generally want to have all the landing tables for one system in the same DB schema.

The suggested file organization in your project looks like this:
```
/ingest/                        - the base folder for ingestion, to be added to the macro path
  /system_xxx/                  - the folder to hold ingestion for a system / domain
    /system_xxx__setup.sql              - the file with the setup and naming macros, same for in and out
    /system_xxx__table_xyz.sql          - a file with the macro to ingest the individual Landing Table
/export/                        - the base folder for export, to be added to the macro path
  /system_yyy/                  - the folder to hold export for a system / domain
    /system_yyy__setup.sql              - the file with the setup and naming macros, same for in and out
    /system_yyy__export_abc.sql         - a file with the macro to export the individual table/view to a set of files
    ...
```
If you have both ingestion and export for a system/domain using the same stage and file formats, 
you can create a single setup file, but this is quite uncommon.

For extended examples of use you can look at the [STONKS sample dbt project](https://github.com/pragmatic-data/stonks).

## Ingestion and Export Setup File
The setup for ingestion or export consists in creating the database objects needed to start ingesting 
or exporting files. To perform it we use the `inout_setup_sql()` macro from this PDP package, 
coupled with a YAML block holding your configuration to be passed to the macro.

This is encapsulated into a setup file with a few helper macros, that simplify running the setup SQL 
before running the ingestion macros to make sure that the correct database objects are in place 
when we process each Landing Table.

The setup file contains the runner macro to execute the setup `run_SOURCE_XXX_ingestion_setup()`
and other helper macros that provide access to object names, so that you can define them once 
in the YAML configuration and reuse them everywhere in your Landing Table macros and dbt project.

All these helper macros contain the name of the source system where the data originates,
in this case `SOURCE_XXX`, to keep the names unique in the dbt project while allowing to ingest files 
from multiple systems with different setup needs.

The quickest setup to ingest a new system is to copy one setup file and replace the `SOURCE_XXX`
name of the old system with the new one and then changing the YAML configuration as needed.

The setup file, that you can use as your first blueprint, looks like this:
```YAML
/* **Provide the configuration to set up the schema, file format and stage**
 * This macro also converts the YAML into a set of nested Pythion dictionaries.
 */
{% macro get_SOURCE_XXX_ingestion_cfg() %}
{% set ingestion_cfg %}
landing:
    #database:   "{{ target.database }}"     #-- Leave empty, commented out or remove to use the DB for the env (target.database)
    schema:     LAND_SOURCE_XXX
    comment:    "'Landing table schema for CSV files from SYSTEM SOURCE_XXX.'"

file_format:
    name: SOURCE_XXX_CSV__FF
    definition:
        TYPE: "'CSV'"                               #-- note the double quotes (for YAML) around the single quotes needed for Snowflake
        SKIP_HEADER: 1                              #-- Set to 0 and handle afterwards, when we have more than one header in each file
        FIELD_DELIMITER: "','"                      
        FIELD_OPTIONALLY_ENCLOSED_BY: "'\\042'"     #-- '\042' double quote
        COMPRESSION: "'AUTO'" 
        ERROR_ON_COLUMN_COUNT_MISMATCH: TRUE
        #-- EMPTY_FIELD_AS_NULL: TRUE               #-- sometimes you need this
        #-- NULL_IF: ('', '\\N')                    #-- sometimes you need this too
        #-- ENCODING: "'ISO-8859-1'"                #-- For nordic languages

stage:
    name: SOURCE_XXX_CSV__STAGE
    definition:
        DIRECTORY: ( ENABLE = true )
        COMMENT: "'Stage for CSV files from SOURCE_XXX.'"
        # FILE_FORMAT:                    #-- leave empty (or remove) to use the FF from the stage
        # STORAGE_INTEGRATION: <some descriptive name>                # The storage integration to use
        # URL: "'azure://XXXXXX.blob.core.windows.net/SYSTEM_XXX/'"   # example for Azure

{% endset %}

{{ return(fromyaml(ingestion_cfg)) }}     #-- parses and returns a dictionary from the YAML text 
{% endmacro %}

/* **Generate the SQL to set up the schema, file format and stage**
 * We keep in a separated macro so it's easy to inspect :)
 */
{%  macro get_SOURCE_XXX_ingestion_setup_sql() %}
  {% do return(inout_setup_sql(cfg = get_SOURCE_XXX_ingestion_cfg())) %}
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

## Landing Table ingestion file

To ingest data into a Landing Table, you create a dedicated `.sql` file that contains 
all the necessary configuration and logic. This file typically includes two main parts: 
- a YAML block for configuration and 
- a macro that executes the two-step ingestion process:
  1. Landing Table creation, using a CREATE TABLE command, if the LT does not exist;
  2. Ingestion of data from files, using the COPY INTO command;

This approach keeps the definition of the landing table and the ingestion 
parameters together, making it easy to manage and understand the data loading 
process for each table.

Accordingly, the file is composed of two main parts:

1. A variable `ingestion_cfg` that encapsulates the YAML configuration with 
   the Landing Table structure (`landing_table`) and ingestion parameters (`ingestion`).
   The line with `set cfg = fromyaml(ingestion_cfg)` converts the YAML into a dictionary.

3. A call to the `pragmatic_data.run_CSV_ingestion` or `pragmatic_data.run_semi_structured_ingestion`  
   library macro, passing the configuration to execute the two-step ingestion process.

The ingestion file for a `CASH_TRANSACTIONS` Landing Table looks like this:

```YAML
{% macro load_SOURCE_XXX_CashTransactions(recreate_table = false) %}
{%- set ingestion_cfg -%}
ingestion:
   pattern: 'cash_transactions/.*CashTransactions.*[.]csv.gz'   #-- path to the files
   stage_name: "{{ get_SOURCE_XXX_ingestion_stage_name() }}"
   format_name:   #-- leave empty or remove to use the default FF of the stage 

landing_table:
   db_name:     "{{ get_SOURCE_XXX_ingestion_db_name() }}"
   schema_name: "{{ get_SOURCE_XXX_ingestion_schema_name() }}"
   table_name:  CASH_TRANSACTIONS
   columns:
      - ClientAccountID               #-- No type specification means TEXT
      - AccountAlias: TEXT            #-- Explicit type definition
      - FXRateToBase: NUMBER(38, 5)
        # . . . list all columns in the CSV to have them in the LT 
{%- endset -%}

{%- set cfg = fromyaml(ingestion_cfg) -%}

{% do pragmatic_data.run_CSV_ingestion(
  landing_table_dict = cfg['landing_table'],
  ingestion_dict  = cfg['ingestion'],
  recreate_table = recreate_table
  ) %}

{% endmacro %}

```

With this file in place, you can execute the ingestion from your command line using dbt run-operation:

`dbt run-operation load_SOURCE_XXX_CashTransactions`

Or to recreate the table before loading:

`dbt run-operation load_SOURCE_XXX_CashTransactions --args '{recreate_table: true}'`

### Load All tables macro
While invoking the setup and each landing table macro individually with the `run-operation`
command is simple and handy during development, when you have many systems and LTs to
ingest before a `dbt build` of your project that would not be ideal. 

To make the ingestion process smooth you can create an optional macro to execute 
the setup and load of all tables from a source system in one go, as shown in the 
following example.

```
-- This is an example of a macro that can load all tables for a given source system
{% macro load_all_SOURCE_XXX(recreate_tables=false) %}

    {% do run_SOURCE_XXX_ingestion_setup() %}
    
    {% do load_SOURCE_XXX_CashTransactions(recreate_tables) %}
    {#- {% do load_SOURCE_XXX_OtherTable(recreate_tables) %} #}
{% endmacro %}
```

If you have many systems, and you ingest them all in one go, you can create a higher 
level `load_all` macro that invokes the `load_all_...` macros for the different 
systems you have.

This will load all your macros sequentially.

An advanced setup, to take advantage of the inherent `dbt build`parallelism,
is to invoke the ingestion macros from dedicated dbt models. 
These models cannot write the data in the landing tables, as the `COPY INTO` 
command is not suitable for this, but they can log an audit row in an ingestion 
control table and load the data in the landing table as a "side effect" of 
the model being run.

In such setup you have to take care that the LT models depend on the setup model
for the right system, and that the STG dbt models that use the LT data depend on
the audit/ingestion model used to invoke the ingestion macro.

Once that is in place, the ingestion becomes part of the DAG and is run in parallel 
and invoked as any other model when you include the upstream models. 
That is not the case if you orchestrate the load of the landing tables using the macros.   
