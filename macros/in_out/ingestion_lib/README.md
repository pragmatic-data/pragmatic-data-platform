# PDP Ingestion layer 
Welcome to the **ingestion layer** of the Pragmatic Data Platform (PDP) package.

**Table of Contents**
- [General ingestion and export process](../README.md#general-ingestion-process) in the in_out folder.
- [Ingestion and export Setup](../README.md#ingestion-and-export-setup) in the in_out folder.
- [Landing Tables Macros](#landing-tables-macros)
  - [Current, YAML based Landing Table ingestion](#current-yaml-based-landing-table-ingestion)
  - [Original (legacy), SQL based Landing Table ingestion](#original-legacy-sql-based-landing-table-ingestion)
- [Ingestion Process Macros](#ingestion-process-macros)
- [Ingestion Base Macros](#ingestion-base-macros)

## Ingestion and export playbook
The playbook to ingest data from files or export data to files is the following:
1. create a setup file to define names and the shared DB objects (schema, file format, stage)  
   This is explained in the [Ingestion Setup](../README.md#ingestion-and-export-setup) section

2. create a .sql file with the YAML config or SQL code for each table to be ingested or exported
   A. for ingestion read the [Landing Tables Macros](#landing-tables-macros) section below
   B. for export read the [Export Macros](../export_lib/README.md) section

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

----
### &#169;  Copyright 2022-2025 Roberto Zagni
All right reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

Unless agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

If you are unable to accept the above terms, you may not use this
file and any content of this repository, and you must not keep any copy
of the content of this repository.
