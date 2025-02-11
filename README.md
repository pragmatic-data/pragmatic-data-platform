# Pragmatic Data Platform
Welcome to the Pragmatic Data Platform (PDP) package.

This repository contains a set of macros that you can import 
in your dbt projects to help you build a pragmatic data platform 
as described in my books: "Data Engineering with dbt" and
"Building A Pragmatic Data Platform with dbt and Snowflake".

## Installation
TL;DR add the following into your `packages.yml` or `dependencies.yml` file 
to pin to a specific version (suggested):
```
  # Pragmatic Data Platform package
  - git: https://github.com/RobMcZag/pragmatic-data-platform.git
    revision: v0.2.0
```

or the following to stay on the latest, unexpected and unpredictable changes released to 'main' or any other branch you pick:
```
  # Pragmatic Data Platform package
  - git: https://github.com/RobMcZag/pragmatic-data-platform.git
    revision: main
    warn-unpinned: false
```

For the full explanation on how to install packages, please [read the dbt docs](https://docs.getdbt.com/docs/build/packages).

----
Table of Contents
* [Installation instructions](#installation-instructions)
* [Ingestion layer](#ingestion-layer)
  * [Base Macros](#base-macros)
    * [create_landing_table_sql](#create_landing_table_sql)
    * [ingest_into_landing_sql](#ingest_into_landing_sql)
    * [ingest_semi_structured_into_landing_sql](#ingest_semi_structured_into_landing_sql)
  * [Process Macros](#process-macros)
    * [run_CSV_ingestion](#run_csv_ingestion)
    * [run_SEMI_STRUCT_ingestion](#run_semi_struct_ingestion)
* [Storage layer](#storage-layer)
  * [Staging models](#staging-models)
  * [History models - Single version per load](#history-models---single-version-per-load) 
  * [History models - Multiple versions per load](#history-models---multiple-versions-per-load) 
* [Refined layer](#refined-layer)
* [Delivery layer](#delivery-layer)

----

## Ingestion layer
Ingestion of files into Landing Tables in the Pragmatic Data Platform is based on two operations:
1. creating the landing table, if not exists
2. ingestion of all new files since the last ingestion

For details on the parameters, the format of the dictionaries and examples of use
of the macros in this section, please look at the [README](macros/ingestion_lib/README.md) 
file in the `ingestion_lib` folder and the [ingestion](models/ingestion) 
folder in the Integration Tests.

### Base Macros 
To automate the ingestion operations there are the following three base macros:

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

### Process Macros
The original Pragmatic Data Platform playbook was to use the above macros to create very clean macros 
that ingested the desired data into a Landing Table. One macro for each LT.

Given the repetitions in these macros and to simplyfy even more the usage of the core PDP ingestion macros 
we have crystallized the most common way to use the base macro in the following two macros (one for CSV, 
one for SEMI-STRUCTURED formats) that take the input needed for the base macros and automate the ingestion process.
The input is passed in two dictionaries that describe the Landing Table and the files to load.

#### run_CSV_ingestion(...)
Creates one landing table -if not exists- and ingests the data from the designated files.
This macro is a wrapper that executes the Base Macros and logs their output.

#### run_SEMI_STRUCT_ingestion(...)
Creates one landing table -if not exists- and ingests the data from the designated files.
This macro is a wrapper that executes the Base Macros and logs their output.

## Storage layer

### Staging models

#### Stage ([source](macros/structural/storage/stage/stage.sql)) ([docs](macros/structural/storage/stage/stage_macros_docs.yml))

The macro to build a STAGE model (default prefix STG) from metadata passed as YAML.

**Usage with local YAML definition:**  

The simplest usage is by having the metadata definitions in YAML in the model itself.
```text
{%- set local_yaml_config -%}
  ... <YAML definition of the parameters>
{%- endset -%}

{%- set metadata_dict = fromyaml(local_yaml_config) -%}
```
The YAML is parsed, converted into a Python dictionary and its top levels passed to the macro. 
```text
{{- pragmatic_data.stage(
    source_model            = ref('GENERIC_TWO_COLUMN_TABLE'),
    source                  = metadata_dict['source'],
    calculated_columns      = metadata_dict['calculated_columns'],
    hashed_columns          = metadata_dict['hashed_columns'],
    default_records         = metadata_dict['default_records'],
    remove_duplicates       = metadata_dict['remove_duplicates'],
) }}
```

**Usage with external YAML definition:**  

An alternative usage of the macro is with YAML stored in a separate YAML file.  
In such a case common configurations can be defined once in YAML and recalled by the use of YAML anchors (& and *).  
The YAML config must be under the `config`attribute of the model containing the stage macro.   
```yaml
  - name: YOUR_STAGE_MODEL
    config:
      source_columns:     *stg_model_source_columns
      source:             *stg_model_source
      calculated_columns: *stg_model_calculated_columns
      default_records:    *security_default_records
      hashed_columns:     *stg_model_hashed_columns
      remove_duplicates: 

```
The above config is used in the model named `YOUR_STAGE_MODEL` as shown below:
```text
{{ pragmatic_data.stage(
    source_model            = ref('GENERIC_TWO_COLUMN_TABLE'),
    source                  = config.require('source'),
    calculated_columns      = config.require('calculated_columns'),
    hashed_columns          = config.require('hashed_columns'),
    default_records         = config.require('default_records'),
    remove_duplicates       = config.require('remove_duplicates')
) }}
```

### History models - Single version per load
Macros are [there](macros/structural/storage/single_version), docs are "coming soon..." :)

### History models - Multiple versions per load
Macros are [there](macros/structural/storage/multiple_versions), docs are "coming soon..." :)

## Refined layer
Macros are [there](macros/structural/refined), docs are "coming soon..." :)

## Delivery layer
Macros are [there](macros/structural/delivery), docs are "coming soon..." :)

## Export layer
Macros are [there](macros/export_lib), docs are "coming soon..." :)


###   Copyright 2022-2025 Roberto Zagni
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
