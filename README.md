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
## Table of Contents
* [Installation instructions](#installation-instructions)
* [Ingestion and extraction layer](#ingestion-and-extraction-layer)
* [Storage layer](#storage-layer)
  * [Staging models](#staging-models)
  * [History models - Single version per load](#history-models---single-version-per-load) 
  * [History models - Multiple versions per load](#history-models---multiple-versions-per-load) 
* [Refined layer](#refined-layer)
* [Delivery layer](#delivery-layer)

----

## Ingestion layer
Macros are [there](macros/ingestion_lib), docs are "coming soon..." :)

## Storage layer
The storage layer is used to create an immutable history of the incoming data in the most usable format, 
without altering the semantic of the received data.  

This is usually achieved by the collaboration of a staging (STG) view and an history (HIST) table.

The staging models are used to adapt the incoming data by:
* providing good column names, 
* filtering out headers and other not-data rows
* making the data usable (e.g. parsing text into dates, timestamps and numbers, converting timezones, ...), 
* extracting desired columns from nested structures
* calculating handy derived columns (like IS_DELETED or FILE_TYPE) to incapsulate export specific logic
* adding default values for dimension like inputs
* calculating HashKeys for Primary Keys and Foreign Keys of relevant Business Concepts
* filter out older versions if we want to just store the latest version for each run
* identify or calculate the EFECTIVE_DATE (or EFECTIVE_TS) from the input payload
* eventually filter the input to reduce the load effort

The history models are used to:
* store the new versions received from the STG model.  
  One option is to store all the new versions, even multiple changes per run, creating a fully auditable history of changes;
  another option is to store only one version (the most recent) at each run, reducing the load time and storage footprint.
* recognize and mark as deleted the rows deleted in the source system.  
  To be able to recognize deletions we need either a full export in the STG (hard for bigger table as facts)
  or a separate list of deleted keys (friendlyer for bigger tables). Note that we can also calculate the list 
  of deleted entities ourselves with a full export containing only the columns of the key.

Please note that if we configure the change data capture of our source to add a status column to the export,
we can use the normal history to track the changes as the status column tells us when a row is deleted 
and when it changes it also produces a new version, that we record and we can contestually mark as deleted.

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
