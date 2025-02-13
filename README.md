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
* [Storage layer](#storage-layer)
  * [Staging models](#staging-models)
  * [History models - Single version per load](#history-models---single-version-per-load) 
  * [History models - Multiple versions per load](#history-models---multiple-versions-per-load) 
* [Refined layer](#refined-layer)
* [Delivery layer](#delivery-layer)

----

## Ingestion layer
Ingestion of files into Landing Tables in the PDP is based on three operations:
1. creation of the shared DB objects (schema, file format and stage), if they not exists
2. creation of the individual landing table, if not exists
3. ingestion of all the new files since the last ingestion into the individual landing table

The playbook to ingest files is therefore the following:
1. create a setup file to define names and the shared DB objects (schema, file format, stage)  
   This is explained in the [Ingestion Setup](macros/ingestion_lib/README.md#ingestion-setup) section
2. create an ingestion file for each landing table  
   This is explained in the [Landing Tables Macros](macros/ingestion_lib/README.md#landing-tables-macros) section

The suggested file organization looks like this:
```
/ingestion/                       - a base folder for ingestion macros, to be added to the macro path
  /system_xxx/
    /system_xxx__setup.sql        - the file with the setup and naming macros
    /system_xxx__table_xyz.sql    - the file with the macro to ingest the individual Landing Table
  /system_zzz/
    ...
```

For more details and examples of the ingestion process,
please look at the [README](macros/ingestion_lib/README.md) file in the `ingestion_lib` folder 
and the [ingestion](integration_tests/models/ingestion) folder in the Integration Tests.

## Storage layer
The storage layer of the Pragmatic Data Platform takes care of storing effectively the incoming source data, 
with its changes, and making easily accessible in the most usable way possible.   
It provides a stable foundational layer upon which you can build reliable Business Concepts 
to power your analytics and AI/ML workloads.  

This is usually carried out in three steps for each entity to be stored:
1. one **staging model** (**STG**), usually deployed as a view.  
   The role of the STG model is to make the data usable without changing the semantic of the data.  
   For moder details look at the [stage README](macros/structural/storage/stage/README.md).  
   The typical operations done in an STG model are:
   - to adapt the incoming column names, making them as clearly understood as possible
   - to adapt the incoming source data, making it as usable as possible
   - to flatten the structure and extract codes and values
   - to define the identity (Hash Key) and versioning (Hash Diff) of the entity being stored
   - to make explicit the eventual EFFECTIVITY date or timestamp (when things became valid in the real world)
   - to finalize the metadata to be stored along with each entity version 

3. one **history model** (**HIST**), always deployed as an incrementally loaded table.  
   The role of the HIST model is to permanently and immutable store the new versions from the input.
   The typical operations done in a HIST model are:
    - to identify for each instance of the entity the different versions presented in the input (STG model)
    - to store the new version(s) from the input in the HIST table  
      (storing all or just the latest version depends on what HIST macro is used)

4. one optional **SCD/Version model**  (**SCD/VER**), usually deployed as a view.  
   The role of the SCD/VER model is to make the data stored in the HIST table easily usable 
   by exploiting the historization process and its metadata.  
   The model name is usually SCD (as Slowly Changing Dimension) for dimension like entities and
   VER (as VERsions) for fact, event or mapping like entities.  
   It provides the following features:
   - validity range of the version (VALID_FROM - VALID_TO)
   - boolean IS_CURRENT column
   - versioning metadata (version number, version count, ingestion and history load batch numbers)
   - SCD Key pointing to the specific version, to be used as PK/FK in SCD Type 2 use cases

Almost all the effort to code the storage layer goes into the STG models, as they perform a lot of useful operations.
Luckily, most of the content for the STG models can be generated and eventually refined by developers.  
Traditionally the STG models were coded with very simple SQL, but its is now possible simplify even more the
process by using the [stage macro](macros/structural/storage/stage/README.md) and providing some configuration in YAML.
This is especially useful when there is a high level of structural repetitions that can be exploited with YAML anchors.

The core of the storage layer are the HIST models that store the new versions of the source data.  
They are broadly divided in two groups:
- the HIST macros that store only the latest version from the input, if newer that the latest in the HIST
- the HIST macros that store all the new version from the input

The SCD/Ver models enrich the content of the HIST table with some very handy columns that makes much easier to work
with historical data. As an example they make trivial to get only the current versions or filter by validity period.
They are optional, but given the risible effort to create them, we suggest to always create them, unless it really makes
little or no sense. One such case are high volume immutable events, where VER views add nothing but time & cost.

A deeper discussion of the STG, HIST and SCD/VER macros is provided in 
the [storage layer README](macros/structural/storage/README.md) file.

The following image recaps the flow from ingestion to storage and the action in each step:  
<img src="assets/Ingestion and Storage LOWRES.png" alt="Ingestion and Storage" width="600">

## Refined layer
The refined layer is where Business Concepts are built from the stable data available in the storage layer.

The key effort here is to clearly organize and name the models and folders, to build a modular system where 
higher level concepts are built from lower ones, with each model ideally implementing only one business rule (per CTE) 
and therefore having only one reason to change (per CTE).

### Time joins
At the moment the only macro in the refined layer is the `time_join()` macro that is used to simplify with YAML 
the creation of time based joins for entities with full history versions.

For more details, check the [README file](macros/structural/refined/README.md) for the Refined layer.

## Delivery layer
The delivery layer is where we serve the dataset for each specific set of use cases.
Usually we create one data mart for each set of related use cases, often we end up with one data mart per business unit.

The data mart use the Business Concepts available in the refined layer to deliver the desired dimensions, 
base and composite facts. In most cases we just pick a subset of the available columns from existing business concepts. 

Creating multiple data mart allows to personalize each one to make it easier to consume data by the target users.
The personalization usually covers column naming, specific filtering and sometimes extra ad-hoc calculations.

### Self completing dimensions
At the moment the delivery layer provides a macro to create **self completing dimensions**, together with one example.  
A self completing dimensions is like a normal dimension (SCD1 or 2) based on a Business Concept from the refined layer, 
with the twist that it completes itself by adding a default record entry for each key existing in a connected fact, 
but not in the refined Business Concept.  
This is very useful to manage late arrival info efficiently and effectively.  
This pattern allows to process the facts only once, keeping the actual FK in the facts, without generating orphans. 
It exploits the characteristics of column oriented DBs to reverse the process to remove orphans, letting 
the dimension itself to create entries for the missing FKs. 
This way we avoid orphans by pointing to a default record created just-in-time while allowing the FK in the facts 
to be properly used if/when an entry for the FK becomes available in the Business Concept table.

For more details, check the [README file](macros/structural/delivery/README.md) for the Refined layer.

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
