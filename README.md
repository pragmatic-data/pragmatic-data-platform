# Pragmatic Data Platform

Welcome to the Pragmatic Data Platform (PDP) package. 🚀

This repository contains a set of macros to help you build a 
Pragmatic Data Platform following the principles described in my books: 
1. "**Data Engineering with dbt**" published in 2023 and
2. "**Building A Pragmatic Data Platform with dbt and Snowflake**" to be published towards the end of 2025.  
  This book is co-authored with Jakob Brandel.

----

## PDP package installation
TL;DR add the following into your `packages.yml` or `dependencies.yml` file 
to pin to a specific version (suggested):
```
  # Pragmatic Data Platform package
  - git: https://github.com/pragmatic-data/pragmatic-data-platform.git
    revision: 0.4.11
```

For the full explanation on how to install packages, please [read the dbt docs](https://docs.getdbt.com/docs/build/packages).

----

## Table of Contents
* [PDP package installation](#pdp-package-installation)
* [PDP Quick Intro](#pragmatic-data-platform-quick-intro)
* [Macros](#macros)
    * [Ingestion and Export layers](#ingestion-and-export-layer-macros)
    * [Storage layer](#storage-layer-macros)
    * [Refined layer](#refined-layer-macros)
    * [Delivery layer](#delivery-layer-macros)
    * [Generic Tests](#generic-tests)
* [Pragmatic Data Platform Overview](#pragmatic-data-platform-overview)
    * [Ingestion and Export layers](#ingestion-and-export-layer)
    * [Storage layer](#storage-layer)
    * [Refined layer](#refined-layer)
    * [Delivery layer](#delivery-layer)

----

## Pragmatic Data Platform Quick Intro
To get a quick grasp of the Pragmatic Data Platform I suggest two quick readings:
- the [Technical introduction to the PDP](TECH_INTRO.md) in this same folder.  
  This is a compact, example driven, introduction to the key PDP concepts.
- The [STONKS Sample Project Analysis](https://raw.githack.com/pragmatic-data/stonks/main/Sample_Project_Analysis.html).  
   Provides a good starting point to understand the Pragmatic Data Platform
   by analyzing the Stonks project, the **extensive sample project** described in the second book.

----

## Macros

The package provides macros that support the different layers of the Pragmatic Data Platform architecture.

### Ingestion and Export Layer macros

These macros handle loading data from files into tables and exporting data from tables back to files, primarily using the `COPY INTO` command.

|Macro| Description                                                                                                                    | Details                                                       |
|---|--------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------|
|`inout_setup_sql()`| Generates and executes the SQL to create File Formats and Stages.                                                              | [details](macros/in_out/README.md#ingestion-and-export-setup) |
|`run_csv_ingestion()`| Orchestrate the ingestion of CSV files into a landing table, creating it if does not exists.                                   | [details](macros/in_out/ingestion_lib/README.md)              |
|`run_semi_structured_ingestion()`| Orchestrate the ingestion of semi-structured files (like JSON or Parquet) into a landing table, creating it if does not exists. | [details](macros/in_out/ingestion_lib/README.md)              |
|`run_table_export()`| Orchestrates the export of a table's data to files in a stage, using the selected File Format.                                 | [details](macros/in_out/export_lib/README.md)                 |

### Storage Layer macros

The core of the PDP, this layer is responsible for efficiently storing source data, tracking its changes, and making it easily accessible.

The key pattern to historize incoming source data is with the STG >> HIST >> VER models.

#### STG - Staging data
|Macro|Description|Details|
|---|---|---|
|`stage()`|**The primary macro for staging models.** It standardizes column naming, adapts data types, flattens structures, and generates key metadata like hash keys and hash diffs based on YAML configuration. This is where most of a developer's effort is focused.|[details](macros/structural/storage/stage/README.md)|

#### HIST - Historization patterns
| Macro                                    | Description                                                                                                                                                                                |Details|
|------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---|
| `save_history _with_multiple_versions()` | Stores **_all_** new versions of an entity found in the source input, allowing for **multiple intra-batch changes**. Needs a sort for the incoming versions.                               |[details](macros/structural/storage/multiple_versions/README.md)|
| `save_history()`                         | Stores the latest version of an entity from the source if it's different from the latest version already in the history table. Expects a **single** version per entity per batch. |[details](macros/structural/storage/single_version/README.md)|
| `save_history _with_deletion()`          | An extension of `save_history` that also handles hard deletes in the input by marking records as deleted in the history table. Needs inputs with a full export.                            |[details](macros/structural/storage/single_version/README.md)|
| `save_history _with_deletion _from_list()` | An extension of `save_history` that marks records as deleted in the history table. Requires a model providing the list of deleted entities by key.                                         |[details](macros/structural/storage/single_version/README.md)|

The `save_history_with_multiple_versions()` allows to reload the full history from a Landing Table
in a single step, correctly identifying and storing all the versions. This is the default pattern,
as it covers most use cases and provides for full auditing.

The `save_history()` variants allow for simpler and slightly quicker historization, but require that 
the input contains only a single version of an entity for each batch. This is normal with full exports, 
where you might also want to recognize deletions, and transactional workloads (high volume, high speed). 

#### VER - Version enrichment and current data helpers 
| Macro                                             | Description                                                                                                                                                                                                               |Details|
|---------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---|
| `versions_from_history _with_multiple_versions()` | Builds a view on top of a `history` table to provide a Slowly Changing Dimension Type 2 representation, including `VALID_FROM`, `VALID_TO`, and `IS_CURRENT` columns. Includes also a stable version key (`DIM_SCD_KEY`). |[details](macros/structural/storage/multiple_versions/README.md)|
| `current_from_history()`                          | Builds a CTE on top of a `history` table to provide the current (latest) version of each entity on the selected timeline.                                                                                                 |[details](macros/structural/storage/single_version/README.md)|

We suggest to create a VER model calling the `versions_from_history_with_multiple_versions()` macro
for each historical table, as it makes trivial to use and reference both historical and current data.

The `current_from_history()` macro is retained for internal use and backward compatibility.

#### Helper macros
|Macro| Description                                                                                                                                                                   |Details|
|---|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---|
|`pdp_hash()`| The macro used behind the scenes by `stage()` to generate consistent hash values. You can also use it directly if you need to create hashed columns outside of the STG model. ||

### Refined Layer macros

This is where you build reusable Business Concepts from the stable data in the Storage Layer.

|Macro|Description|Details|
|---|---|---|
|`time_join()`|Simplifies the creation of time-based joins between entities that have a full history, using a declarative YAML configuration.|[details](macros/structural/refined/README.md)|

### Delivery Layer macros

This layer serves specific, use-case-driven datasets (data marts) to end-users.

| Macro                          |Description|Details|
|--------------------------------|---|---|
| `self_completing _dimension()` |Creates a dimension that automatically adds default records for keys that exist in a fact table but are missing in the dimension's source business concept. This is a powerful pattern for handling late-arriving data efficiently.|[details](macros/structural/delivery/README.md)|

### Generic Tests

The package also includes custom generic tests to enforce data quality and integrity across your PDP project.

#### Ingestion validatioin tests
|Test| Description                                                                          |
|---|--------------------------------------------------------------------------------------|
|`test_all_files_from_stage`| Checks that all files in the stage have been ingested in the Landing Table.          |
|`test_file_order_is_correct`| Validates that the files have been loaded in the Landing Table in the correct order. |

#### Storage layer tests
|Test| Description                                                                                                                    |
|---|--------------------------------------------------------------------------------------------------------------------------------|
|`test_keys_from_landing`| Checks that all the keys from the landing layer exist in the history table.                                                    |
|`test_keys_and_ts_from_landing`| Checks that all the keys from the landing layer exist in the history table, with the latest ingestion timestamps matching.     |
|`test_has_sortable_versions`| Ensures that versions of an entity can be reliably sorted. Used to validate the use of `save_history_with_multiple_versions()` |
|`no_hash_collisions`| Checks for hash collisions on a given column. Usually applied on the history tables.                                           |

#### Delivery layer tests
|Test|Description|
|---|---|
|`has_default_key`|Used with dimensions to ensure a default key exists.|
|`warn_on_multiple_default_key`|Warns if a dimension has more than one default key.|

#### Generic use tests
|Test| Description                                                                          |
|---|--------------------------------------------------------------------------------------|
|`not_empty`| A simple test to ensure a model is not empty. |


----

## Pragmatic Data Platform Overview
The Pragmatic Data Platform is a layered architecture designed for simplicity, maintainability, and scalability. 
The macros in this package are built to support this structure.

### Ingestion and Export layer

Ingestion and Export are specular operations on files, one loading data from files into tables and 
the other writing data from tables into files.
To work on files they both need to setup the DB objects for File Formats and Stages in a Schema of a Database.  
The operations to read or write the data are also similar, both using the `COPY INTO` command.

More details in the [ingestion and export README](macros/in_out/README.md) 
or jump directly to one of the section of the file:  
* [Ingestion and Export playbook](macros/in_out/README.md#ingestion-and-export-playbook)
* [Ingestion and Export setup](macros/in_out/README.md#ingestion-and-export-setup)

<!-- 
For more details and examples of the **ingestion** specific process,
please look at the [Ingestion layer README](macros/in_out/ingestion_lib/README.md) file in the `ingestion_lib` folder 
and the [ingestion](integration_tests/models/in_out/ingestion_code_gen) folder in the Integration Tests.

For more details and examples of the **export** specific process,
please look at the [Export layer README](macros/in_out/export_lib/README.md) file in the `export_lib` folder 
and the [export](integration_tests/models/in_out/system_A/export) folder in the Integration Tests.
-->

### Storage layer
The storage layer takes care of storing effectively the incoming source data, 
with its changes, and making it usable and easily accessible. 
It provides a stable foundational layer upon which you can build reliable **Business Concepts** 
to power your analytics and AI/ML workloads.  

The following image highlights the data flow from ingestion to storage and the key actions in each step:  
<img src="assets/Ingestion and Storage LOWRES.png" alt="Ingestion and Storage" width="600">

The historization is carried out in three steps for each entity to be stored:
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

2. one **history model** (**HIST**), always deployed as an incrementally loaded table.  
   The role of the HIST model is to permanently and immutably store the new versions from the input.  
   Select the right historization pattern based on your input and needs.  
   The typical operations done in a HIST model are:
    - to identify for each instance of the entity the different versions presented in the input (STG model)
    - to store the new version(s) from the input in the HIST table  
      (storing all or just the latest version depends on what HIST macro is used)

3. one **SCD/Version model**  (**VER**), usually deployed as a view.  
   The role of the VER model is to make the data stored in the HIST table easily usable 
   by exploiting the metadata from the historization process.  
   The model transforms an immutable, insert only history in a flexible one that provides 
   the typical Slowly Changing Dimensions type 2 features:
   - validity range of the version (VALID_FROM - VALID_TO)
   - boolean IS_CURRENT column
   - SCD Key pointing to the specific version, to be used as PK/FK in SCD Type 2 use cases
   - versioning metadata (version number, version count, ingestion and history load batch numbers)

Almost all the (little) effort to code the storage layer goes into the STG models, 
as they require the metadata/configuration to perform their many useful operations.
Luckily, most of the content for the STG models can be generated and just be refined by developers. 

The core of the storage layer are the HIST models that store the new versions of the source data.  
They are broadly divided in two groups:
- the HIST macros that can process all the new version from the input (multiple versions per batch)
- the HIST macros that can process only the latest version from the input (one version per batch)

The VER models enrich the content of the HIST table with powerful columns that make easy to work
with historical data. As an example they make trivial to get the current version or filter by validity period.
They are optional, but given the risible effort to create them, we suggest to always create them, unless it really makes
little or no sense. One such case are high volume immutable events, where VER views add nothing but time & cost.

A deeper discussion of the STG, HIST and SCD/VER macros is provided in 
the [storage layer README](macros/structural/storage/README.md) file.

### Refined layer
The refined layer is where Business Concepts are built from the stable data available in the storage layer.

The key effort here is to clearly organize and name the models and folders, to build a modular system where 
higher level concepts are built from lower ones, with each model ideally implementing only one business rule (per CTE) 
and therefore having only one reason to change (per CTE).

#### Time joins
At the moment the only macro in the refined layer is the `time_join()` macro that is used to simplify with YAML 
the creation of time based joins for entities with full history versions.

For more details, check the [README file](macros/structural/refined/README.md) for the Refined layer.

### Delivery layer
The delivery layer is where we serve the dataset for each specific set of use cases.
Usually we create one data mart for each set of related use cases, often we end up with one data mart per business unit.

The data mart use the Business Concepts available in the refined layer to deliver the desired dimensions, 
base and composite facts. In most cases we just pick a subset of the available columns from existing business concepts. 

Creating multiple data mart allows to personalize each one to make it easier to consume data by the target users.
The personalization usually covers column naming, specific filtering and sometimes extra ad-hoc calculations.

#### Self completing dimensions
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

----
### &#169;  Copyright 2022-2025 Roberto Zagni
   All right reserved.

This software is licensed with a dual license based on the use of the software.

Licensed under the PROPRIETARY LICENSE for use in consulting, billable work or 
any other work performed in association with a commercial transaction or when it 
is not explicitly granted another licence.

Licensed under the APACHE LICENSE only for use in personal, non commercial, academic
or internal use in own systems by Legal Entities for work performed by employees of 
such Legal Entities. This explicitly excludes any consulting or work for hire use.

Unless agreed to in writing, software distributed under the Licenses is distributed 
on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express 
or implied. See the Licenses for the specific language governing permissions and 
limitations under the License.  

If you are unable to accept the above terms, you may not use this file and any 
content of this repository, and you must not keep any copy of the content of this 
repository.
