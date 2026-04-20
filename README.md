# Pragmatic Data Platform

Welcome to the Pragmatic Data Platform (PDP) package. 🚀

This repository contains a set of macros to help you build a
Pragmatic Data Platform following the principles described in the
[Pragmatic Data Platform Overview](#pragmatic-data-platform-overview) section at the end of this readme
and in detail in the books:
1. "**Data Engineering with dbt**" published in 2023 and
2. "**Building A Pragmatic Data Platform with dbt and Snowflake**" to be published towards March/April 2026.  
  This book is co-authored with Jakob Brandel.

----

## PDP package installation
TL;DR add the following into your `packages.yml` or `dependencies.yml` file 
to pin a specific version:
```
  # Pragmatic Data Platform package
  - git: https://github.com/pragmatic-data/pragmatic-data-platform.git
    revision: 0.4.17
```

For the full explanation on how to install packages, please [read the dbt docs](https://docs.getdbt.com/docs/build/packages).

----

## Table of Contents
* [PDP package installation](#pdp-package-installation)

* [Macros](#macros)
    * [Ingestion and Export macros](#ingestion-and-export-macros)
        - [`inout_setup_sql()`](#inout_setup_sql)
        - [`run_file_ingestion()`](#run_file_ingestion)
        - [`run_clean_landing_table()`](#run_clean_landing_table)
        - [`run_create_pipe()`](#run_create_pipe)
        - [`run_table_export()`](#run_table_export)
        - [`run_csv_ingestion()`](#run_csv_ingestion) *(deprecated)*
        - [`run_semi_structured_ingestion()`](#run_semi_structured_ingestion) *(deprecated)*
        
    * [Storage layer macros](#storage-layer-macros)
        - STG models
          - [`stage()`](#stage)
          - [`landing_filter()`](#landing_filter)
          - [`pdp_hash()`](#pdp_hash)
        - HIST models - ingest multiple versions per batch
          - [`save_history_with_multiple_versions()`](#save_history_with_multiple_versions)
        - HIST models - ingest single version per batch
          - [`save_history()`](#save_history)
          - [`save_history_with_deletion()`](#save_history_with_deletion)
          - [`save_history_with_deletion_from_list()`](#save_history_with_deletion_from_list)
        - VER models
          - [`versions_from_history_with_multiple_versions()`](#versions_from_history_with_multiple_versions)
          - [`current_from_history()`](#current_from_history)

    * [Refined layer macros](#refined-layer-macros)
        - [`time_join()`](#time_join)

    * [Delivery layer macros](#delivery-layer-macros)
        - [`self_completing_dimension()`](#self_completing_dimension)

* [Generic Tests](#generic-tests)
  - [Ingestion Tests](#ingestion-tests)
    - [`test_all_files_from_stage`](#test_all_files_from_stage)
    - [`test_file_order_is_correct`](#test_file_order_is_correct)

  - [Storage Layer Tests](#storage-layer-tests)
    - [`test_keys_from_landing`](#test_keys_from_landing)
    - [`test_keys_and_ts_from_landing`](#test_keys_and_ts_from_landing)
    - [`test_has_sortable_versions`](#test_has_sortable_versions)
    - [`no_hash_collisions`](#no_hash_collisions)

  - [Delivery Layer Tests](#delivery-layer-tests)
    - [`has_default_key`](#has_default_key)
    - [`warn_on_multiple_default_key`](#warn_on_multiple_default_key)

  - [Generic use tests](#generic-use-tests)
    - [`not_empty`](#not_empty)

* [Pragmatic Data Platform Overview](#pragmatic-data-platform-overview)
    * [Quick Intro](#quick-intro)
    * [Design Philosophy](#design-philosophy)
    * [Architecture](#architecture)
    * [Data Flow](#data-flow)
    * [Ingestion and Export](#ingestion-and-export)
    * [Storage layer](#storage-layer)
    * [Refined layer](#refined-layer)
    * [Delivery layer](#delivery-layer)
    * [The Soft Mesh Approach](#the-soft-mesh-approach)
    * [Key Architectural Decisions](#key-architectural-decisions)
    * [Engineering Practices](#engineering-practices)
    * [Comparison with Other Methodologies](#comparison-with-other-methodologies)

----

## Macros

The package provides macros that support the different layers of the Pragmatic Data Platform architecture.

### Ingestion and Export macros

These macros handle loading data from files into tables and exporting data from tables back to files.
Ingestion can be done as a direct batch load (`run_file_ingestion`) or via a Snowpipe for continuous ingestion (`run_create_pipe`).
Both rely on Snowflake stages and file formats set up with `inout_setup_sql`.

|Macro|Description|Details|
|---|---|---|
|<a name="inout_setup_sql"></a>`inout_setup_sql()`|Generates and executes the SQL to create File Formats and Stages.|[details](macros/in_out/README.md#ingestion-and-export-setup)|
|<a name="run_file_ingestion"></a>`run_file_ingestion()`|Orchestrates the full ingestion cycle for CSV or semi-structured files into a landing table (creates it if it doesn't exist, runs `COPY INTO`, optionally cleans up old rows). **Recommended for most use cases.**|[details](macros/in_out/ingestion_lib/README.md#current-yaml-based-landing-table-ingestion)|
|<a name="run_clean_landing_table"></a>`run_clean_landing_table()`|Standalone cleanup: removes rows from a landing table older than a configured threshold (by number of batches or by age in days). Called automatically by `run_file_ingestion()` when cleanup is configured; also available independently.|[details](macros/in_out/ingestion_lib/README.md)|
|<a name="run_create_pipe"></a>`run_create_pipe()`|Creates a Snowpipe for continuous, near-real-time ingestion of files from a stage into a landing table.|[details](macros/in_out/ingestion_lib/README.md)|
|<a name="run_table_export"></a>`run_table_export()`|Orchestrates the export of a table's data to files in a stage, using the selected File Format.|[details](macros/in_out/export_lib/README.md)|
|<a name="run_csv_ingestion"></a>~~`run_csv_ingestion()`~~|⚠️ **Deprecated.** Use `run_file_ingestion()` instead. Kept for backward compatibility.|[details](macros/in_out/ingestion_lib/README.md)|
|<a name="run_semi_structured_ingestion"></a>~~`run_semi_structured_ingestion()`~~|⚠️ **Deprecated.** Use `run_file_ingestion()` instead. Kept for backward compatibility.|[details](macros/in_out/ingestion_lib/README.md)|


### Storage Layer macros

The core of the PDP, this layer is responsible for efficiently storing source data, tracking its changes, and making it easily accessible.

The key pattern to historize incoming source data is with the STG >> HIST >> VER models.

#### STG - Staging data
|Macro| Description                                                                                                                                                                                                                                                   |Details|
|---|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---|
|<a name="stage"></a>`stage()`| **The primary macro for staging models.** It standardizes column naming, adapts data types, flattens structures, and generates key metadata like hash keys and hash diffs based on YAML configuration. This is where most of a developer's effort is focused. |[details](macros/structural/storage/stage/README.md)|
|<a name="landing_filter"></a>`landing_filter()`|Optional helper that generates a `WHERE` condition for the `source.where` key of `stage()`, avoiding the need to hand-write the SQL. Covers two common landing table filter patterns: keep only the last N ingestion batches, or keep only rows ingested within the last N days or hours.|[details](macros/structural/storage/stage/README.md)|
|<a name="pdp_hash"></a>`pdp_hash()`| The macro used behind the scenes by `stage()` to generate consistent hash values. You can use it directly, if you need to create hashed columns outside of the STG model.                                                                                     ||

The `stage()` macro is where you adapt and make the source data usable without changing its meaning.  
You also explicitly define the PK/FKs to be created with hashed columns (`HKEY`).

#### HIST - Historization patterns
| Macro                                                                                      | Description                                                                                                                                                                                |Details|
|--------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---|
| <a name="save_history_with_multiple_versions"></a>`save_history _with_multiple_versions()` | Stores **_all_** new versions of an entity found in the source input, allowing for **multiple intra-batch changes**. Needs a sort for the incoming versions.                               |[details](macros/structural/storage/multiple_versions/README.md)|
| <a name="save_history"></a>`save_history()`                                                | Stores the latest version of an entity from the source if it's different from the latest version already in the history table. Expects a **single** version per entity per batch. |[details](macros/structural/storage/single_version/README.md)|
| <a name="save_history_with_deletion"></a>`save_history _with_deletion()`                   | An extension of `save_history` that also handles hard deletes in the input by marking records as deleted in the history table. Needs inputs with a full export.                            |[details](macros/structural/storage/single_version/README.md)|
| <a name="save_history_with_deletion_from_list"></a>`save_history _with_deletion _from_list()`       | An extension of `save_history` that marks records as deleted in the history table. Requires a model providing the list of deleted entities by key.                                         |[details](macros/structural/storage/single_version/README.md)|

The `save_history_with_multiple_versions()` allows to reload the full history from a Landing Table
in a single step, correctly identifying and storing all the versions. This is the default pattern,
as it covers most use cases and provides for full auditing.

The `save_history()` variants allow for simpler and slightly quicker historization, but require that 
the input contains only a single version of an entity for each batch. This is normal with full exports, 
where you might also want to recognize deletions, and transactional workloads (high volume, high speed). 

#### VER - Version enrichment and current data helpers 
| Macro                                             | Description                                                                                                                                                                                                               |Details|
|---------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---|
|<a name="versions_from_history_with_multiple_versions"></a>`versions_from_history _with_multiple_versions()` | Builds a view on top of a `history` table to provide a Slowly Changing Dimension Type 2 representation, including `VALID_FROM`, `VALID_TO`, and `IS_CURRENT` columns. Includes also a stable version key (`DIM_SCD_KEY`). |[details](macros/structural/storage/multiple_versions/README.md)|
|<a name="current_from_history"></a>`current_from_history()`                          | Builds a CTE on top of a `history` table to provide the current (latest) version of each entity on the selected timeline.                                                                                                 |[details](macros/structural/storage/single_version/README.md)|

We suggest to create a VER model calling the `versions_from_history_with_multiple_versions()` macro
for each historical table, as it makes trivial to use and reference both historical and current data.

The `current_from_history()` macro is retained for internal use and backward compatibility.

### Refined Layer macros

This is where you build reusable Business Concepts from the stable data in the Storage Layer.

|Macro|Description|Details|
|---|---|---|
|<a name="time_join"></a>`time_join()`|Simplifies the creation of time-based joins between entities that have a full history, using a declarative YAML configuration.|[details](macros/structural/refined/README.md)|

### Delivery Layer macros

This layer serves specific, use-case-driven datasets (data marts) to end-users.

| Macro                          |Description|Details|
|--------------------------------|---|---|
|<a name="self_completing_dimension"></a>`self_completing _dimension()` |Creates a dimension that automatically adds default records for keys that exist in a fact table but are missing in the dimension's source business concept. This is a powerful pattern for handling late-arriving data efficiently.|[details](macros/structural/delivery/README.md)|

## Generic Tests

The package also includes custom generic tests to enforce data quality and integrity across your PDP project.

### Ingestion tests
|Test| Description                                                                          |
|---|--------------------------------------------------------------------------------------|
|<a name="test_all_files_from_stage"></a>`test_all_files_from_stage`| Checks that all files in the stage have been ingested in the Landing Table.          |
|<a name="test_file_order_is_correct"></a>`test_file_order_is_correct`| Validates that the files have been loaded in the Landing Table in the correct order. |

### Storage layer tests
|Test| Description                                                                                                                    |
|---|--------------------------------------------------------------------------------------------------------------------------------|
|<a name="test_keys_from_landing"></a>`test_keys_from_landing`| Checks that all the keys from the landing layer exist in the history table.                                                    |
|<a name="test_keys_and_ts_from_landing"></a>`test_keys_and_ts_from_landing`| Checks that all the keys from the landing layer exist in the history table, with the latest ingestion timestamps matching.     |
|<a name="test_has_sortable_versions"></a>`test_has_sortable_versions`| Ensures that versions of an entity can be reliably sorted. Used to validate the use of `save_history_with_multiple_versions()` |
|<a name="no_hash_collisions"></a>`no_hash_collisions`| Checks for hash collisions on a given column. Usually applied on the history tables.                                           |

### Delivery layer tests
|Test|Description|
|---|---|
|<a name="has_default_key"></a>`has_default_key`|Used with dimensions to ensure a default key exists.|
|<a name="warn_on_multiple_default_key"></a>`warn_on_multiple_default_key`|Warns if a dimension has more than one default key.|

### Generic use tests
|Test| Description                                                                          |
|---|--------------------------------------------------------------------------------------|
|<a name="not_empty"></a>`not_empty`| A simple test to ensure a model is not empty. |


----

## Pragmatic Data Platform Overview

**Sections:**
[Quick Intro](#quick-intro) •
[Design Philosophy](#design-philosophy) •
[Architecture](#architecture) •
[Data Flow](#data-flow) •
[Ingestion and Export](#ingestion-and-export) •
[Storage layer](#storage-layer) •
[Refined layer](#refined-layer) •
[Delivery layer](#delivery-layer) •
[Soft Mesh](#the-soft-mesh-approach) •
[Key Decisions](#key-architectural-decisions) •
[Engineering Practices](#engineering-practices) •
[Methodology Comparisons](#comparison-with-other-methodologies)

### Quick Intro
To get a quick grasp of the Pragmatic Data Platform I suggest two quick readings:
- the [Technical introduction to the PDP](TECH_INTRO.md) in this same folder.
  This is a compact, example-driven introduction to the key PDP concepts.
- The [STONKS Sample Project Analysis](https://raw.githack.com/pragmatic-data/stonks/main/Sample_Project_Analysis.html).
  Provides a good starting point to understand the Pragmatic Data Platform
  by analyzing the Stonks project, the sample project described in the second book.

----

### Design Philosophy

The PDP is built on a single overriding principle: **pragmatism**. Rather than rigidly adhering
to any single data modeling methodology, it takes the best ideas from Kimball, Inmon, Data Vault,
and Data Mesh and combines them into an approach that is practical, implementable, and adaptable
to real-world constraints.

**Core Tenets:**

1. **Pragmatism first**: Choose the simplest approach that meets your needs. Avoid over-engineering.
   Start simple, evolve when complexity is justified by real requirements.
2. **History is sacred**: The complete, immutable history of source data is a non-negotiable asset.
   Once data enters the platform, it is never lost or overwritten. This enables auditability,
   regulatory compliance, and the ability to reprocess historical data with new business rules.
3. **Separation of concerns**: Data storage, business logic, and data presentation are kept in
   distinct layers. Changes in one layer should not force changes in the others.
4. **Statelessness where possible**: The Refined and Delivery layers are logically stateless —
   they can be rebuilt from scratch at any time from the Storage Layer. Only the Storage Layer
   (specifically HIST tables) maintains state.
5. **Software engineering discipline**: Data platforms are software. Apply version control, CI/CD,
   automated testing, code review, documentation, modularity, and the Single Responsibility Principle.
6. **Pattern-based development**: Use repeatable patterns and templates to build the platform
   consistently and efficiently. Standardization reduces cognitive load, speeds onboarding,
   and minimizes errors.
7. **Business-oriented modeling**: Models in downstream layers use the ubiquitous language of the
   business, not technical jargon from source systems.

For how PDP compares with Kimball, Inmon, Data Vault, Data Mesh, and Medallion architectures,
see [Comparison with Other Methodologies](#comparison-with-other-methodologies) below.

----

### Architecture

<img src="assets/Pragmatic-data-architecture LOWRES.png" alt="Pragmatic Data Platform Architecture" width="700">

The PDP is structured around three core layers, each with a distinct purpose and clear boundaries.
Data flows in a single direction:

| Layer | Purpose | State | Orientation |
|---|---|---|---|
| **Storage** | Faithful historical record of source data | Stateful (append-only HIST tables) | Source-oriented |
| **Refined** | Business logic and integration | Stateless (rebuildable) | Business-oriented |
| **Delivery** | User-facing data products | Stateless (rebuildable) | Consumer-oriented |

**Ingestion** is optional but highly valuable when you need to load data from files.
It happens before and outside of dbt models with the goal to load data "as-is"
into Landing Tables in Snowflake with little effort. 
The PDP provides macros (`run_file_ingestion`, `run_create_pipe`)
to automate data loading entirely within your dbt project via `dbt run-operation`,
with no separate tooling required.

----

### Data Flow

Data moves through the platform in a single direction, with each layer adding a clearly defined
type of value.

**Source → Storage**: Files land in Snowflake via the ingestion layer (or any external tool) into
Landing Tables. Each source entity is then processed through a three-step pipeline: a **STG** model
(view) that cleans, standardizes, and generates hash keys; a **HIST** model (incremental) that incrementally stores every new version of each entity; and a **VER** model (view) that exposes the history with `IS_CURRENT`, `VALID_FROM`/`VALID_TO`, and `SCD_KEY` — making it trivial to query current or historical data.

**Storage → Refined**: This is where all business logic lives.
VER models are the primary input to the Refined Layer, with 
`WHERE IS_CURRENT` giving the current state of an entity for common analyses; 
the `time_join` macro wraps Snowflake's ASOF JOIN for accurate point-in-time enrichment when historical accuracy matters. Multiple source histories can be merged into a single golden record.

**Refined → Delivery**: Refined models feed lightweight delivery models — dimensions, facts,
reports — organized into data marts, each targeting a specific consumer or use case.
The same refined data can be exposed in different forms for different consumers without duplicating
business logic. **Logic once in Refined, present multiple times in Delivery.**

----

### Ingestion and Export

Ingestion and Export are specular operations on files, one loading data from files into tables and
the other writing data from tables into files.
To work on files they both need to setup the DB objects for File Formats and Stages in a Schema of a Database.
The operations to read or write the data are also similar, both using the `COPY INTO` command.

More details in the [ingestion and export README](macros/in_out/README.md)
or jump directly to one of the sections:
* [Ingestion and Export playbook](macros/in_out/README.md#ingestion-and-export-playbook)
* [Ingestion and Export setup](macros/in_out/README.md#ingestion-and-export-setup)

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
   For more details look at the [stage README](macros/structural/storage/stage/README.md).
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

3. one **SCD/Version model** (**VER**), usually deployed as a view.
   The role of the VER model is to make the data stored in the HIST table easily usable
   by exploiting the metadata from the historization process.
   The model transforms an immutable, insert-only history into a flexible one that provides
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
- the HIST macros that can process all the new versions from the input (multiple versions per batch)
- the HIST macros that can process only the latest version from the input (one version per batch)

The VER models enrich the content of the HIST table with powerful columns that make it easy to work
with historical data. As an example they make trivial to get the current version or filter by validity period.
They are optional, but given the negligible effort to create them, we suggest to always create them, unless it really makes
little or no sense. One such case is high volume immutable events, where VER views add nothing but time & cost.

A deeper discussion of the STG, HIST and SCD/VER macros is provided in
the [storage layer README](macros/structural/storage/README.md) file.

### Refined layer
The refined layer is where Business Concepts are built from the stable data available in the storage layer.

The key effort here is to clearly organize and name the models and folders, to build a modular system where
higher level concepts are built from lower ones, with each model ideally implementing only one business rule (per CTE)
and therefore having only one reason to change (per CTE).

#### Time joins
The `time_join()` macro simplifies with YAML the creation of time-based joins for entities with full history versions.

For more details, check the [README file](macros/structural/refined/README.md) for the Refined layer.

**Refined layer model prefixes:**

| Prefix | Name | Purpose |
|---|---|---|
| `ref_` | Refined | Core reusable business concept. Cleaned, integrated, current state. |
| `refh_` | Refined History | Full versioned history of a business concept. SCD2, point-in-time analysis. |
| `ts_` | Time Series | One value per time interval per entity. |
| `int_` | Intermediate | Private stepping-stone in a complex transformation. Not for direct consumption. |
| `agg_` / `aggh_` | Aggregated | Summarized data at a higher grain, for performance or simplified analysis. |
| `map_` | Mapping | Bridge table linking keys between source systems (MDM). |
| `mhist_` | Merged History | Combined version history from multiple homogeneous sources for the same entity. |
| `mdd_` | Master Data Dimension | Golden record from an official MDM source. |
| `pivot_` | Pivot | Row-to-column transformation. |
| `brg_` | Bridge | Resolves many-to-many relationships between entities. |
| `spine_` | Spine | Base scaffold ensuring all entities are represented (e.g. date spine). |

### Delivery layer
The delivery layer is where we serve the dataset for each specific set of use cases.
Usually we create one data mart for each set of related use cases, often ending up with one data mart per business unit.

The data mart uses the Business Concepts available in the refined layer to deliver the desired dimensions,
base and composite facts. In most cases we just pick a subset of the available columns from existing business concepts.

Creating multiple data marts allows personalizing each one to make it easier to consume data by the target users.
The personalization usually covers column naming, specific filtering and sometimes extra ad-hoc calculations.

#### Self completing dimensions
At the moment the delivery layer provides a macro to create **self completing dimensions**, together with one example.
A self completing dimension is like a normal dimension (SCD1 or 2) based on a Business Concept from the refined layer,
with the twist that it completes itself by adding a default record entry for each key existing in a connected fact,
but not in the refined Business Concept.
This is very useful to manage late arrival info efficiently and effectively.
This pattern allows to process the facts only once, keeping the actual FK in the facts, without generating orphans.
It exploits the characteristics of column oriented DBs to reverse the process to remove orphans, letting
the dimension itself to create entries for the missing FKs.
This way we avoid orphans by pointing to a default record created just-in-time while allowing the FK in the facts
to be properly used if/when an entry for the FK becomes available in the Business Concept table.

For more details, check the [README file](macros/structural/delivery/README.md) for the Delivery layer.

**Delivery layer model prefixes:**

| Prefix | Name | Purpose |
|---|---|---|
| `dim_` | Dimension | Current descriptive attributes (SCD Type 1). One row per entity. |
| `scd_` | Slowly Changing Dimension | All historical versions (SCD Type 2). Includes `SCD_KEY` for version-specific joins. |
| `fct_` | Fact | Numeric measures with foreign keys to dimensions. |
| `rpt_` | Report | Denormalized wide table. Pre-joined facts + dimensions for specific use cases. |
| `filter_` | Filter | Reusable business-specific cohort or filter definition. |

----

### The Soft Mesh Approach

The PDP's "soft mesh" is a pragmatic approach to Data Mesh that balances the benefits of
decentralization with the practical reality that most organizations aren't ready for a full Data Mesh.

Rather than requiring a big bang transformation, the soft mesh keeps everything on the same
infrastructure — all projects organized with the same patterns, naming conventions and
governance structure. This makes federated governance straightforward and keeps technical barriers low:
no new tooling, no new patterns, just natural project boundaries that reflect organizational boundaries  within a familiar environment.

The PDP's flexibility supports multiple starting points: begin with a soft mesh plan from day one,
or start with standard multi-layered PDP projects and evolve toward a mesh later — the architecture
supports both paths without redesign. A common and effective pattern is to place the Storage Layer
in one mesh component that provides clean, historized data to all consumers, with one or more
consumer-oriented projects (owning their Refined and Delivery layers) as separate mesh nodes.

*For a detailed discussion of project types, governance model, and when to evolve,
see [Soft Mesh Architecture](docs/SOFT_MESH.md).*

----

### Key Architectural Decisions

**Stateless Refined and Delivery Layers**
All models outside the Storage Layer's HIST tables are logically stateless and fully rebuildable.
Business rules can be changed and applied retroactively to all historical data. Bugs are fixed
without complex backfilling. The platform is resilient to corruption — drop and rebuild.

**Source-Oriented Storage, Business-Oriented Refined**
The Storage Layer stays semantically aligned with source systems — no business rules, no integration.
Business logic and cross-source integration happen exclusively in the Refined Layer.
Source changes are absorbed in Storage without cascading downstream;
business rule changes don't touch source-aligned models.

**Star Schemas in Delivery Only**
Kimball dimensional modeling is embraced in the Delivery Layer but explicitly excluded from Storage.
Star schemas are brittle for storage and hard to adapt when source processes change.
The PDP separates storage (source-aligned, resilient) from presentation (Kimball, user-friendly).

**YAML-Driven Pattern Application with PDP Macros**
Ingestion, STG, HIST, VER models and more are driven by YAML configuration blocks rather than copy-pasted SQL.
YAML enforces consistency across the team and lets the PDP macros generate optimized SQL automatically.
Pure SQL is used for one-off models where no repeatable pattern applies. This is common for the business logic in the ref layer.

**Hash-Based Keys (HKEY and HDIFF)**
Entity identity is managed with MD5 hashes of business key columns (HKEY) and payload columns (HDIFF).
Hash keys are deterministic, require no database sequences, support parallel processing,
and enable efficient change detection by comparing a single HDIFF value instead of column-by-column.

**Incremental HIST, Views for VER**
HIST is the only stateful component — materialized as an incremental table so only new changes
are processed on each run. VER is always a view on HIST, ensuring no data duplication and
automatic consistency. This directly enables the stateless principle for all downstream layers.

----

### Engineering Practices

**Core DataOps Principles**  
The PDP treats data engineering as software engineering:

- **Version control**: All code — SQL, YAML, configurations — in Git.
- **Branching strategy**: Feature branches and pull requests for all changes.
- **CI/CD**: Automated build and test on every PR commit.
- **Environment management**: Separate DEV, QA, and PROD environments. Developers work in
  personal dev schemas; changes are promoted through QA before reaching production.
- **Automated testing**: Tests what matters at every layer.
- **Code review**: Every change goes through peer review.
- **Documentation**: Treated as a first-class artifact — models, columns, and business rules
  documented in YAML and rendered via `dbt docs`.

**Testing strategy by layer:**

| Layer | Focus | Test types |
|---|---|---|
| **Ingestion** | Data completeness, freshness | `all_files_from_stage`, `file_order_is_correct`, freshness checks |
| **Storage** | Source data integrity | `not_null`, `unique`, column-level constraints on source tables |
| **Refined** | Transformation correctness | `expression_is_true`, singular tests (reconciliation), unit tests |
| **Delivery** | Contract compliance, data quality | Model contracts (enforced), data tests, column constraints, relationships |

----

### Comparison with Other Methodologies

The PDP positions itself as a synthesis, not a competitor:

- **vs. Kimball**: PDP uses star schemas in the Delivery Layer but rejects them as the primary
  storage model. Star schemas are designed around specific business processes, making them brittle
  for storage and hard to adapt when processes change.
- **vs. Inmon**: PDP shares Inmon's belief in a business-centered, integrated data warehouse and
  the importance of historical tracking, but uses a more pragmatic layered approach rather than
  a strict 3NF enterprise model.
- **vs. Data Vault 2.0**: PDP shares Data Vault's emphasis on historization and hash-based keys
  but simplifies the architecture. Instead of Hubs, Links, and Satellites, PDP uses a streamlined
  STG → HIST → VER pipeline that achieves similar goals with less complexity.
- **vs. Data Mesh**: PDP is a natural stepping stone toward a Data Mesh. Its domain-driven
  organization, modular architecture, and clear interfaces align with Data Mesh principles.
  The soft mesh approach lets you start centralized and evolve when ready.
- **vs. Medallion**: The PDP's Storage Layer is not just "raw data landing" — it actively
  historicizes and versions data with HKEY/HDIFF patterns. The layers have much clearer,
  more principled boundaries than Bronze/Silver/Gold.

----
### &#169;  Copyright 2022-2026 Roberto Zagni
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
