# Storage Layer

The Storage layer is the foundation of the Pragmatic Data Platform. Its purpose is to
hold the complete, immutable history of every entity that enters the platform — making
data safe, auditable, and fully rebuildable from raw sources at any point in time.

**Table of Contents**
- [The STG → HIST → VER pattern](#the-stg--hist--ver-pattern)
- [HKEY and HDIFF: hash-based keys](#hkey-and-hdiff-hash-based-keys)
- [Historization patterns](#historization-patterns)
- [End-to-end example](#end-to-end-example)
- [Sub-directories](#sub-directories)

## The STG → HIST → VER pattern

Every source entity in the Storage layer is modelled as three objects:

| Object | Prefix | Materialisation | Role |
|--------|--------|-----------------|------|
| **Stage** | `stg_` | View | Reads from the landing table, renames/casts columns, computes business keys (HKEY) and change-detection hashes (HDIFF). Input to HIST. |
| **History** | `hist_` | Incremental table | Append-only store of every distinct version of an entity. New rows are appended only when the HDIFF changes. |
| **Versions** | `ver_` | View | Reads HIST and adds temporal metadata: `VALID_FROM`, `VALID_TO`, `IS_CURRENT`, `VERSION_NUMBER`, `VERSION_COUNT`. |

```
Landing Table (LT)
      │
      ▼ optional filter (landing_filter)
   STG (view)            ← stage()
      │
      ▼ append new versions only
   HIST (incremental)    ← save_history_with_multiple_versions()
      │
      ▼ add validity window
   VER (view)            ← versions_from_history_with_multiple_versions()
```

STG is the only model that reads directly from a landing table. HIST and VER never read
raw data — they consume the clean, keyed output of STG.

Both HIST and VER derive their names from the STG model they come from:
`STG_ORDERS` → `HIST_ORDERS` → `VER_ORDERS`. Each entity is a self-contained triplet.

**Special cases**  

In special cases we can use one additional light model to align the pipeline with the grain or sorting of the entities we want to historicize:

- SRC models are placed before the STG model when reading from a LT holding semi-structured data with arrays.
  The SRC model is used to just apply FLATTEN on the array so that the STG can process one element of the array at a time.

- DELTA models are used when the LT contains embedded entities that we want to historicize separately from the main entity
  and the two entities can change at different times. This is common when processing ERP and application reports.
  The STG model can prepare all the columns, but sometimes we need to add a DELTA model between the STG and HIST model of the embedded entity,
  to reorder the rows or to filter out undesired rows with only changes in the main entity.

Another common situation is when you want to historicize separately, in two HIST tables, some contents of a single LT.
In such a situation you can just hve two parallel pipelines, with two STG reading what they need fro the LT and their HIST
and VER models after each STG.

Even when adding an extra model an historization pipeline must never have JOIN operations.

## HKEY and HDIFF: hash-based keys

Using HKEY and HDIFF columns to identify and compare versions of entities makes the application of patterns trivial,
supports efficient parallel processing, greatly simplifies the reasoning about entities and the code of the macros
implementing the patterns. It also improves the pipeline performance, that is a great, albeit secondary, benefit.

**HKEY (Hash Key)** is the surrogate key for an entity. It is computed as an MD5 hash
of the business key columns and is stable across all time: the same entity always has the
same HKEY. Used as the primary key in HIST and as the join key everywhere in the platform,
but if the entity has a single column in the business key, you are free to use that instead.

**HDIFF (Hash Diff)** is the change-detection fingerprint. It is an MD5 hash of all
content columns (including the business key, excluding metadata columns). Two rows
with the same HKEY but different HDIFF represent two distinct versions of the same entity.
By having the business key in the HDIFF we can improve the performance of our HIST tables
by making a single column comparison to decide what needs to be stored.

Both are computed in STG by `stage()` via the `hashed_columns` config key:

```yaml
hashed_columns:
    ORDER_HKEY:           # surrogate key — hash of business key columns
        - ORDER_ID

    ORDER_HDIFF:          # change fingerprint — hash of all content columns
        - STATUS
        - TOTAL_PRICE
        - CUSTOMER_ID
        - UPDATED_AT
```

The hash function is `MD5_BINARY(CONCAT_WS('-|-', col1::text, col2::text, ...))` chains the columns
you name using a seprator and `'-***-'` as the null placeholder, ensuring consistent hashing regardless of null handling.
All column passed to a hash must be cast-able to text.

The STG model of one entity can have multiple HKEYs when it participates in multiple relationships:
one for itself and one for each other entity it has a foreign key towards.
(e.g. a stock trade has its own `TRADE_HKEY`/`TRADE_HDIFF` plus a `SECURITY_HKEY` as a foreign key).

## Historization patterns

Choose the right HIST macro based on the entity's lifecycle needs.
All HIST patterns support using the VER model to add `VALID_FROM`/`VALID_TO` versioning,
the `IS_CURRENT` column and the `DIM_SCD_KEY` used to provide slowly-changing dimensions (SCD)
of type 2 in the Delivery layer. Plus a few other handy metadata columns.

### Multiple versions (default)

This is the general purpose macro that stores all new changes at every run, making your pipeline simple and reliable.
**MUST use when** you must keep all the distinct changes an entity goes through over time.
E.g. a trade that gets amended, a security whose attributes change, a position that evolves daily.
This pattern allows you to ingest any number of changes (versions) at every historization run.
To achieve that you must provide a list of one or more column names to sort the rows coming from the STG table.

→ See [multiple_versions/README.md](multiple_versions/README.md)

Core macros: `save_history_with_multiple_versions()`, `versions_from_history_with_multiple_versions()`

### Single version

**Use when** you are fine to record only one change (the current or latest version) for every load cycle
or when entities are deleted from the source and you need to record that as a logical deletion.
To recognize deletions and record them in the HIST you either need a full export from the source
or a model providing a list of deleted keys.
You must send only one version per instance to these HIST models. Use the `remove_duplicates` parameter
in the YAML config of the STG model to be sure to keep the latest version (if you risk having more than one).

→ See [single_version/README.md](single_version/README.md)

Macros: `save_history()`, `save_history_with_deletion()`, `save_history_with_deletion_from_list()`

### Technical timeline considerations

The process of receiving and historicizing data goes through a sequence of steps, each performed on the
newly available rows from the source. These rows get logically grouped in a batch of rows that have been processed
at the same time for a specific step.

It is useful to have some names for these logical grouping of source rows based on the technical timeline:

- export batch - the rows exported at the same time (usually in a single file, but not necessarily)
  Usually recognized by the FILE_LAST_MODIFIED_TS_UTC column, loaded as metadata by the PDP ingestion.

- ingestion batch - the rows ingested at the same time in the landing table. It contains one or more export batches.
  Usually recognized by the INGESTION_TS_UTC column, added as metadata in the LT by the PDP ingestion.

- input batch - the rows in the landing table newer than the per-key current row in the HIST table.
  These are the only rows processed by the HIST model when you leave enabled the high water mark ingestion.
  It contains zero or more ingestion batches. Once historicized it becomes a hist batch.

- hist batch - the rows historicized at the same time in the HIST table. It contains one or more ingestion batches.
  Usually recognized by the HIST_LOAD_TS_UTC column, added by the HIST macro.
  It is logically divided in one or more load batches (based on their definition).

- load batch - a flexible definition for your HIST table, allowing you to select how to partition the historicized data
  to select the current version. This is usually based on one of the elements in the technical timeline described aboce,
  most of the times it is the same as ingestion batch. This is generally a good logical partitioning choice,
  but requires some extra sort column (sort_expr) to deterministically sort versions in initial loads, restarts and multiversion loads.


## End-to-end example

The following illustrates the full STG → HIST → VER pattern for a **Trades** entity,
adapted from the [STONKS example project](https://github.com/RobMcZag/stonks).

**STG_IB_TRADES.sql** — stage view, renames source columns, computes keys:
```jinja
{%- set source_model = source('IB', 'TRADES') %}
{%- set configuration -%}
source:
    columns:
        include_all: false
    where: STARTSWITH(CLIENTACCOUNTID, 'U')

calculated_columns:
    - Transaction_ID: TransactionID
    - EFFECTIVITY_DATE: DateTime::TIMESTAMP_NTZ
    - RECORD_SOURCE: FROM_FILE
    - FILE_ROW_NUMBER
    - INGESTION_TS_UTC

hashed_columns:
    TRADE_HKEY:
        - Transaction_ID
    TRADE_HDIFF:
        - Transaction_ID
        - ASSET_CLASS
        - Quantity
        - Trade_Price_FX
        - SECURITY_CODE
{%- endset -%}

{%- set cfg = fromyaml(configuration) -%}

{{- pragmatic_data.stage(
    source_model       = source_model,
    source             = cfg['source'],
    calculated_columns = cfg['calculated_columns'],
    hashed_columns     = cfg['hashed_columns'],
) }}
```

**HIST_IB_TRADES.sql** — incremental table, appends only changed versions:
```jinja
{{ config(materialized='incremental') }}

{{ pragmatic_data.save_history_with_multiple_versions(
    input_rel   = ref('STG_IB_TRADES'),
    key_column  = 'TRADE_HKEY',
    diff_column = 'TRADE_HDIFF',
    sort_expr   = 'EFFECTIVITY_DATE, Transaction_ID, RECORD_SOURCE, FILE_ROW_NUMBER',
) }}
```

**VER_IB_TRADES.sql** — view adding temporal metadata to each version:
```jinja
{{ pragmatic_data.versions_from_history_with_multiple_versions(
    history_rel         = ref('HIST_IB_TRADES'),
    key_column          = 'TRADE_HKEY',
    diff_column         = 'TRADE_HDIFF',
    version_sort_column = 'EFFECTIVITY_DATE',
) }}
```

The resulting VER model adds these columns to every row:

| Column | Description |
|--------|-------------|
| `VALID_FROM` | Effective date of this version |
| `VALID_TO` | Effective date of the next version (`9999-09-09` for the current one) |
| `IS_CURRENT` | `true` for the most recent version of each key |
| `VERSION_NUMBER` | Sequential version index (1 = oldest) |
| `VERSION_COUNT` | Total number of versions for this key |
| `DIM_SCD_HKEY` | Stable SCD surrogate key usable as a PK in slowly-changing dimensions |

## Sub-directories

| Directory | Contents |
|-----------|----------|
| [stage/](stage/README.md) | `stage()` and `landing_filter()` — build and filter STG models |
| [multiple_versions/](multiple_versions/README.md) | `save_history_with_multiple_versions()`, `versions_from_history_with_multiple_versions()` |
| [single_version/](single_version/README.md) | `save_history()`, `save_history_with_deletion()`, `save_history_with_deletion_from_list()` |

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
