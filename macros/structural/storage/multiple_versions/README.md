# Storage — Multiple Versions Macros

These are the **default historization macros** for the Storage layer.

Use them when a source entity can exist in multiple distinct states over time (versions) and you need to keep all of
them — for auditing, point-in-time analysis, building slowly-changing dimensions (SCD) or troubleshooting.

Or just use them as they offer exceptional ease of use, great performance and immense resilience to data hiccups:
with a single run you can ingest a day or ten years of data without loosing a single version.
For better versioning list the columns to sort the input from the STG model in the desired version order,
or stay with the default of sorting by the ingestion timestamp.

All the PDP historization patterns provide insert-only hsitorization: new versions are added and nothing is ever
changed once it lands in the HIST table. This provides both the best possible performance on column oriented
databases, and the guarantee that your data is safe and prefectly auditable.

For context and a full example on the overall STG → HIST → VER pattern, see the
[Storage layer README](../README.md).

**Table of Contents**
- [When to use](#when-to-use)
- [`save_history_with_multiple_versions()`](#save_history_with_multiple_versions)  
  Main HIST macro pattern in the PDP. Identifies and persists any number of new versions from a STG input into a HIST table.
- [`versions_from_history_with_multiple_versions()`](#versions_from_history_with_multiple_versions)  
  User friendly and functional interface to the contents of the HIST table. Provides IS_CURRENT, VALID_FROM/VALID_TO, ...
- [`current_from_history_with_multiple_versions()`](#current_from_history_with_multiple_versions-helper)  
   Helper macro. Mostly for internal use.

## When to use

**Pretty much always.** MUST use the multiple-versions pattern when:

- You want to be able to load any lenght of history in a single run, correctly storing all changes.
- An entity changes over time and all past states are meaningful (e.g. a security whose name changes, a position that evolves daily, an order that gets amended)
- You need/want to keep an accurate audit of all the changes you have received (for compliance, security, ... or just debugging).
- You are building an accurate slowly-changing dimension in the Delivery layer and you do not want to miss any version.
- You need accurate point-in-time joins using the `VALID_FROM`/`VALID_TO` columns fro VER in a `ASOF JOIN`

This pattern is quick and very resilient, giving you for free a perfect insert-only history that do not miss any change. Most of the time is the right choice.

If you need to track logical deletions or precise auditing is optional and your table is so big (billions of rows) that sorting might be slow, see
[single_version/README.md](../single_version/README.md).

---

## save_history_with_multiple_versions

Appends rows to the HIST table only when the entity's HDIFF changes — representing a
genuine state change (a new version). On normal runs it reads the current state of each key from
HIST, compares to incoming STG data, and stores only rows that represent a change.
On full-refresh runs it rebuilds HIST from scratch. After the initial development period,
it is suggested to lock the full-refresh feature for HIST tables.

The macro handles multiple versions of the same key arriving in a single input batch
(e.g. the same trade exported twice with slightly different attributes): it uses a `LAG`
window function over `sort_expr` to detect intra-batch changes and correctly stores
only the first copy of each new version in the batch. The first (oldest) version of the input
batch is checked against the latest (most recent) version of the HIST table.

**Signature**:
```jinja
{{ pragmatic_data.save_history_with_multiple_versions(
    input_rel,
    key_column,
    diff_column,
    history_rel             = this,
    sort_expr               = var('pdp.sort_expr', 'INGESTION_TS_UTC'),
    load_ts_column          = var('pdp.load_ts_column', 'INGESTION_TS_UTC'),
    high_watermark_column   = var('pdp.high_watermark_column', 'INGESTION_TS_UTC'),
    high_watermark_test     = var('pdp.high_watermark_test', '>'),
    input_filter_expr       = 'true',
    history_filter_expr     = 'true',
) }}
```

**Parameters**:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `input_rel` | Relation | — | **Required.** The STG model to read from, e.g. `ref('STG_ORDERS')` |
| `key_column` | string | — | **Required.** Name of the HKEY column (surrogate key) |
| `diff_column` | string | — | **Required.** Name of the HDIFF column (change fingerprint) |
| `history_rel` | Relation | `this` | The HIST table to compare against. BETTER LEFT ALONE. Override to merge history from an external source or for special test setups. |
| `sort_expr` | string | `INGESTION_TS_UTC` | Expression to order versions within the same key. Use a timeline that works well with the high-watermark logic. |
| `load_ts_column` | string | `INGESTION_TS_UTC` | Column holding the ingestion timestamp. Used for incremental high-watermark logic. |
| `high_watermark_column` | string | `INGESTION_TS_UTC` | Column used for the per-key high-watermark filter. Rows with a value greater than the current HIST max are loaded. |
| `high_watermark_test` | string | `>` | Comparison operator for the high-watermark filter (`>` or `>=`). |
| `input_filter_expr` | string | `'true'` | Additional SQL WHERE condition on the input before comparison. |
| `history_filter_expr` | string | `'true'` | Additional SQL WHERE condition when reading current state from HIST. |

**Column added by the macro** to every row stored in HIST:

| Column | Description |
|--------|-------------|
| `HIST_LOAD_TS_UTC` | Timestamp of the dbt run that stored this row (`run_started_at`) |

**Example** — from [STONKS](https://github.com/RobMcZag/stonks):
```jinja
{{ config(materialized='incremental') }}

{{ pragmatic_data.save_history_with_multiple_versions(
    input_rel   = ref('STG_IB_TRADES'),
    key_column  = 'TRADE_HKEY',
    diff_column = 'TRADE_HDIFF',
    sort_expr   = 'RECORD_SOURCE, FILE_ROW_NUMBER',
) }}
```

The `sort_expr` is optional, defaulting to the `pdp.sort_expr` variable or the `INGESTION_TS_UTC` column, if the variable is not set.  
When provided it must sort effectively the changes in the "new input batch" created by the per-key high-watermark column values in the HIST and in the STG.
Technical metadata columns like (`RECORD_SOURCE`, `FILE_ROW_NUMBER`) work well as they align naturally with the defualt high-watermark on the technical timeline
and the row number break ties between rows that come from the same file.
To validate your sorting expressino consider the case when you restart the ingestion and load after a stop and you need to process many of your usual (micro- or macro-)batches.
You can use a business effectivity date like (`EFFECTIVITY_DATE`) as the primary sort key, as it is only used for sorting the "new inputs",
it does not need to be compared with the high-watermark column to determine what are the new columns to be processed.

---

## versions_from_history_with_multiple_versions

This is your simple and functional interface to the contents of the HIST table.

It reads a HIST table and enriches each row with temporal metadata, turning raw history into
a fully-featured slowly-changing dimension (SCD) view. Typically used as the `VER_` view
that sits directly on top of the corresponding HIST table.

The single column passed as **`version_sort_column`** is the column used to pick the values for the VALID_FROM/VALID_TO columns.  
It is also the first column in sorting of the versions out of the HIST table, followed by the technical timeline and the optional `extra_sort_columns`.  
It is common that this is a column on the business timeline, like `EFFECTIVITY_DATE`, so that you sort your history on that business timeline.

It is possible to build multiple VER models over the same HIST table to have different views along different timelines.

**Signature**:
```jinja
{{ pragmatic_data.versions_from_history_with_multiple_versions(
    history_rel,
    key_column,
    diff_column,
    version_sort_column = var('pdp.sort_expr', 'INGESTION_TS_UTC'),
    load_ts_column      = var('pdp.load_ts_column', 'INGESTION_TS_UTC'),
    hist_load_ts_column = var('pdp.hist_load_ts_column', 'HIST_LOAD_TS_UTC'),
    selection_expr      = '*',
    history_filter_expr = 'true',
    extra_sort_columns  = none,
) }}
```

**Parameters**:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `history_rel` | Relation | — | **Required.** The HIST model, e.g. `ref('HIST_ORDERS')` |
| `key_column` | string | — | **Required.** Name of the HKEY column |
| `diff_column` | string | — | **Required.** Name of the HDIFF column |
| `version_sort_column` | string | `INGESTION_TS_UTC` | Column that defines the business order of versions. Should match the `sort_expr` used in the HIST macro. |
| `load_ts_column` | string | `INGESTION_TS_UTC` | Ingestion timestamp column. Used for `ingestion_batch` numbering. |
| `hist_load_ts_column` | string | `HIST_LOAD_TS_UTC` | Load timestamp column (when the row was written to HIST). Used for `load_batch` numbering. |
| `selection_expr` | string | `'*'` | Columns to select from HIST. Override to project only specific columns. |
| `history_filter_expr` | string | `'true'` | SQL WHERE condition to filter rows from HIST before adding metadata. |
| `extra_sort_columns` | list or string | `none` | Additional columns for tie-breaking the version order. |

**Columns added by the macro** (window functions — no new data):

| Column | Description |
|--------|-------------|
| `VERSION_COUNT` | Total number of versions for this key |
| `VERSION_NUMBER` | Sequential version index for this key (1 = oldest) |
| `INGESTION_BATCH` | Dense rank of the ingestion timestamp among all ingestion timestamps for this key |
| `LOAD_BATCH` | Dense rank of the HIST load timestamp for this key |
| `DIM_SCD_HKEY` | Stable SCD surrogate key: `MD5(diff_column, version_sort_column)`. Use as PK in Delivery SCD dimensions. |
| `VALID_FROM` | Value of `version_sort_column` for this version |
| `VALID_TO` | Value of `version_sort_column` for the next version; `'9999-09-09'` for the current version (configurable via `pdp.end_of_time`) |
| `IS_CURRENT` | `true` for the most recent version (`VERSION_NUMBER = VERSION_COUNT`) |

**Example** — from [STONKS](https://github.com/RobMcZag/stonks):
```jinja
{{ pragmatic_data.versions_from_history_with_multiple_versions(
    history_rel         = ref('HIST_IB_TRADES'),
    key_column          = 'TRADE_HKEY',
    diff_column         = 'TRADE_HDIFF',
    version_sort_column = 'EFFECTIVITY_DATE',
) }}
```

The `version_sort_column` drives `VALID_FROM`, `VALID_TO`, and the version ordering.
Setting it to the business effectivity date (rather than `INGESTION_TS_UTC`) ensures that
the temporal window reflects business reality, not the technical loading sequence.

---

## current_from_history_with_multiple_versions (helper)

Internal helper that returns only the most recent row for each key from a HIST table that
may contain multiple versions per ingestion batch. Used internally by
`save_history_with_multiple_versions()` during incremental runs to read the current state
for change comparison.

Available for advanced use cases where you need the current state of a multiple-version
HIST model outside of the standard incremental logic or if you have not built a VER model.

As you should build the VER model on top of all your HIST tables, you will use the `IS_CURRENT` column
created by the VER when you want to keep only the current versions.

**Signature**:
```jinja
{{ pragmatic_data.current_from_history_with_multiple_versions(
    history_rel,
    key_column,
    sort_expr           = var('pdp.sort_expr', 'INGESTION_TS_UTC'),
    load_ts_column      = var('pdp.load_ts_column', 'INGESTION_TS_UTC'),
    qualify_function    = '(rn = cnt) and rank',
    selection_expr      = '*',
    history_filter_expr = 'true',
) }}
```

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
