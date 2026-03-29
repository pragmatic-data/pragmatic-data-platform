# Storage — Single Version Macros

The the suggested historization macro for the Storage layer is the [`save_history_with_multiple_versions()`](../multiple_versions/README.md) macro.

Use the macros in this page when you need to record logical deletions alongside new and changed rows
or for a few special cases as discussed below.

For context on the overall STG → HIST → VER pattern, see the [Storage layer README](../README.md).

**Table of Contents**
- [When to use which macro](#when-to-use-which-macro)
- [save_history](#save_history)
- [save_history_with_deletion](#save_history_with_deletion)
- [save_history_with_deletion_from_list](#save_history_with_deletion_from_list)
- [current_from_history (helper)](#current_from_history-helper)

## When to use which macro

The macros in this page are needed/useful only in specific situations. They need to receive in input only one change per key, making them limited compared to the [save_history_with_multiple_versions](../multiple_versions/README.md) that can receive any number of changes.

In general it is suggested to use the [multiple_versions](../multiple_versions/README.md) pattern of historization.
It allows to ingest data for arbitrary periods without losing any change, at the "cost" of providing a list of column to sort
these changes.

Use the macros in this page when:

- you need to recognize deleted instances
- you have very big inputs (hundreds of million rows) and avoiding their sorting saves enough time to accept the limitation
  of limiting your input to the most current version, with the risk of losing intermediate changes
- your inputs are immutable/rarely mutable transactions, so you want the flexibility to be able to run the ingestion as many
  time as you wish, even on overlapping periods, without loading duplicates, and recognize and store the occasional variation.

| Macro | Use case |
| ----- | -------- |
| `save_history` | Entities that can change over time when quick change detection is all you need — no deletion tracking. |
| `save_history_with_deletion` | Entities that can disappear from the source. A `deleted = true` row is added when a key is no longer present in the input. This requires a reliable full export as input. |
| `save_history_with_deletion_from_list` | Deletions are communicated as a separate feed (an explicit list of keys to mark deleted) rather than by absence from the source. This allows to mark deletions also for delta inputs. |

Please note that if you have a CDC that can add a status column from which you can identify that a row is deleted, you can use the normal historization process as the "deletion" becomes just a normal change of the instance, once you add the status column in the HDIFF.

Please note that you can use the [`versions_from_history_with_multiple_versions()`](../multiple_versions/README.md#versions_from_history_with_multiple_versions) macro to provide the `IS_CURRENT`, `VALID_FROM`/`VALID_TO` and
other useful columns on top of any HIST model. As usual provide a time based column to pick the versioning timeline.

---

## save_history

Appends new rows to HIST only when the entity's HDIFF changes — one row per unique state, no deletion tracking.
Suitable for reference data, lookup tables, and any entity where you want to track changes but do not need
an accurate audit and you are fine to limit your input to the latest change only.
It does not detect or record deletions.

Unlike [`save_history_with_multiple_versions()`](../multiple_versions/README.md), this macro does not support
multiple versions of the same key arriving in the same input batch. If accurate auditing and
intra-batch versioning is needed, you must use the multiple-versions macro.

**Signature**:

```jinja
{{ pragmatic_data.save_history(
    input_rel,
    key_column,
    diff_column,
    load_ts_column          = var('pdp.load_ts_column', 'INGESTION_TS_UTC'),
    high_watermark_column   = var('pdp.high_watermark_column', 'INGESTION_TS_UTC'),
    high_watermark_test     = var('pdp.high_watermark_test', '>'),
    input_filter_expr       = 'true',
    history_filter_expr     = 'true',
    order_by_expr           = none,
    has_mutable_entities    = true,
    history_rel             = this,
) }}
```

**Parameters**:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `input_rel` | Relation | — | **Required.** The STG model input. |
| `key_column` | string | — | **Required.** Name of the HKEY column. |
| `diff_column` | string | — | **Required.** Name of the HDIFF column. |
| `load_ts_column` | string | `INGESTION_TS_UTC` | Ingestion timestamp column. |
| `high_watermark_column` | string | `INGESTION_TS_UTC` | Column for the **global** high-watermark filter: only rows with a value greater than the MAX value in the HIST are considered for loading. |
| `high_watermark_test` | string | `>` | Comparison operator for the high-watermark (`>` or `>=`). |
| `input_filter_expr` | string | `'true'` | Additional SQL WHERE condition on input rows. |
| `history_filter_expr` | string | `'true'` | Additional SQL WHERE condition when reading from HIST. |
| `order_by_expr` | string | `none` | Optional `ORDER BY` expression on the output. |
| `has_mutable_entities` | bool | `true` | Set to `false` for append-only / immutable entities (e.g. events, log entries) to simplify the history lookup and improve performance. |
| `history_rel` | Relation | `this` | Override to merge history from an external HIST table or for test setups. |

---

## save_history_with_deletion

Like `save_history`, but also writes a deletion marker row (with `deleted = true`) to HIST
when a key that was previously active is no longer present in the current input. The
deletion row copies non-key attribute columns from the last known state of the entity.

Use this when the source system communicates deletions implicitly — i.e. by omitting a
previously existing record from its next export. This macro needs a full export as input.

**Signature**:
```jinja
{{ pragmatic_data.save_history_with_deletion(
    input_rel,
    key_column,
    diff_column,
    load_ts_column      = var('pdp.load_ts_column', 'INGESTION_TS_UTC'),
    effectivity_column  = none,
) }}
```

**Parameters**:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `input_rel` | Relation | — | **Required.** STG model with the current source snapshot. |
| `key_column` | string | — | **Required.** HKEY column name. |
| `diff_column` | string | — | **Required.** HDIFF column name. |
| `load_ts_column` | string | `INGESTION_TS_UTC` | Ingestion timestamp column. |
| `effectivity_column` | string | `none` | If set, the deletion row's effectivity column is overwritten with `run_started_at` to correctly record when the deletion was detected along the timeline. |

**Column added**:

| Column | Description |
|--------|-------------|
| `deleted` | `false` for normal rows, `true` for deletion marker rows |

---

## save_history_with_deletion_from_list

Like `save_history_with_deletion`, but deletions are driven by an explicit list of keys
supplied as a separate relation (`del_rel`), rather than by absence from the main source.

Use this when:

- Deletions are communicated as a separate file or feed (e.g. a daily delete list)
- You cannot rely on absence from the main source as the deletion signal (e.g. when you get a delta export)
- The source may omit records for reasons other than deletion (e.g. provides only a subset of rows, like active in the last 3 months)

Important: if a key appears in both `input_rel` and `del_rel`, the outcome depends on its
current state in HIST. See the source file (`save_history_with_deletion_from_list.sql`)
for detailed notes on key priority rules.

**Signature**:
```jinja
{{ pragmatic_data.save_history_with_deletion_from_list(
    input_rel,
    key_column,
    diff_column,
    del_rel,
    del_key_column,
    load_ts_column          = var('pdp.load_ts_column', 'INGESTION_TS_UTC'),
    high_watermark_column   = var('pdp.high_watermark_column', 'INGESTION_TS_UTC'),
    high_watermark_test     = var('pdp.high_watermark_test', '>'),
    input_filter_expr       = 'true',
    history_filter_expr     = 'true',
    order_by_expr           = none,
    effectivity_column      = none,
) }}
```

**Parameters**:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `input_rel` | Relation | — | **Required.** STG model with rows to store. |
| `key_column` | string | — | **Required.** HKEY column in `input_rel`. |
| `diff_column` | string | — | **Required.** HDIFF column in `input_rel`. |
| `del_rel` | Relation | — | **Required.** Model or source containing the list of keys to delete. |
| `del_key_column` | string | — | **Required.** Column in `del_rel` holding the keys to mark as deleted. |
| `load_ts_column` | string | `INGESTION_TS_UTC` | Ingestion timestamp column. |
| `high_watermark_column` | string | `INGESTION_TS_UTC` | High-watermark column for incremental filtering. |
| `high_watermark_test` | string | `>` | Comparison operator for the high-watermark. |
| `input_filter_expr` | string | `'true'` | Additional filter on input rows. |
| `history_filter_expr` | string | `'true'` | Additional filter when reading from HIST. |
| `order_by_expr` | string | `none` | Optional `ORDER BY` on the output. |
| `effectivity_column` | string | `none` | If set, the deletion row's effectivity column is overwritten with `run_started_at` to correctly position the deletion in the timeline. |

---

## current_from_history (helper)

Internal helper that returns the most recent row for each key from any HIST table, using
a `QUALIFY` window function. Used internally by `save_history` and the
`save_history_with_deletion*` macros during incremental runs.

Also available for advanced use cases where you need to read just the current state of
a single-version HIST model.

**Signature**:
```jinja
{{ pragmatic_data.current_from_history(
    history_rel,
    key_column,
    selection_expr      = '*',
    load_ts_column      = 'LOAD_TS_UTC',
    history_filter_expr = 'true',
    qualify_function    = 'row_number',
) }}
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `history_rel` | Relation | — | **Required.** The HIST table to query. |
| `key_column` | string | — | **Required.** HKEY column name. |
| `selection_expr` | string | `'*'` | Columns to select. |
| `load_ts_column` | string | `'LOAD_TS_UTC'` | Column used for `ORDER BY` in the `QUALIFY` window to pick the latest row. |
| `history_filter_expr` | string | `'true'` | Additional WHERE condition. |
| `qualify_function` | string | `'row_number'` | Window function used in the `QUALIFY` clause. |

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
