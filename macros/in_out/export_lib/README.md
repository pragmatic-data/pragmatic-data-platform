# Export Macros

The export library provides macros to export data from Snowflake tables or views to files
in an internal or external stage. It mirrors the ingestion library: where ingestion uses
`COPY INTO <table>`, export uses `COPY INTO <@stage>`.

For setup context (stages, file formats, naming macros) see the [in_out README](../README.md).

**Table of Contents**

- [**`run_table_export()`**](#run_table_export) macro
- |->> [Exported file organisation](#exported-file-organisation)
- |->> [Helper macros](#helper-macros)

## run_table_export()

The export process macro.  
Exports a dbt relation to files in a Snowflake stage, organised under a date-based path.
Supports flags to control idempotency, pre-export cleanup, and completion signalling
via a sentinel dummy file.

**Signature**:

```jinja
{{ pragmatic_data.run_table_export(
    table_ref,
    export_path_cfg,
    stage_cfg,
    flags,
) }}
```

**Parameters**:

| Parameter | Type | Description |
|-----------|------|-------------|
| `table_ref` | Relation | **Required.** dbt relation to export, e.g. `ref('RPT_POSITIONS_DAILY_VALUES')` |
| `export_path_cfg` | dict | **Required.** Path configuration — see keys below. |
| `stage_cfg` | dict | **Required.** Stage and file format configuration — see keys below. |
| `flags` | dict | Process behaviour flags — see keys below. |

**`export_path_cfg` keys**:

| Key | Default | Description |
|-----|---------|-------------|
| `export_path_base` | `<db>/<schema>/<table>/` | Base path prefix inside the stage. Include a trailing `/`. |
| `export_path_date_part` | today (`YYYY_MM_DD`) | Date string appended after `export_path_base`. Override for backfills or static paths. |
| `export_file_name_prefix` | `<table_name>__` | Prefix for exported file names. Snowflake appends a part number suffix automatically. |

**`stage_cfg` keys**:

| Key | Description |
|-----|-------------|
| `stage_name` | **Required.** Fully-qualified stage name, e.g. from `get_SOURCE_XXX_stage_fq_name()` |
| `format_name` | File format to use for the export. |

**`flags` keys**:

| Key | Default | Description |
|-----|---------|-------------|
| `only_one_export` | `false` | When `true`, skips the export if a sentinel file already exists in the target path. Use for idempotent exports. |
| `remove_folder_before_export` | `false` | When `true`, runs `REMOVE @<path>` before exporting to delete any existing files. |
| `create_dummy_file` | `false` | When `true`, writes an empty sentinel file after a successful export to signal completion. |

**Example**:

```jinja
{% macro export_RPT_POSITIONS_DAILY_VALUES() %}

{% set table_ref = ref('RPT_POSITIONS_DAILY_VALUES') %}
{% set yaml_config %}
export_path_cfg:
    export_path_base:        PORTFOLIO_REPORTING/positions/
    export_file_name_prefix: POSITIONS__

stage_cfg:
    format_name: "{{ get_PORTFOLIO_export_csv_ff_name() }}"
    stage_name:  "{{ get_PORTFOLIO_export_stage_name() }}"

flags:
    only_one_export:             true
    remove_folder_before_export: true
    create_dummy_file:           true
{% endset %}
{%- set cfg_dict = fromyaml(yaml_config) -%}

{{ pragmatic_data.run_table_export(
    table_ref       = table_ref,
    export_path_cfg = cfg_dict['export_path_cfg'],
    stage_cfg       = cfg_dict['stage_cfg'],
    flags           = cfg_dict['flags'],
) }}

{% endmacro %}
```

Run with: `dbt run-operation export_RPT_POSITIONS_DAILY_VALUES`

## Exported file organisation

Files are exported to:
`<stage_name>/<export_path_base><date_part>/<file_name_prefix><part_N>.csv.gz`

The date part defaults to today in `YYYY_MM_DD` format and can be overridden to support
backfills or fixed export paths (e.g. for integrations that always read from the same path).

With `only_one_export: true` and `create_dummy_file: true` the workflow is:

1. Check for a sentinel file in the target path — skip everything if found
2. Optionally remove all existing files in the path
3. Export data to files
4. Write a sentinel file to mark the export as complete

This makes the export safely re-runnable: re-triggering on the same day will not
overwrite already-exported files.


## Helper macros

#### export_to_stage_sql(table_name, stage_with_path, format_name)

Generates the `COPY INTO @<stage_path>` SQL statement, wrapped in a transaction.
Called internally by `run_table_export()`.

#### check_dummy_exists(stage_name, export_path)

Returns `true` if a sentinel file exists in the target path.
Called by `run_table_export()` when `only_one_export: true`.

#### export_dummy_file_sql(stage_with_path)

Generates SQL to write an empty sentinel file to the stage path.
Called by `run_table_export()` when `create_dummy_file: true`.

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
