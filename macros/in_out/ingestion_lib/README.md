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
The macros in this folder are the evolution of the original macros presented in
"Data Engineering with dbt" (Packt, 2023).

They generate the SQL code to perform the operations required during ingestion:
1. Landing Table creation (`CREATE TABLE IF NOT EXISTS`)
2. Ingestion of data from files (`COPY INTO`)
3. Optional cleanup of old data from the landing table (DELETE)

The recommended way to use them today is via **`run_file_ingestion()`** — a single
process macro that handles the full ingestion cycle from a compact YAML config.

### Current, YAML-based Landing Table ingestion

Create one `.sql` file per landing table (name it after the table, e.g. `RAW_ORDERS.sql`,
placed in a subfolder of `macro/` or an `ingestion/` folder added to `macro-paths`).
Define a macro that encodes the config as YAML and calls `run_file_ingestion()`.

```
{% macro load_RAW_ORDERS(recreate_table=false) %}

{%- set yaml_str -%}
ingestion:
    pattern:     '.*/raw_orders/.*/RAW_ORDERS.*[.]csv.gz'
    stage_name:  "{{ get_SOURCE_XXX_stage_fq_name() }}"
    format_name: "{{ get_SOURCE_XXX_csv_format_fq_name() }}"

landing_table:
    db_name:     "{{ get_SOURCE_XXX_ingestion_db_name() }}"
    schema_name: "{{ get_SOURCE_XXX_ingestion_schema_name() }}"
    table_name:  RAW_ORDERS
    columns:
        - ORDERKEY:   NUMBER NOT NULL
        - CUSTOMERKEY: NUMBER
        - ORDERSTATUS: TEXT
        - TOTALPRICE:  NUMBER(18,4)
        - ORDERDATE:   DATE
    cleanup:                      # optional — omit to skip cleanup
        keep_n_batches: 7         # keep last 7 ingestion batches
        # keep_days: 30           # alternative: keep last 30 days (mutually exclusive)
{%- endset -%}

{%- set metadata_dict = fromyaml(yaml_str) -%}

{% do pragmatic_data.run_file_ingestion(
        landing_table_dict = metadata_dict['landing_table'],
        ingestion_dict     = metadata_dict['ingestion'],
        recreate_table     = recreate_table
) %}

{% endmacro %}
```

Simple getter macros abstract away DB/schema/stage names so the config stays DRY.

### Original (legacy), SQL-based Landing Table ingestion

The original approach used two manually written macros per landing table — one for the
`CREATE TABLE` and one wrapping `COPY INTO`. Configuration was expressed as macro
parameters rather than YAML, resulting in significant copy-paste repetition.
This pattern is kept here for reference only; **new tables should use `run_file_ingestion()`**.

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
    {% do run_query(RAW_ORDERS_create_table_sql(full_table_name)) %}
    {% do run_query(ingest_into_landing_sql(
            full_table_name,
            field_count      = field_count,
            file_pattern     = pattern,
            full_stage_name  = stage_name,
            full_format_name = format_name
    )) %}
{%- endmacro %}
```

## Ingestion Process Macros

Process macros encapsulate the full ingestion cycle. They call the Base Macros internally
and log progress. Run via `dbt run-operation`.

#### run_file_ingestion(...) ✅ recommended
Creates the landing table if it doesn't exist, runs `COPY INTO` using named field
expressions, and — if the `cleanup` key is present in `landing_table_dict` — removes
old rows from the landing table after ingestion.
Supports `add_file_content_key` for deduplication by file hash.

#### run_clean_landing_table(...)
Standalone operation: reads the `cleanup` key from `landing_table_dict` and deletes
rows older than the configured threshold. Logs how many rows were deleted.
Called automatically by `run_file_ingestion()` when `cleanup` is configured; also
available as an independent `dbt run-operation` for one-off or scheduled cleanup.

#### run_CSV_ingestion(...) ⚠️ deprecated
> **Deprecated** — use `run_file_ingestion()` instead.
> Kept for backward compatibility. Will be removed in a future major version.

Creates one landing table (if not exists) and ingests CSV files using positional
column references (`$1, $2, ...`). Column order must match the file format definition.

#### run_semi_structured_ingestion(...) ⚠️ deprecated
> **Deprecated** — use `run_file_ingestion()` instead.
> Kept for backward compatibility. Will be removed in a future major version.

Creates one landing table (if not exists) and ingests semi-structured files
(JSON, Parquet, Avro) using Snowflake's `$1:field_name::TYPE` path syntax.

## Ingestion Base Macros (helpers/)

Internal macros used by the process macros above. Available for advanced use cases
where the standard process macros don't cover a specific requirement.

#### create_landing_table_sql(...)
Generates `CREATE TRANSIENT TABLE ... IF NOT EXISTS` (or `CREATE OR REPLACE` when
`recreate_table=true`) from a `landing_table_dict`. Automatically appends the four
metadata columns: `FROM_FILE`, `FILE_ROW_NUMBER`, `FILE_LAST_MODIFIED_TS_UTC`,
`INGESTION_TS_UTC`.

#### landing_table_fqn(...)
Returns the fully-qualified table name (`db.schema.table`) from a `landing_table_dict`.
Defaults `db_name` to `target.database` if not provided. Raises a compiler error if
`schema_name` or `table_name` are missing.

#### clean_landing_table_sql(...)
Generates a `DELETE FROM` statement that removes rows older than the configured
threshold. Reads all parameters from `landing_table_dict.cleanup`:
- `keep_n_batches` — deletes rows not in the last N distinct ingestion timestamps
- `keep_days` — deletes rows older than N days (relative to `CURRENT_DATE()`)
- `from_date` — optional anchor date instead of `CURRENT_DATE()` (useful for backfills)
- `ts_column` — name of the timestamp column (default: `'INGESTION_TS_UTC'`)

Exactly one of `keep_n_batches` / `keep_days` must be set — raises a compiler error
if both or neither are provided. Returns an empty string if `cleanup` is absent from
the dict (safe to call as a standalone).

#### ingest_into_landing_sql(...)
Generates `ALTER STAGE REFRESH; BEGIN; COPY INTO ... ($1..$N); COMMIT;` for CSV files.
Used by `run_CSV_ingestion()`.

#### ingest_files_into_landing_sql(...)
Like `ingest_into_landing_sql()` but uses named field expressions via `copy_into__sql()`.
Used by `run_file_ingestion()`.

#### ingest_semi_structured_into_landing_sql(...)
Like `ingest_files_into_landing_sql()` but for semi-structured formats.
Used by `run_semi_structured_ingestion()`.

#### copy_into__sql(...)
Generates the `COPY INTO` statement body with named field expressions. Used by
`ingest_files_into_landing_sql()` and `ingest_semi_structured_into_landing_sql()`.

#### field_definitions(...)
Normalises the `field_expressions` list from `ingestion_dict` into a list of
`{name, expression}` dicts used by `copy_into__sql()`.

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
