# Stage Macros

Staging is the first step of the historization pipeline using the STG → HIST → VER pattern in the Pragmatic Data Platform (PDP).

While HIST and VER steps are fully automated from a handful of inputs, the staging step is where the edveloper's choices shine
to make the historicized data as usable as possible for the models in the refined layer, or in other projects in a data mesh setup,
without altering the semantic of the incoming data.

The typical operations performed in a STG model cover column renaming, data type casting, column content splitting,
application of system specific or global technical rules (like upper/lower case, padding/unpadding)... and more.
For auditing and extreme caution is always possible to keep both the original input and the adapted one.

A key step for the STG model in the PDP pipeline is to calculate the key of the entity (HKEY) and the change fingerprint (HDIFF)
used to streamline the historization pattern pipeline. It is advisable to also calculate HKEYs for the foreign keys in the entity,
especially if the business key is a compound, multi column key.

Other optional operations performed by the STG model include adding eventual default records and filtering the input, both with a
where predicate and/or a qualify window expression.

For context and a full example on the overall STG → HIST → VER pattern, see the [Storage layer README](../README.md).

**Table of Contents**

- [`stage()`](#stage-source-docs)  
  The macro to generate the staging model from guided/simplified input as a YAML document.
- [`landing_filter()`](#landing_filter-source)  
  The macro to easily generate the expression to filter the STG input based on time duration or number of ingestion batches.

## Stage ([source](stage.sql)) ([docs](stage_macros_docs.yml))

The macro to build a STAGE model (default prefix STG) from metadata passed as YAML.

Reads from a source model (a landing table or any upstream relation), applies column
selection/renaming/casting via `source` and `calculated_columns`, computes hash-based
surrogate keys and change fingerprints via `hashed_columns`, optionally injects
default records, and optionally deduplicates rows using a `QUALIFY` window.

**Signature**:
```jinja
{{ pragmatic_data.stage(
    source_model,
    source              = none,
    calculated_columns  = none,
    hashed_columns      = none,
    default_records     = none,
    remove_duplicates   = none,
) }}
```

**Parameters**:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `source_model` | Relation | — | **Required.** The upstream model or source to read from, e.g. `source('MY_SRC', 'MY_TABLE')` or `ref('SOME_MODEL')` |
| `source` | dict | `none` | Column selection config: `columns.include_all` (bool), `columns.exclude_columns`, `columns.replace_columns`, `columns.rename_columns`, and `where` (SQL filter). |
| `calculated_columns` | list | `none` | List of `- ALIAS: EXPRESSION` mappings. Use `- COL_NAME` to pass through with the same name. To provide string literals: `"'value'"` or `!value`. |
| `hashed_columns` | dict or list | `none` | Hash definitions. Each key is the output column name; the value is a list of source columns to hash. Used to compute HKEY and HDIFF, and eventually HKEYs for the foreign keys. |
| `default_records` | list | `none` | List of named default/unknown records to inject (e.g. `-1 Unknown`, `-2 Missing`). Each entry is a dict of `- COL: EXPRESSION` pairs. |
| `remove_duplicates` | dict | `none` | Deduplication config using a `QUALIFY` clause: `partition_by` (list), `order_by` (list), `qualify_function` (default `row_number()`), `qualify_operator` (default `=`), `qualify_value` (default `1`). |

For a sample of the YAML definition and macro usage see [End-to-end example](../README.md#end-to-end-example)

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

## landing_filter ([source](landing_filter.sql))

Generates a SQL `WHERE` condition to filter a STG model to only read the most recent
data from a landing table — by number of batches, by age in days, or by age in hours.
Returns a plain SQL string, intended for use in the `source.where` key passed to `stage()`.

On `--full-refresh` always returns `true` (no filter — reload everything).
With no parameters returns `true` (no filter).
Multiple parameters are combined with `AND` (safe intersection).

**Signature**:
```jinja
{{ pragmatic_data.landing_filter(
    source_rel,
    n_batches   = none,
    since_days  = none,
    since_hours = none,
    ts_column   = 'INGESTION_TS_UTC'
) }}
```

| Parameter | Type | Default | Description |
|---|---|---|---|
| `source_rel` | Relation | — | The landing table reference, e.g. `source('MY_SRC', 'MY_TABLE')` |
| `n_batches` | int | `none` | Keep only rows from the last N distinct ingestion timestamps |
| `since_days` | int | `none` | Keep only rows ingested within the last N days |
| `since_hours` | int | `none` | Keep only rows ingested within the last N hours |
| `ts_column` | string | `'INGESTION_TS_UTC'` | Name of the ingestion timestamp column in the landing table |

**Usage example**:
```jinja
{%- set source_model = source('SYSTEM_A', 'MY_LANDING_TABLE') -%}

{{- pragmatic_data.stage(
    source_model = source_model,
    source = {
        'where': pragmatic_data.landing_filter(source_model, n_batches=7),
        ...
    },
    ...
) }}
```

**SQL generated** — `n_batches=7`:
```sql
INGESTION_TS_UTC >= (
    SELECT MIN(INGESTION_TS_UTC)
    FROM (
        SELECT DISTINCT INGESTION_TS_UTC
        FROM MY_DB.MY_SCHEMA.MY_LANDING_TABLE
        ORDER BY INGESTION_TS_UTC DESC
        LIMIT 7
    )
)
```

**SQL generated** — `since_days=30`:
```sql
INGESTION_TS_UTC >= DATEADD(DAY, -30, CURRENT_TIMESTAMP())
```

**SQL generated** — `since_hours=24`:
```sql
INGESTION_TS_UTC >= DATEADD(HOUR, -24, CURRENT_TIMESTAMP())
```

**SQL generated** — `n_batches=7, since_days=30` (AND):
```sql
INGESTION_TS_UTC >= (SELECT MIN(...) ... LIMIT 7)
    AND INGESTION_TS_UTC >= DATEADD(DAY, -30, CURRENT_TIMESTAMP())
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
