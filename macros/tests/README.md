# Generic Tests

Custom generic tests to enforce data quality and integrity across your PDP project.
For a quick overview see the [Generic Tests section](../../README.md#generic-tests) of the main README.

## Ingestion tests

### `all_files_from_stage`

Checks that all files matching a pattern in the stage have been ingested in the Landing Table.
Returns files present in the stage but missing from the model.

| Parameter | Required | Default | Description |
|---|---|---|---|
| `pattern` | yes | | Regex pattern to match file paths in the stage directory listing. |
| `stage_name` | yes | | Fully qualified stage name (e.g. `MY_DB.MY_SCHEMA.MY_STAGE`). |
| `min_file_size` | no | `None` | Minimum file size in bytes. Files smaller than this are excluded from the check. This is used to filter out files that are empty or have only the header and therefore have no data rows in the LT. |
| `relative_path_prefix` | no | `None` | Prefix prepended to the `RELATIVE_PATH` from the stage directory when comparing with the model. Use when the path from the stage listing needs an extra prefix to match the full paths stored from the ingestion. This usually happens when the stage URL includes subfolders that then do not appear in the stage LIST output. |
| `stage_refresh_before_test` | no | `True` | Whether to refresh the stage metadata before running the check. Set to `False` for stages that do not support `ALTER STAGE ... REFRESH` (e.g. stages on GCS) or if you are doing a single refresh before ingestion + tests to save a lot of time when you work with big stages with hundred of thousand files. |

**Minimal example:**
```yaml
models:
  - name: my_landing_table
    columns:
      - name: FROM_FILE
        tests:
          - pragmatic_data.all_files_from_stage:
              pattern: '.*\.csv'                         # regex to match files
              stage_name: MY_DB.MY_SCHEMA.MY_STAGE        # fully qualified stage
```

**All parameters:**
```yaml
models:
  - name: my_landing_table
    columns:
      - name: FROM_FILE
        tests:
          - pragmatic_data.all_files_from_stage:
              pattern: '.*\.csv'                         # regex to match files in the stage
              stage_name: MY_DB.MY_SCHEMA.MY_STAGE        # fully qualified stage
              min_file_size: 100                           # ignore files smaller than 100 bytes
              relative_path_prefix: 'data/'                # prepend to the STAGE LIST to match the stored full path
              stage_refresh_before_test: false             # skip ALTER STAGE ... REFRESH to save time
```

### `file_order_is_correct`

Validates that files have been loaded in the Landing Table in the "correct" order.

We usually define the correct order by producing file names that sort as they are produced and then we check that file names are monotonically non-decreasing (i.e. increasing or equal) when sorted by ingestion timestamp.
We could check on the file last modified timestamp in place of the name, but names are more robust and file modification dates can get changed just by moving/downloading the files.

| Parameter | Required | Default | Description |
|---|---|---|---|
| `column_name` | no | `'FROM_FILE'` | Column in the model that contains the column with the "correct" sorting, usually the file name. |
| `time_column_name` | no | `'INGESTION_TS_UTC'` | Column in the model that contains the ingestion timestamp. |

**Minimal example** (uses defaults `FROM_FILE` and `INGESTION_TS_UTC`):
```yaml
models:
  - name: my_landing_table
    columns:
      - name: FROM_FILE
        tests:
          - pragmatic_data.file_order_is_correct        # uses FROM_FILE and INGESTION_TS_UTC columns
```

**All parameters:**
```yaml
models:
  - name: my_landing_table
    columns:
      - name: SOURCE_FILE_PATH                           # column with the file name
        tests:
          - pragmatic_data.file_order_is_correct:
              column_name: SOURCE_FILE_PATH               # override default FROM_FILE
              time_column_name: LOADED_AT                  # override default INGESTION_TS_UTC
```

## Storage layer tests

### `keys_from_landing`

Checks that all distinct keys from the landing relation exist in the history table.
Returns keys present in landing but missing from history, signaling an error as the History is mising data available in the LT. 

| Parameter | Required | Default | Description |
|---|---|---|---|
| `landing_rel` | yes | | The landing relation (a `ref()` or `source()`) to compare against. |
| `key_fields_landing` | yes | | Comma-separated list of key column expressions to select from the landing relation. |
| `key_fields_hist` | yes | | Comma-separated list of key column expressions to select from the history model. Must produce the same number of columns as `key_fields_landing`. |
| `landing_rel_filter` | no | `None` | Optional SQL `WHERE` clause condition to filter the landing relation (without the `WHERE` keyword). |

**Minimal example:**
```yaml
models:
  - name: HIST_customers
    tests:
      - pragmatic_data.keys_from_landing:
          landing_rel: ref('LT_customers')                # landing table to compare
          key_fields_landing: 'CUSTOMER_ID'               # key columns in landing
          key_fields_hist: 'CUSTOMER_ID'                  # key columns in history
```

**All parameters:**
```yaml
models:
  - name: HIST_customers
    tests:
      - pragmatic_data.keys_from_landing:
          landing_rel: ref('LT_customers')                # landing table to compare
          key_fields_landing: 'CUSTOMER_ID, REGION'       # composite key in landing
          key_fields_hist: 'CUSTOMER_ID, REGION_CODE'     # matching key in history
          landing_rel_filter: "REGION = 'EU'"             # only check EU records
```

### `keys_and_ts_from_landing`

Checks that all keys from the landing relation exist in the history table, with the latest ingestion timestamps matching.
For each key, it compares the most recent ingestion timestamp in landing against the most recent in history.

This is the strongest validation test, as it ensures that all the key exist in the HIST with the latest ingestion
timestamp, but will produce false positives when there are full loads or duplicates, as the latest ingestion
might have the same value as a previous one and the HIST -correctly- has the first timestamp a value appeared.

| Parameter | Required | Default | Description |
|---|---|---|---|
| `landing_rel` | yes | | The landing relation (a `ref()` or `source()`) to compare against. |
| `key_fields_landing` | yes | | Comma-separated list of key column expressions to select from the landing relation. |
| `key_fields_hist` | yes | | Comma-separated list of key column expressions to select from the history model. Must produce the same number of columns as `key_fields_landing`. |
| `ingestion_ts_landing` | no | `'INGESTION_TS_UTC'` | Ingestion timestamp column in the landing relation. |
| `ingestion_ts_hist` | no | `'INGESTION_TS_UTC'` | Ingestion timestamp column in the history model. |
| `load_ts_hist` | no | `'LOAD_TS_UTC'` | Load timestamp column in the history model. Used together with `ingestion_ts_hist` to pick the latest row per key. |
| `key_fields_landing_validation` | no | `None` | Alternative key expression for the landing side of the EXCEPT comparison. Defaults to `key_fields_landing` when not set. |

**Minimal example:**
```yaml
models:
  - name: HIST_customers
    tests:
      - pragmatic_data.keys_and_ts_from_landing:
          landing_rel: ref('LT_customers')                # landing table to compare
          key_fields_landing: 'CUSTOMER_ID'               # key columns in landing
          key_fields_hist: 'CUSTOMER_ID'                  # key columns in history
```

**All parameters:**
```yaml
models:
  - name: HIST_customers
    tests:
      - pragmatic_data.keys_and_ts_from_landing:
          landing_rel: ref('LT_customers')                # landing table to compare
          key_fields_landing: 'CUSTOMER_ID, REGION'       # composite key in landing
          key_fields_hist: 'CUSTOMER_ID, REGION'          # matching key in history
          ingestion_ts_landing: 'INGESTED_AT'             # override ingestion ts column in landing
          ingestion_ts_hist: 'INGESTED_AT'                # override ingestion ts column in history
          load_ts_hist: 'LOADED_AT'                       # override load ts column in history
          key_fields_landing_validation: 'CUSTOMER_ID'    # different key expression for the EXCEPT
```

### `has_sortable_versions`

Ensures that versions of an entity can be reliably sorted for historization.
Must be applied on the STG model to validate the use of `save_history_with_multiple_versions()`.
Fails when a single (key, sort_columns) combination maps to more than one value of the diff column.

This test is different from a unicity test, as a single key can appear multiple times in a sort bucket, 
but always with the same diff value (i.e. the sort is fine for historization if for each sort bucket 
we have only one distinct diff, not necessarily a single row as it would be for a unicity test).

| Parameter | Required | Default | Description |
|---|---|---|---|
| `key_column` | yes | | The entity key column (e.g. the business key or surrogate key). |
| `diff_column` | yes | | The column whose distinct values are counted per (key, sort_columns) group. Typically the diff hash. |
| `sort_columns` | yes | | Comma-separated columns used to sort during th ehistorization (e.g. `'RECORD_SOURCE, LOAD_TS_UTC'`). |
| `multiple_equal_versions_is_ok` | no | `true` | When `true`, fails if there are multiple *distinct* diff values for the same sort position. When `false`, fails if there are multiple rows at all (even with the same diff value). |

**Minimal example:**
```yaml
models:
  - name: STG_model
    tests:
      - pragmatic_data.has_sortable_versions:
          key_column: 'CONTRACT_ID'                       # entity key
          diff_column: 'DIFF_HASH'                        # version diff hash
          sort_columns: 'RECORD_SOURCE, LOAD_TS_UTC'      # columns that order versions
```

**All parameters:**
```yaml
models:
  - name: STG_model
    tests:
      - pragmatic_data.has_sortable_versions:
          key_column: 'CONTRACT_ID'                       # entity key
          diff_column: 'DIFF_HASH'                        # version diff hash
          sort_columns: 'RECORD_SOURCE, LOAD_TS_UTC'      # columns that order versions
          multiple_equal_versions_is_ok: false            # fail even on duplicate identical versions
```

### `no_hash_collisions`

Checks for hash collisions on a given column. Fails when two or more distinct tuples of the hashed fields produce the same hash value. Usually applied on history tables.

| Parameter | Required | Default | Description |
|---|---|---|---|
| `column_name` | yes | | The column containing the hash value to check. |
| `hashed_fields` | yes | | Comma-separated list of the source columns that were hashed into `column_name`. |

**Example** (all parameters are required):
```yaml
models:
  - name: HIST_customers
    columns:
      - name: DIFF_HASH
        tests:
          - pragmatic_data.no_hash_collisions:
              hashed_fields: 'NAME, EMAIL, PHONE'         # source columns that were hashed
```

## Delivery layer tests

### `has_default_key`

Ensures a model has the default key row described in the test. Configured with `severity = 'error'`.
Fails when no row from the model matches the expected default key value and record source.

Multiple copies of this test can be used to check the presence of mulitple default keys, 
typicaly with a different key value and the same record source value.

| Parameter | Required | Default | Description |
|---|---|---|---|
| `column_name` | yes | | The key column of the model. |
| `default_key_value` | no | `'-1'` | The expected value of the default key. |
| `record_source_field_name` | no | `'RECORD_SOURCE'` | The column containing the record source identifier. |
| `default_key_record_source` | no | `'System.DefaultKey'` | The expected record source value for the default key row. |

**Minimal example** (uses PDP defaults for the default key):
```yaml
models:
  - name: DIM_customers
    columns:
      - name: CUSTOMER_SK
        tests:
          - pragmatic_data.has_default_key
```

**All parameters:**
```yaml
models:
  - name: DIM_customers
    columns:
      - name: CUSTOMER_SK
        tests:
          - pragmatic_data.has_default_key:
              default_key_value: '0'                       # override default key value
              record_source_field_name: 'SRC'              # override record source column
              default_key_record_source: 'System.Default'  # override record source value
```

### `warn_on_multiple_default_key`

Warns if a dimension has more than one default key. Configured with `severity = 'warn'`.
Returns rows where the record source is `default_key_record_source` but the key value is *not* the expected default.

| Parameter | Required | Default | Description |
|---|---|---|---|
| `column_name` | yes | | The surrogate key column of the dimension. |
| `default_key_value` | no | `'-1'` | The expected value of the default key. |
| `record_source_field_name` | no | `'RECORD_SOURCE'` | The column containing the record source identifier. |
| `default_key_record_source` | no | `'System.DefaultKey'` | The expected record source value for the default key row. |

**Minimal example** (uses PDP defaults for the default key):
```yaml
models:
  - name: DIM_customers
    columns:
      - name: CUSTOMER_SK
        tests:
          - pragmatic_data.warn_on_multiple_default_key
```

**All parameters:**
```yaml
models:
  - name: DIM_customers
    columns:
      - name: CUSTOMER_SK
        tests:
          - pragmatic_data.warn_on_multiple_default_key:
              default_key_value: '0'                       # override default key value
              record_source_field_name: 'SRC'              # override record source column
              default_key_record_source: 'System.Default'  # override record source value
```

## Generic use tests

### `not_empty`

A simple test to ensure a model is not empty. Fails if any row has a zero-length value in the tested column.

| Parameter | Required | Default | Description |
|---|---|---|---|
| `column_name` | yes | | The column to check for empty values. |

**Example** (all parameters are required):
```yaml
models:
  - name: my_model
    columns:
      - name: STATUS
        tests:
          - pragmatic_data.not_empty                       # fails if any row has LEN(STATUS) = 0
```
