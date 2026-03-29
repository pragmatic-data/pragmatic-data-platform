# Delivery Layer

The Delivery layer is the **last mile** of the Pragmatic Data Platform. The external interface of the dta platform.

It publishes the Refined data as user-friendly data products — dimensional models, reports, and
application datasets — shaped for direct consumption by BI tools, applications, and ML.

**Table of Contents**
- [Design principles](#design-principles)
- [Organisation and naming](#organisation-and-naming)
- [`self_completing_dimension()`](#self_completing_dimension)

## Design principles

The Delivery layer is **stateless**: every model is fully rebuildable from Refined with
a `dbt build -s 03_delivery --full-refresh`. Like the Refined layer, this makes it safe to correct mistakes
and evolve data products without complex operations or risk of data loss.

The mindset shifts in Delivery: you are delivering **data products** with a defined interface
and defined consumers — not a general-purpose data layer.

- Each Data Mart groups data products for a specific business case, area or application
- When **Kimball dimensional modeling** is desired, this is the pace to apply it (not in Refined or Storage)
- Keep models consumer-specific; avoid wide "all-purpose" tables that serve no one well
- Design by contract: use dbt model contracts to lock down the interface for consumers

## Organisation and naming

Delivery models in dbt live in `03_delivery/` and are organised into **categories**
(marts, apps, ml) and then into individual **Data Mart** folders within each category:

```
03_delivery/
    marts/
        portfolio_analysis/
            DIM_PORTFOLIOS.sql
            SCD_SECURITIES.sql
            FACT_POSITION_TRANSACTIONS.sql
            RPT_POSITIONS_DAILY_VALUES.sql
    apps/
        current_positions/
            RPT_POSITIONS_CURRENT_VALUES.sql
```

Naming prefixes:

| Prefix | Meaning |
|--------|---------|
| `dim_` | Dimension table (standard, current snapshot) |
| `scd_` | Slowly-changing dimension (with `IS_CURRENT`, `VALID_FROM`, `VALID_TO`) |
| `fct_` | Fact table |
| `rpt_` | Report / denormalised view for a specific consumer |
| `filter_` | Pseudo-dimension used to filter a dimension or fact for a specific segment or consumer or set of entities. |

---

## self_completing_dimension

The `self_completing_dimension` macro builds a **self-completing (self-healing) dimension**:
a dimension that automatically adds default placeholder rows for any foreign keys that
appear in related fact tables but are missing from the dimension itself.

This guarantees referential integrity in the Delivery layer without NULL-handling
everywhere: every foreign key in a fact has a corresponding dimension row — even if source
data is incomplete or arrives late. Missing keys receive the attribute values of the
configured default/unknown record (e.g. the `-1 / Unknown` row) untile when they become available.

Use it on any dimension that:

- Has fact tables whose foreign key may reference entities not yet present in the dimension
- Has a default/unknown record (identified by `dim_default_key_value`).  
  You can easily add default records with the specific feature of the STG macro.

When `fact_defs` is empty, the macro simply projects the dimension with excluded columns
removed — no orphan check is performed and no rows are added.

**Signature**:
```jinja
{{ pragmatic_data.self_completing_dimension(
    dim_rel,
    dim_key_column,
    dim_default_key_value   = '-1',
    ref_columns_to_exclude  = [],
    fact_defs               = [],
) }}
```

**Parameters**:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `dim_rel` | Relation | — | **Required.** The source dimension model (typically a `refh_` or `ref_` model). |
| `dim_key_column` | string | — | **Required.** The primary key column of the dimension (the HKEY). |
| `dim_default_key_value` | string | `'-1'` | Value of the default/unknown record. Missing foreign keys are filled using this row's attribute values. |
| `ref_columns_to_exclude` | list | `[]` | Columns to drop from the output (e.g. internal metadata not needed by consumers). |
| `fact_defs` | list | `[]` | Fact models to check for orphan foreign keys. Each entry is a dict: `{model: 'FCT_...', key: 'KEY_COL'}`. |

**`fact_defs` entry format**:
```yaml
fact_defs:
    - model: FCT_ORDERS
      key:   PRODUCT_HKEY
    - model: RPT_DAILY_SALES
      key:   PRODUCT_HKEY
```

**Example** — from [STONKS](https://github.com/RobMcZag/stonks), building a
self-completing securities dimension referenced by two fact tables:

```jinja
{% set configuration %}
fact_defs:
    - model: TS_IB_REPORTED_POSITIONS_DAILY_VALUES
      key: SECURITY_HKEY
    - model: AGG_IB_DIVIDENDS
      key: SECURITY_HKEY

ref_columns_to_exclude:
    - FILE_LAST_MODIFIED_TS_UTC
    - FILE_ROW_NUMBER
    - SECURITY_HDIFF
    - VERSION_COUNT
    - VERSION_NUMBER
    - INGESTION_BATCH
    - LOAD_BATCH
{% endset %}
{%- set cfg = fromyaml(configuration) -%}

{{ pragmatic_data.self_completing_dimension(
    dim_rel               = ref('REFH_IB_SECURITIES'),
    dim_key_column        = 'SECURITY_HKEY',
    dim_default_key_value = '6BB61E3B7BCE0931DA574D19D1D82C88',
    ref_columns_to_exclude = cfg.ref_columns_to_exclude,
    fact_defs              = cfg.fact_defs
) }}
```
NOTE: when you use an HKEY as `dim_key_column` you need to pass a hash in the `dim_default_key_value`.
In the example '6BB61E3B7BCE0931DA574D19D1D82C88' is the string representation of the HKEY for a BK = '-1';  
[other common HKEYs: ('-2') => '5D7B9ADCBE1C629EC722529DD12E5129' // ('-1', '-1') => '0D08A098929FAA77EAFB17A3266A80AE']

If you use a business key (BK) as `dim_key_column` you need to pass the business value in the `dim_default_key_value`,
often something like  '-1' that is the common BK of the 'Unknown' default record.

Any `SECURITY_HKEY` value found in either fact table that is not already in `REFH_IB_SECURITIES` will be added in the DIM
in a new row with the attribute values of the `-1` (Unknown) security record, ensuring that every fact row can now be
equi-joined to the dimension.

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
