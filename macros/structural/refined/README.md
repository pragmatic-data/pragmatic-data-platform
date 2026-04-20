# Refined Layer

The Refined layer is the **business heart** of the Pragmatic Data Platform. It transforms
the source-oriented, immutable history stored in the Storage layer into valuable,
business-oriented information expressed in the ubiquitous language of the business domains.

**Table of Contents**
- [Design principles](#design-principles)
- [Organisation and naming](#organisation-and-naming)
- [time_join](#time_join)

## Design principles

The Refined layer is **stateless**: every model is fully rebuildable from Storage by running `dbt build -s 02_refined --full-refresh`.  
This simplicity is what makes the layer trustworthy and easy to maintain — any mistake can be corrected
and any change applied by updating the logic and rebuilding.

Models in the Refined layer:

- Are organised by **business domain**, not by source system.  
  A firt set of source specific models can be useful to move from the surce specific data model
  to business entities and a more business oriented data model. A common example is to resolve
  source specific surrogate keys into proper business keys interoperable across sources (MDM).
- Express business concepts in **domain language** (not source system entity and column names)
- Combine and enrich Storage data to produce business relevant information
- Can use the `HIST_LOAD_TS_UTC` high-watermark for efficient incremental loads where needed

Unlike Storage — which is tightly coupled to source structure — Refined reason to exist is to integrate
data from multiple sources, apply business rules, and evolve independently of upstream
source changes as long as the HIST contract (accesse through the VER model) remains stable.

## Organisation and naming

Refined models live in `02_refined/` and are organised into **business domain** folders.
The naming prefix encodes the type and temporal scope of each model:

| Prefix | Meaning |
|--------|---------|
| `ref_` | Current-state view of a business entity (e.g. reads VER with `where IS_CURRENT`) |
| `refh_` | Full version history of a business entity (e.g. reads VER, all versions) |
| `ts_` | Time series: one row per entity per time period |
| `int_` | Intermediate model for complex transformations (not exposed directly) |
| `agg_` | Aggregated current-state data |
| `aggh_` | Aggregated historical data |
| `map_` | Mapping table between two key spaces |
| `mhist_` | Multi-source historised entity (merges history from several sources) |
| `mdd_` | Master data definition (MDM — cross-domain authoritative entity) |
| `pivot_` | Pivoted or transposed data |
| `brg_` | Bridge table for many-to-many relationships |
| `spine_` | Provides the full range of values in an interval. E.g. a date spine to build a calendar. |

---

## time_join

The `time_join` macro implements a **temporal join** (Snowflake `ASOF JOIN`) between a
base fact or event table and one or more secondary tables. It joins each
row from the base table to the version of each secondary table that was active at the time of
the base table record.

Use it whenever you need to enrich facts or events with dimension attributes or other facts as they
were at the moment of the event — for example: joining a dividend payment to the security
master data and position amount that was in effect on the settlement date, or enriching a trade with the
position data current at trade execution time.

This macro automatically support in cremental loading and can also use a high-watermark pattern
to reduce the part of the base table being processed.

The base table is accessible with the alias `bt`, while the other table with `t1, t2, ...` following
the order in which they are declared.

**Signature**:
```jinja
{{ pragmatic_data.time_join(
    base_table_dict,
    joined_tables_dict,
    calculated_columns      = [],
    high_watermark_column   = var('pdp.high_watermark_column', var('pdp.hist_load_ts_column', 'HIST_LOAD_TS_UTC')),
    high_watermark_test     = var('pdp.high_watermark_test', '>'),
) }}
```

**Parameters**:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `base_table_dict` | dict | — | **Required.** Configuration for the base (fact/event) table. |
| `joined_tables_dict` | dict | — | **Required.** Configuration for each secondary table to join. Keys are relation references. |
| `calculated_columns` | list | `[]` | Additional computed columns appended after all joins. Format: `- ALIAS: EXPRESSION`. |
| `high_watermark_column` | string | `HIST_LOAD_TS_UTC` | Column on the base table used for incremental loading. |
| `high_watermark_test` | string | `>` | Comparison operator for the incremental filter. |

**`base_table_dict` keys**:

| Key | Required | Description |
|-----|----------|-------------|
| `name` | yes | Relation reference as a string, e.g. `"{{ ref('VER_ORDERS') }}"` |
| `include_all_columns` | no | `true` to select `bt.*` from the base table |
| `exclude_column_list` | no | Columns to exclude when `include_all_columns: true` |
| `columns` | no | List of columns to select. Format: `- ALIAS: SOURCE_COL` or `- COL_NAME` |
| `filter` | no | SQL WHERE condition applied to the base table |

**Each entry in `joined_tables_dict`** (keyed by relation reference string):

| Key | Required | Description |
|-----|----------|-------------|
| `join_columns` | yes | Mapping of base table key column(s) to secondary table key column(s): `BASE_COL: Tn_COL` |
| `time_column` | yes | Single-entry mapping `BASE_COL: Tn_COL` defining the `MATCH_CONDITION` for the ASOF JOIN. The default operator is `>=` (base event occurs at or after the secondary table version becomes effective). |
| `time_operator` | no | Overrides the ASOF JOIN match operator (default `>=`) |
| `columns` | yes | List of columns to select from the joined secondary table |
| `filters` | no | List of additional WHERE conditions applied to the joined secondary table |

**Example** — from [STONKS](https://github.com/RobMcZag/stonks),
enriching dividend payments with security master data and position data as of the settlement date:

```jinja
{%- set configuration -%}
base_table:
    name: "{{ ref('VER_IB_CASH_TRANSACTIONS') }}"
    filter: TRANSACTION_CATEGORY = 'DIVIDENDS' and bt.IS_CURRENT
    include_all_columns: false
    columns:
        - TRANSACTION_ID
        - DIVIDEND_SECURITY_SYMBOL: SECURITY_SYMBOL
        - SECURITY_CODE
        - AMOUNT_IN_BASE
        - SETTLE_DATE
        - EFFECTIVITY_DATE
        - TRANSACTION_HKEY
        - PORTFOLIO_HKEY

joined_tables:
    {{ ref('REFH_IB_SECURITIES') }}:
        join_columns:
            SECURITY_CODE: SECURITY_CODE
        time_column:
            SETTLE_DATE: EFFECTIVITY_DATE      # base SETTLE_DATE >= dimension EFFECTIVITY_DATE
        columns:
            - SECURITY_NAME
            - LISTING_EXCHANGE
            - CONID
            - ISIN

    {{ ref('REFH_IB_POSITIONS_REPORTED') }}:
        filter: SIDE != 'Closed'
        join_columns:
            POSITION_HKEY: POSITION_HKEY
        time_column:
            EFFECTIVITY_DATE: EFFECTIVITY_DATE
        columns:
            - POSITION_QUANTITY: quantity
            - SIDE

calculated_columns:
    - DIM_SECURITY_SYMBOL: COALESCE(t1.SECURITY_SYMBOL, DIVIDEND_SECURITY_SYMBOL)
    - DIM_SECURITY_HKEY:   COALESCE(t1.SECURITY_HKEY, bt.SECURITY_HKEY)
{%- endset -%}

{%- set cfg = fromyaml(configuration) -%}

{{- pragmatic_data.time_join(
    base_table_dict    = cfg['base_table'],
    joined_tables_dict = cfg['joined_tables'],
    calculated_columns = cfg['calculated_columns']
) }}
```

Notes on the example:

- The base table alias is always `bt`; joined tables get aliases `t1`, `t2`, etc. in order
- Calculated columns can reference `bt`, `t1`, `t2` directly in SQL expressions
- The `ASOF JOIN` finds the most recent secondary table row whose `time_column` is `time_operator` (< | <= | >= | >) the
  base table's time column value. Use the correct `time_operator` to look for esults in the right direction
  starting from the base table event time. 

As a different example using an invoice as a base table you would look for an order (for the same customer and product)
that happend before of the invoice and a delivery or return that happend after the invoice (assuming you invoice when you ship).

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
