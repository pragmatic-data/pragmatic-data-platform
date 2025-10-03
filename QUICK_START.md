# Pragmatic Data Platform: Quick Start!

This document provides a practical, terse guide for data engineers to build a 
data platform following the Pragmatic Data Platform (PDP) principles. 

It assumes knowledge of dbt, SQL, and general data warehousing concepts. 
The focus is on the implementation patterns and the use of the PDP dbt package.

## 1. Core Philosophy

The PDP approach prioritizes simplicity, resilience to upstream changes, and developer productivity. This is achieved through:

1. **A Layered Architecture:** Strict separation of concerns between data ingestion, storage, business logic, and data delivery.

2. **A Resilient Storage Layer:** All platform state (current and historical) is managed in the Storage layer. This layer absorbs upstream changes, minimizing rework in downstream models. The `STG >> HIST >> VER` model pattern is the cornerstone of this layer's resilience.

3. **Pattern & Configuration-Driven Development:** reliance on logical and code patterns powered by macros and YAML configuration to define transformations, eliminating boilerplate SQL, reducing cognitive load and enforcing consistency.

4. **Automation:** Using dbt macros to standardize common patterns like ingestion, staging, historization (SCD Type 2), time joins and self completing dimensions.

5. **Modular code for Business Logic:** The Refined layer is built with organized, modular models that transform source data into reusable, well-defined business concepts.

6. **Delivery Layer as an Interface:** The Delivery layer serves data marts through lightweight models that act as stable, external interfaces, enforcing data contracts with consumers like BI applications, AI/ML models, customer facing APPs and feedback loops into operational systems.


The architecture consists of the optional ingestion layer and three core layers built within dbt:

- `ingestion` (optional, built in YAML, deployed with dbt macros)

- `01_storage` (dbt models, uses YAML to configure the STG models)

- `02_refined` (dbt models)

- `03_delivery` (dbt models)


In this introduction we will build a set of models for a single data entity through all layers, using examples from the stonks sample project.  
You can find the sample project on GitHub: https://github.com/pragmatic-data/stonks.

## 2. The Ingestion Layer (Optional)

The PDP provides macros to automate the setup of ingestion infrastructure and the execution of data loading into **Landing Tables** (LT). This is very helpful if you have or can have access to file based data exports.

The ingestion setup and execution is done via `dbt run-operation` and keeps all ingestion logic declarative and version-controlled within your dbt project.

The process is typically two-fold:

1. **Ingestion Setup**: A YAML configuration file defines the specifics for an external data source. A macro reads this file to generate all the necessary DDL to create schema, file formats, and the stages to read the source files in Snowflake.

2. **Landing Table ingestion**: Once the infrastructure is created, a LT specific configuration is created in YAML and used by a macro to execute the `COPY INTO` commands, loading data from the external stage into the corresponding landing table. This operation can be scheduled or triggered and incrementally loads only new files, working in a simple and efficient way.

Find out the details in the [Ingestion and Export README](macros/in_out/README.md).

## 3. The Storage Layer: The Platform's Foundation

The Storage Layer is the foundational component of the PDP.  
It takes data from landing tables and transforms it into a clean, historized, 
and reliable format. For each source entity it applies a consistent pattern using 
three model types: Staging (`STG_`), History (`HIST_`), and Versioning (`VER_`).

### Step 3.1: Define the Source

First, define your raw data source in a `.yml` file within your models directory. 
This is standard dbt practice to make external data reachable by dbt models.

**File: `models/01_storage/interactive_brokers/source_interactive_brokers.yml`**

```
version: 2

sources:
  - name: IB
    schema: LAND_IB
    description: "Data from Interactive Brokers Flex Queries"
    tables:
      - name: CASH_TRANSACTIONS
        description: "Contains cash transactions, including dividends and taxes."
```

### Step 3.2: The Staging Model (`STG_`)

The staging model's only role is to clean, standardize, and prepare a source entity 
for historization. 

The configuration that defines its operation is included directly in the SQL file 
for clarity and simplicity. It’s possible to put it in `.yml` files when the reuse 
of definitions provides enough value to offset the added complexity and diminished 
readability.

**File: `models/01_storage/interactive_brokers/cash_transactions/STG_IB_CASH_TRANSACTIONS.sql`**

```
{%- set source_model = source('IB', 'CASH_TRANSACTIONS') %}

{%- set configuration -%}
source:
    columns: 
        include_all: false          #-- True enables using exclude/replace/rename lists 
                                    #-- False does not include any source col
    where: STARTSWITH(CLIENTACCOUNTID, 'U')   #-- Filters the Landing Table input

calculated_columns:
    - Transaction_ID: TransactionID
    - Transaction_Type: Type
    - TX_DESCRIPTION: DESCRIPTION

    #-- Core FKs => Account, Security and Position (if related to a position) + Desc
    - BROKER_CODE: '!IB'                      #-- a string starting with '!' is considered a literal
    - CLIENT_ACCOUNT_CODE: "'U***' || RIGHT(CLIENTACCOUNTID, 4)"
    - SECURITY_CODE: coalesce(SECURITYID, CONID)
    - LISTING_EXCHANGE: LISTINGEXCHANGE

    #-- TX Details: Description and Classification
    - Transaction_Category: |
        CASE
            WHEN ASSET_CLASS = 'STK' and TYPE IN ('Dividends', 'Payment In Lieu Of Dividends', 'Withholding Tax', 'Other Fees') 
                THEN 'DIVIDENDS'
            WHEN ASSET_CLASS is null and TYPE IN ('Broker Interest Received', 'Withholding Tax', 'Broker Interest Paid') 
                THEN 'INTERESTS'
            WHEN ASSET_CLASS is null and TYPE IN ('Deposits/Withdrawals') 
                THEN 'DEPOSITS'
            WHEN ASSET_CLASS is null and TYPE IN ('Other Fees') 
                THEN 'COSTS'
            ELSE 'UNKNOWN'
        END
    # . . . more column definitions

hashed_columns: 
    TRANSACTION_HKEY:             #-- Primary key, better to be defined here ;)
        - Transaction_ID
        - Transaction_Type
        - TX_DESCRIPTION
    POSITION_HKEY:                #-- Foreign key, useful to be defined here
        - BROKER_CODE
        - CLIENT_ACCOUNT_CODE
        - SECURITY_CODE
        - LISTING_EXCHANGE
    PORTFOLIO_HKEY:               #-- Foreign key, useful to be defined here
        - BROKER_CODE
        - CLIENT_ACCOUNT_CODE
    SECURITY_HKEY:                #-- Foreign key, useful to be defined here
        - SECURITY_CODE
        - LISTING_EXCHANGE

    TRANSACTION_HDIFF:            #-- How to track changes for this entity
        - Transaction_Type
        - Transaction_ID
        - TX_DESCRIPTION
        #-- FKs
        - BROKER_CODE
          . . . more columns, all or most of the payload (no metadata)

#default_records:                  #-- define eventual default records. Useful for dimension entities.

#remove_duplicates:                #-- not needed, contents commented out or removed
#    qualify_function: row_number()
#    qualify_operator: =
#    qualify_value: 1
#    partition_by:
#        - TRANSACTION_HKEY
#    order_by:
#        - REPORT_DATE asc
#        - RECORD_SOURCE
#        - FILE_ROW_NUMBER

{%- endset -%}

{%- set cfg = fromyaml(configuration) -%}

{{- pragmatic_data.stage(
    source_model            = source_model,
    source                  = cfg['source'],
    calculated_columns      = cfg['calculated_columns'],
    hashed_columns          = cfg['hashed_columns'],
    remove_duplicates       = cfg['remove_duplicates'],
) }}
```

**Key Actions of the `pdp.stage` macro:**

- **Column Selection & Renaming:** Selects and renames columns (`DESCRIPTION` -> `TX_DESCRIPTION`).

- **Create new calculated columns**: Define new columns, usually from existing ones (`Transaction_Category`) or static data ( `BROKER_CODE`)

- **Data Typing:** Casts all columns to the correct `datatype`.

- **Unit conversion:** Concerts the data to the correct representation,
  be it a different`unit` or `timezone`.

- **Nested object extraction:** Extracts the desired columns from nested 
  semi-structured objects to be accessed like normal SQL columns.

- **Metadata Columns:** Automatically adds ingestion metadata (`FROM_FILE`, `INGESTION_TS_UTC`, `FILE_ROW_NUMBER`, etc.).

- **Business Key Hashes (`..._HKEY`):** Creates a binary hash of the columns listed under each key name. 
  This is used for the primary key (`<ENTITY>_HKEY`) listing the BK columns that uniquely identifies an entity instance,
  and for the foreign keys (`<OTHER_ENTITY>_HKEY`) that are connected to the entity being stored.

- **Hash Diff (`<ENTITY>_HDIFF`):** Creates a binary hash of all the columns that count as a change. 
  It includes the business key. This is used to detect changes in the record's attributes, 
  generating a new version of the instance.

In short, the STG macro takes the source data and adjusts it to be easy to use downstream,
without changing the meaning of the data.

### Step 3.3: The History Model (`HIST_`)

This is a pure, insert-only ledger.  
Its sole purpose is to capture every unique version (identified by the `HDIFF`) 
of a record as it arrives, along with a timestamp of when that version was first recorded. 
It contains no validity logic and existing rows are never updated. 
It is the immutable source of truth for all historical data. 
This model **must** be materialized as `incremental`.

**File: `models/01_storage/interactive_brokers/cash_transactions/HIST_IB_CASH_TRANSACTIONS.sql`**

```
{{ config( materialized='incremental') }}

{{ pragmatic_data.save_history_with_multiple_versions(
    input_rel               = ref('STG_IB_CASH_TRANSACTIONS'), 
    key_column              = 'TRANSACTION_HKEY',
    diff_column             = 'TRANSACTION_HDIFF',

    sort_expr               = 'REPORT_DATE',
) }}
```

**Key Actions of `pdp.save_history...`:**

- **Insert-Only Pattern**: On subsequent runs, it compares the `HDIFF` of incoming records from the `STG` model against all records in the `HIST` model for a matching `HKEY`. It sorts the versions for a single entity to properly evaluate the evolution of changes.

- **New Version Detection**: If a new, different `HDIFF` from the last version of an 
  entity, or for a new entity instance is found, it inserts this new version 
  into the history table.


### Step 3.4: The Version Model (`VER_`)

This is the "intelligence" layer built on top of the raw history.  
It enriches the historical data with temporal logic, making it easy for 
downstream models to consume. If you need the current data, you can simply 
filter using `WHERE IS_CURRENT` out of the VER model.

**File: `models/01_storage/interactive_brokers/cash_transactions/VER_IB_CASH_TRANSACTIONS.sql`**

```
{{ pragmatic_data.versions_from_history_with_multiple_versions(
    history_rel             = ref('HIST_IB_CASH_TRANSACTIONS'), 
    key_column              = 'TRANSACTION_HKEY',
    diff_column             = 'TRANSACTION_HDIFF',

    version_sort_column     = 'REPORT_DATE'
) }}
```

**Key Actions of `pdp.versions_from_history...`:**

- **Calculate Validity**: It generates the `VALID_FROM` and `VALID_TO` columns 
  for each historical record in the history table.

- **Create `SCD_KEY`**: It generates the version-specific `SCD_KEY`.
  This allows to identify exactly that specific version in the whole history table.

- **Flag Current Version**: It adds a boolean `IS_CURRENT` column, 
  making it simple for downstream models to get the latest data.

- **Expose Full History**: The resulting model contains the complete history of an entity, 
  now enriched with columns that make querying for specific points in time, 
  or for just the current state, simple and efficient.
  The output, for dimensional entities, is a ready to use SCD2 table.


### Step 3.5: Documentation and Testing

We usually have one `.yml` file for all three models of the `STG >> HIST >> VER` pattern 
to highlight their tight integration and define tests and documentation in one single place. 

The accompanying `.yml` file is used in the usual dbt style, 
providing descriptions and tests.

**File: `models/01_storage/interactive_brokers/cash_transactions/ib_cash_transactions.yml`**

```
version: 2

models:
  - name: STG_IB_CASH_TRANSACTIONS
    description: "Staging model for cash transactions from Interactive Brokers."
    data_tests:
      - dbt_utils.unique_combination_of_columns:      #-- Max one version per report
          combination_of_columns:
            - Transaction_Type
            - Transaction_ID
            - TX_DESCRIPTION
            - REPORT_DATE
            - RECORD_SOURCE   #-- all files have all TXs since the start of the year => unique in each file

    columns:
      - name: TRANSACTION_HKEY
        tests:
          - not_null
      - name: Transaction_Category
        description: The category of the transaction
        data_tests:
          - accepted_values:
              values: ['DIVIDENDS', 'INTERESTS', 'DEPOSITS', 'COSTS', 'UNKNOWN']
```

## 4. The Refined Layer: Applying Business Logic

The Refined Layer is the real heart of your data platform.  
It's where you shift from simply storing data to incrementally building reusable 
business concepts and applying complex logic. Models in this layer integrate data 
from different sources, creating a unified and well-governed view of business 
entities and processes. This is where the core business value is generated.

**File: `models/02_refined/interactive_brokers/positions_calculated/REF_IB_POSITIONS_TRANSACTIONS.sql`**

```
-- This model unifies all transactions that affect a position's quantity
-- it brings the HKEY from the source models and creates a new one for the unified concept

WITH trades AS (
    SELECT . . .
    FROM {{ ref('VER_IB_TRADES') }}
    WHERE IS_CURRENT and ASSET_CLASS != 'CASH'

), transfers AS (
    SELECT . . .
    FROM {{ ref('VER_IB_TRANSFERS') }}
    WHERE IS_CURRENT and ASSET_CLASS != 'CASH'

), corp_actions AS (
    SELECT . . .
    FROM {{ ref('REF_IB_CORPORATE_ACTIONS_TXS') }}

), all_transactions AS (
    SELECT * FROM trades
    UNION ALL
    SELECT * FROM transfers
    UNION ALL
    SELECT * FROM corp_actions
)

SELECT * FROM all_transactions
```

## 5. The Delivery Layer: Preparing Data for Consumers

The Delivery Layer is the final stage, where data is prepared for specific consumers 
and use cases. It acts as the "shop front" of the data platform, providing clean, 
reliable, and easy-to-use data products. These models are typically lightweight 
views or tables that act as stable interfaces, enforcing clear data contracts 
with their users.

### Step 5.1: Dimensions (`DIM_`)

Dimensions provide descriptive context. 

They are typically simple, but often quite wide, selecting from a refined model.
Try to limit the interface you expose to what is really needed by the users.

**File: `models/03_delivery/marts/portfolio_analysis/DIM_PORTFOLIOS.sql`**

```
SELECT ... # Pick ONLY the column needed in this data mart
FROM {{ ref('REF_IB_PORTFOLIOS') }}
```

### Step 5.2: Facts (`FACT_`)

Fact tables contain quantitative measures and foreign keys to dimension tables. 

When they represent business concepts they are also built with simple SQL, 
as they can just select the desired columns from the REF model built for the concept. 
We can also build more complex, composite facts for the specific need of one data mart 
by joining base facts, dimensions and mapping tables.

**File: `models/03_delivery/marts/portfolio_analysis/FACT_POSITION_TRANSACTIONS.sql`**

```
SELECT ... # Pick ONLY the column needed in this data mart
FROM {{ ref('REF_IB_POSITIONS_TRANSACTIONS') }}
```

### Step 5.3: Reports and Advanced Use Cases (`RPT_`)

Beyond traditional star schemas, the Delivery layer serves a wide range of consumers.
`RPT_` models are a common pattern for creating pre-aggregated or denormalized tables 
optimized for a specific BI dashboard or report.

This layer is also critical for more advanced use cases, providing trusted, 
well-structured data for:

- **User-Facing Applications:** Powering analytics embedded directly within customer 
  portals or internal applications.

- **AI/ML Pipelines:** Serving clean, documented feature sets for training and 
  running machine learning models.

- **Reverse ETL / Feedback Loops:** Sending enriched data back into operational 
  systems (like a CRM or marketing platform) to drive business actions.


**File: `models/03_delivery/apps/current_positions/RPT_POSITIONS_CURRENT_VALUES.sql`**

```
SELECT
    pcv.POSITION_HKEY,
    p.PORTFOLIO_NAME,
    p.PORTFOLIO_CURRENCY,
    s.SECURITY_NAME,
    s.ASSET_CLASS,
    s.LISTING_EXCHANGE,
    pcv.QUANTITY,
    pcv.COST_PER_SHARE,
    pcv.POSITION_COST,
    pcv.MARKET_PRICE,
    pcv.MARKET_VALUE,
    pcv.UNREALIZED_PNL
FROM
    {{ ref('TS_IB_REPORTED_POSITIONS_DAILY_VALUES') }} pcv
INNER JOIN
    {{ ref('DIM_PORTFOLIOS') }} p
    ON pcv.PORTFOLIO_HKEY = p.PORTFOLIO_HKEY
INNER JOIN
    {{ ref('DIM_SECURITIES') }} s
    ON pcv.SECURITY_HKEY = s.SECURITY_HKEY
WHERE
    pcv.IS_CURRENT
```

## 6. Identity Management and Changes

The PDP relies on a set of consistent, hashed keys to manage entity identity 
and track changes over time. This system is fundamental to the resilience of 
the Storage Layer and the clarity of the Delivery Layer.

### 6.1 Core Keys for Tracking Entities and Changes

- **`<ENTITY>_HKEY` (The "Who")**:

    - **What it is**: A persistent surrogate key that uniquely identifies a business entity throughout its lifetime (e.g., a specific security, a customer, a product).

    - **How it's made**: By hashing the natural business key(s) in the `STG` model.

    - **Purpose**: It acts as the primary key for the entity. You use it to join tables when you want to know something about the entity itself, typically its _current_ state or _all_ states. It is the key that links a fact to the Type 1 dimension (`DIM_`).

- **`<ENTITY>_HDIFF` (The "What has changed?")**:

    - **What it is**: A hash of all the descriptive and key attributes (the business key and "payload") of a record.

    - **How it's made**: It is generated by the `pdp.stage` macro by hashing the business key (`HKEY`) and all the descriptive columns. You exclude the ones considered metadata or that do not create a new version. A typical hard decision is about a `updated_at` column. That can be included in the `HDIFF` if you want to track all updates, also the ones that do not change any descriptive column, while you exclude if you want to track only real changes in the descriptive data.

    - **Purpose**: Its primary role is to efficiently detect changes. In the `HIST_` models, the `pdp.save_history` macro compares the `HDIFF` of an incoming record with the `HDIFF` of the current version. If they are different, it means the record has changed, and a new version is created. This avoids costly column-by-column comparisons.

- **`SCD_KEY` (The "What at When")**:

    - **What it is**: A unique key for a _specific version_ of an entity at a particular point in time.

    - **How it's made**: It is created in the `VER_` model by concatenating the `<ENTITY>_HKEY` and the `VALID_FROM` timestamp.

    - **Purpose**: It allows you to link a fact record directly to the exact historical state of a dimension that was valid when the fact occurred. This is crucial for accurate point-in-time reporting and analysis. A fact table will often contain both the `HKEY` (to link to the current state in the `DIM_` table) and the `SCD_KEY` (to link to the historical version in the `SCD_` table).


### 6.2 Serving Entity Data: DIM vs. SCD Models

The Delivery layer can expose entity information in two distinct ways to serve different analytical needs, following standard data warehousing patterns. They can -and often do- coexist.

- **`DIM_` Models (SCD Type 1 - "What is it now?")**

    - **Structure**: Contains exactly **one row per entity**, representing the **most recent, current version** of its attributes. If a security's name changes, the old name is overwritten.

    - **Primary Key**: The `<ENTITY>_HKEY` (e.g., `SECURITY_HKEY`).

    - **Use Case**: For reports and analyses where only the current state of the entity matters. It's simple, fast, and answers the majority of business questions. For example, "Show me the current value of all my holdings."

- **`SCD_` Models (SCD Type 2 - "What was it then?")**

    - **Structure**: Contains the **entire history of every entity**. A single entity can have **multiple rows**, one for each version of its attributes over time, delineated by `VALID_FROM` and `VALID_TO` columns.

    - **Primary Key**: The `SCD_KEY` (e.g., `SECURITY_SCD_KEY`). The model also contains the `<ENTITY>_HKEY` to link all versions of the same entity together.

    - **Use Case**: For historical analysis and auditing. It allows you to reconstruct the state of your data at any point in the past. For example, "What was the listing exchange for this security on the date I sold it?"