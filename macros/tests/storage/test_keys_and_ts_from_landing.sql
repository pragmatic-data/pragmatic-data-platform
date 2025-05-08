{% test keys_and_ts_from_landing( 
    model, 
    landing_rel,
    key_fields_landing,
    key_fields_hist,
    ingestion_ts_landing = 'INGESTION_TS_UTC',
    ingestion_ts_hist = 'INGESTION_TS_UTC',
    load_ts_hist = 'LOAD_TS_UTC',
    key_fields_landing_validation = None
) -%}

WITH

lt_keys as (
  SELECT distinct {{ key_fields_landing }}, {{ ingestion_ts_landing }}
  FROM {{ landing_rel }}
  QUALIFY row_number() OVER(partition by {{ key_fields_landing }} order by {{ ingestion_ts_landing }} desc) = 1
)

, hist_keys as (
  SELECT distinct {{ key_fields_hist }}, {{ ingestion_ts_hist }}
  FROM {{ model }}
  QUALIFY row_number() OVER(partition by {{ key_fields_hist }} order by {{ load_ts_hist }} desc, {{ ingestion_ts_hist }} desc) = 1
)

{%- if not key_fields_landing_validation %}{% set key_fields_landing_validation=key_fields_landing %}{% endif -%}
, validation_errors as (
    SELECT * FROM lt_keys
    EXCEPT
    SELECT * FROM hist_keys
)

SELECT * FROM validation_errors

{%- endtest %}