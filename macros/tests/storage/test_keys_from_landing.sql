{% test keys_from_landing( 
    model, 
    landing_rel,
    key_fields_landing,
    key_fields_hist,
    landing_rel_filter = None
) -%}

WITH

lt_keys as (
  SELECT distinct {{ key_fields_landing }}
  FROM {{ landing_rel }}
  {%- if landing_rel_filter %}
  WHERE {{landing_rel_filter}}
  {%- endif %}
)

, hist_keys as (
  SELECT distinct {{ key_fields_hist }}
  FROM {{ model }}
)

, validation_errors as (
    SELECT * FROM lt_keys
    EXCEPT
    SELECT * FROM hist_keys
)

SELECT * FROM validation_errors

{%- endtest %}