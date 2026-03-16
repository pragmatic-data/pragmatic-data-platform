-- Test analysis for landing_filter macro.
-- Run: dbt compile --select TEST_LANDING_FILTER
-- Then inspect: target/compiled/.../analyses/TEST_LANDING_FILTER.sql

-- Case 1: n_batches only
SELECT '1_n_batches' as test_case, * FROM {{ ref('GENERIC_TWO_COLUMN_TABLE') }}
WHERE {{ pragmatic_data.landing_filter(ref('GENERIC_TWO_COLUMN_TABLE'), n_batches=7) }}

UNION ALL

-- Case 2: since_days only
SELECT '2_since_days' as test_case, * FROM {{ ref('GENERIC_TWO_COLUMN_TABLE') }}
WHERE {{ pragmatic_data.landing_filter(ref('GENERIC_TWO_COLUMN_TABLE'), since_days=30) }}

UNION ALL

-- Case 3: since_hours only
SELECT '3_since_hours' as test_case, * FROM {{ ref('GENERIC_TWO_COLUMN_TABLE') }}
WHERE {{ pragmatic_data.landing_filter(ref('GENERIC_TWO_COLUMN_TABLE'), since_hours=24) }}

UNION ALL

-- Case 4: n_batches AND since_days combined (AND logic)
SELECT '4_combined_and' as test_case, * FROM {{ ref('GENERIC_TWO_COLUMN_TABLE') }}
WHERE {{ pragmatic_data.landing_filter(ref('GENERIC_TWO_COLUMN_TABLE'), n_batches=7, since_days=30) }}

UNION ALL

-- Case 5: no parameters — returns true
SELECT '5_no_params' as test_case, * FROM {{ ref('GENERIC_TWO_COLUMN_TABLE') }}
WHERE {{ pragmatic_data.landing_filter(ref('GENERIC_TWO_COLUMN_TABLE')) }}
