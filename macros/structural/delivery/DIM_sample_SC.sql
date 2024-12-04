{{ config(alias='DIM_SECURITY_INFO') }}

{{ pragmatic_data.self_completing_dimension(
    dim_rel = ref('REF_SECURITY_INFO_ABC_BANK'),
    dim_key_column  = 'SECURITY_CODE',
    dim_default_key_value = '-1',
    ref_columns_to_exclude = ['SECURITY_HKEY', 'SECURITY_HDIFF'],

    fact_defs = [ {'model': 'REF_POSITION_ABC_BANK', 'key': 'SECURITY_CODE'} ]

) }}
    -- fact_ref = ref('TABLE'),
    -- fact_foriegn_key = 'TABLE_CODE'
