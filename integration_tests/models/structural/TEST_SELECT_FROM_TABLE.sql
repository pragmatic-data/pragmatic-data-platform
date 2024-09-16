{{- pragmatic_data.select_from_table( ref('GENERIC_TWO_COLUMN_TABLE') ) }}
UNION
{{- pragmatic_data.select_from_table( ref('GENERIC_TWO_COLUMN_TABLE'), column_expression = 'Column1,Column2') }}
UNION
{{- pragmatic_data.select_from_table( None, column_expression = "'ddd' as Column1, 444 as Column2") }}
UNION
{{- pragmatic_data.select_from_table( column_expression = "'eee' as Column1, 555 as Column2") }}
