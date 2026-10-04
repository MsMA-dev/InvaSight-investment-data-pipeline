with source_data as (
    select 'Internal Ledger' as source_name, 'Internal' as source_type
    union all
    select 'EQUITY_API', 'External'
    union all
    select 'Metal Market Data', 'External'
)

select md5(source_name) as source_key, source_name, source_type
from source_data
