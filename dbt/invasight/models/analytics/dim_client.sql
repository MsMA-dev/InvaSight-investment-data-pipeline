select distinct
    md5(client_id) as client_key,
    client_id,
    client_name
from {{ ref('stg_ledger_transactions') }}
where client_id is not null
