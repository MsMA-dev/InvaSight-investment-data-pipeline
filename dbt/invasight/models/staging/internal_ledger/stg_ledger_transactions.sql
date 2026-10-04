-- Typed ledger transactions. RAW keeps every CSV column as text so a bad
-- value never blocks the load; casting happens here.

select
    transaction_id,
    client_id,
    client_name,
    portfolio_id,
    ticker,
    transaction_type,
    try_cast(quantity as decimal(38, 4)) as quantity,
    try_cast(price as decimal(38, 4)) as price,
    try_cast(fee_amount as decimal(38, 4)) as fees,
    currency as currency_code,
    try_cast(left(transaction_ts, 19) as timestamp) as transaction_ts,
    try_cast(transaction_date as date) as transaction_date,
    batch_id,
    _loaded_at
from {{ source('raw', 'ledger_transactions_raw') }}
