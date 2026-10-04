select distinct
    md5(currency_code) as currency_key,
    currency_code
from (
    select currency_code from {{ ref('stg_ledger_transactions') }}
    union
    select target_currency as currency_code from {{ ref('stg_exchange_rates') }}
    union
    select currency_code from {{ ref('int_fx_rates_to_sar') }}
) as currencies
where currency_code is not null
