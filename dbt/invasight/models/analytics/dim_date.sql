with all_dates as (
    select transaction_date as full_date from {{ ref('stg_ledger_transactions') }}
    union
    select date as full_date from {{ ref('stg_equity_prices') }}
    union
    select cast(api_timestamp as date) as full_date from {{ ref('stg_precious_metals') }}
    union
    select rate_date as full_date from {{ ref('stg_exchange_rates') }}
)

select distinct
    cast(
        extract(year from full_date) * 10000
        + extract(month from full_date) * 100
        + extract(day from full_date)
    as integer) as date_key,
    full_date,
    extract(year from full_date) as year,
    extract(quarter from full_date) as quarter,
    extract(month from full_date) as month,
    extract(day from full_date) as day
from all_dates
where full_date is not null
