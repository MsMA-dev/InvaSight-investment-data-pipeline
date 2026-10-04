with equity_assets as (
    select distinct ticker, 'EQUITY' as asset_type, 1 as priority
    from {{ ref('stg_equity_prices') }}
),

metal_assets as (
    select ticker, 'METAL' as asset_type, 0 as priority
    from (values ('XAU'), ('XAG'), ('XPT'), ('XPD')) as t(ticker)
),

-- Tickers traded in the ledger but not (yet) priced still need a dimension row
ledger_assets as (
    select distinct ticker, 'EQUITY' as asset_type, 2 as priority
    from {{ ref('stg_ledger_transactions') }}
    where ticker is not null
),

combined as (
    select * from equity_assets
    union all
    select * from metal_assets
    union all
    select * from ledger_assets
),

deduped as (
    select ticker, asset_type
    from combined
    where ticker is not null
    qualify row_number() over (partition by ticker order by priority) = 1
)

select
    md5(ticker) as asset_key,
    ticker,
    ticker as asset_name,
    asset_type,
    case
        when ticker in ('XAU', 'XAG', 'XPT', 'XPD') then 'METALS'
        when ticker in ('AAPL', 'MSFT', 'GOOGL', 'IBM', 'SAP') then 'TECHNOLOGY'
        when ticker = 'VOD' then 'TELECOMMUNICATIONS'
        when ticker in ('GLD', 'SPY') then 'ETF'
        else 'OTHER'
    end as sector
from deduped
