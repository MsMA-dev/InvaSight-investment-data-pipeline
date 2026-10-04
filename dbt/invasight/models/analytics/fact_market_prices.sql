-- Grain: one row per asset, date and source, with the SAR-converted price.

with fx as (
    select * from {{ ref('int_fx_rates_to_sar') }}
),

equity_prices as (
    select
        ticker,
        date,
        close_price as price,
        'USD' as currency_code,
        'EQUITY_API' as source_name,
        loaded_at
    from {{ ref('stg_equity_prices') }}
),

-- One row per metal per day (USD per troy ounce)
metals as (
    select * from {{ ref('stg_precious_metals') }} where success
),

metal_prices as (
    select 'XAU' as ticker, cast(api_timestamp as date) as date, gold_usd as price, 'USD' as currency_code, 'Metal Market Data' as source_name, loaded_at from metals where gold_usd is not null
    union all
    select 'XAG', cast(api_timestamp as date), silver_usd, 'USD', 'Metal Market Data', loaded_at from metals where silver_usd is not null
    union all
    select 'XPT', cast(api_timestamp as date), platinum_usd, 'USD', 'Metal Market Data', loaded_at from metals where platinum_usd is not null
    union all
    select 'XPD', cast(api_timestamp as date), palladium_usd, 'USD', 'Metal Market Data', loaded_at from metals where palladium_usd is not null
),

staging_prices as (
    select * from equity_prices
    union all
    select * from metal_prices
),

-- Most recent SAR rate on or before each price date (see fact_holdings)
prices_with_fx as (
    select
        p.*,
        fx.rate_date as fx_rate_date,
        fx.rate_to_sar
    from staging_prices p
    {{ asof_left_join('fx', 'fx', 'p.date >= fx.rate_date', 'p.currency_code = fx.currency_code') }}
),

joined as (
    select
        a.asset_key,
        d.date_key,
        curr.currency_key,
        s.source_key,
        p.price,
        p.rate_to_sar as exchange_rate,
        p.fx_rate_date,
        p.price * p.rate_to_sar as price_sar,
        p.loaded_at
    from prices_with_fx p
    inner join {{ ref('dim_asset') }} a on p.ticker = a.ticker
    inner join {{ ref('dim_date') }} d on p.date = d.full_date
    left join {{ ref('dim_currency') }} curr on p.currency_code = curr.currency_code
    left join {{ ref('dim_source') }} s on p.source_name = s.source_name
)

select
    {{ dbt_utils.generate_surrogate_key(['asset_key', 'date_key', 'source_key']) }} as market_price_key,
    asset_key,
    date_key,
    currency_key,
    source_key,
    price,
    exchange_rate,
    fx_rate_date,
    price_sar,
    loaded_at
from joined
qualify row_number() over (
    partition by asset_key, date_key, source_key
    order by loaded_at desc
) = 1
