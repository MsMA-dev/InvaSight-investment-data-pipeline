-- Grain: one row per client and date.

with daily_positions as (
    select
        client_key,
        date_key,
        asset_key,
        sum(quantity) as total_quantity,
        sum(value_sar) as total_investment,
        sum(fees) as total_fees
    from {{ ref('fact_holdings') }}
    group by client_key, date_key, asset_key
),

-- Value each position at the latest market price on or before that date
position_prices as (
    select
        p.client_key,
        p.date_key,
        p.asset_key,
        p.total_quantity,
        p.total_investment,
        p.total_fees,
        coalesce(m.price_sar, 0) as price_sar
    from daily_positions p
    left join {{ ref('fact_market_prices') }} m
        on p.asset_key = m.asset_key
       and m.date_key <= p.date_key
    qualify row_number() over (
        partition by p.client_key, p.date_key, p.asset_key
        order by m.date_key desc
    ) = 1
),

portfolio as (
    select
        client_key,
        date_key,
        sum(total_investment) as total_investment,
        sum(total_quantity * price_sar) as portfolio_value_sar,
        sum(total_fees) as total_fees
    from position_prices
    group by client_key, date_key
),

with_previous as (
    select
        *,
        lag(portfolio_value_sar) over (partition by client_key order by date_key) as prev_value_sar
    from portfolio
)

select
    {{ dbt_utils.generate_surrogate_key(['client_key', 'date_key']) }} as portfolio_summary_key,
    date_key,
    client_key,
    cast(total_investment as decimal(38, 4)) as total_investment,
    cast(portfolio_value_sar as decimal(38, 4)) as portfolio_value_sar,
    cast(total_fees as decimal(38, 4)) as total_fees,
    -- (today - previous day) / previous day; 0 on a client's first day
    cast(
        coalesce((portfolio_value_sar - prev_value_sar) / nullif(prev_value_sar, 0), 0)
    as decimal(38, 6)) as daily_return,
    -- P&L = current portfolio value - total investment
    cast(portfolio_value_sar - total_investment as decimal(38, 4)) as pnl_sar,
    cast(portfolio_value_sar as decimal(38, 4)) as currency_exposure_sar
from with_previous
