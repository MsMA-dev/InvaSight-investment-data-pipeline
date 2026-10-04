-- Grain: one row per ledger transaction (BUY or SELL), valued in SAR.

with ledger as (
    select * from {{ ref('stg_ledger_transactions') }}
),

fx as (
    select * from {{ ref('int_fx_rates_to_sar') }}
),

-- Most recent SAR rate on or before each transaction date. FX is fetched once
-- a day, so intraday transactions and weekends use the latest published rate.
-- No fallback: a missing rate leaves the SAR columns NULL instead of 1.0.
ledger_with_fx as (
    select
        l.*,
        fx.rate_date as fx_rate_date,
        fx.rate_to_sar
    from ledger l
    {{ asof_left_join('fx', 'fx', 'l.transaction_date >= fx.rate_date', 'l.currency_code = fx.currency_code') }}
)

select
    md5(l.transaction_id) as holding_key,
    d.date_key,
    a.asset_key,
    c.client_key,
    curr.currency_key,
    l.transaction_type,
    l.quantity,
    case l.transaction_type
        when 'BUY' then l.quantity
        when 'SELL' then -l.quantity
    end as signed_quantity,
    l.price,
    l.rate_to_sar as exchange_rate,
    l.fx_rate_date,
    l.fees,
    l.quantity * l.price as transaction_value,
    l.quantity * l.price * l.rate_to_sar as value_sar
from ledger_with_fx l
left join {{ ref('dim_date') }} d on l.transaction_date = d.full_date
left join {{ ref('dim_asset') }} a on l.ticker = a.ticker
left join {{ ref('dim_client') }} c on l.client_id = c.client_id
left join {{ ref('dim_currency') }} curr on l.currency_code = curr.currency_code
