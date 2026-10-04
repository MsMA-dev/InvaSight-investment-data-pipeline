-- SAR per 1 unit of each currency, per date. Everything is reported in SAR.
-- Rates are normalised to a USD base first: the FX task rebases to USD before
-- landing, but older loads arrived EUR-based and are rebased here.

with rates as (
    select rate_date, base_currency, target_currency, exchange_rate
    from {{ ref('stg_exchange_rates') }}
    where exchange_rate > 0
),

-- Loads that already arrived USD-based (preferred when both exist)
usd_native as (
    select
        rate_date,
        target_currency,
        exchange_rate as usd_to_currency,
        0 as priority
    from rates
    where base_currency = 'USD'
),

eur_to_usd as (
    select rate_date, exchange_rate as eur_to_usd
    from rates
    where base_currency = 'EUR'
      and target_currency = 'USD'
),

-- Legacy EUR-based loads converted to a USD base
usd_rebased as (
    select
        r.rate_date,
        r.target_currency,
        r.exchange_rate / e.eur_to_usd as usd_to_currency,
        1 as priority
    from rates r
    inner join eur_to_usd e
        on r.rate_date = e.rate_date
    where r.base_currency = 'EUR'

    union all

    -- EUR is the base of those loads, so it never appears in their rates
    select
        rate_date,
        'EUR' as target_currency,
        1 / eur_to_usd as usd_to_currency,
        1 as priority
    from eur_to_usd
),

-- One USD-based rate per date and currency
usd_rates as (
    select rate_date, target_currency, usd_to_currency
    from (
        select * from usd_native
        union all
        select * from usd_rebased
    ) as all_rates
    qualify row_number() over (
        partition by rate_date, target_currency
        order by priority
    ) = 1
),

usd_to_sar as (
    select rate_date, usd_to_currency as usd_to_sar
    from usd_rates
    where target_currency = 'SAR'
)

-- SAR per 1 unit of currency = usd_to_sar / usd_to_currency
select
    r.rate_date,
    r.target_currency as currency_code,
    s.usd_to_sar / r.usd_to_currency as rate_to_sar
from usd_rates r
inner join usd_to_sar s
    on r.rate_date = s.rate_date
