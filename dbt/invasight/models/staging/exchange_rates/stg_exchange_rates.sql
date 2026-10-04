-- One row per date, base currency and target currency

with raw_parsed as (
    select
        {{ json_str('src.raw', ['base']) }} as base_currency,
        try_cast({{ json_str('src.raw', ['date']) }} as date) as rate_date,
        {{ epoch_to_timestamp(json_str('src.raw', ['timestamp'])) }} as rate_timestamp,
        f.key as target_currency,
        try_cast(cast(f.value as varchar) as decimal(38, 6)) as exchange_rate,
        src.file_name,
        src.loaded_at
    from {{ source('raw', 'fx_rates_raw') }} as src,
        {{ flatten_object(json_get('src.raw', ['rates'])) }} as f
    where try_cast({{ json_str('src.raw', ['success']) }} as boolean) = true
)

select
    rate_date,
    rate_timestamp,
    base_currency,
    target_currency,
    exchange_rate,
    file_name,
    loaded_at
from raw_parsed
qualify row_number() over (
    partition by rate_date, base_currency, target_currency
    order by loaded_at desc, file_name desc
) = 1
