-- One row per metal-prices API response. USDXAU etc. are USD per troy ounce.

with parsed as (
    select
        try_cast({{ json_str('raw', ['success']) }} as boolean) as success,
        {{ json_str('raw', ['base']) }} as base_currency,
        {{ epoch_to_timestamp(json_str('raw', ['timestamp'])) }} as api_timestamp,
        try_cast({{ json_str('raw', ['rates', 'USDXAU']) }} as decimal(38, 12)) as gold_usd,
        try_cast({{ json_str('raw', ['rates', 'USDXAG']) }} as decimal(38, 12)) as silver_usd,
        try_cast({{ json_str('raw', ['rates', 'USDXPT']) }} as decimal(38, 12)) as platinum_usd,
        try_cast({{ json_str('raw', ['rates', 'USDXPD']) }} as decimal(38, 12)) as palladium_usd,
        file_name,
        loaded_at
    from {{ source('raw', 'metal_prices_raw') }}
)

select *
from parsed
qualify row_number() over (
    partition by api_timestamp
    order by loaded_at desc, file_name desc
) = 1
