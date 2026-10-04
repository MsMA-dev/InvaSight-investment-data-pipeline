-- One row per ticker and trading day, from the nested Alpha Vantage payload:
-- { "<ticker>": { "Meta Data": {...}, "Time Series (Daily)": { "<date>": {ohlcv} } } }

with source as (
    select raw, file_name, loaded_at
    from {{ source('raw', 'equity_prices_raw') }}
),

-- Step 1: one row per ticker (outer JSON keys)
tickers as (
    select
        source.file_name,
        source.loaded_at,
        ticker_flat.key as ticker,
        ticker_flat.value as ticker_data
    from source, {{ flatten_object('source.raw') }} as ticker_flat
    where ticker_flat.key != 'Meta Data'
),

-- Step 2: one row per ticker and date
dates_flat as (
    select
        tickers.file_name,
        tickers.loaded_at,
        tickers.ticker,
        date_flat.key as trade_date,
        date_flat.value as ohlcv
    from tickers, {{ flatten_object(json_get('tickers.ticker_data', ['Time Series (Daily)'])) }} as date_flat
),

-- Step 3: extract and cast
cleaned as (
    select
        ticker,
        try_cast(trade_date as date) as trade_date,
        try_cast({{ json_str('ohlcv', ['1. open']) }} as double) as open_price,
        try_cast({{ json_str('ohlcv', ['2. high']) }} as double) as high_price,
        try_cast({{ json_str('ohlcv', ['3. low']) }} as double) as low_price,
        try_cast({{ json_str('ohlcv', ['4. close']) }} as double) as close_price,
        try_cast({{ json_str('ohlcv', ['5. volume']) }} as bigint) as volume,
        file_name,
        loaded_at,
        'alpha_vantage' as source
    from dates_flat
)

-- Step 4: every daily pull repeats ~100 days of history, so the same
-- ticker+date arrives many times; keep the most recently loaded copy
select
    ticker,
    trade_date as date,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    file_name,
    source,
    loaded_at
from cleaned
qualify row_number() over (
    partition by ticker, trade_date
    order by loaded_at desc, file_name desc
) = 1
