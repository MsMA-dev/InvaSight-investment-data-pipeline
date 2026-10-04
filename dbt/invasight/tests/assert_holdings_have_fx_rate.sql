-- Every transaction must find an FX rate on or before its date. A NULL here
-- means FX data is missing for that currency/period, and value_sar would be
-- silently dropped from every SAR total on the dashboard.

select holding_key, date_key, currency_key
from {{ ref('fact_holdings') }}
where exchange_rate is null
   or value_sar is null
