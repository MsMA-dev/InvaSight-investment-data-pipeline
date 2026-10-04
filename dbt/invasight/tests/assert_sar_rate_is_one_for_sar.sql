-- Sanity check on the FX triangulation: converting SAR to SAR must give 1.

select rate_date, rate_to_sar
from {{ ref('int_fx_rates_to_sar') }}
where currency_code = 'SAR'
  and abs(rate_to_sar - 1) > 0.000001
