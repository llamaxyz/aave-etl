{{ config(materialized='table') }}

select 
  block_day
  , stk_token_symbol
  , emission_usd_per_day * 365 / safety_module_cover as annual_cost_of_cover_percent
-- from datamart.safety_module_totals_by_day 
from {{ ref('safety_module_totals_by_day') }}
order by block_day, stk_token_symbol