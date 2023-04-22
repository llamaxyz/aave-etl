{{ config(materialized='table') }}

select 
  block_day
  , stk_token_symbol
  , staked_amt_usd as staked_value_usd
-- from datamart.safety_module_totals_by_day 
from {{ ref('safety_module_totals_by_day') }}
order by block_day, stk_token_symbol