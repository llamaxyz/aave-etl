{{ config(materialized='table') }}

select 
  block_day
  , stk_token_symbol
  , stk_token_supply as stk_tokens
-- from datamart.safety_module_totals_by_day 
from {{ ref('safety_module_totals_by_day') }}
order by block_day, stk_token_symbol