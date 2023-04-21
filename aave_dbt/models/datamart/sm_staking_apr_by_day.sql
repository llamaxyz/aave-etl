{{ config(materialized='table') }}

select 
  block_day
  , stk_token_symbol
  , emission_apr as staking_apr
-- from datamart.safety_module_totals_by_day 
from {{ ref('safety_module_totals_by_day') }}