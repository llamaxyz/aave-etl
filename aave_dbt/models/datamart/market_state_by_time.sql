{{ config(materialized='table') }}

with all_data as (
select
  block_hour as block_time
  , block_height
  , market
  , reserve
  , atoken_symbol
  , unbacked_atokens
  , scaled_accrued_to_treasury
  , atoken_supply
  , stable_debt
  , variable_debt
  , liquidity_rate
  , variable_borrow_rate
  , stable_borrow_rate
  , average_stable_rate 
  , liquidity_index
  , variable_borrow_index
  , available_liquidity
  , deposit_apy
  , variable_borrow_apy
  , stable_borrow_apy
  , av_stable_borrow_apy
-- from datamart.market_state_by_hour
from {{ ref('market_state_by_hour' )}}
union all
select 
  block_day as block_time
  , block_height
  , market
  , reserve
  , atoken_symbol
  , unbacked_atokens
  , scaled_accrued_to_treasury
  , atoken_supply
  , stable_debt
  , variable_debt
  , liquidity_rate
  , variable_borrow_rate
  , stable_borrow_rate
  , average_stable_rate 
  , liquidity_index
  , variable_borrow_index
  , available_liquidity
  , deposit_apy
  , variable_borrow_apy
  , stable_borrow_apy
  , av_stable_borrow_apy
-- from datamart.market_state_by_day 
from {{ ref('market_state_by_day' )}}
)

select distinct * from all_data
order by market, atoken_symbol, block_time

