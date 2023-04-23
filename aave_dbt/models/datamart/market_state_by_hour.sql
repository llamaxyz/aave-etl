{{ config(materialized='table') }}

select
  p.block_hour
  , p.block_height
  , p.market
  , p.reserve
  , p.symbol as atoken_symbol
  , p.unbacked_atokens
  , p.scaled_accrued_to_treasury
  , p.atoken_supply
  , p.stable_debt
  , p.variable_debt
  , p.liquidity_rate
  , p.variable_borrow_rate
  , p.stable_borrow_rate
  , p.average_stable_rate 
  , p.liquidity_index
  , p.variable_borrow_index
  , p.available_liquidity
  -- as per https://docs.aave.com/risk/liquidity-risk/borrow-interest-rate
  , pow(1 + p.liquidity_rate / (60*60*24*365),60*60*24*365) - 1 as deposit_apy
  , pow(1 + p.variable_borrow_rate / (60*60*24*365),60*60*24*365) - 1 as variable_borrow_apy
  , pow(1 + p.stable_borrow_rate / (60*60*24*365),60*60*24*365) - 1 as stable_borrow_apy
  , pow(1 + p.average_stable_rate / (60*60*24*365),60*60*24*365) - 1 as av_stable_borrow_apy
-- from protocol_data_lake.protocol_data_by_hour p
from {{ source('protocol_data_lake','protocol_data_by_hour')}} p
order by market, symbol, block_hour