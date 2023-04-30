{{ config(materialized='table') }}

-- Upstream table all_treasury_balances uses end of day balances
-- This is translated to a start of day balance by incrementing the block day by 1

select
  date_add(block_day, interval 1 day) as block_day
  , display_chain
  , display_market
  , collector_label
  , token as token_held_address
  , symbol as token_held_symbol
  , underlying_reserve
  , underlying_reserve_symbol
  , sum(value_usd) as value_usd
  , sum(value_native) as value_native
-- from datamart.all_treasury_balances
from {{ref('all_treasury_balances')}}
group by block_day, display_chain, display_market, collector_label, token, symbol, underlying_reserve, underlying_reserve_symbol
order by block_day, display_chain, display_market, collector_label, token, symbol, underlying_reserve, underlying_reserve_symbol
