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

, deduplicated as (
select distinct * from all_data
)

select 
  d.*
  , c.chain
  , c.display_chain
  , c.display_market
  , a.reserve_symbol
  , p.usd_price
  , d.atoken_supply * p.usd_price as deposits_usd
  , d.stable_debt * p.usd_price as stable_debt_usd
  , d.variable_debt * p.usd_price as variable_debt_usd
from deduplicated d
  left join {{ ref('chains_markets') }} c on (m.market = c.market)
  -- left join datamart.chains_markets c on (d.market = c.market)
  left join {{ ref('aave_atokens') }} a on (m.market = a.market and m.reserve = a.reserve)
  -- left join datamart.aave_atokens a on (d.market = a.market and d.reserve = a.reserve)
  left join {{ source('financials_data_lake' 'aave_oracle_prices_by_day') }} p on (date_trunc(d.block_time, day) = p.block_day and d.reserve = p.reserve and d.market = p.market)
  -- left join financials_data_lake.aave_oracle_prices_by_day p on (date_trunc(d.block_time, day) = p.block_day and d.reserve = p.reserve and d.market = p.market)
order by d.market, d.atoken_symbol, d.block_time

