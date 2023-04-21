{{ config(materialized='table') }}

select 
  s.block_day
  , s.stk_token_address
  , s.stk_token_symbol
  , coalesce(ps.usd_price, pu.usd_price, cg.price_usd) as stk_token_price
  , s.stk_token_supply 
  , s.unstaked_token_supply
  , s.stk_token_supply / s.unstaked_token_supply as staked_proportion
  , s.emission_per_day
  , s.reward_token_address
  , s.reward_token_symbol
  , pr.usd_price as reward_token_price
  , s.stk_token_supply * coalesce(ps.usd_price, pu.usd_price, cg.price_usd) as staked_amt_usd
  , s.emission_per_day * pr.usd_price as emission_usd_per_day
  , s.emission_per_day * pr.usd_price * 365 / 
      (s.stk_token_supply * coalesce(ps.usd_price, pu.usd_price, cg.price_usd)) as emission_apr
  -- safety module will use up to 0.3 the staked balance in a shortfall event
  , s.stk_token_supply * coalesce(ps.usd_price, pu.usd_price, cg.price_usd) * 0.3 as safety_module_cover
  -- cost of cover is the annualised emissions divided by the cover
  , s.emission_per_day * pr.usd_price * 365 / 
    (s.stk_token_supply * coalesce(ps.usd_price, pu.usd_price, cg.price_usd) * 0.3) as cost_of_cover_apr

-- from protocol_data_lake.safety_module_rpc s
from {{ source('protocol_data_lake','safety_module_rpc') }} s 
--   left join warehouse.token_prices_by_day pr on (
  left join {{ source('warehouse','token_prices_by_day') }} pr on (
    s.block_day = pr.block_day and
    s.reward_token_address = pr.reserve
  )
--   left join warehouse.token_prices_by_day ps on (
  left join {{ source('warehouse','token_prices_by_day') }} ps on (
    s.block_day = ps.block_day and
    s.stk_token_address = ps.reserve
  )
--   left join warehouse.token_prices_by_day pu on (
  left join {{ source('warehouse','token_prices_by_day') }} pu on (
    s.block_day = pu.block_day and
    s.unstaked_token_address = pu.reserve
  )
--   left join protocol_data_lake.coingecko_data_by_day cg on (
  left join {{ source('protocol_data_lake','coingecko_data_by_day') }} cg on (
    s.block_day = cg.block_day and
    s.stk_token_address = cg.address
  )
order by block_day
