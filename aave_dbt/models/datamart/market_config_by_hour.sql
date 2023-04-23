{{ config(materialized='table') }}

select
  p.block_hour
  , p.block_height
  , p.market
  , p.reserve
  , p.symbol as atoken_symbol
  , p.decimals
  , p.ltv
  , p.liquidation_threshold
  , p.liquidation_bonus
  , p.reserve_factor
  , p.usage_as_collateral_enabled
  , p.borrowing_enabled
  , p.stable_borrow_rate_enabled
  , p.is_active
  , p.is_frozen
  , p.reserve_emode_category
  , p.borrow_cap
  , p.supply_cap
  , p.is_paused 
  , p.siloed_borrowing
  , p.liquidation_protocol_fee
  , p.unbacked_mint_cap
  , p.debt_ceiling
  , date_trunc(p.block_hour, day) as block_day
  , last_value(e.emode_category_name ignore nulls) over (partition by p.market, p.symbol order by p.block_hour) as emode_category_name
  , last_value(e.emode_ltv ignore nulls) over (partition by p.market, p.symbol order by p.block_hour) as emode_ltv
  , last_value(e.emode_liquidation_bonus ignore nulls) over (partition by p.market, p.symbol order by p.block_hour) as emode_liquidation_bonus
  , last_value(e.emode_liquidation_threshold ignore nulls) over (partition by p.market, p.symbol order by p.block_hour) as emode_liquidation_threshold
-- from protocol_data_lake.protocol_data_by_hour p
from {{ source('protocol_data_lake','protocol_data_by_hour')}} p
--   left join protocol_data_lake.emode_config_by_day e on (
  left join {{ source('protocol_data_lake','emode_config_by_day')}} e on (
    date_trunc(p.block_hour, day) = e.block_day and
    p.market = e.market and 
    p.reserve_emode_category = e.reserve_emode_category
  )
order by market, symbol, block_hour