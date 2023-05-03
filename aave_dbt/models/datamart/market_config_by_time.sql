{{ config(materialized='table') }}

with all_data as (
select
  block_hour as block_time
  , block_height
  , market
  , reserve
  , atoken_symbol
  , decimals
  , ltv
  , liquidation_threshold
  , liquidation_bonus
  , reserve_factor
  , usage_as_collateral_enabled
  , borrowing_enabled
  , stable_borrow_rate_enabled
  , is_active
  , is_frozen
  , reserve_emode_category
  , borrow_cap
  , supply_cap
  , is_paused
  , siloed_borrowing
  , liquidation_protocol_fee
  , unbacked_mint_cap
  , debt_ceiling
  , emode_category_name
  , emode_ltv
  , emode_liquidation_bonus
  , emode_liquidation_threshold
-- from datamart.market_config_by_hour
from {{ ref('market_config_by_hour' )}}
union all
select 
  block_day as block_time
  , block_height
  , market
  , reserve
  , atoken_symbol
  , decimals
  , ltv
  , liquidation_threshold
  , liquidation_bonus
  , reserve_factor
  , usage_as_collateral_enabled
  , borrowing_enabled
  , stable_borrow_rate_enabled
  , is_active
  , is_frozen
  , reserve_emode_category
  , borrow_cap
  , supply_cap
  , is_paused
  , siloed_borrowing
  , liquidation_protocol_fee
  , unbacked_mint_cap
  , debt_ceiling
  , emode_category_name
  , emode_ltv
  , emode_liquidation_bonus
  , emode_liquidation_threshold
-- from datamart.market_config_by_day 
from {{ ref('market_config_by_day' )}}
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
from deduplicated d
  left join {{ ref('chains_markets') }} c on (d.market = c.market)
  -- left join datamart.chains_markets c on (d.market = c.market)
  left join {{ ref('aave_atokens') }} a on (d.market = a.market and d.reserve = a.reserve)
  -- left join datamart.aave_atokens a on (d.market = a.market and d.reserve = a.reserve)
order by d.market, d.atoken_symbol, d.block_time

