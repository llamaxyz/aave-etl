{{ config(materialized='table') }}

with prices as (
select 
  block_day
  , chain
  , reserve
  , usd_price
-- from warehouse.token_prices_by_day
from {{ source('warehouse', 'token_prices_by_day') }}
)
, all_sources as (
select 
  block_time
  , 'aave_' || right(market, 2) as market
  , reserve_symbol as symbol
  , deposits_usd
  , stable_debt_usd + variable_debt_usd as borrows_usd
  , deposits_usd - (stable_debt_usd + variable_debt_usd) as tvl_usd
  , deposit_apy
-- from datamart.market_state_by_time
from {{ ref('market_state_by_time') }}
where market in ('ethereum_v2','ethereum_v3')
  and reserve_symbol in ('DAI','USDC','USDT')
union all 
select 
  c.block_hour as block_time
  , c.compound_version as market
  , c.underlying_symbol as symbol
  , c.deposits * p.usd_price as deposits_usd
  , c.borrows * p.usd_price as borrows_usd
  , (c.deposits - c.borrows) * p.usd_price as tvl_usd
  , c.supply_apy as deposit_apy
-- from protocol_data_lake.compound_v2_by_hour c
from {{ source('protocol_data_lake', 'compound_v2_by_hour') }} c
  left join prices p on (date_trunc(c.block_hour, day) = p.block_day and c.underlying_address = p.reserve and c.chain = p.chain)
union all
select 
  c.block_day as block_time
  , c.compound_version as market
  , c.underlying_symbol as symbol
  , c.deposits * p.usd_price as deposits_usd
  , c.borrows * p.usd_price as borrows_usd
  , (c.deposits - c.borrows) * p.usd_price as tvl_usd
  , c.supply_apy as deposit_apy
-- from protocol_data_lake.compound_v2_by_day c
from {{ source('protocol_data_lake', 'compound_v2_by_day') }} c
  left join prices p on (c.block_day = p.block_day and c.underlying_address = p.reserve and c.chain = p.chain)
union all 
select 
  c.block_hour as block_time
  , c.compound_version as market
  , c.underlying_symbol as symbol
  , c.deposits * p.usd_price as deposits_usd
  , c.borrows * p.usd_price as borrows_usd
  , (c.deposits - c.borrows) * p.usd_price as tvl_usd
  , c.supply_apy as deposit_apy
-- from protocol_data_lake.compound_v3_by_hour c
from {{ source('protocol_data_lake', 'compound_v3_by_hour') }} c
  left join prices p on (date_trunc(c.block_hour, day) = p.block_day and c.underlying_address = p.reserve and c.chain = p.chain)
union all 
select 
  c.block_day as block_time
  , c.compound_version as market
  , c.underlying_symbol as symbol
  , c.deposits * p.usd_price as deposits_usd
  , c.borrows * p.usd_price as borrows_usd
  , (c.deposits - c.borrows) * p.usd_price as tvl_usd
  , c.supply_apy as deposit_apy
-- from protocol_data_lake.compound_v3_by_day c
from {{ source('protocol_data_lake', 'compound_v3_by_day') }} c
  left join prices p on (c.block_day = p.block_day and c.underlying_address = p.reserve and c.chain = p.chain)
)

select 
  block_time 
  , market 
  , symbol 
  , deposits_usd 
  , borrows_usd 
  , tvl_usd 
  , deposit_apy 
  , deposits_usd * deposit_apy as deposits_mul_apy
from all_sources
where deposits_usd > 0
order by block_time, market, symbol

