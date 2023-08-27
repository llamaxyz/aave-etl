{{ config(materialized='table') }}

with prices as (
select
  block_day
  , symbol
  , price_usd as usd_price
-- from protocol_data_lake.coingecko_data_by_day 
from {{ source('protocol_data_lake', 'coingecko_data_by_day') }}
where true
  and chain = 'ethereum' 
  and symbol in ('USDT','USDC','DAI')
)
, compound as (
select
  block_day as block_time
  , compound_version as market
  , underlying_symbol as symbol
  , deposits as deposits_native
  , supply_apy as deposit_apy
-- from protocol_data_lake.compound_v2_by_day
from {{ source('protocol_data_lake', 'compound_v2_by_day') }}
union all 
select
  block_day as block_time
  , compound_version as market
  , underlying_symbol as symbol
  , deposits as deposits_native
  , supply_apy as deposit_apy
-- from protocol_data_lake.compound_v3_by_day
from {{ source('protocol_data_lake', 'compound_v3_by_day') }}
union all 
select
  block_hour as block_time
  , compound_version as market
  , underlying_symbol as symbol
  , deposits as deposits_native
  , supply_apy as deposit_apy
-- from protocol_data_lake.compound_v2_by_hour
from {{ source('protocol_data_lake', 'compound_v2_by_hour') }}
union all 
select
  block_hour as block_time
  , compound_version as market
  , underlying_symbol as symbol
  , deposits as deposits_native
  , supply_apy as deposit_apy
-- from protocol_data_lake.compound_v3_by_hour
from {{ source('protocol_data_lake', 'compound_v3_by_hour') }}
)

, combined_daily as (
select
  date_trunc(block_time, day) as block_day
  , 'aave_' || right(market, 2) as market
  , reserve_symbol as symbol
  , avg(atoken_supply) as deposits_native
  , avg(deposit_apy) as deposit_apy
-- from datamart.market_state_by_time 
from {{ ref('market_state_by_time') }}
where market in ('ethereum_v2', 'ethereum_v3') and reserve_symbol in ('USDT','USDC','DAI')
group by date_trunc(block_time, day), market, reserve_symbol
union all 
select
  date_trunc(block_time, day) as block_day
  , market
  , symbol
  , avg(deposits_native) as deposits_native
  , avg(deposit_apy) as deposit_apy
from compound
group by date_trunc(block_time, day), market, symbol
)

, pre_calc as (
select 
  d.block_day
  , d.market
  , d.symbol 
  , d.deposits_native
  , d.deposit_apy
  , p.usd_price as price_usd
  , coalesce(lead(usd_price) over (partition by d.market, d.symbol order by d.block_day), usd_price) as end_price_usd
  , d.deposits_native * p.usd_price as deposits_usd
from combined_daily d
left join prices p on (d.block_day = p.block_day and d.symbol = p.symbol)
order by block_day, market, symbol
)

select 
  block_day
  , market 
  , symbol
  , deposits_native 
  , deposit_apy 
  , price_usd 
  , end_price_usd
  , deposits_native * price_usd as deposits_usd
  , deposits_native * deposit_apy / 365 as earnings_native
  , deposits_native * deposit_apy / 365 * price_usd as earnings_usd
  , deposits_native * (1 + deposit_apy / 365) * (end_price_usd - price_usd) as price_change_usd 
from pre_calc
order by block_day, market, symbol 