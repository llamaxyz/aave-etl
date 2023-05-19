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

, times as (
select distinct
  block_time
-- from datamart.market_state_by_time
from {{ ref('market_state_by_time') }}
where market in ('ethereum_v2','ethereum_v3')
order by block_time
)

, all_markets as (
select 
  t.block_time
  , last_value('aave_' || right(a.market, 2) ignore nulls) over (order by t.block_time range between unbounded preceding and current row) as market
  , last_value(a.reserve_symbol ignore nulls) over (order by t.block_time range between unbounded preceding and current row) as symbol
  , last_value(a.deposits_usd ignore nulls) over (order by t.block_time range between unbounded preceding and current row) as deposits_usd
  , last_value(a.stable_debt_usd + a.variable_debt_usd ignore nulls) over (order by t.block_time range between unbounded preceding and current row) as borrows_usd
  , last_value(a.deposits_usd - (a.stable_debt_usd + a.variable_debt_usd) ignore nulls) over (order by t.block_time range between unbounded preceding and current row) as tvl_usd
  , last_value(a.deposit_apy ignore nulls) over (order by t.block_time range between unbounded preceding and current row) as deposit_apy
from times t 
  -- left join datamart.market_state_by_time a
  left join {{ ref('market_state_by_time') }} a
    on (t.block_time = a.block_time and a.market = 'ethereum_v2' and a.reserve_symbol = 'USDT')
union all
select 
  t.block_time
  , last_value('aave_' || right(a.market, 2) ignore nulls) over (order by t.block_time range between unbounded preceding and current row) as market
  , last_value(a.reserve_symbol ignore nulls) over (order by t.block_time range between unbounded preceding and current row) as symbol
  , last_value(a.deposits_usd ignore nulls) over (order by t.block_time range between unbounded preceding and current row) as deposits_usd
  , last_value(a.stable_debt_usd + a.variable_debt_usd ignore nulls) over (order by t.block_time range between unbounded preceding and current row) as borrows_usd
  , last_value(a.deposits_usd - (a.stable_debt_usd + a.variable_debt_usd) ignore nulls) over (order by t.block_time range between unbounded preceding and current row) as tvl_usd
  , last_value(a.deposit_apy ignore nulls) over (order by t.block_time range between unbounded preceding and current row) as deposit_apy
from times t 
  -- left join datamart.market_state_by_time a
  left join {{ ref('market_state_by_time') }} a
    on (t.block_time = a.block_time and a.market = 'ethereum_v2' and a.reserve_symbol = 'USDC')
union all
select 
  t.block_time
  , last_value('aave_' || right(a.market, 2) ignore nulls) over (order by t.block_time range between unbounded preceding and current row) as market
  , last_value(a.reserve_symbol ignore nulls) over (order by t.block_time range between unbounded preceding and current row) as symbol
  , last_value(a.deposits_usd ignore nulls) over (order by t.block_time range between unbounded preceding and current row) as deposits_usd
  , last_value(a.stable_debt_usd + a.variable_debt_usd ignore nulls) over (order by t.block_time range between unbounded preceding and current row) as borrows_usd
  , last_value(a.deposits_usd - (a.stable_debt_usd + a.variable_debt_usd) ignore nulls) over (order by t.block_time range between unbounded preceding and current row) as tvl_usd
  , last_value(a.deposit_apy ignore nulls) over (order by t.block_time range between unbounded preceding and current row) as deposit_apy
from times t 
  -- left join datamart.market_state_by_time a
  left join {{ ref('market_state_by_time') }} a
    on (t.block_time = a.block_time and a.market = 'ethereum_v2' and a.reserve_symbol = 'DAI')
union all 
select 
  t.block_time
  , last_value('aave_' || right(a.market, 2) ignore nulls) over (order by t.block_time range between unbounded preceding and current row) as market
  , last_value(a.reserve_symbol ignore nulls) over (order by t.block_time range between unbounded preceding and current row) as symbol
  , last_value(a.deposits_usd ignore nulls) over (order by t.block_time range between unbounded preceding and current row) as deposits_usd
  , last_value(a.stable_debt_usd + a.variable_debt_usd ignore nulls) over (order by t.block_time range between unbounded preceding and current row) as borrows_usd
  , last_value(a.deposits_usd - (a.stable_debt_usd + a.variable_debt_usd) ignore nulls) over (order by t.block_time range between unbounded preceding and current row) as tvl_usd
  , last_value(a.deposit_apy ignore nulls) over (order by t.block_time range between unbounded preceding and current row) as deposit_apy
from times t 
  -- left join datamart.market_state_by_time a
  left join {{ ref('market_state_by_time') }} a
    on (t.block_time = a.block_time and a.market = 'ethereum_v3' and a.reserve_symbol = 'USDT')
union all
select 
  t.block_time
  , last_value('aave_' || right(a.market, 2) ignore nulls) over (order by t.block_time range between unbounded preceding and current row) as market
  , last_value(a.reserve_symbol ignore nulls) over (order by t.block_time range between unbounded preceding and current row) as symbol
  , last_value(a.deposits_usd ignore nulls) over (order by t.block_time range between unbounded preceding and current row) as deposits_usd
  , last_value(a.stable_debt_usd + a.variable_debt_usd ignore nulls) over (order by t.block_time range between unbounded preceding and current row) as borrows_usd
  , last_value(a.deposits_usd - (a.stable_debt_usd + a.variable_debt_usd) ignore nulls) over (order by t.block_time range between unbounded preceding and current row) as tvl_usd
  , last_value(a.deposit_apy ignore nulls) over (order by t.block_time range between unbounded preceding and current row) as deposit_apy
from times t 
  -- left join datamart.market_state_by_time a
  left join {{ ref('market_state_by_time') }} a
    on (t.block_time = a.block_time and a.market = 'ethereum_v3' and a.reserve_symbol = 'USDC')
union all
select 
  t.block_time
  , last_value('aave_' || right(a.market, 2) ignore nulls) over (order by t.block_time range between unbounded preceding and current row) as market
  , last_value(a.reserve_symbol ignore nulls) over (order by t.block_time range between unbounded preceding and current row) as symbol
  , last_value(a.deposits_usd ignore nulls) over (order by t.block_time range between unbounded preceding and current row) as deposits_usd
  , last_value(a.stable_debt_usd + a.variable_debt_usd ignore nulls) over (order by t.block_time range between unbounded preceding and current row) as borrows_usd
  , last_value(a.deposits_usd - (a.stable_debt_usd + a.variable_debt_usd) ignore nulls) over (order by t.block_time range between unbounded preceding and current row) as tvl_usd
  , last_value(a.deposit_apy ignore nulls) over (order by t.block_time range between unbounded preceding and current row) as deposit_apy
from times t 
  -- left join datamart.market_state_by_time a
  -- from {{ ref('market_state_by_time') }} a
    on (t.block_time = a.block_time and a.market = 'ethereum_v3' and a.reserve_symbol = 'DAI')
union all
select 
  t.block_time
  , last_value(a.compound_version ignore nulls) over (partition by symbol order by t.block_time range between unbounded preceding and current row) as market
  , last_value(a.underlying_symbol ignore nulls) over (partition by symbol order by t.block_time range between unbounded preceding and current row) as symbol
  , last_value(a.deposits * p.usd_price ignore nulls) over (partition by symbol order by t.block_time range between unbounded preceding and current row) as deposits_usd
  , last_value(a.borrows * p.usd_price ignore nulls) over (partition by symbol order by t.block_time range between unbounded preceding and current row) as borrows_usd
  , last_value((a.deposits + a.borrows) * p.usd_price ignore nulls) over (partition by symbol order by t.block_time range between unbounded preceding and current row) as tvl_usd
  , last_value(a.supply_apy ignore nulls) over (partition by symbol order by t.block_time range between unbounded preceding and current row) as deposit_apy
from times t 
  -- left join protocol_data_lake.compound_v2_by_hour a
  left join {{ source('protocol_data_lake','compound_v2_by_hour') }} a
    on (t.block_time = a.block_hour)
  left join prices p on (date_trunc(a.block_hour, day) = p.block_day and a.underlying_address = p.reserve and a.chain = p.chain)
union all 
select 
  t.block_time
  , last_value(a.compound_version ignore nulls) over (partition by symbol order by t.block_time range between unbounded preceding and current row) as market
  , last_value(a.underlying_symbol ignore nulls) over (partition by symbol order by t.block_time range between unbounded preceding and current row) as symbol
  , last_value(a.deposits * p.usd_price ignore nulls) over (partition by symbol order by t.block_time range between unbounded preceding and current row) as deposits_usd
  , last_value(a.borrows * p.usd_price ignore nulls) over (partition by symbol order by t.block_time range between unbounded preceding and current row) as borrows_usd
  , last_value((a.deposits + a.borrows) * p.usd_price ignore nulls) over (partition by symbol order by t.block_time range between unbounded preceding and current row) as tvl_usd
  , last_value(a.supply_apy ignore nulls) over (partition by symbol order by t.block_time range between unbounded preceding and current row) as deposit_apy
from times t 
  -- left join protocol_data_lake.compound_v3_by_hour a
  left join {{ source('protocol_data_lake','compound_v3_by_hour') }} a
    on (t.block_time = a.block_hour)
  left join prices p on (date_trunc(a.block_hour, day) = p.block_day and a.underlying_address = p.reserve and a.chain = p.chain)
union all
select 
  t.block_time
  , last_value(a.compound_version ignore nulls) over (partition by symbol order by t.block_time range between unbounded preceding and current row) as market
  , last_value(a.underlying_symbol ignore nulls) over (partition by symbol order by t.block_time range between unbounded preceding and current row) as symbol
  , last_value(a.deposits * p.usd_price ignore nulls) over (partition by symbol order by t.block_time range between unbounded preceding and current row) as deposits_usd
  , last_value(a.borrows * p.usd_price ignore nulls) over (partition by symbol order by t.block_time range between unbounded preceding and current row) as borrows_usd
  , last_value((a.deposits + a.borrows) * p.usd_price ignore nulls) over (partition by symbol order by t.block_time range between unbounded preceding and current row) as tvl_usd
  , last_value(a.supply_apy ignore nulls) over (partition by symbol order by t.block_time range between unbounded preceding and current row) as deposit_apy
from times t 
  -- left join protocol_data_lake.compound_v2_by_day a
  left join {{ source('protocol_data_lake','compound_v2_by_day') }} a
    on (t.block_time = a.block_day)
  left join prices p on (date_trunc(a.block_day, day) = p.block_day and a.underlying_address = p.reserve and a.chain = p.chain)
union all 
select 
  t.block_time
  , last_value(a.compound_version ignore nulls) over (partition by symbol order by t.block_time range between unbounded preceding and current row) as market
  , last_value(a.underlying_symbol ignore nulls) over (partition by symbol order by t.block_time range between unbounded preceding and current row) as symbol
  , last_value(a.deposits * p.usd_price ignore nulls) over (partition by symbol order by t.block_time range between unbounded preceding and current row) as deposits_usd
  , last_value(a.borrows * p.usd_price ignore nulls) over (partition by symbol order by t.block_time range between unbounded preceding and current row) as borrows_usd
  , last_value((a.deposits + a.borrows) * p.usd_price ignore nulls) over (partition by symbol order by t.block_time range between unbounded preceding and current row) as tvl_usd
  , last_value(a.supply_apy ignore nulls) over (partition by symbol order by t.block_time range between unbounded preceding and current row) as deposit_apy
from times t 
  -- left join protocol_data_lake.compound_v3_by_day a
  left join {{ source('protocol_data_lake','compound_v3_by_day') }} a
    on (t.block_time = a.block_day)
  left join prices p on (date_trunc(a.block_day, day) = p.block_day and a.underlying_address = p.reserve and a.chain = p.chain)
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
from all_markets
where deposits_usd > 0
order by block_time, market, symbol