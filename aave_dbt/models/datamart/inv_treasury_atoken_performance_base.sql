{{ config(materialized='table') }}


with rates as (
select
  block_day
  , market
  , symbol
  , reserve
  , liquidity_rate
-- from protocol_data_lake.protocol_data_by_day
from {{ source('protocol_data_lake','protocol_data_by_day')}}
where 1=1
  and market not in ('ethereum_v1','fantom_v3','harmony_v3')
)

, balances as (
select
  b.block_day
  , b.market
  , b.token as atoken
  , b.symbol
  , a.reserve
  , a.reserve_symbol
  , b.balance
-- from financials_data_lake.collector_atoken_balances_by_day b
from {{ source('financials_data_lake','collector_atoken_balances_by_day')}} b
--   left join datamart.aave_atokens a on (b.token = a.atoken and b.market = a.market)
  left join {{ ref('aave_atokens') }} a on (b.token = a.atoken and b.market = a.market)
where 1=1
  and b.market not in ('ethereum_v1','fantom_v3','harmony_v3')
)

, prices as (
select 
  block_day
  , market
  , reserve
  , symbol as reserve_symbol
  , usd_price
-- from financials_data_lake.aave_oracle_prices_by_day
from {{ source('financials_data_lake','aave_oracle_prices_by_day')}} 
where 1=1
  and market not in ('ethereum_v1','fantom_v3','harmony_v3')
)

, eth_prices as (
select 
  block_day
  , usd_price as eth_usd
-- from financials_data_lake.aave_oracle_prices_by_day
from {{ source('financials_data_lake','aave_oracle_prices_by_day')}} 
where 1=1
  and market='ethereum_v2'
  and symbol = 'WETH'
order by block_day
)

, calcs as (
select 
  b.block_day
  , b.market
  , b.symbol
  , b.reserve_symbol
  , l.balance_group
  , l.stable_class
  , b.balance as balance_native
  , lead(b.balance) over (partition by b.market, b.symbol order by b.block_day) as end_balance_native
  , r.liquidity_rate
  , p.usd_price
  , lead(p.usd_price) over (partition by b.market, b.symbol order by b.block_day) as end_usd_price
  , e.eth_usd
  , lead(e.eth_usd) over (partition by b.market, b.symbol order by b.block_day) as end_eth_usd
  , b.balance * p.usd_price as balance_usd
  , b.balance * p.usd_price / e.eth_usd as balance_eth
  , b.balance * r.liquidity_rate / 365 as earnings_native
  , b.balance * r.liquidity_rate / 365 * p.usd_price as earnings_usd
  , b.balance * r.liquidity_rate / 365 * p.usd_price / e.eth_usd as earnings_eth
from balances b
  left join rates r on (
    b.block_day=r.block_day and
    b.market = r.market and
    b.symbol = r.symbol
  )
   left join prices p on (
    b.block_day = p.block_day and
    b.market = p.market and
    b.reserve = p.reserve
  )
  left join eth_prices e on (b.block_day = e.block_day)
  -- left join warehouse.balance_group_lookup l on (
  left join {{ source('warehouse','balance_group_lookup') }} l on (
    b.market = l.market and 
    b.symbol = l.atoken_symbol
  )
)

select 
  block_day
  , market
  , symbol
  , balance_group
  , stable_class
  , balance_native
  , balance_usd
  , balance_eth 
  , usd_price
  , usd_price / eth_usd as eth_price
  , earnings_native
  , earnings_usd
  , earnings_eth
  , (balance_native + earnings_native) * (end_usd_price - usd_price) as price_change_usd
  , (balance_native + earnings_native) * (end_usd_price / end_eth_usd - usd_price / eth_usd) as price_change_eth
  , end_balance_native - earnings_native - balance_native as receipts_native
  , (end_balance_native - earnings_native - balance_native) * usd_price as receipts_usd
  , (end_balance_native - earnings_native - balance_native) * usd_price / eth_usd as receipts_eth
from calcs 
where 1=1
  and balance_native > 0
order by market,symbol,block_day

