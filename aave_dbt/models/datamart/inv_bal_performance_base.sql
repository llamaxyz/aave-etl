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
  , b.token as reserve
  , b.symbol
  , b.balance
-- from financials_data_lake.non_atoken_balances_by_day b
from {{ source('financials_data_lake','non_atoken_balances_by_day')}} b
where 1=1
  and b.symbol in ('BAL','B-80BAL-20WETH')
  and b.block_day >= '2022-07-20'
order by block_day
)

, prices as (
select 
  block_day
  , market
  , reserve
  , symbol 
  , usd_price
-- from financials_data_lake.aave_oracle_prices_by_day
from {{ source('financials_data_lake','aave_oracle_prices_by_day')}} 
where 1=1
  and market in ('ethereum_v2')
  and symbol = 'BAL'
)

, cg_prices as (
select 
  block_day
  , 'ethereum_v2' as market
  , address as reserve
  , symbol 
  , price_usd as usd_price
-- from protocol_data_lake.coingecko_data_by_day
from {{ source('protocol_data_lake','coingecko_data_by_day')}} 
where 1=1
  and symbol = 'B-80BAL-20WETH'
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
  , b.reserve
  , l.balance_group
  , l.stable_class
  , b.balance as balance_native
  , lead(b.balance) over (partition by b.market, b.symbol order by b.block_day) as end_balance_native
  , coalesce(r.liquidity_rate, 0) as liquidity_rate
  , coalesce(p.usd_price, c.usd_price) as usd_price
  , lead(coalesce(p.usd_price, c.usd_price)) over (partition by b.market, b.symbol order by b.block_day) as end_usd_price
  , e.eth_usd
  , lead(e.eth_usd) over (partition by b.market, b.symbol order by b.block_day) as end_eth_usd
  , b.balance * coalesce(p.usd_price, c.usd_price) as balance_usd
  , b.balance * coalesce(p.usd_price, c.usd_price) / e.eth_usd as balance_eth
  , b.balance * coalesce(r.liquidity_rate, 0) / 365 as earnings_native
  , b.balance * coalesce(r.liquidity_rate, 0) / 365 * coalesce(p.usd_price, c.usd_price) as earnings_usd
  , b.balance * coalesce(r.liquidity_rate, 0) / 365 * coalesce(p.usd_price, c.usd_price) / e.eth_usd as earnings_eth
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
   left join cg_prices c on (
    b.block_day = c.block_day and
    b.market = c.market and
    b.reserve = c.reserve
  )
  left join eth_prices e on (b.block_day = e.block_day)
  left join warehouse.balance_group_lookup l on (
    b.market = l.market and 
    b.symbol = l.atoken_symbol
  )
order by block_day desc
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
order by block_day, market,symbol

