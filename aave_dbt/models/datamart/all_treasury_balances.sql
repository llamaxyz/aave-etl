{{ config(materialized='table') }}


with usd_balance as (
select 
  block_day
  , chain
  , display_chain
  , market
  , display_name as display_market
  , collector
  , collector_label
  , token
  , symbol
  , underlying_reserve
  , underlying_reserve_symbol
  , replace(measure, '_usd', '') as measure
  , measure_type
  , balance_group
  , stable_class
  , value as value_usd
-- from datamart.all_financials
from {{ref('all_financials')}}
where 1=1
  and measure in ('end_balance_usd', 'end_accrued_fees_usd', 'end_paraswap_fees_claimable_usd')
  and collector_label != 'Ethereum V2 Incentives Controller' -- excluded from treasury holdings, included in protocol holdings
  and currency = 'usd'
)

, native_balance as (
select 
  block_day
  , market
  , collector
  , token
  , measure 
  , value as value_native
-- from datamart.all_financials
from {{ref('all_financials')}}
where 1=1
  and measure in ('end_balance', 'end_accrued_fees', 'end_paraswap_fees_claimable')
  and collector_label != 'Ethereum V2 Incentives Controller' -- excluded from treasury holdings, included in protocol holdings
  and currency = 'native'
)


select 
  u.block_day
  , u.chain
  , u.display_chain
  , u.market
  , u.display_market
  , u.collector
  , u.collector_label
  , u.token
  , u.symbol
  , u.underlying_reserve
  , u.underlying_reserve_symbol
  , u.measure
  , u.measure_type
  , u.balance_group
  , u.stable_class
  , u.value_usd
  , n.value_native
from usd_balance u
  left join native_balance n on (
    u.block_day = n.block_day and
    u.market = n.market and
    u.collector = n.collector and 
    u.token = n.token and
    u.measure = n.measure
  )
order by market, collector_label, symbol, block_day