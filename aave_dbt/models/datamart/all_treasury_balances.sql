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
union all 
-- Paraswap Legacy Fees - not in financials table, hacked in here for balances only
SELECT 
  date_add(f.block_day, interval -1 day) as block_day -- hack to align with financials table
  , f.chain
  , c.display_chain
  , f.market
  , 'Paraswap Legacy Fees' as display_market
  , f.paraswap_legacy_claimer as collector
  , 'Paraswap Legacy Fees' as collector_label
  , f.reserve 
  , f.symbol 
  , f.reserve as underlying_reserve
  , f.symbol as underlying_reserve_symbol
  , 'end_paraswap_legacy_claimable_usd' as measure
  , 'balance' as measure_type
  , coalesce(b.balance_group, 'Other Token') as balance_group
  , coalesce(b.stable_class, 'unstablecoin') as stable_class
  , f.claimable * p.usd_price as value_usd
  , f.claimable as value_native
-- FROM warehouse.paraswap_legacy_claimable_fees f
--   left join datamart.chains_markets c on (f.chain = c.chain and f.market = c.market)
--   left join warehouse.balance_group_lookup b on (f.market = b.market and f.reserve = b.reserve and f.reserve = b.atoken and f.symbol = b.atoken_symbol)
--   left join warehouse.token_prices_by_day p on (f.block_day = p.block_day and f.chain = p.chain and f.reserve = p.reserve and f.symbol = p.symbol)
FROM {{ source('warehouse','paraswap_legacy_claimable_fees')}} f  
  left join {{ref('chains_markets')}} c on (f.chain = c.chain and f.market = c.market)
  left join {{ source('warehouse','balance_group_lookup')}} b on (f.market = b.market and f.reserve = b.reserve and f.reserve = b.atoken and f.symbol = b.atoken_symbol)
  left join {{ source('warehouse','token_prices_by_day')}} p on (f.block_day = p.block_day and f.chain = p.chain and f.reserve = p.reserve and f.symbol = p.symbol)

order by market, collector_label, symbol, block_day