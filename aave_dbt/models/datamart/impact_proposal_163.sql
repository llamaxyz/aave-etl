{{ config(materialized='table') }}

with wide_format as (
SELECT 
  s.block_day
  , s.market
  , s.reserve
  , s.atoken_symbol
  , s.atoken_supply - s.variable_debt - s.stable_debt as tvl
  , (s.atoken_supply - s.variable_debt - s.stable_debt) * p.usd_price as tvl_usd
  , s.atoken_supply as deposits
  , s.atoken_supply * p.usd_price as deposits_usd
  , s.variable_debt + s.stable_debt as borrows
  , (s.variable_debt + s.stable_debt) * p.usd_price as borrows_usd
  , n.revenue as revenue
  , u.revenue as revenue_usd 
  , p.usd_price
-- FROM aave-prod.warehouse.market_state_by_day s
FROM {{ source('warehouse','market_state_by_day')}} s
--   left join (select * from aave-prod.datamart.all_revenue where currency = 'native' and token in ('0x513c7e3a9c69ca3e22550ef58ac1c0088e918fff','0xe50fa9b3c56ffb159cb0fca61f5c9d750e8128c8')) n on (
  left join (select * from {{ref('all_revenue')}} where currency = 'native' and token in ('0x513c7e3a9c69ca3e22550ef58ac1c0088e918fff','0xe50fa9b3c56ffb159cb0fca61f5c9d750e8128c8')) n on (
    s.block_day = n.block_day and
   s.market = n.market and
    s.reserve = n.underlying_reserve
  )
--   left join (select * from aave-prod.datamart.all_revenue where currency = 'usd' and token in ('0x513c7e3a9c69ca3e22550ef58ac1c0088e918fff','0xe50fa9b3c56ffb159cb0fca61f5c9d750e8128c8')) u on (
  left join (select * from {{ref('all_revenue')}} where currency = 'usd' and token in ('0x513c7e3a9c69ca3e22550ef58ac1c0088e918fff','0xe50fa9b3c56ffb159cb0fca61f5c9d750e8128c8')) u on (
    s.block_day = u.block_day and
    s.market = u.market and
    s.reserve = u.underlying_reserve
  )
--   left join financials_data_lake.aave_oracle_prices_by_day p on (
  left join {{ source('financials_data_lake','aave_oracle_prices_by_day')}} p on (
    s.block_day = p.block_day and
    s.market = p.market and
    s.reserve = p.reserve
  )
where 1=1
  and s.market = 'arbitrum_v3'
  and s.reserve in ('0x5979d7b546e38e414f7e9822514be443a4800529', '0x82af49447d8a07e3bd95bd0d56f35241523fbab1') -- (wstETH, WETH)
  and cast(s.block_day as date) > DATE_ADD(DATE '2023-02-27', INTERVAL -6 MONTH) -- 6 months before proposal
)

, by_asset as (
select 
  *
from wide_format
unpivot (
  value for measure in (
    tvl
    , tvl_usd
    , deposits
    , deposits_usd
    , borrows
    , borrows_usd
    , revenue
    , revenue_usd
    , usd_price
  )
)
)
select 
  block_day
  , market
  , reserve
  , atoken_symbol
  , measure
  , value
from by_asset 
union all
select 
  block_day
  , market
  , 'aggregate' as reserve
  , 'aggregate' as atoken_symbol
  , measure
  , sum(value) as value
from by_asset 
where measure in ('revenue_usd')
group by block_day, market, reserve, atoken_symbol, measure
order by market, atoken_symbol, measure, block_day