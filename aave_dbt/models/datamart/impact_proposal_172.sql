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
--   left join (select * from aave-prod.datamart.all_revenue where currency = 'native' and token in ('0xea1132120ddcdda2f119e99fa7a27a0d036f7ac9','0x6d80113e533a2c0fe82eabd35f1875dcea89ea97')) n on (
  left join (select * from {{ref('all_revenue')}} where currency = 'native' and token in ('0xea1132120ddcdda2f119e99fa7a27a0d036f7ac9','0x6d80113e533a2c0fe82eabd35f1875dcea89ea97')) n on (
    s.block_day = n.block_day and
   s.market = n.market and
    s.reserve = n.underlying_reserve
  )
--   left join (select * from aave-prod.datamart.all_revenue where currency = 'usd' and token in ('0xea1132120ddcdda2f119e99fa7a27a0d036f7ac9','0x6d80113e533a2c0fe82eabd35f1875dcea89ea97')) u on (
  left join (select * from {{ref('all_revenue')}} where currency = 'usd' and token in ('0xea1132120ddcdda2f119e99fa7a27a0d036f7ac9','0x6d80113e533a2c0fe82eabd35f1875dcea89ea97')) u on (
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
  and s.market = 'polygon_v3'
  and s.reserve in ('0x3a58a54c066fdc0f2d55fc9c89f0415c92ebf3c4', '0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270') -- (stMATIC, WMATIC)
  and cast(s.block_day as date) > DATE_ADD(DATE '2023-03-12', INTERVAL -6 MONTH) -- 6 months before proposal
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
where measure in ('revenue', 'revenue_usd')
group by block_day, market, reserve, atoken_symbol, measure
order by market, atoken_symbol, measure, block_day