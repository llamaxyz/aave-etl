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
--   left join (select * from aave-prod.datamart.all_revenue where currency = 'native' and token = '0x030ba81f1c18d280636f32af80b9aad02cf0854e') n on (
  left join (select * from {{ref('all_revenue')}} where currency = 'native' and token = '0x030ba81f1c18d280636f32af80b9aad02cf0854e') n on (
    s.block_day = n.block_day and
    s.market = n.market and
    s.reserve = n.underlying_reserve
  )
--   left join (select * from aave-prod.datamart.all_revenue where currency = 'usd' and token = '0x030ba81f1c18d280636f32af80b9aad02cf0854e') u on (
  left join (select * from {{ref('all_revenue')}} where currency = 'usd' and token = '0x030ba81f1c18d280636f32af80b9aad02cf0854e') u on (
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
  and s.market = 'ethereum_v2'
  and s.reserve in ('0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2') -- (WETH)
  and cast(s.block_day as date) > DATE_ADD(DATE '2023-02-21', INTERVAL -6 MONTH) -- 6 months before proposal
)

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
order by block_day, measure