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
  -- left join (select * from aave-prod.datamart.all_revenue where currency = 'native' and token in ('0x8dae6cb04688c62d939ed9b68d32bc62e49970b1','0x513c7e3a9c69ca3e22550ef58ac1c0088e918fff','0x3df8f92b7e798820ddcca2ebea7babda2c90c4ad')) n on (
  left join (select * from {{ref('all_revenue')}} where currency = 'native' and token in ('0x8dae6cb04688c62d939ed9b68d32bc62e49970b1','0x513c7e3a9c69ca3e22550ef58ac1c0088e918fff','0x3df8f92b7e798820ddcca2ebea7babda2c90c4ad')) n on (
    s.block_day = n.block_day and
    s.market = n.market and
    s.reserve = n.underlying_reserve
  )
  -- left join (select * from aave-prod.datamart.all_revenue where currency = 'usd' and token in ('0x8dae6cb04688c62d939ed9b68d32bc62e49970b1','0x513c7e3a9c69ca3e22550ef58ac1c0088e918fff','0x3df8f92b7e798820ddcca2ebea7babda2c90c4ad')) u on (
  left join (select * from {{ref('all_revenue')}} where currency = 'usd' and token in ('0x8dae6cb04688c62d939ed9b68d32bc62e49970b1','0x513c7e3a9c69ca3e22550ef58ac1c0088e918fff','0x3df8f92b7e798820ddcca2ebea7babda2c90c4ad')) u on (
    s.block_day = u.block_day and
    s.market = u.market and
    s.reserve = u.underlying_reserve
  )
  -- left join financials_data_lake.aave_oracle_prices_by_day p on (
  left join {{ source('financials_data_lake','aave_oracle_prices_by_day')}} p on (
    s.block_day = p.block_day and
    s.market = p.market and
    s.reserve = p.reserve
  )
where 1=1
  and s.market in ('ethereum_v2', 'polygon_v2', 'polygon_v3')
  and s.reserve in ('0xd533a949740bb3306d119cc777fa900ba034cd52', '0x172370d5cd63279efa6d502dab29171933a610af') -- (CRV, CRV Polygon)
  and cast(s.block_day as date) > DATE_ADD(DATE '2023-03-13', INTERVAL -6 MONTH) -- 6 months before proposal
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