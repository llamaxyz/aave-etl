 {{ config(materialized='table') }}

with sd_rewards as (
SELECT 
  block_day
  , market
  , reserve
  , reward_token_symbol
  , supply_rewards + variable_debt_rewards + stable_debt_rewards as sd_emissions_paid
  , supply_rewards_apr
  , variable_borrow_rewards_apr
--  FROM `aave-prod.warehouse.incentives_by_day` 
FROM {{ source('warehouse','incentives_by_day')}}
 where market = 'polygon_v3'
   and reward_token_symbol = 'SD'
)

, ldo_rewards as (
SELECT 
  block_day
  , market
  , reserve
  , reward_token_symbol
  , supply_rewards + variable_debt_rewards + stable_debt_rewards as ldo_emissions_paid
  , supply_rewards_apr
  , variable_borrow_rewards_apr
--  FROM `aave-prod.warehouse.incentives_by_day` 
FROM {{ source('warehouse','incentives_by_day')}}
 where market = 'polygon_v3'
   and reward_token_symbol = 'LDO'
)

, stmatic_rewards as (
SELECT 
  block_day
  , market
  , reserve
  , reward_token_symbol
  , supply_rewards + variable_debt_rewards + stable_debt_rewards as stmatic_emissions_paid
  , supply_rewards_apr
  , variable_borrow_rewards_apr
--  FROM `aave-prod.warehouse.incentives_by_day` 
FROM {{ source('warehouse','incentives_by_day')}}
 where market = 'polygon_v3'
   and reward_token_symbol = 'stMATIC'
)

, maticx_rewards as (
SELECT 
  block_day
  , market
  , reserve
  , reward_token_symbol
  , supply_rewards + variable_debt_rewards + stable_debt_rewards as maticx_emissions_paid
  , supply_rewards_apr
  , variable_borrow_rewards_apr
--  FROM `aave-prod.warehouse.incentives_by_day` 
FROM {{ source('warehouse','incentives_by_day')}}
 where market = 'polygon_v3'
   and reward_token_symbol = 'MaticX'
)

, wide_format as (
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
  , s.liquidity_rate as deposit_apy
  , s.variable_borrow_rate as borrow_apy
  , coalesce(sd.supply_rewards_apr, 0) + coalesce(ld.supply_rewards_apr, 0) + coalesce(sm.supply_rewards_apr, 0) + coalesce(mx.supply_rewards_apr, 0)as deposit_rewards_apr
  , coalesce(sd.variable_borrow_rewards_apr, 0) + coalesce(ld.variable_borrow_rewards_apr, 0) + coalesce(sm.variable_borrow_rewards_apr, 0) + coalesce(mx.variable_borrow_rewards_apr, 0)as variable_borrow_rewards_apr
  , s.liquidity_rate + coalesce(sd.supply_rewards_apr, 0) + coalesce(ld.supply_rewards_apr, 0) + coalesce(sm.supply_rewards_apr, 0) + coalesce(mx.supply_rewards_apr, 0) as deposit_apy_inc_rewards
  , s.variable_borrow_rate + coalesce(sd.variable_borrow_rewards_apr, 0) + coalesce(ld.variable_borrow_rewards_apr, 0) + coalesce(sm.variable_borrow_rewards_apr, 0) + coalesce(mx.variable_borrow_rewards_apr, 0)as variable_borrow_apy_inc_rewards
  , coalesce(sd.sd_emissions_paid, 0) as sd_emissions_paid
  , coalesce(ld.ldo_emissions_paid, 0) as ldo_emissions_paid
  , coalesce(sm.stmatic_emissions_paid, 0) as stmatic_emissions_paid
  , coalesce(mx.maticx_emissions_paid, 0) as maticx_emissions_paid
-- FROM aave-prod.warehouse.market_state_by_day s
FROM {{ source('warehouse','market_state_by_day')}} s
--   left join (select * from aave-prod.datamart.all_revenue where currency = 'native' and token in ('0x80ca0d8c38d2e2bcbab66aa1648bd1c7160500fe','0x6d80113e533a2c0fe82eabd35f1875dcea89ea97','0xea1132120ddcdda2f119e99fa7a27a0d036f7ac9')) n on (
  left join (select * from {{ref('all_revenue')}} where currency = 'native' and token in ('0x80ca0d8c38d2e2bcbab66aa1648bd1c7160500fe','0x6d80113e533a2c0fe82eabd35f1875dcea89ea97','0xea1132120ddcdda2f119e99fa7a27a0d036f7ac9')) n on (
    s.block_day = n.block_day and
   s.market = n.market and
    s.reserve = n.underlying_reserve
  )
--   left join (select * from aave-prod.datamart.all_revenue where currency = 'usd' and token in ('0x80ca0d8c38d2e2bcbab66aa1648bd1c7160500fe','0x6d80113e533a2c0fe82eabd35f1875dcea89ea97','0xea1132120ddcdda2f119e99fa7a27a0d036f7ac9')) u on (
  left join (select * from {{ref('all_revenue')}} where currency = 'usd' and token in ('0x80ca0d8c38d2e2bcbab66aa1648bd1c7160500fe','0x6d80113e533a2c0fe82eabd35f1875dcea89ea97','0xea1132120ddcdda2f119e99fa7a27a0d036f7ac9')) u on (
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
  left join sd_rewards sd on (
    s.block_day = sd.block_day and
    s.market = sd.market and
    s.reserve = sd.reserve
  )
  left join ldo_rewards ld on (
    s.block_day = ld.block_day and
    s.market = ld.market and
    s.reserve = ld.reserve
  )
  left join stmatic_rewards sm on (
    s.block_day = sm.block_day and
    s.market = sm.market and
    s.reserve = sm.reserve
  )
  left join maticx_rewards mx on (
    s.block_day = mx.block_day and
    s.market = mx.market and
    s.reserve = mx.reserve
  )
where 1=1
  and s.market = 'polygon_v3'
  and s.reserve in ('0xfa68fb4628dff1028cfec22b4162fccd0d45efb6', '0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270', '0x3a58a54c066fdc0f2d55fc9c89f0415c92ebf3c4') -- (MaticX, WMATIC, stMATIC)
  and cast(s.block_day as date) > DATE_ADD(DATE '2023-03-06', INTERVAL -6 MONTH) -- 6 months before proposal
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
    , deposit_apy
    , borrow_apy
    , deposit_rewards_apr
    , variable_borrow_rewards_apr
    , deposit_apy_inc_rewards
    , variable_borrow_apy_inc_rewards
    , sd_emissions_paid
    , ldo_emissions_paid
    , stmatic_emissions_paid
    , maticx_emissions_paid
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

