{{ config(materialized='table') }}

-- Upstream table all_treasury_balances uses end of day balances
-- This is translated to a start of day balance by incrementing the block day by 1

with stables as (
select
  date_add(block_day, interval 1 day) as block_day
  , display_chain
  , sum(value_usd) as stablecoin_value_usd
-- from datamart.all_treasury_balances
from {{ref('all_treasury_balances')}}
where stable_class = 'stablecoin'
group by block_day, display_chain
)
, totals as (
select
  date_add(block_day, interval 1 day) as block_day
  , display_chain
  , sum(value_usd) as value_usd
-- from datamart.all_treasury_balances
from {{ref('all_treasury_balances')}}
group by block_day, display_chain
)
, ex_aave as (
select
  date_add(block_day, interval 1 day) as block_day
  , display_chain
  , sum(value_usd) as ex_aave_value_usd
-- from datamart.all_treasury_balances
from {{ref('all_treasury_balances')}}
where symbol != 'AAVE'
group by block_day, display_chain
)
select
  t.block_day
  , t.display_chain
  , coalesce(t.value_usd, 0) as value_usd
  , coalesce(s.stablecoin_value_usd, 0) as stablecoin_value_usd
  , coalesce(a.ex_aave_value_usd, 0) as ex_aave_value_usd
from totals t 
  left join stables s on (t.block_day = s.block_day and t.display_chain = s.display_chain)
  left join ex_aave a on (t.block_day = a.block_day and t.display_chain = a.display_chain)
  order by block_day, display_chain
