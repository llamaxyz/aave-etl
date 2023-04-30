{{ config(materialized='table') }}

-- Upstream table all_treasury_balances uses end of day balances
-- This is translated to a start of day balance by incrementing the block day by 1

select
  date_add(block_day, interval 1 day) as block_day
  , 'total_treasury_value' as measure
  , sum(value_usd) as value_usd
-- from datamart.all_treasury_balances
from {{ref('all_treasury_balances')}}
group by block_day, measure
union all
select
  date_add(block_day, interval 1 day) as block_day
  , 'total_treasury_value_ex_aave' as measure
  , sum(value_usd) as value_usd
-- from datamart.all_treasury_balances
from {{ref('all_treasury_balances')}}
where underlying_reserve_symbol != 'AAVE'
group by block_day, measure
union all
select
  date_add(block_day, interval 1 day) as block_day
  , 'total_stablecoin_value' as measure
  , sum(value_usd) as value_usd
-- from datamart.all_treasury_balances
from {{ref('all_treasury_balances')}}
where stable_class = 'stablecoin'
group by block_day, measure
order by block_day, measure