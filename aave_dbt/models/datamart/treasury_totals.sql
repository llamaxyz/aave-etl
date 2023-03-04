{{ config(materialized='table') }}

select
  block_day
  , 'total_treasury_value' as measure
  , sum(value_usd) as value_usd
-- from datamart.all_treasury_balances
from {{ref('all_treasury_balances')}}
group by block_day, measure
union all
select
  block_day
  , 'total_treasury_value_ex_aave' as measure
  , sum(value_usd) as value_usd
-- from datamart.all_treasury_balances
from {{ref('all_treasury_balances')}}
where underlying_reserve_symbol != 'AAVE'
group by block_day, measure
union all
select
  block_day
  , 'total_stablecoin_value' as measure
  , sum(value_usd) as value_usd
-- from datamart.all_treasury_balances
from {{ref('all_treasury_balances')}}
where stable_class = 'stablecoin'
group by block_day, measure
order by block_day, measure