{{ config(materialized='table') }}

select
  block_day
  , balance_group as stablecoin_group
  , sum(value_usd) as value_usd
-- from datamart.all_treasury_balances
from {{ref('all_treasury_balances')}}
where stable_class = 'stablecoin'
group by block_day, balance_group
order by stablecoin_group, block_day