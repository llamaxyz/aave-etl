{{ config(materialized='table') }}

select
  block_day
  , display_chain
  , display_market
  , collector_label
  , balance_group
  , sum(value_usd) as value_usd
  , sum(value_native) as value_native
-- from datamart.all_treasury_balances
from {{ref('all_treasury_balances')}}
group by block_day, display_chain, display_market, collector_label, balance_group
order by block_day, display_chain, display_market, collector_label, balance_group
