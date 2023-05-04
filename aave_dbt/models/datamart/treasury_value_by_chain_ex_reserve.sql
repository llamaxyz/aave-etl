{{ config(materialized='table') }}

-- Upstream table all_treasury_balances uses end of day balances
-- This is translated to a start of day balance by incrementing the block day by 1

select
  date_add(block_day, interval 1 day) as block_day
  , display_chain
  , sum(value_usd) as ex_reserve_usd
-- from datamart.all_treasury_balances
from {{ref('all_treasury_balances')}}
where collector_label != 'Ecosystem Reserve'
group by block_day, display_chain
order by block_day, display_chain