{{ config(materialized='table') }}

with stables as (
select 
  block_day
  , sum(earnings_usd) / sum(deposits_usd) * 365 as stable_reference_rate_ex_price
  , sum(earnings_usd + price_change_usd) / sum(deposits_usd) * 365 as stable_reference_rate
-- from dev.inv_defi_ref_stable_rate_base
from {{ ref('inv_defi_ref_stable_rate_base') }}
group by block_day
)

, eth as (
select 
  partition_date as block_day
  , apr as eth_reference_rate
-- from protocol_data_lake.beacon_chain_staking_returns_by_day
from {{ source('protocol_data_lake', 'beacon_chain_staking_returns_by_day') }}
)

select 
  s.block_day
  , s.stable_reference_rate
  , s.stable_reference_rate_ex_price
  , last_value(e.eth_reference_rate ignore nulls) over (order by s.block_day range between unbounded preceding and current row) as eth_reference_rate
from stables s 
  left join eth e on s.block_day = e.block_day
order by s.block_day