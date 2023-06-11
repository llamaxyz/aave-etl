{{ config(materialized='table') }}

with stables as (
select 
  block_time
  , sum(deposits_mul_apy) / sum(deposits_usd) as stable_reference_rate
-- from datamart.inv_defi_ref_stable_rate_base
from {{ ref('inv_defi_ref_stable_rate_base') }}
group by block_time
)

, eth as (
select 
  partition_date as block_time
  , apr as eth_reference_rate
-- from protocol_data_lake.beacon_chain_staking_returns_by_day
from {{ source('protocol_data_lake', 'beacon_chain_staking_returns_by_day') }}
)

select 
  s.block_time
  , s.stable_reference_rate
  , last_value(e.eth_reference_rate ignore nulls) over (order by s.block_time range between unbounded preceding and current row) as eth_reference_rate
from stables s 
  left join eth e on (date_trunc(s.block_time, day) = e.block_time)
order by block_time