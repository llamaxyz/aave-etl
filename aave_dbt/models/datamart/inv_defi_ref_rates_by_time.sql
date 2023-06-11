{{ config(materialized='table') }}

with stables as (
select 
  block_time
  , sum(deposits_mul_apy) / sum(deposits_usd) as risk_free_stable_rate
-- from datamart.risk_free_stable_rate_base
from {{ ref('inv_defi_ref_stable_rate_base') }}
group by block_time
)

, eth as (
select 
  partition_date as block_time
  , apr as risk_free_eth_rate
-- from protocol_data_lake.beacon_chain_staking_returns_by_day
from {{ source('protocol_data_lake', 'beacon_chain_staking_returns_by_day') }}
)

select 
  s.block_time
  , s.risk_free_stable_rate
  , last_value(e.risk_free_eth_rate ignore nulls) over (order by s.block_time range between unbounded preceding and current row) as risk_free_eth_rate
from stables s 
  left join eth e on (date_trunc(s.block_time, day) = e.block_time)
order by block_time