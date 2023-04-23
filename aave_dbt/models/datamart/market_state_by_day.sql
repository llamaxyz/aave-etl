{{ config(materialized='table') }}

select
  *	
  except (_dagster_partition_type, _dagster_partition_key, _dagster_partition_time, _dagster_load_timestamp)
from {{ source('warehouse','market_state_by_day')}}
order by market, atoken_symbol, block_day

