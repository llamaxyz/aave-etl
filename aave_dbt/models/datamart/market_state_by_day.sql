{{ config(materialized='table') }}

select
  *
from {{ source('warehouse','market_state_by_day')}}
order by market, atoken_symbol, block_day

