{{ config(materialized='table') }}

select
  *
from {{ source('warehouse','market_config_by_day')}}
order by market, atoken_symbol, block_day

