{{ config(materialized='table') }}

select
  block_day
  , block_height
  , chain
  , address
  , symbol
  , decimals
  , total_supply
-- from protocol_data_lake.matic_lsd_token_supply_by_day
from {{ source('protocol_data_lake', 'matic_lsd_token_supply_by_day') }}
order by chain, symbol, block_day