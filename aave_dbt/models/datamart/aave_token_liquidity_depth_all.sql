{{ config(materialized='table') }}

select
  fetch_time
  , from_asset
  , from_asset_price
  , from_amount_native
  , from_amount_usd
  , to_asset
  , to_asset_price
  , to_amount_native
  , to_amount_usd
  , price_impact
-- from `aave-prod.protocol_data_lake.aave_token_liquidity_depth`
from {{ source('protocol_data_lake', 'aave_token_liquidity_depth') }}
order by fetch_time, from_amount_usd 

