{{ config(materialized='table') }}

select
  from_asset
  , from_asset_price
  , from_amount_native
  , from_amount_usd
  , to_asset
  , to_asset_price
  , to_amount_native
  , to_amount_usd
  , price_impact
-- from `aave-prod.protocol_data_lake.aave_token_liquidity_depth`
-- where fetch_time = (select max(fetch_time) from protocol_data_lake.aave_token_liquidity_depth)
from {{ source('protocol_data_lake', 'aave_token_liquidity_depth') }}
where fetch_time = (select max(fetch_time) from {{ source('protocol_data_lake', 'aave_token_liquidity_depth') }})
