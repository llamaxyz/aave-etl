{{ config(materialized='table') }}

SELECT
  d.display_chain as chain
  , d.display_name as market
  , l.loop_market
  , l.from_asset
  , l.to_asset
  , concat(l.from_asset, '-', l.to_asset) as pair
  , l.from_asset_price
  , l.to_asset_price
  , l.from_amount_usd
  , l.price_impact
  , l.fetch_time
-- FROM warehouse.liquidity_depth l
from {{ source('warehouse', 'liquidity_depth') }} l
--   left join financials_data_lake.display_names d on (
  left join {{ source('financials_data_lake', 'display_names') }} d on (
    l.market = d.market and 
    l.chain = d.chain
  )
order by l.fetch_time, d.display_chain, d.display_name, l.from_asset, l.to_asset, l.from_amount_usd