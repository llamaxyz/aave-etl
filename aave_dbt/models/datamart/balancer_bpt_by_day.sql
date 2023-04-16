{{ config(materialized='table') }}

select
  block_day
  , chain
  , symbol
  , name
  , pool
  , denom as bpt_underlying_asset
  , underlying_asset_price
  , rate
  , actual_supply
  , bpt_usd_price as bpt_price_usd
  , pool_tvl_usd
  , ((rate / coalesce(lag(rate) over (partition by chain, pool order by block_day), rate)) - 1) * 365  as daily_apr
-- from warehouse.balancer_bpt_by_day
from {{ source('warehouse','balancer_bpt_by_day') }}
order by chain, pool, block_day 