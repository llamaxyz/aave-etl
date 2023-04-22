{{ config(materialized='table') }}

with labelled as (
select 
  block_day
  , display_name || ' ' || display_chain as aave_market
  , tvl_usd
-- from datamart.asset_tvl_by_day 
from {{ ref('asset_tvl_by_day') }}
where market in (
  'ethereum_v1'
  , 'ethereum_v2'
  , 'ethereum_v3'
  , 'aave_arc'
  , 'avax_v2'
  , 'polygon_v2'
  , 'polygon_v3'
)
)

select 
  block_day
  , aave_market 
  , sum(tvl_usd) as tvl
from labelled 
group by block_day, aave_market 
order by block_day, aave_market 