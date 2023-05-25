{{ config(materialized='table') }}

with labelled as (
select 
  block_day
  , display_name || ' ' || display_chain as aave_market
  , tvl_usd
-- from datamart.asset_tvl_by_day 
from {{ ref('asset_tvl_by_day') }}
where 1=1 
  and market in (
    'ethereum_v1'
    , 'ethereum_v2'
  )
  and block_day < '2022-08-01'
union all 
select 
  block_day
  , display_name || ' ' || display_chain as aave_market
  , tvl_usd
-- from datamart.asset_tvl_by_day 
from {{ ref('asset_tvl_by_day') }}
where 1=1
  and market in (
    'ethereum_v1'
    , 'ethereum_v2'
    , 'aave_arc'
    , 'avax_v2'
    , 'polygon_v2'
  )
  and block_day between '2022-08-01' and '2023-02-17'
union all 
select 
  block_day
  , display_name || ' ' || display_chain as aave_market
  , tvl_usd
-- from datamart.asset_tvl_by_day 
from {{ ref('asset_tvl_by_day') }}
where 1=1
  and market in (
    'ethereum_v1'
    , 'ethereum_v2'
    , 'ethereum_v3'
    , 'aave_arc'
    , 'avax_v2'
    , 'polygon_v2'
  )
  and block_day between '2023-02-18' and '2023-03-15'
union all 
select 
  block_day
  , display_name || ' ' || display_chain as aave_market
  , tvl_usd
-- from datamart.asset_tvl_by_day 
from {{ ref('asset_tvl_by_day') }}
where 1=1
  and market in (
    'ethereum_v1'
    , 'ethereum_v2'
    , 'ethereum_v3'
    , 'aave_arc'
    , 'avax_v2'
    , 'polygon_v2'
    , 'polygon_v3'
  )
  and block_day > '2023-03-15'
)

select 
  block_day
  , aave_market 
  , sum(tvl_usd) as tvl
from labelled 
group by block_day, aave_market 
order by block_day, aave_market 