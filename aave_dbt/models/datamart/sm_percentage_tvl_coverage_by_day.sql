{{ config(materialized='table') }}

with tvl as (
select 
  block_day
  , sum(tvl_usd) as tvl
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
group by block_day
)

select 
  t.block_day
  , s.protocol_cover / t.tvl as percentage_covered
from tvl t
--   left join datamart.sm_protocol_cover_by_day s on (
  left join {{ ref('sm_protocol_cover_by_day') }} s on (
    t.block_day = s.block_day
  )
where 1=1
  and t.tvl is not null
  and s.protocol_cover is not null
order by t.block_day