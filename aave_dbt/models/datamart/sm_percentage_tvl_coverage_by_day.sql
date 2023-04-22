{{ config(materialized='table') }}

with tvl as (
select 
  block_day
  , sum(tvl) as tvl
-- from datamart.sm_covered_markets_tvl_by_day 
from {{ ref('sm_covered_markets_tvl_by_day') }}
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