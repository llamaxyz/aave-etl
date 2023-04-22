{{ config(materialized='table') }}

select 
  block_day
  , sum(safety_module_cover) as protocol_cover
-- from datamart.safety_module_totals_by_day 
from {{ ref('safety_module_totals_by_day') }}
group by block_day
order by block_day