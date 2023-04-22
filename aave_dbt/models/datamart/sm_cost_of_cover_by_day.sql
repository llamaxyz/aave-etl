{{ config(materialized='table') }}

select 
  block_day
  , sum(emission_usd_per_day) * 365 / sum(safety_module_cover) as annual_cost_of_cover_percent
-- from datamart.safety_module_totals_by_day 
from {{ ref('safety_module_totals_by_day') }}
group by block_day
order by block_day