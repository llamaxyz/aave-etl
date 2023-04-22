{{ config(materialized='table') }}

select 
  block_day
  , sum(emission_usd_per_day) as emission_usd
-- from datamart.safety_module_totals_by_day 
from {{ ref('safety_module_totals_by_day') }}
group by block_day
order by block_day