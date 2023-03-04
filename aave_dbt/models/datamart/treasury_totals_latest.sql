{{ config(materialized='table') }}


select 
  *
-- from datamart.treasury_totals
from {{ ref('treasury_totals') }}
-- where block_day = (select max(block_day) from datamart.treasury_totals)
where block_day = (select max(block_day) from {{ ref('treasury_totals') }})
