{{ config(materialized='table') }}

select distinct
  chain
  , market
  , display_chain
  , display_name as display_market 
-- from financials_data_lake.display_names
from {{ source('financials_data_lake','display_names')}}
where display_name not in ("Ecosystem Reserve", 'Incentives Controller V2')
order by chain, market