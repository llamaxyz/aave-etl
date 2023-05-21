{{ config(materialized='table') }}

select
  block_day
  , safety_module_token
  , count(holder_address) as token_holders
-- from protocol_data_lake.safety_module_token_hodlers_by_day
from {{ source('protocol_data_lake','safety_module_token_hodlers_by_day') }}
group by block_day, safety_module_token
order by block_day, safety_module_token