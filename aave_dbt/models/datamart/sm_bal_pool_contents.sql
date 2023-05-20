{{ config(materialized='table') }}

SELECT 
  block_day
  , safety_module_token
  , symbol
  , weight
  , balance
-- FROM `aave-prod.protocol_data_lake.safety_module_bal_pool_contents`
from {{ source('protocol_data_lake','safety_module_bal_pool_contents')}}
order by block_day, safety_module_token, symbol
