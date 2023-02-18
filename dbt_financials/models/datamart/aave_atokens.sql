{{ config(materialized='table') }}

select 
  atoken
  , atoken_symbol
  , atoken_decimals
  , reserve
  , symbol as reserve_symbol
  , decimals as reserve_decimals
  , name as reserve_name
  , market
  , pool as lending_pool
from {{ source('financials_data_lake','market_tokens_by_day')}}
where block_day = (select max(block_day) from {{ source('financials_data_lake','market_tokens_by_day')}})

