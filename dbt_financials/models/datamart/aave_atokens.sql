{{ config(materialized='table') }}

with markets_chains as (
select distinct
  market
  , chain
from  {{ source('financials_data_lake','block_numbers_by_day')}}
)


select 
  t.atoken
  , t.atoken_symbol
  , t.atoken_decimals
  , t.reserve
  , t.symbol as reserve_symbol
  , t.decimals as reserve_decimals
  , t.name as reserve_name
  , t.market
  , m.chain
  , t.pool as lending_pool
from {{ source('financials_data_lake','market_tokens_by_day')}} t
  left join markets_chains m on t.market = m.market
where block_day = (select max(block_day) from {{ source('financials_data_lake','market_tokens_by_day')}})

