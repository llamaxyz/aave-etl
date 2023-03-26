{{ config(materialized='table') }}

select
  market
  , chain
  , display_name
  , display_chain
  , token
  , symbol
  , underlying_reserve
  , underlying_reserve_symbol
  , currency
  , block_day
  , sum(value) as revenue
-- from datamart.all_financials
from {{ref('all_financials')}}
where 1=1
  and measure_type = 'income'
group by 
  market
  , chain
  , display_name
  , display_chain
  , token
  , symbol
  , underlying_reserve
  , underlying_reserve_symbol
  , currency
  , block_day
order by 
  market
  , chain
  , display_name
  , display_chain
  , token
  , symbol
  , underlying_reserve
  , underlying_reserve_symbol
  , currency
  , block_day