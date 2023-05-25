{{ config(materialized='table') }}

select
  e.block_day
  , e.token as token_symbol
  , p.usd_price as price_usd
  , e.balance as balance_native
  , p.usd_price * e.balance as balance_usd
-- from protocol_data_lake.erc20_balances_by_day e
from {{ source('protocol_data_lake','erc20_balances_by_day')}} e
--   left join warehouse.token_prices_by_day p on (
  left join {{ source('warehouse','token_prices_by_day') }} p on (
    e.chain = p.chain and
    e.token_address = p.reserve and 
    e.block_day = p.block_day
  )
where e.wallet_address = '0x89c51828427f70d77875c6747759fb17ba10ceb0'
order by e.block_day