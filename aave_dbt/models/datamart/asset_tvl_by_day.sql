{{ config(materialized='table') }}

with names as (
select distinct
  market
  , chain
  , display_name
  , display_chain
-- from financials_data_lake.display_names
from {{ source('financials_data_lake', 'display_names') }}
where display_name not in ('Ecosystem Reserve', 'Incentives Controller V2')
)

SELECT 
  s.block_day
  , s.reserve
  , p.symbol as reserve_symbol
  , s.atoken_symbol
  , s.market
  , n.chain
  , n.display_name
  , n.display_chain
  , s.atoken_supply as deposits
  , s.stable_debt as stable_loans
  , s.variable_debt as variable_loans
  , p.usd_price
  , s.available_liquidity as tvl
  , s.atoken_supply * p.usd_price as deposits_usd
  , s.stable_debt * p.usd_price as stable_loans_usd
  , s.variable_debt * p.usd_price as variable_loans_usd
  , s.available_liquidity * p.usd_price as tvl_usd

-- FROM `aave-prod.datamart.market_state_by_day` s
from {{ ref('market_state_by_day') }} s
  left join names n on (
    s.market = n.market
  )
  -- left join (select * from warehouse.token_prices_by_day where symbol not in ('ETH','MATIC','AVAX','ONE','FTM')) p on (
  left join (select * from {{ source('warehouse', 'token_prices_by_day') }} where symbol not in ('ETH','MATIC','AVAX','ONE','FTM')) p on (
    n.chain = p.chain and 
    s.reserve = p.reserve and 
    s.block_day = p.block_day
  )
