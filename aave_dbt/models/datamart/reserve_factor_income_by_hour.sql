{{ config(materialized='table') }}

select 
  c.block_hour
  , c.market
  , m.chain
  , m.display_chain
  , m.display_market
  , c.atoken_symbol
  , c.reserve_factor
  , s.variable_borrow_rate
  , s.variable_debt 
  , s.stable_borrow_rate
  , s.stable_debt
  , s.variable_debt * s.variable_borrow_rate / 365 * c.reserve_factor as variable_debt_protocol_income
  , s.stable_debt * s.stable_borrow_rate / 365 * c.reserve_factor as stable_debt_protocol_income
  , p.usd_price
  , (s.variable_debt * s.variable_borrow_rate + s.stable_debt * s.stable_borrow_rate) / 365 * c.reserve_factor * p.usd_price as daily_income_usd
-- from datamart.market_config_by_hour c
from {{ ref('market_config_by_hour') }} c
--   left join datamart.market_state_by_hour s on (
  left join {{ ref('market_state_by_hour') }} s on (
    c.block_hour = s.block_hour and 
    c.reserve = s.reserve and 
    c.market = s.market 
  )
--   left join datamart.chains_markets m on (
  left join {{ ref('chains_markets') }} m on (
    c.market = m.market
  )
--   left join warehouse.token_prices_by_day p on (
  left join {{ source('warehouse','token_prices_by_day') }} p on (
    date_trunc(c.block_hour, day) = p.block_day and 
    c.reserve = p.reserve and 
    m.chain = p.chain 
  )
order by c.block_hour, c.market