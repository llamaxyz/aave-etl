 {{ config(materialized='table') }}

select 
  block_day
  , block_height
  , market
  , reserve
  , symbol
  , reward_token_address
  , reward_token_symbol
  , supply_rewards
  , supply_rewards_usd
  , supply_rewards_apr
  , variable_debt_rewards
  , variable_debt_rewards_usd
  , variable_borrow_rewards_apr as variable_debt_rewards_apr
  , stable_debt_rewards
  , stable_debt_rewards_usd
  , stable_borrow_rewards_apr as stable_debt_rewards_apr
-- from warehouse.incentives_by_day
from {{ source('warehouse', 'incentives_by_day') }}
order by block_day, market, symbol, reward_token_symbol