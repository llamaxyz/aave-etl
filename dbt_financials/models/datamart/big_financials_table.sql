-- PK is collector contract & token address and day
-- union of atoken & non atoken tables

-- bring together the atokens * non-atokens into one table
with token_measures as (
select 
  collector
  , chain
  , market
  , token
  , symbol
  , block_day
  , balance
  , scaled_balance
  , accrued_fees
  , tokens_in_external
  , tokens_in_internal
  , tokens_out_external
  , tokens_out_internal
  , minted_to_treasury_amount
  , minted_amount
from warehouse.atoken_measures_by_day
union all
select
  contract_address as collector
  , chain
  , market
  , token
  , symbol
  , block_day
  , balance
  , 0 as scaled_balance
  , 0 as accrued_fees
  , tokens_in_external
  , tokens_in_internal
  , tokens_out_external
  , tokens_out_internal
  , 0 as minted_to_treasury_amount
  , 0 as minted_amount
from warehouse.non_atoken_measures_by_day
)

-- add underlying reserve to join pricing
, token_measures_reserves as (
select
  t.collector
  , t.chain
  , t.market
  , t.token
  , t.symbol
  , coalesce(a.reserve, t.token) as underlying_reserve
  , coalesce(a.reserve_symbol, t.symbol) as underlying_reserve_symbol
  , t.block_day
  , t.balance
  , t.scaled_balance
  , t.accrued_fees
  , t.tokens_in_external
  , t.tokens_in_internal
  , t.tokens_out_external
  , t.tokens_out_internal
  , t.minted_to_treasury_amount
  , t.minted_amount
from token_measures t 
  left join datamart.aave_atokens a on (t.token = a.atoken and t.chain = a.chain)
)

, balances_prices as (
select
  t.collector
  , t.chain
  , t.market
  , t.token
  , t.symbol
  , t.underlying_reserve
  , t.underlying_reserve_symbol
  , t.block_day
  , t.balance as start_balance
  , lead(t.balance) over (partition by t.collector, t.chain, t.market, t.token order by t.block_day) as end_balance
  , t.scaled_balance
  , t.accrued_fees as start_accrued_fees
  , lead(t.accrued_fees) over (partition by t.collector, t.chain, t.market, t.token order by t.block_day) as end_accrued_fees
  , t.tokens_in_external
  , t.tokens_in_internal
  , t.tokens_out_external
  , t.tokens_out_internal
  , t.minted_to_treasury_amount
  , t.minted_amount
  , p.usd_price as start_usd_price
  , lead(p.usd_price) over (partition by t.collector, t.chain, t.market, t.token order by t.block_day) as end_usd_price
  , coalesce(r.sm_stkAAVE_claims, 0) as sm_stkAAVE_claims
  , coalesce(r.sm_stkABPT_claims, 0) as sm_stkABPT_claims
  , coalesce(r.lm_aave_v2_claims, 0) as lm_aave_v2_claims
  -- , coalesce(r.sm_stkAAVE_owed, 0) as sm_stkAAVE_owed not implemented yet
  -- , coalesce(r.sm_stkABPT_owed, 0) as sm_stkABPT_owed
  -- , coalesce(r.lm_aave_v2_owed, 0) as lm_aave_v2_owed
from token_measures_reserves t 
  left join financials_data_lake.aave_oracle_prices_by_day p on (t.underlying_reserve = p.reserve and t.block_day = p.block_day and t.market = p.market)
  left join warehouse.user_rewards_by_day r on (t.market = r.market and t.block_day = r.block_day and t.collector = r.vault_address and t.token = r.token_address)
-- where t.symbol = 'AAVE'
-- order by t.market, t.token, t.collector, t.block_day
)

-- apply the fix for double transfer on liqs on V3 - PR682
-- V3 mainnet has aave-v3-core==3.0.1 deployed from inception which contains PR682
select 
  collector 
  , chain
  , market
  , token 
  , symbol 
  , underlying_reserve 
  , underlying_reserve_symbol 
  , block_day
  , start_balance 
  , end_balance 
  , scaled_balance
  , start_accrued_fees 
  , end_accrued_fees
  , tokens_in_external
  , tokens_in_internal
  , tokens_out_external
  , tokens_out_internal
  , minted_to_treasury_amount
  , minted_amount
  , start_usd_price
  , end_usd_price 
  , sm_stkAAVE_claims
  , sm_stkABPT_claims
  , lm_aave_v2_claims
  , case
      when scaled_balance = 0 then (tokens_in_external+tokens_in_internal-minted_amount)/(1+1)
      else (tokens_in_external+tokens_in_internal-minted_amount)/(1+start_balance/scaled_balance) 
    end as liq_adjust
from balances_prices
where 1=1
  and end_balance is not null
  and market != 'ethereum_v3'
union all 
select 
  collector 
  , chain
  , market
  , token 
  , symbol 
  , underlying_reserve 
  , underlying_reserve_symbol 
  , block_day
  , start_balance 
  , end_balance 
  , scaled_balance
  , start_accrued_fees 
  , end_accrued_fees
  , tokens_in_external
  , tokens_in_internal
  , tokens_out_external
  , tokens_out_internal
  , minted_to_treasury_amount
  , minted_amount
  , start_usd_price
  , end_usd_price 
  , sm_stkAAVE_claims
  , sm_stkABPT_claims
  , lm_aave_v2_claims
  , 0 as liq_adjust
from balances_prices
where 1=1
  and end_balance is not null
  and market = 'ethereum_v3'



