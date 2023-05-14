{{ config(materialized='table') }}

-- lookup table for the native chain gas tokens
with gas_token_markets as (
select distinct
  market
  , chain
  , collector
-- from financials_data_lake.eth_balances_by_day 
from  {{ source('financials_data_lake','eth_balances_by_day')}}
where 1=1
  and (chain = 'ethereum' and market = 'ethereum_v2')
  or (chain = 'ethereum' and market = 'ethereum_v1')
  or chain != 'ethereum'
)

-- bring together the atokens & non-atokens & gas tokens into one table
, token_measures as (
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
  , 0 as paraswap_fees_claimable
-- from warehouse.atoken_measures_by_day
from  {{ source('warehouse','atoken_measures_by_day')}}
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
  , paraswap_fees_claimable
-- from warehouse.non_atoken_measures_by_day
from  {{ source('warehouse','non_atoken_measures_by_day')}}
union all
select distinct
  e.collector
  , e.chain
  , m.market
  , e.wrapped_gas_token as token
  , e.gas_token as symbol
  , e.block_day
  , e.balance
  , 0 as scaled_balance
  , 0 as accrued_fees
  , 0 as tokens_in_external
  , 0 as tokens_in_internal
  , 0 as tokens_out_external
  , 0 as tokens_out_internal
  , 0 as minted_to_treasury_amount
  , 0 as minted_amount
  , 0 as paraswap_fees_claimable
-- from financials_data_lake.eth_balances_by_day e
from  {{ source('financials_data_lake','eth_balances_by_day')}} e
  left join gas_token_markets m on (
    e.chain = m.chain and 
    e.collector = m.collector
  )
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
  , t.paraswap_fees_claimable
from token_measures t 
  -- left join datamart.aave_atokens a on (t.token = a.atoken and t.chain = a.chain)
  left join {{ref('aave_atokens')}} a on (t.token = a.atoken and t.chain = a.chain)
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
  , lead(t.balance) over (partition by t.collector, t.chain, t.market, t.token, t.symbol order by t.block_day) as end_balance
  , t.scaled_balance
  , t.accrued_fees as start_accrued_fees
  , lead(t.accrued_fees) over (partition by t.collector, t.chain, t.market, t.token, t.symbol order by t.block_day) as end_accrued_fees
  , t.tokens_in_external
  , t.tokens_in_internal
  , t.tokens_out_external
  , t.tokens_out_internal
  , t.minted_to_treasury_amount
  , t.minted_amount
  , coalesce(p.usd_price, c.price_usd) as start_usd_price
  , lead(coalesce(p.usd_price, c.price_usd)) over (partition by t.collector, t.chain, t.market, t.token, t.symbol order by t.block_day) as end_usd_price
  , coalesce(r.sm_stkAAVE_claims, 0) as sm_stkAAVE_claims
  , coalesce(r.sm_stkABPT_claims, 0) as sm_stkABPT_claims
  , coalesce(r.lm_aave_v2_claims, 0) as lm_aave_v2_claims
  -- , coalesce(r.sm_stkAAVE_owed, 0) as sm_stkAAVE_owed not implemented yet
  -- , coalesce(r.sm_stkABPT_owed, 0) as sm_stkABPT_owed
  -- , coalesce(r.lm_aave_v2_owed, 0) as lm_aave_v2_owed
  , t.paraswap_fees_claimable as start_paraswap_fees_claimable
  , lead(t.paraswap_fees_claimable) over (partition by t.collector, t.chain, t.market, t.token, t.symbol order by t.block_day) as end_paraswap_fees_claimable
from token_measures_reserves t 
  -- left join financials_data_lake.aave_oracle_prices_by_day p on (t.underlying_reserve = p.reserve and t.block_day = p.block_day and t.market = p.market)
  -- left join protocol_data_lake.coingecko_data_by_day c on (t.underlying_reserve = c.address and t.block_day = c.block_day and t.chain = c.chain)
  -- left join warehouse.user_rewards_by_day r on (t.market = r.market and t.block_day = r.block_day and t.collector = r.vault_address and t.token = r.token_address)
  left join {{ source('financials_data_lake','aave_oracle_prices_by_day')}} p on (t.underlying_reserve = p.reserve and t.block_day = p.block_day and t.market = p.market)
  left join {{ source('protocol_data_lake','coingecko_data_by_day')}} c on (t.underlying_reserve = c.address and t.block_day = c.block_day and t.chain = c.chain)
  left join {{ source('warehouse','user_rewards_by_day')}} r on (t.market = r.market and t.block_day = r.block_day and t.collector = r.vault_address and t.token = r.token_address)
)


, token_level_calcs_staging as (
-- apply the fix for double transfer on liqs on V3 - PR682
-- V3 mainnet has aave-v3-core==3.0.1 deployed from inception which contains PR682
-- only apply to the buggy v3 markets prior to the fix implementation
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
  , start_paraswap_fees_claimable
  , end_paraswap_fees_claimable
  , case
      when scaled_balance = 0 then (tokens_in_external+tokens_in_internal-minted_amount)/(1+1)
      else (tokens_in_external+tokens_in_internal-minted_amount)/(1+start_balance/scaled_balance) 
    end as liq_adjust
from balances_prices
where 1=1
  and end_balance is not null
  and market in ('arbitrum_v3','avax_v3','fantom_v3','harmony_v3','optimism_v3','polygon_v3')
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
  , start_paraswap_fees_claimable
  , end_paraswap_fees_claimable
  , 0 as liq_adjust
from balances_prices
where 1=1
  and end_balance is not null
  and market not in ('arbitrum_v3','avax_v3','fantom_v3','harmony_v3','optimism_v3','polygon_v3')
)


, token_level_calcs as (
select 
  collector 
  , chain
  , market
  , token 
  -- aPAX renamed to aUSDP - subgraph returns aPAX
  , case when token = '0x2e8f4bdbe3d47d7d7de490437aea9915d930f1a3' then 'aUSDP' else symbol end as symbol
  , underlying_reserve 
  , case when token = '0x8e870d67f660d95d5be530380d0ec0bd388289e1' then 'aUSDP' else underlying_reserve_symbol end as underlying_reserve_symbol
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
  , end_accrued_fees - start_accrued_fees + minted_to_treasury_amount + end_paraswap_fees_claimable - start_paraswap_fees_claimable as protocol_fees_accrued
  , start_usd_price
  , end_usd_price 
  , sm_stkAAVE_claims
  , sm_stkABPT_claims
  , lm_aave_v2_claims
  , start_paraswap_fees_claimable
  , end_paraswap_fees_claimable
  , liq_adjust
  , tokens_in_external - liq_adjust - minted_amount + minted_to_treasury_amount as tokens_in_external_adjust -- liqs only occur externally
  , tokens_in_external - liq_adjust - minted_amount as protocol_fees_received
  , case when collector = '0x25f2226b597e8f9514b3f68f00f494cf4f286491' and market = 'ethereum_v2'
      then tokens_out_external - (sm_stkAAVE_claims + sm_stkABPT_claims) else 0 end as ecosystem_reserve_spend
  , case when not (collector in ('0xd784927ff2f95ba542bfc824c8a8a98f3495f6b5', '0x25f2226b597e8f9514b3f68f00f494cf4f286491') and chain = 'ethereum')
      then tokens_out_external else 0 end as treasury_spend
  , end_balance - (tokens_in_external + tokens_in_internal  - liq_adjust - minted_amount + minted_to_treasury_amount) + tokens_out_external + tokens_out_internal - start_balance as atoken_interest
  , (end_balance + end_accrued_fees) * (end_usd_price - start_usd_price) as price_change_usd
  , start_balance * start_usd_price as start_balance_usd
  , end_balance * end_usd_price as end_balance_usd
  , start_accrued_fees * start_usd_price as start_accrued_fees_usd
  , end_accrued_fees * end_usd_price as end_accrued_fees_usd
  , tokens_in_external * start_usd_price as tokens_in_external_usd
  , tokens_in_internal * start_usd_price as tokens_in_internal_usd
  , tokens_out_external * start_usd_price as tokens_out_external_usd
  , tokens_out_internal * start_usd_price as tokens_out_internal_usd
  , (tokens_in_external - liq_adjust - minted_amount + minted_to_treasury_amount) * start_usd_price as tokens_in_external_adjust_usd
  , (tokens_in_external - liq_adjust - minted_amount) * start_usd_price as protocol_fees_received_usd
  , (end_balance - (tokens_in_external + tokens_in_internal  - liq_adjust - minted_amount + minted_to_treasury_amount) + tokens_out_external + tokens_out_internal - start_balance) * start_usd_price as atoken_interest_usd
  , (end_accrued_fees - start_accrued_fees + minted_to_treasury_amount + end_paraswap_fees_claimable - start_paraswap_fees_claimable) * start_usd_price as protocol_fees_accrued_usd
  , minted_to_treasury_amount * start_usd_price as minted_to_treasury_amount_usd
  , sm_stkAAVE_claims * start_usd_price as sm_stkAAVE_claims_usd
  , sm_stkABPT_claims * start_usd_price as sm_stkABPT_claims_usd
  , lm_aave_v2_claims * start_usd_price as lm_aave_v2_claims_usd
  , start_paraswap_fees_claimable * start_usd_price as start_paraswap_fees_claimable_usd
  , end_paraswap_fees_claimable * end_usd_price as end_paraswap_fees_claimable_usd
  , case when collector = '0x25f2226b597e8f9514b3f68f00f494cf4f286491' and market = 'ethereum_v2'
      then (tokens_out_external - (sm_stkAAVE_claims + sm_stkABPT_claims)) * start_usd_price else 0 end as ecosystem_reserve_spend_usd
  , case when not (collector in ('0xd784927ff2f95ba542bfc824c8a8a98f3495f6b5', '0x25f2226b597e8f9514b3f68f00f494cf4f286491') and chain = 'ethereum')
      then tokens_out_external * start_usd_price else 0 end as treasury_spend_usd
  from token_level_calcs_staging
)

, unpivot_source as (
select
  collector
  , chain
  , market
  , token
  , symbol
  , underlying_reserve
  , underlying_reserve_symbol
  , block_day
  , start_balance_usd
  , end_balance_usd
  , start_accrued_fees_usd
  , end_accrued_fees_usd
  , tokens_in_internal_usd
  , tokens_in_external_adjust_usd
  , tokens_out_external_usd
  , tokens_out_internal_usd
  , protocol_fees_received_usd
  , protocol_fees_accrued_usd
  , atoken_interest_usd
  , sm_stkAAVE_claims_usd
  , sm_stkABPT_claims_usd
  , lm_aave_v2_claims_usd
  , start_paraswap_fees_claimable_usd
  , end_paraswap_fees_claimable_usd
  , ecosystem_reserve_spend_usd
  , treasury_spend_usd
  , price_change_usd
  , start_balance
  , end_balance
  , start_accrued_fees
  , end_accrued_fees
  , tokens_in_internal
  , tokens_in_external_adjust
  , tokens_out_external
  , tokens_out_internal
  , protocol_fees_received
  , protocol_fees_accrued
  , atoken_interest
  , sm_stkAAVE_claims
  , sm_stkABPT_claims
  , lm_aave_v2_claims
  , start_paraswap_fees_claimable
  , end_paraswap_fees_claimable
  , ecosystem_reserve_spend
  , treasury_spend
from token_level_calcs
)



, long_format as (
select
  *
from unpivot_source
unpivot(value for measure in (start_balance_usd, end_balance_usd, start_accrued_fees_usd, end_accrued_fees_usd, tokens_in_internal_usd, tokens_in_external_adjust_usd, tokens_out_external_usd, tokens_out_internal_usd, protocol_fees_received_usd, protocol_fees_accrued_usd, atoken_interest_usd, sm_stkAAVE_claims_usd, sm_stkABPT_claims_usd, lm_aave_v2_claims_usd, start_paraswap_fees_claimable_usd, end_paraswap_fees_claimable_usd, ecosystem_reserve_spend_usd, treasury_spend_usd, price_change_usd, start_balance, end_balance, start_accrued_fees, end_accrued_fees, tokens_in_internal, tokens_in_external_adjust, tokens_out_external, tokens_out_internal, protocol_fees_received, protocol_fees_accrued, atoken_interest, sm_stkAAVE_claims, sm_stkABPT_claims, lm_aave_v2_claims, start_paraswap_fees_claimable, end_paraswap_fees_claimable, ecosystem_reserve_spend, treasury_spend))
)



select
  l.*
  , t.measure_type 
  , t.currency
  , d.display_chain
  , d.display_name
  , c.label as collector_label
  , b.balance_group
  , b.stable_class
from long_format l
  -- left join financials_data_lake.tx_classification t on (l.measure = t.measure)
  -- left join financials_data_lake.display_names d on (l.collector = d.collector and l.chain = d.chain and l.market = d.market)
  -- left join warehouse.aave_internal_addresses c on (l.collector = c.contract_address and l.chain = c.chain)
  -- left join warehouse.balance_group_lookup b on (l.market = b.market and l.token = b.atoken and l.underlying_reserve = b.reserve and l.symbol = b.atoken_symbol)
  left join {{ source('financials_data_lake','tx_classification') }} t on (l.measure = t.measure)
  left join {{ source('financials_data_lake','display_names') }} d on (l.collector = d.collector and l.chain = d.chain and l.market = d.market)
  left join {{ source('warehouse','aave_internal_addresses') }} c on (l.collector = c.contract_address and l.chain = c.chain)
  left join {{ source('warehouse','balance_group_lookup') }} b on (l.market = b.market and l.token = b.atoken and l.underlying_reserve = b.reserve and l.symbol = b.atoken_symbol)
where t.measure_type is not null
order by display_chain, display_name, block_day, symbol


