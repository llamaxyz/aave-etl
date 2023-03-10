{{ config(materialized='table') }}

with atoken_prices as (
  select distinct
    p.reserve
    , p.symbol
    , a.atoken
    , a.atoken_symbol
    , p.usd_price
--   from warehouse.token_prices_by_day p
  from {{ source('warehouse','token_prices_by_day')}} p
--   left join (select atoken, reserve, atoken_symbol from datamart.aave_atokens where chain = 'ethereum') a
  left join (select atoken, reserve, atoken_symbol from {{ref('aave_atokens')}} where chain = 'ethereum') a
    on (p.reserve = a.reserve)
  where p.chain = 'ethereum'
    -- and p.block_day = (select max(block_day) from warehouse.token_prices_by_day)
    and p.block_day = (select max(block_day) from {{ source('warehouse','token_prices_by_day') }} )
)

, reserve_prices as (
  select distinct 
    reserve
    , symbol
    , usd_price
--   from warehouse.token_prices_by_day
  from {{ source('warehouse','token_prices_by_day')}}
  where chain = 'ethereum'
    -- and block_day = (select max(block_day) from warehouse.token_prices_by_day)
    and block_day = (select max(block_day) from {{ source('warehouse','token_prices_by_day') }} )
)


select 
  m.vendor_label
  , m.stream_label
  , m.proposal_id
  , m.stream_contract
  , m.token
  , m.stream_id
  , m.symbol
  , m.term
  , 'https://app.aave.com/governance/proposal/?proposalId=' || m.proposal_id as proposal_url
  , coalesce(s.deposit_day, '1970-01-01') as stream_create_date
  , coalesce(s.start_time, '1970-01-01') as stream_start_time
  , coalesce(s.stop_time, '1970-01-01') as stream_stop_time
  , coalesce(s.deposit, 0) as total_payment_native
  , coalesce(s.vested, 0) as vested_native
  , coalesce(s.unvested, 0) as unvested_native
  , coalesce(s.claims, 0) as claimed_native
  , coalesce(s.unclaimed, 0) as unclaimed_native
  , m.upfront_native
  , m.bonus_usd / coalesce(a.usd_price, r.usd_price) as bonus_native
  , coalesce(a.usd_price, r.usd_price) as usd_price
-- from financials_data_lake.streams_metadata m
from {{ source('financials_data_lake','streams_metadata')}} m
--   left join financials_data_lake.streaming_payments_state s 
  left join {{ source('financials_data_lake','streaming_payments_state')}} s
    on (
          m.stream_contract = s.contract_address and
          m.token = s.token_address and 
          m.stream_id = s.stream_id
        )
  left join atoken_prices a 
    on m.token = a.atoken
  left join reserve_prices r 
    on m.token = r.reserve
