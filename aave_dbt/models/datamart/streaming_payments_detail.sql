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
  , case when m.symbol = 'AAVE' then 'AAVE' else 'stables' end as token_type
  , s.deposit as total_payment_native
  , s.vested as vested_native
  , s.unvested as unvested_native
  , s.claims as claimed_native
  , s.unclaimed as unclaimed_native
  , m.upfront_native
  , m.bonus_native
  , s.deposit * coalesce(a.usd_price, r.usd_price) as total_payment_usd
  , s.vested * coalesce(a.usd_price, r.usd_price) as vested_usd
  , s.unvested * coalesce(a.usd_price, r.usd_price) as unvested_usd
  , s.claims * coalesce(a.usd_price, r.usd_price) as claimed_usd
  , s.unclaimed * coalesce(a.usd_price, r.usd_price) as unclaimed_usd
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
