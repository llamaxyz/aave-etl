{{ config(materialized='table') }}

with pre_process as (
select
  vendor_label
  , stream_label
  , term
  , proposal_id
  , proposal_url
  , stream_create_date
  , stream_start_time 
  , stream_stop_time
  , coalesce(stream_amount_AAVE, 0) as stream_amount_AAVE
  , coalesce(remaining_amount_AAVE, 0) as remaining_amount_AAVE
  , coalesce(upfront_native_AAVE, 0) as upfront_amount_AAVE
  , coalesce(bonus_native_AAVE, 0) as bonus_amount_AAVE
  , coalesce(stream_amount_AAVE, 0) + coalesce(upfront_native_AAVE, 0) + coalesce(bonus_native_AAVE, 0) as total_contract_amount_AAVE
  , coalesce(usd_price_AAVE, 0) as usd_price_AAVE
  , coalesce(stream_amount_aDAI, 0) as stream_amount_aDAI
  , coalesce(remaining_amount_aDAI, 0) as remaining_amount_aDAI
  , coalesce(upfront_native_aDAI, 0) as upfront_amount_aDAI
  , coalesce(bonus_native_aDAI, 0) as bonus_amount_aDAI
  , coalesce(stream_amount_aDAI, 0) + coalesce(upfront_native_aDAI, 0) + coalesce(bonus_native_aDAI, 0) as total_contract_amount_aDAI
  , coalesce(usd_price_aDAI, 0) as usd_price_aDAI
  , coalesce(stream_amount_aUSDC, 0) as stream_amount_aUSDC
  , coalesce(remaining_amount_aUSDC, 0) as remaining_amount_aUSDC
  , coalesce(upfront_native_aUSDC, 0) as upfront_amount_aUSDC
  , coalesce(bonus_native_aUSDC, 0) as bonus_amount_aUSDC
  , coalesce(stream_amount_aUSDC, 0) + coalesce(upfront_native_aUSDC, 0) + coalesce(bonus_native_aUSDC, 0) as total_contract_amount_aUSDC
  , coalesce(usd_price_aUSDC, 0) as usd_price_aUSDC
  , coalesce(stream_amount_aUSDT, 0) as stream_amount_aUSDT
  , coalesce(remaining_amount_aUSDT, 0) as remaining_amount_aUSDT
  , coalesce(upfront_native_aUSDT, 0) as upfront_amount_aUSDT
  , coalesce(bonus_native_aUSDT, 0) as bonus_amount_aUSDT
  , coalesce(stream_amount_aUSDT, 0) + coalesce(upfront_native_aUSDT, 0) + coalesce(bonus_native_aUSDT, 0) as total_contract_amount_aUSDT
  , coalesce(usd_price_aUSDT, 0) as usd_price_aUSDT
from 
  (
  select 
    vendor_label
    , stream_label
    , symbol
    , term
    , proposal_id
    , proposal_url
    , stream_create_date
    , stream_start_time 
    , stream_stop_time
    , sum(total_payment_native) as stream_amount
    , sum(unvested_native) as remaining_amount
    , sum(upfront_native) as upfront_native
    , sum(bonus_native) as bonus_native
    , avg(usd_price) as usd_price
--   from streaming_payments_detail
  from {{ ref('streaming_payments_detail') }}
  group by vendor_label, stream_label, symbol, term, proposal_id, proposal_url, stream_create_date, stream_start_time, stream_stop_time
  )
  pivot
  (
    sum(stream_amount) as stream_amount
    , sum(remaining_amount) as remaining_amount
    , sum(upfront_native) as upfront_native
    , sum(bonus_native) as bonus_native
    , avg(usd_price) as usd_price
    for symbol in ('AAVE','aDAI','aUSDC','aUSDT')
  )
)

select 
  vendor_label
  , stream_label
  , term
  , proposal_id
  , proposal_url
  , max(stream_create_date) as stream_create_date
  , max(stream_start_time) as stream_start_time
  , max(stream_stop_time) as stream_stop_time
  , sum(stream_amount_AAVE) as stream_amount_AAVE
  , sum(remaining_amount_AAVE) as remaining_amount_AAVE
  , sum(upfront_amount_AAVE) as upfront_amount_AAVE
  , sum(bonus_amount_AAVE) as bonus_amount_AAVE
  , sum(total_contract_amount_AAVE) as total_contract_amount_AAVE
  , sum(usd_price_AAVE) as usd_price_AAVE
  , sum(stream_amount_aDAI) as stream_amount_aDAI
  , sum(remaining_amount_aDAI) as remaining_amount_aDAI
  , sum(upfront_amount_aDAI) as upfront_amount_aDAI
  , sum(bonus_amount_aDAI) as bonus_amount_aDAI
  , sum(total_contract_amount_aDAI) as total_contract_amount_aDAI
  , sum(usd_price_aDAI) as usd_price_aDAI
  , sum(stream_amount_aUSDC) as stream_amount_aUSDC
  , sum(remaining_amount_aUSDC) as remaining_amount_aUSDC
  , sum(upfront_amount_aUSDC) as upfront_amount_aUSDC
  , sum(bonus_amount_aUSDC) as bonus_amount_aUSDC
  , sum(total_contract_amount_aUSDC) as total_contract_amount_aUSDC
  , sum(usd_price_aUSDC) as usd_price_aUSDC
  , sum(stream_amount_aUSDT) as stream_amount_aUSDT
  , sum(remaining_amount_aUSDT) as remaining_amount_aUSDT
  , sum(upfront_amount_aUSDT) as upfront_amount_aUSDT
  , sum(bonus_amount_aUSDT) as bonus_amount_aUSDT
  , sum(total_contract_amount_aUSDC) + sum(total_contract_amount_aUSDT) as total_contract_amount_aUSDT
  , sum(usd_price_aUSDT) as usd_price_aUSDT
  , sum(stream_amount_aDAI) + sum(stream_amount_aUSDC) + sum(stream_amount_aUSDT) as stream_amount_stables
  , sum(remaining_amount_aDAI) + sum(remaining_amount_aUSDC) + sum(remaining_amount_aUSDT) as remaining_amount_stables
  , sum(upfront_amount_aDAI) + sum(upfront_amount_aUSDC) + sum(upfront_amount_aUSDT) as upfront_amount_stables
  , sum(bonus_amount_aDAI) + sum(bonus_amount_aUSDC) + sum(bonus_amount_aUSDT) as bonus_amount_stables
  , sum(total_contract_amount_aDAI) + sum(total_contract_amount_aUSDC) + sum(total_contract_amount_aUSDT) as total_contract_amount_stables
from pre_process
group by vendor_label, stream_label, term, proposal_id, proposal_url

