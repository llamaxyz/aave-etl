{{ config(materialized='table') }}

select
  vendor_label
  , stream_label
  , coalesce(stream_amount_stables, 0) as stream_amount_stables
  , coalesce(remaining_amount_stables, 0) as remaining_amount_stables
  , coalesce(upfront_native_stables, 0) as upfront_amount_stables
  , coalesce(bonus_native_stables, 0) as bonus_amount_stables
  , coalesce(stream_amount_stables, 0) + coalesce(upfront_native_stables, 0) + coalesce(bonus_native_stables, 0) as total_contract_amount_stables
  , coalesce(stream_amount_AAVE, 0) as stream_amount_AAVE
  , coalesce(remaining_amount_AAVE, 0) as remaining_amount_AAVE
  , coalesce(upfront_native_AAVE, 0) as upfront_amount_AAVE
  , coalesce(bonus_native_AAVE, 0) as bonus_amount_AAVE
  , coalesce(stream_amount_AAVE, 0) + coalesce(upfront_native_AAVE, 0) + coalesce(bonus_native_AAVE, 0) as total_contract_amount_AAVE
from 
  (
  select 
    vendor_label
    , stream_label
    , token_type
    , sum(total_payment_native) as stream_amount
    , sum(unvested_native) as remaining_amount
    , sum(upfront_native) as upfront_native
    , sum(bonus_native) as bonus_native
--   from streaming_payments_state
  from {{ ref('streaming_payments_detail')}}
  group by vendor_label, stream_label, token_type
  )
  pivot
  (
    sum(stream_amount) as stream_amount
    , sum(remaining_amount) as remaining_amount
    , sum(upfront_native) as upfront_native
    , sum(bonus_native) as bonus_native
    for token_type in ('AAVE','stables')
  )