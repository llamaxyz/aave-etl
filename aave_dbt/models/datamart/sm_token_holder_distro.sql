{{ config(materialized='table') }}

with holders as (
select 
  safety_module_token
  , holder_address
  , case 
    when balance > 100000 then 'holders >100k'
    when balance > 10000 then 'holders 10k-100k'
    when balance > 1000 then 'holders 1k-10k'
    when balance > 100 then 'holders 100-1k'
    when balance > 10 then 'holders 10-100'
    when balance > 1 then 'holders 1-10'
    else 'holders <1'
  end as holder_bucket
  , balance
  , total_supply
-- from protocol_data_lake.safety_module_token_hodlers_by_day
from {{ source('protocol_data_lake','safety_module_token_hodlers_by_day') }}
where 1=1
  and safety_module_token = 'stkAAVE'
--   and block_day = (select max(block_day) from protocol_data_lake.safety_module_token_hodlers_by_day)
  and block_day = (select max(block_day) from {{ source('protocol_data_lake','safety_module_token_hodlers_by_day') }})
union all
select 
  safety_module_token
  , holder_address
  , case 
    when balance > 100000000 then 'holders >100m'
    when balance > 10000000 then 'holders 10m-100m'
    when balance > 1000000 then 'holders 1m-10m'
    when balance > 100000 then 'holders 100k-1m'
    when balance > 10000 then 'holders 10k-100k'
    when balance > 1000 then 'holders 1k-10k'
    when balance > 100 then 'holders 100-1k'
    when balance > 10 then 'holders 10-100'
    when balance > 1 then 'holders 1-10'
    else 'holders <1'
  end as holder_bucket
  , balance
  , total_supply
-- from protocol_data_lake.safety_module_token_hodlers_by_day
from {{ source('protocol_data_lake','safety_module_token_hodlers_by_day') }}
where 1=1
  and safety_module_token = 'stkABPT'
--   and block_day = (select max(block_day) from protocol_data_lake.safety_module_token_hodlers_by_day)
  and block_day = (select max(block_day) from {{ source('protocol_data_lake','safety_module_token_hodlers_by_day') }})
)

, sort_orders  as (
select * from unnest([
  struct('holders >100m' as holder_bucket, 11 as sort_order)
  , ('holders 10m-100m', 10)
  , ('holders 1m-10m', 9)
  , ('holders >100k', 8)
  , ('holders 100k-1m', 7)
  , ('holders 10k-100k', 6)
  , ('holders 1k-10k', 5)
  , ('holders 100-1k', 4)
  , ('holders 10-100', 3)
  , ('holders 1-10', 2)
  , ('holders <1', 1)
])
)

, agg as (
select 
  safety_module_token
  , holder_bucket
  , sum(balance) as bucket_balance
  , sum(balance) / avg(total_supply) as balance_percentage
  , count(holder_address) as holder_count
from holders
group by safety_module_token, holder_bucket
)

, holder_totals as (
select 
  safety_module_token
  , count(*) as holder_total
from holders
group by safety_module_token
)

select 
  a.*
  , a.holder_count / h.holder_total as holder_percentage
from agg a 
  left join sort_orders s on (a.holder_bucket = s.holder_bucket)
  left join holder_totals h on (a.safety_module_token = h.safety_module_token)
order by safety_module_token, sort_order desc

