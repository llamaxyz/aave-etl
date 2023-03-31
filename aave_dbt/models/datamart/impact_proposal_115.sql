 {{ config(materialized='table') }}

with purchase_days as (
select 
  'proposal_115' as proposal
  , t.block_day
  , t.token as token_address
  , t.symbol
  , t.tokens_in_external as purchase_amount
  , t.tokens_in_external * p.usd_price as purchase_amount_usd
  , p.usd_price
-- from warehouse.non_atoken_measures_by_day t
from  {{ source('warehouse','non_atoken_measures_by_day')}} t
-- left join warehouse.token_prices_by_day p on (
left join {{ source('warehouse','token_prices_by_day')}} p on (
  t.block_day = p.block_day and
  t.token = p.reserve 
)
where 1=1
  and t.contract_address = '0x464c71f6c2f760dda6093dcb91c24c39e5d6e18c'
  and t.token = '0xba100000625a3754423978a60c9317c58a424e3d'
  and p.chain = 'ethereum'
  and t.tokens_in_external > 0
  and t.block_day between '2022-11-14' and '2022-12-14'
union all
select 
  'proposal_87' as proposal
  , t.block_day
  , t.token as token_address
  , t.symbol
  , t.tokens_in_external as purchase_amount
  , t.tokens_in_external * p.usd_price as purchase_amount_usd
  , p.usd_price
-- from warehouse.non_atoken_measures_by_day t
from  {{ source('warehouse','non_atoken_measures_by_day')}} t
-- left join warehouse.token_prices_by_day p on (
left join {{ source('warehouse','token_prices_by_day')}} p on (
  t.block_day = p.block_day and
  t.token = p.reserve 
)
where 1=1
  and t.contract_address = '0x464c71f6c2f760dda6093dcb91c24c39e5d6e18c'
  and t.token = '0xba100000625a3754423978a60c9317c58a424e3d'
  and p.chain = 'ethereum'
  and t.tokens_in_external > 0
  and t.block_day between '2022-07-18' and '2022-07-20'
union all
select 
  'aggregate' as proposal
  , t.block_day
  , t.token as token_address
  , t.symbol
  , t.tokens_in_external as purchase_amount
  , t.tokens_in_external * p.usd_price as purchase_amount_usd
  , p.usd_price
-- from warehouse.non_atoken_measures_by_day t
from  {{ source('warehouse','non_atoken_measures_by_day')}} t
-- left join warehouse.token_prices_by_day p on (
left join {{ source('warehouse','token_prices_by_day')}} p on (
  t.block_day = p.block_day and
  t.token = p.reserve 
)
where 1=1
  and t.contract_address = '0x464c71f6c2f760dda6093dcb91c24c39e5d6e18c'
  and t.token = '0xba100000625a3754423978a60c9317c58a424e3d'
  and p.chain = 'ethereum'
  and t.tokens_in_external > 0
  and t.block_day between '2022-07-18' and '2022-12-14'
order by proposal desc, block_day
)

, current_price as (
select 
  block_day
  , reserve
  , usd_price
from warehouse.token_prices_by_day 
where 1=1
  and reserve = '0xba100000625a3754423978a60c9317c58a424e3d'
  and block_day = (select max(block_day) from warehouse.token_prices_by_day)
)

, purchase_price as (
select 
  proposal
  , token_address
  , symbol 
  , sum(purchase_amount) as purchase_amount 
  , sum(purchase_amount_usd) as purchase_amount_usd 
  , sum(purchase_amount_usd) / sum(purchase_amount) as av_purchase_price 
from purchase_days 
group by proposal, token_address, symbol
)

select 
  p.proposal
  , c.block_day
  , p.token_address
  , p.symbol
  , round(p.purchase_amount, 5) as purchase_amount
  , p.purchase_amount_usd
  , p.av_purchase_price
  , c.usd_price as current_price
  , p.purchase_amount * c.usd_price as current_value
  , p.purchase_amount * c.usd_price - p.purchase_amount_usd as net_value_change
from purchase_price p
  left join current_price c on p.token_address = c.reserve
order by proposal desc