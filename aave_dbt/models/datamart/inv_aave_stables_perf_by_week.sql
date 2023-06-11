{{ config(materialized='table') }}

with base as (
select 
  block_day
  , date_trunc(block_day, week) as time_group
  , least(date_diff(current_date(), cast(date_trunc(block_day, week) as date), day) + 1, 7) as days_in_time_group
  , market 
  , balance_group 
  , stable_class
  , symbol 
  , balance_native
  , balance_usd
  , balance_eth
  , usd_price
  , eth_price
  , earnings_native
  , earnings_usd 
  , earnings_eth
-- from datamart.inv_treasury_atoken_performance_base
from {{ ref('inv_treasury_atoken_performance_base') }}
where price_change_usd is not null
  and stable_class = 'stablecoin'
  and block_day > '2022-01-01' -- start on a week boundary
)

, first_vals as (
select 
  time_group
  , market 
  , balance_group 
  , stable_class
  , symbol 
  , days_in_time_group
  , avg(balance_native) over (partition by market, balance_group, stable_class, symbol, time_group order by time_group) as balance_native
  , avg(balance_usd) over (partition by market, balance_group, stable_class, symbol, time_group order by time_group) as balance_usd
  , avg(balance_eth) over (partition by market, balance_group, stable_class, symbol, time_group order by time_group) as balance_eth
  , first_value(usd_price) over (partition by market, balance_group, stable_class, symbol, time_group order by time_group) as start_price_usd
  , first_value(eth_price) over (partition by market, balance_group, stable_class, symbol, time_group order by time_group) as start_price_eth
  , first_value(usd_price) over (partition by market, balance_group, stable_class, symbol order by time_group) as origin_price_usd
  , first_value(eth_price) over (partition by market, balance_group, stable_class, symbol order by time_group) as origin_price_eth
  , earnings_native
  , earnings_usd
  , earnings_eth
from base
order by market, symbol, block_day
)

, by_asset as (
select 
    time_group
  , market 
  , balance_group 
  , stable_class
  , symbol
  , any_value(days_in_time_group) as days_in_time_group
  , any_value(balance_native) as balance_native
  , any_value(balance_usd) as balance_usd
  , any_value(balance_eth) as balance_eth
  , any_value(start_price_usd) as start_price_usd
  , any_value(start_price_eth) as start_price_eth
  , any_value(origin_price_usd) as origin_price_usd
  , any_value(origin_price_eth) as origin_price_eth
  , sum(earnings_native) as earnings_native
  , sum(earnings_usd) as earnings_usd
  , sum(earnings_eth) as earnings_eth
from first_vals
group by time_group, market, balance_group, stable_class, symbol
order by time_group, market, balance_group, stable_class, symbol
)

, last_vals as (
select 
    time_group
  , market 
  , balance_group 
  , stable_class
  , symbol
  , days_in_time_group
  , balance_native
  , balance_usd
  , balance_eth
  , start_price_usd
  , coalesce(lead(start_price_usd) over (partition by market, balance_group, stable_class, symbol order by time_group), start_price_usd)  as end_price_usd
  , (coalesce(lead(start_price_usd) over (partition by market, balance_group, stable_class, symbol order by time_group), start_price_usd) - origin_price_usd) * balance_native as price_change_usd
  , start_price_eth
  , coalesce(lead(start_price_eth) over (partition by market, balance_group, stable_class, symbol order by time_group), start_price_eth) as end_price_eth
  , (coalesce(lead(start_price_eth) over (partition by market, balance_group, stable_class, symbol order by time_group), start_price_eth) - origin_price_eth) * balance_native as price_change_eth
  , earnings_native
  , earnings_usd
  , earnings_eth
from by_asset
)

, by_time_group as (
select 
  time_group
  , any_value(days_in_time_group) as days_in_time_group
  , sum(balance_usd) as balance_usd
  , sum(balance_eth) as balance_eth
  , sum(price_change_usd) as price_change_usd
  , sum(price_change_eth) as price_change_eth
  , sum(earnings_usd) as earnings_usd
  , sum(earnings_eth) as earnings_eth
  , sum(earnings_usd)  / sum(balance_usd) as usd_rate_ex_price
  , sum(earnings_eth) / sum(balance_eth) as eth_rate_ex_price
from last_vals
group by time_group
order by time_group
)

, raw_ref_rates as (
select
  block_time
  , date_trunc(block_time, day) as block_day
  , date_trunc(block_time, week) as time_group
  , risk_free_stable_rate as stable_reference_rate
  , risk_free_eth_rate as eth_reference_rate
-- from datamart.inv_defi_ref_rates_by_time
from {{ ref('inv_defi_ref_rates_by_time') }}
)

, ref_rates_by_day as (
select 
  block_day
  , time_group
  , avg(stable_reference_rate) as stable_reference_rate
  , avg(eth_reference_rate) as eth_reference_rate
from raw_ref_rates 
group by block_day, time_group
)

, ref_rates as (
select 
  time_group
  , avg(stable_reference_rate) as stable_reference_rate
  , avg(eth_reference_rate) as eth_reference_rate
from ref_rates_by_day
group by time_group
)

select
  a.time_group as week
  -- , a.days_in_time_group
  -- , a.balance_usd
  -- , a.balance_eth
  -- , a.price_change_usd
  -- , a.price_change_eth
  -- , a.earnings_usd
  -- , a.earnings_eth
  -- , a.usd_rate_ex_price
  , a.usd_rate_ex_price / a.days_in_time_group * 365 as usd_apr_ex_price
  , 1 + sum(a.usd_rate_ex_price) over (order by a.time_group rows between unbounded preceding and current row) + a.price_change_usd / a.balance_usd as usd_index_inc_price
  , 1 + sum(a.usd_rate_ex_price) over (order by a.time_group rows between unbounded preceding and current row) as usd_index_ex_price
  -- , a.eth_rate_ex_price
  -- , a.eth_rate_ex_price / a.days_in_time_group * 365 as eth_apr_ex_price
  -- , 1 + sum(a.eth_rate_ex_price) over (order by a.time_group rows between unbounded preceding and current row) + a.price_change_eth / a.balance_eth as eth_index_inc_price
  -- , 1 + sum(a.eth_rate_ex_price) over (order by a.time_group rows between unbounded preceding and current row) as eth_index_ex_price
  , r.stable_reference_rate
  , 1 + sum(r.stable_reference_rate / 365 * days_in_time_group)  over (order by a.time_group rows between unbounded preceding and current row) as stable_reference_index
  -- , r.eth_reference_rate
  -- , 1 + sum(r.eth_reference_rate / 365 * days_in_time_group)  over (order by a.time_group rows between unbounded preceding and current row) as eth_reference_index
from by_time_group a 
  left join ref_rates r on (a.time_group = r.time_group)
order by a.time_group