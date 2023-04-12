import sys
import pandas as pd
import numpy as np
from dagster import (#AssetIn,  # SourceAsset,; Output,
                    #  DailyPartitionsDefinition, 
                     ExperimentalWarning,
                     MetadataValue, #MultiPartitionKey,
                    #  MultiPartitionsDefinition, Optional, PartitionKeyRange,
                    #  PartitionMapping, PartitionsDefinition,
                    #  StaticPartitionsDefinition, 
                    asset,# op,
                    #  LastPartitionMapping
                    AllPartitionMapping,
                    IdentityPartitionMapping,
                    AssetIn
                     )
from icecream import ic

from aave_data.resources.financials_config import * #pylint: disable=wildcard-import, unused-wildcard-import

from aave_data.resources.helpers import (
    standardise_types
)
from aave_data.assets.financials.data_lake import (
    market_day_multipartition,
)

if not sys.warnoptions:
    import warnings
    warnings.filterwarnings("ignore", category=ExperimentalWarning)

@asset(
    compute_kind='python',
    code_version="1",
    io_manager_key = 'data_warehouse_io_manager',
    ins={
        "protocol_data_by_day": AssetIn(key_prefix="protocol_data_lake"),
        "emode_config_by_day": AssetIn(key_prefix="protocol_data_lake"),
    }
)
def market_config_by_day(context, protocol_data_by_day, emode_config_by_day) -> pd.DataFrame:
    """
    Returns the config parameters set for each token in each market on each day

    Args:
        context: dagster context object
        protocol_data_by_day: the output of protocol_data_by_day

    Returns:
        A dataframe a column for each config parameter and a row for each token in each market on each day

    """

    return_val = protocol_data_by_day[
        [
            'block_day',
            'block_height',
            'market',
            'reserve',
            'symbol',
            'decimals',
            'ltv',
            'liquidation_threshold',
            'liquidation_bonus',
            'reserve_factor',
            'usage_as_collateral_enabled',
            'borrowing_enabled',
            'stable_borrow_rate_enabled',
            'is_active',
            'is_frozen',
            'reserve_emode_category',
            'borrow_cap',
            'supply_cap',
            'is_paused',
            'siloed_borrowing',
            'liquidation_protocol_fee',
            'unbacked_mint_cap',
            'debt_ceiling',
        ]
    ]

    return_val = return_val.merge(emode_config_by_day, how='left', on = ['block_day', 'block_height', 'market', 'reserve_emode_category'])

    return_val.drop(columns=['emode_price_address'], inplace=True)

    return_val.rename(columns={'symbol': 'atoken_symbol'}, inplace=True)

    context.add_output_metadata(
        {
            "num_records": len(return_val),
            "preview": MetadataValue.md(return_val.head().to_markdown()),
        }
    )
    return return_val

@asset(
    compute_kind='python',
    code_version="1",
    io_manager_key = 'data_warehouse_io_manager',
    ins={
        "protocol_data_by_day": AssetIn(key_prefix="protocol_data_lake"),
    }
)
def market_state_by_day(context, protocol_data_by_day) -> pd.DataFrame:
    """
    Returns the state data for each token in each market on each day
    This also calcs APY for each rate as per the AAVE UI display

    Args:
        context: dagster context object
        protocol_data_by_day: the output of protocol_data_by_day

    Returns:
        A dataframe a column for each data point and a row for each token in each market on each day

    """
    SECONDS_IN_YEAR = 60*60*24*365

    def rate_to_apy(rate):
        # as per https://docs.aave.com/risk/liquidity-risk/borrow-interest-rate
        return (1 + rate / SECONDS_IN_YEAR) ** SECONDS_IN_YEAR - 1
        

    return_val = protocol_data_by_day[
        [
            'block_day',
            'block_height',
            'market',
            'reserve',
            'symbol',
            'unbacked_atokens',
            'scaled_accrued_to_treasury',
            'atoken_supply',
            'stable_debt',
            'variable_debt',
            'liquidity_rate',
            'variable_borrow_rate',
            'stable_borrow_rate',
            'average_stable_rate',
            'liquidity_index',
            'variable_borrow_index',
            'available_liquidity',
        ]
    ]

    return_val.rename(columns={'symbol': 'atoken_symbol'}, inplace=True)

    return_val = return_val.assign(
        deposit_apy=lambda x: rate_to_apy(x.liquidity_rate),
        variable_borrow_apy=lambda x: rate_to_apy(x.variable_borrow_rate),
        stable_borrow_apy=lambda x: rate_to_apy(x.stable_borrow_rate),
        av_stable_borrow_apy=lambda x: rate_to_apy(x.average_stable_rate),
    )

    context.add_output_metadata(
        {
            "num_records": len(return_val),
            "preview": MetadataValue.md(return_val.head().to_markdown()),
        }
    )
    return return_val


@asset(
    compute_kind='python',
    code_version="1",
    io_manager_key = 'data_warehouse_io_manager',
    ins={
        "raw_incentives_by_day": AssetIn(key_prefix="protocol_data_lake"),
        "protocol_data_by_day": AssetIn(key_prefix="protocol_data_lake"),
        "aave_oracle_prices_by_day": AssetIn(key_prefix="financials_data_lake"),
    }
)
def incentives_by_day(context,
                      raw_incentives_by_day,
                      protocol_data_by_day,
                      aave_oracle_prices_by_day) -> pd.DataFrame:
    """
    Returns the state data for each token in each market on each day
    This also calcs APY for each rate as per the AAVE UI display

    TODO - aave v2 markets do not have pricing yet.  This requires a new pricing table.
      The oracle function in v2 is hardcoded to zero.  V3 currently OK.

    Args:
        context: dagster context object
        protocol_data_by_day: the output of protocol_data_by_day

    Returns:
        A dataframe a column for each data point and a row for each token in each market on each day

    """
    SECONDS_IN_DAY = 60*60*24
    
    def safe_div(x, y):
        if y: return x / y
        else: return 0

    incentives = raw_incentives_by_day[
        [
            'block_day',
            'block_height',
            'market',
            'underlying_asset',
            'token_address',
            'token_type',
            'reward_token_address',
            'reward_token_symbol',
            'reward_token_decimals',
            'emission_per_second',
            'emission_end_timestamp',
            'reward_price_feed',
            'price_feed_decimals',
        ]
    ]

    # calc the emission per day in native and USD
    incentives['end_date'] = pd.to_datetime(incentives.emission_end_timestamp, unit='s', utc=True)
    incentives['emission_per_day'] = incentives.emission_per_second * SECONDS_IN_DAY / 10 ** incentives.reward_token_decimals
    incentives['emission_per_day'] = np.where(incentives.block_day > incentives.end_date, 0, incentives.emission_per_day)
    incentives['emission_per_day_usd'] = incentives.emission_per_day * incentives.reward_price_feed / 10 ** incentives.price_feed_decimals

    # get rid of the unneeded columns
    incentives.drop(columns=['emission_per_second', 'emission_end_timestamp', 'reward_price_feed', 'price_feed_decimals', 'reward_token_decimals'], inplace=True)
    incentives.rename(columns={'underlying_asset': 'reserve'}, inplace=True)

    # only keep the ones with a non-zero emission
    incentives = incentives.query('emission_per_day_usd > 0')

    # trim down the protocol data to the needed columns
    protocol_data = protocol_data_by_day[
        [
            'block_day',
            'block_height',
            'market',
            'reserve',
            'atoken_supply',
            'stable_debt',
            'variable_debt',
        ]
    ]
    # join with the protocol data to get the total supply and total debt
    incentives = incentives.merge(protocol_data, how='left')

    # join with the price data to get the total supply and total debt
    incentives = incentives.merge(aave_oracle_prices_by_day, how='left')
    
    def apr_calc(row):
        supply_rewards_apr = np.where(row.token_type == 'atoken', 
                                    safe_div(row.emission_per_day_usd, row.atoken_supply * row.usd_price) * 365,
                                    0.0)
        variable_debt_rewards_apr = np.where(row.token_type == 'vtoken',
                                            safe_div(row.emission_per_day_usd, row.atoken_supply * row.usd_price) * 365,
                                            0.0)
        stable_debt_rewards_apr = np.where(row.token_type == 'stoken',
                                            safe_div(row.emission_per_day_usd, row.atoken_supply * row.usd_price) * 365,
                                            0.0)
        supply_rewards = np.where(row.token_type == 'atoken', 
                                    row.emission_per_day,
                                    0.0)
        variable_debt_rewards = np.where(row.token_type == 'vtoken', 
                                    row.emission_per_day,
                                    0.0)
        stable_debt_rewards = np.where(row.token_type == 'stoken', 
                                    row.emission_per_day,
                                    0.0)
        supply_rewards_usd = np.where(row.token_type == 'atoken', 
                                    row.emission_per_day_usd,
                                    0.0)
        variable_debt_rewards_usd = np.where(row.token_type == 'vtoken', 
                                    row.emission_per_day_usd,
                                    0.0)
        stable_debt_rewards_usd = np.where(row.token_type == 'stoken', 
                                    row.emission_per_day_usd,
                                    0.0)
        return supply_rewards_apr, variable_debt_rewards_apr, stable_debt_rewards_apr, supply_rewards, variable_debt_rewards, stable_debt_rewards, supply_rewards_usd, variable_debt_rewards_usd, stable_debt_rewards_usd
    
    incentives['supply_rewards_apr'], incentives['variable_borrow_rewards_apr'], incentives['stable_borrow_rewards_apr'],\
        incentives['supply_rewards'], incentives['variable_debt_rewards'], incentives['stable_debt_rewards'],\
            incentives['supply_rewards_usd'], incentives['variable_debt_rewards_usd'], incentives['stable_debt_rewards_usd'] = zip(*incentives.apply(apr_calc, axis=1))
    
    
    incentives.supply_rewards_apr = incentives.supply_rewards_apr.astype(float)
    incentives.variable_borrow_rewards_apr = incentives.variable_borrow_rewards_apr.astype(float)
    incentives.stable_borrow_rewards_apr = incentives.stable_borrow_rewards_apr.astype(float)
    incentives.supply_rewards = incentives.supply_rewards.astype(float)
    incentives.variable_debt_rewards = incentives.variable_debt_rewards.astype(float)
    incentives.stable_debt_rewards = incentives.stable_debt_rewards.astype(float)
    incentives.supply_rewards_usd = incentives.supply_rewards_usd.astype(float)
    incentives.variable_debt_rewards_usd = incentives.variable_debt_rewards_usd.astype(float)
    incentives.stable_debt_rewards_usd = incentives.stable_debt_rewards_usd.astype(float)


    # aggregate by reserve & reward token
    incentives = incentives.groupby(
        [
        'block_day',
        'block_height',
        'market',
        'reserve',
        'symbol',
        'reward_token_symbol',
        'reward_token_address'
        ]
        , as_index=False).agg(
                {   
                    'supply_rewards_apr': 'sum',
                    'variable_borrow_rewards_apr': 'sum',
                    'stable_borrow_rewards_apr': 'sum',
                    'supply_rewards': 'sum',
                    'variable_debt_rewards': 'sum',
                    'stable_debt_rewards': 'sum',
                    'supply_rewards_usd': 'sum',
                    'variable_debt_rewards_usd': 'sum',
                    'stable_debt_rewards_usd': 'sum',
                }
        )
    
    incentives = standardise_types(incentives)

    return incentives

@asset(
    compute_kind="python",
    code_version="1",
    io_manager_key = 'data_warehouse_io_manager',
    ins={
        "liquidity_depth_raw": AssetIn(key_prefix="protocol_data_lake"),
    }
)
def liquidity_depth(context, liquidity_depth_raw):
    """
    Interpolate liquidity depth data

    Takes the raw liquidity depth data and uses it to interpolate at specified targets
    (e.g. 0.01%, 0.0125%, 0.015%, etc.)

    Args:
        context (ExecutionContext): Dagster execution context
        liquidity_depth_raw (DataFrame): Raw liquidity depth data
        liquidity_depth (DataFrame): Interpolated and raw liquidity depth data from previous run

    Returns:
        DataFrame: Interpolated liquidity depth data combined with the raw data
    """
    # todo implement a sensor which updates this asset when the upstream changes
    # todo for now we just read & process everything

    # # get the unique load timestamps
    # load_timestamps = liquidity_depth.fetch_time.unique()

    # # filter the raw data to only include the timestamps that are not in the interpolated data
    # liquidity_depth_raw = liquidity_depth_raw[~liquidity_depth_raw.fetch_time.isin(load_timestamps)]

    # liquidity_depth_raw = pd.read_pickle("output.pkl")

    # if liquidity_depth_raw.empty:
    #     return_val = pd.DataFrame()
    # else:
    # set the range of target price impacts we are interested in
    start = 0.01
    end = 0.05
    increment = 0.0025
    target_price_impact = [i / 10000 for i in range(int(start * 10000), int(end * 10000) + 1, int(increment * 10000))]

    # group the raw data by market, assets & timestamp and add the from_amount_usd and price_impact as lists for use in the interp
    g = liquidity_depth_raw.groupby(['market_key','to_asset', 'from_asset','fetch_time']).agg(
        {
            'from_amount_usd' : lambda x: x.astype(float).to_list(),
            'price_impact' : lambda x: x.astype(float).to_list(),

        }
    ).reset_index()

    # add the target price impact to the grouped data as a list
    g['target_price_impact'] = g.apply(lambda x: target_price_impact, axis=1)
    # interpolate the from_amount_usd to the target price impact using np.interp
    g['new_from_amount'] = g.apply(lambda x: np.interp(x.target_price_impact, x.price_impact, x.from_amount_usd), axis=1)

    # explode the lists back out to a row per price impact
    g = g[['market_key','to_asset', 'from_asset','fetch_time','target_price_impact','new_from_amount']]
    g.rename(columns = {'target_price_impact':'price_impact','new_from_amount':'from_amount_usd'}, inplace=True)
    g = g.explode(['price_impact','from_amount_usd'])

    # calc the to_amount_usd
    g['to_amount_usd'] = (1-g.price_impact) * g.from_amount_usd

    # tag the data as interpolated or raw
    liquidity_depth_raw['is_interpolated'] = False
    g['is_interpolated'] = True

    # add the interpolated data to the raw data
    return_val = pd.concat([liquidity_depth_raw,g], axis=0)
    return_val = return_val.sort_values(['market_key','to_asset', 'from_asset','fetch_time','from_amount_usd']).reset_index(drop=True)

    
    # fix types
    for col in ['from_amount_usd','from_amount_native','to_amount_native','to_amount_usd','price_impact']:
        return_val[col] = return_val[col].astype('Float64')
    # fix types
    for col in ['to_asset_decimals','from_asset_decimals','chain_id']:
        return_val[col] = return_val[col].astype('Int64')

    # fill the missing fields
    for col in ['market','chain','loop_market','to_asset_address','to_asset_decimals','from_asset_address','from_asset_decimals','chain_id','from_asset_price','to_asset_price']:
        return_val[col] = return_val[col].fillna(method='ffill')

    # recalc missing fields
    return_val.from_amount_native = return_val.from_amount_usd / return_val.from_asset_price
    return_val.to_amount_native = return_val.to_amount_usd / return_val.to_asset_price

    return_val = standardise_types(return_val)
    # return_val.info()

    return return_val


if __name__ == "__main__":

    # import time
    # start = time.time()
    # out = interp_liquidity_depth()

    # ic(out.groupby(['is_interpolated']).count())
    start = 0.01
    end = 0.05
    increment = 0.0025
    target_price_impact = [i / 10000 for i in range(int(start * 10000), int(end * 10000) + 1, int(increment * 10000))]

    g = pd.read_pickle("g.pkl")
    g = g.loc[g.market_key == 'polygon_matic']
    ic(g)
    
    g['new_from_amount'] = g.apply(lambda x: np.interp(target_price_impact, x.price_impact, x.from_amount_usd), axis=1)

    # end = time.time()
    # elapsed = end - start
    # ic(elapsed)