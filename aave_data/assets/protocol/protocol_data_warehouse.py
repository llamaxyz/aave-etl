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
    }
)
def market_config_by_day(context, protocol_data_by_day) -> pd.DataFrame:
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
        return supply_rewards_apr, variable_debt_rewards_apr, stable_debt_rewards_apr
    
    incentives['supply_rewards_apr'], incentives['variable_borrow_rewards_apr'], incentives['stable_borrow_rewards_apr'] = zip(*incentives.apply(apr_calc, axis=1))
    
    incentives.supply_rewards_apr = incentives.supply_rewards_apr.astype(float)
    incentives.variable_borrow_rewards_apr = incentives.variable_borrow_rewards_apr.astype(float)
    incentives.stable_borrow_rewards_apr = incentives.stable_borrow_rewards_apr.astype(float)

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
                }
        )
    
    incentives = standardise_types(incentives)

    return incentives

