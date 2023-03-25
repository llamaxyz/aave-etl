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