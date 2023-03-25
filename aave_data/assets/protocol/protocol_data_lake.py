import json
# import sys
from datetime import datetime, timedelta, timezone  # , date, time, timedelta, timezone
import numpy as np
import pandas as pd
import requests
import sys 
from web3 import Web3
from web3.exceptions import BadFunctionCallOutput
from dagster import (AssetIn,  # SourceAsset,; Output,
                     DailyPartitionsDefinition, ExperimentalWarning,
                     MetadataValue, #MultiPartitionKey,
                     MultiPartitionsDefinition, #Optional, PartitionKeyRange,
                     #PartitionMapping, PartitionsDefinition,
                     StaticPartitionsDefinition, asset, #op,
                     #LastPartitionMapping,
                     IdentityPartitionMapping,
                     FreshnessPolicy,
                     MultiPartitionKey,
                     build_op_context
)
from icecream import ic
# from subgrounds.subgrounds import Subgrounds
from eth_abi.abi import decode
from eth_utils.conversions import to_bytes
from shroomdk import ShroomDK
from time import sleep
from random import randint

from aave_data.resources.financials_config import * #pylint: disable=wildcard-import, unused-wildcard-import

from aave_data.resources.helpers import (
    get_raw_reserve_data,
    raw_reserve_to_dataframe,
    standardise_types
)


INITIAL_RETRY = 0.01 #seconds
MAX_RETRIES = 10

from aave_data.assets.financials.data_lake import (
    market_day_multipartition,
    # block_numbers_by_day,
    # market_tokens_by_day,
    # aave_oracle_prices_by_day
)

@asset(
    partitions_def=market_day_multipartition,
    compute_kind="python",
    code_version="1",
    io_manager_key = 'protocol_data_lake_io_manager',
    ins={
        "market_tokens_by_day": AssetIn(key_prefix="financials_data_lake"),
    }
)
def protocol_data_by_day(
    context,
    market_tokens_by_day: pd.DataFrame,
) -> pd.DataFrame:
    """
    Table of the each token in a market with configuration and protocol data
    Data is retrieved on-chain using the Aave Protocol Data Provider (or equivalent)

    Args:
        context: dagster context object
        market_tokens_by_day: the output of market_tokens_by_day for a given market

    Returns:
        A dataframe market config & protocol data for each token in a market

    """
    date, market = context.partition_key.split("|")
    partition_datetime = datetime.strptime(date, '%Y-%m-%d')
    
    protocol_data = pd.DataFrame()

    if not market_tokens_by_day.empty:
        block_height = int(market_tokens_by_day.block_height.values[0])
        chain = CONFIG_MARKETS[market]["chain"]
        context.log.info(f"market: {market}")
        context.log.info(f"date: {date}")
        context.log.info(f"block_height: {block_height}")

        # get the protocol data for each token in the market
        for row in market_tokens_by_day.itertuples():
            reserve = row.reserve
            symbol = row.atoken_symbol
            decimals = row.decimals
            context.log.info(f"getting protocol data for {symbol} {reserve}")
            protocol_row = pd.DataFrame(
                [
                    {
                        "block_day": partition_datetime,
                        "block_height": block_height,       
                        "market": market,
                        "reserve": reserve,
                        "symbol": symbol,
                    }
                ]
            )
            # get the raw data from on-chain
            raw_reserve_data = get_raw_reserve_data(market, chain, reserve, decimals, block_height)

            # convert the raw data to a dataframe
            reserve_data = raw_reserve_to_dataframe(raw_reserve_data)

            # add the metadata
            protocol_row = pd.concat([protocol_row, reserve_data], axis=1)

            # add the row to the return value dataframe
            protocol_data = pd.concat([protocol_data, protocol_row], axis=0)
    
        # fix these values up here - more difficult to do in helper function
        protocol_data.debt_ceiling = protocol_data.debt_ceiling / 10 ** protocol_data.debt_ceiling_decimals
        protocol_data.debt_ceiling = protocol_data.debt_ceiling.astype(int)
        protocol_data.liquidation_protocol_fee = protocol_data.liquidation_protocol_fee / 10000

        protocol_data = standardise_types(protocol_data)

    context.add_output_metadata(
        {
            "num_records": len(protocol_data),
            "preview": MetadataValue.md(protocol_data.head().to_markdown()),
        }
    )
    
    return protocol_data
            
