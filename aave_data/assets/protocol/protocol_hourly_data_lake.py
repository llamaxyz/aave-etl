import json
# import sys
from datetime import datetime, timedelta, timezone # , date, time, timedelta, timezone
import numpy as np
import pandas as pd
import requests
import sys 
from web3 import Web3
from web3.exceptions import BadFunctionCallOutput 
from dagster import (AssetIn,  # SourceAsset,; Output,
                    #  DailyPartitionsDefinition, ExperimentalWarning,
                     MetadataValue, #MultiPartitionKey,
                     MultiPartitionsDefinition, #Optional, PartitionKeyRange,
                     #PartitionMapping, PartitionsDefinition,
                     StaticPartitionsDefinition, asset, #op,
                     #LastPartitionMapping,
                    #  IdentityPartitionMapping,
                    #  FreshnessPolicy,
                    #  MultiPartitionKey,
                    #  build_op_context,
                    #  MultiPartitionMapping,
                    #  DimensionPartitionMapping,
                    #  StaticPartitionMapping,
                     HourlyPartitionsDefinition
)
from icecream import ic
# from subgrounds.subgrounds import Subgrounds
from eth_abi.abi import decode
from eth_abi.exceptions import InsufficientDataBytes
from eth_utils.conversions import to_bytes
from shroomdk import ShroomDK
from time import sleep
from random import randint
from multicall import Call, Multicall
import asyncio

from aave_data.resources.financials_config import * #pylint: disable=wildcard-import, unused-wildcard-import

from aave_data.resources.helpers import (
    get_raw_reserve_data,
    raw_reserve_to_dataframe,
    standardise_types,
    get_quote_from_1inch,
    get_quote_from_1inch_async,
    get_aave_oracle_price,
    get_balancer_bpt_data,
)



INITIAL_RETRY = 0.01 #seconds
MAX_RETRIES = 10
HOURLY_PARTITION_START_DATE=datetime(2023,4,15,0,0,0, tzinfo=timezone.utc)

market_hour_multipartition = MultiPartitionsDefinition(
    {
        "time": HourlyPartitionsDefinition(start_date=HOURLY_PARTITION_START_DATE),
        "market": StaticPartitionsDefinition(['ethereum_v3','polygon_v3']),
    }
)


@asset(
    partitions_def=market_hour_multipartition,
    compute_kind="python",
    #group_name='data_lake',
    code_version="1",
    io_manager_key = 'protocol_data_lake_io_manager'
    # freshness_policy=FreshnessPolicy(maximum_lag_minutes=6*60),
)
def block_numbers_by_hour(context) -> pd.DataFrame:
    """Table with the closest block number to the hourly boundary for a chain

    Uses the defillama API at https://coins.llama.fi/block/{chain}/{timestamp}
    Returns the block height along with the target time and actual block time

    Args:
        context: dagster context object

    Returns:
        A dataframe with the target datetime, the actual datetime, the block height and the chain

    Raises:
        HTTPError from requests module on invalid calls or server errors

    """

    market = context.partition_key.keys_by_dimension['market']
    time = context.partition_key.keys_by_dimension['time']
    # date, market = context.partition_key.split("|")
    partition_datetime = datetime.strptime(time, '%Y-%m-%d-%H:%M')
    context.log.info(f"market: {market}")
    context.log.info(f"date: {time}")
    config_chain = CONFIG_MARKETS[market]['chain']
    llama_chain = CONFIG_CHAINS[config_chain]['defillama_chain']

    unix_timestamp = partition_datetime.timestamp()

    endpoint = f'https://coins.llama.fi/block/{llama_chain}/{unix_timestamp:.0f}'

    response = requests.get(endpoint, timeout=300)
    response.raise_for_status()

    vals = response.json()

    block_height = vals['height']
    block_time = datetime.utcfromtimestamp(vals['timestamp'])

    end_block_hour = partition_datetime + timedelta(hours=1)
    end_block_day_hour = end_block_hour.timestamp()

    endpoint = f'https://coins.llama.fi/block/{llama_chain}/{end_block_day_hour:.0f}'

    response = requests.get(endpoint, timeout=300)
    response.raise_for_status()

    vals = response.json()

    end_block = vals['height'] - 1

    return_val = pd.DataFrame([[partition_datetime, block_time, block_height, end_block]],
                        columns=['block_hour', 'block_time', 'block_height', 'end_block'])


    return_val['chain'] = config_chain
    return_val['market'] = market

    return_val = standardise_types(return_val)

    context.add_output_metadata(
        {
            "num_records": len(return_val),
            "preview": MetadataValue.md(return_val.head().to_markdown()),
        }
    )
    return return_val