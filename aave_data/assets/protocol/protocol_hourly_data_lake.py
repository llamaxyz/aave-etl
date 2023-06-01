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
                     IdentityPartitionMapping,
                    #  FreshnessPolicy,
                    #  MultiPartitionKey,
                    #  build_op_context,
                     MultiPartitionMapping,
                     DimensionPartitionMapping,
                     TimeWindowPartitionMapping,
                     StaticPartitionMapping,
                     HourlyPartitionsDefinition,
                     LastPartitionMapping,
                    #  AssetKey
                    AutoMaterializePolicy
)
from icecream import ic
# from subgrounds.subgrounds import Subgrounds
from eth_abi.abi import decode
from eth_abi.exceptions import InsufficientDataBytes
from eth_utils.conversions import to_bytes
from time import sleep
from random import randint
from multicall import Call, Multicall
import asyncio

from aave_data.resources.financials_config import * #pylint: disable=wildcard-import, unused-wildcard-import

from aave_data.resources.helpers import *


INITIAL_RETRY = 0.01 #seconds
MAX_RETRIES = 10
HOURLY_PARTITION_START_DATE=datetime(2023,4,15,0,0,0, tzinfo=timezone.utc)

market_hour_multipartition = MultiPartitionsDefinition(
    {
        "time": HourlyPartitionsDefinition(start_date=HOURLY_PARTITION_START_DATE, end_offset=1, timezone="UTC"),
        "market": StaticPartitionsDefinition(['ethereum_v3','polygon_v3']),
    }
)


market_day_hour_multipartition_mapping = MultiPartitionMapping(
    {   
        "date": DimensionPartitionMapping(
            dimension_name='time',
            partition_mapping=LastPartitionMapping()
        ),
        "market": DimensionPartitionMapping(
            dimension_name='market',
            partition_mapping=IdentityPartitionMapping()
        )
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

    return_val = pd.DataFrame([[partition_datetime, block_time, block_height]],
                        columns=['block_hour', 'block_time', 'block_height'])


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


@asset(
    partitions_def=market_hour_multipartition,
    compute_kind="python",
    #group_name='data_lake',
    code_version="1",
    io_manager_key = 'protocol_data_lake_io_manager',
    # freshness_policy=FreshnessPolicy(maximum_lag_minutes=6*60),
    # auto_materialize_policy=AutoMaterializePolicy.eager(), # not implemented with partition mapping
    ins={
        "block_numbers_by_hour": AssetIn(key_prefix="protocol_hourly_data_lake"),
        "market_tokens_by_day": AssetIn(key_prefix="financials_data_lake", partition_mapping=market_day_hour_multipartition_mapping),
    }
)
def protocol_data_by_hour(
    context,
    block_numbers_by_hour,
    market_tokens_by_day,
    # market_tokens_by_day_fallback
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

    market = context.partition_key.keys_by_dimension['market']
    time = context.partition_key.keys_by_dimension['time']
    # date, market = context.partition_key.split("|")
    partition_datetime = datetime.strptime(time, '%Y-%m-%d-%H:%M')
    context.log.info(f"market: {market}")
    context.log.info(f"time: {time}")

    # ic(block_numbers_by_hour)
    # ic(market_tokens_by_day)

    protocol_data = pd.DataFrame()

    if not market_tokens_by_day.empty:

        # multicall is not supported on Harmony until block 24185753.  Don't fetch data before this block.
        if (market == 'harmony_v3' and int(market_tokens_by_day.block_height.values[0]) < 24185753):
            protocol_data = pd.DataFrame()
        else:
        
            block_height = int(block_numbers_by_hour.block_height.values[0])
            chain = CONFIG_MARKETS[market]["chain"]
            
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
                            "block_hour": partition_datetime,
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

                if not reserve_data.empty:
                    # add the metadata
                    protocol_row = pd.concat([protocol_row, reserve_data], axis=1)
                    # add the row to the return value dataframe
                    protocol_data = pd.concat([protocol_data, protocol_row], axis=0)

            if not protocol_data.empty:
                # fix these values up here - more difficult to do in helper function
                protocol_data.debt_ceiling = protocol_data.debt_ceiling / 10 ** protocol_data.debt_ceiling_decimals
                protocol_data.debt_ceiling = protocol_data.debt_ceiling.astype(int)
                protocol_data.liquidation_protocol_fee = protocol_data.liquidation_protocol_fee / 10000
                protocol_data.liquidity_rate = protocol_data.liquidity_rate.astype(float)
                protocol_data.variable_borrow_rate = protocol_data.variable_borrow_rate.astype(float)
                protocol_data.stable_borrow_rate = protocol_data.stable_borrow_rate.astype(float)

            protocol_data = standardise_types(protocol_data)

    context.add_output_metadata(
        {
            "num_records": len(protocol_data),
            "preview": MetadataValue.md(protocol_data.head().to_markdown()),
        }
    )

    return protocol_data

@asset(
    partitions_def=market_hour_multipartition,
    compute_kind="python",
    code_version="1",
    io_manager_key = 'protocol_data_lake_io_manager'
)
def compound_v2_by_hour(
    context,
    block_numbers_by_hour,
    ) -> pd.DataFrame:
    """
    Table Compound V2 cTokens and APRs and Borrow/Supply data

    Args:
        context: dagster context object
        block_numbers_by_hour: the output of block_numbers_by_hour for a given market

    Returns:
        A dataframe market config & protocol data for each token in a market
    """

    market = context.partition_key.keys_by_dimension['market']
    time = context.partition_key.keys_by_dimension['time']
    
    # testing vals
    # market = 'ethereum_v3'
    # time = '2023-05-13-06:00'
    # block_height = 17249150

    partition_datetime = datetime.strptime(time, '%Y-%m-%d-%H:%M')
    context.log.info(f"market: {market}")
    context.log.info(f"time: {time}")

    ctoken_data = pd.DataFrame()

    if market in CONFIG_COMPOUND_v2.keys():
            
        block_height = int(block_numbers_by_hour.block_height.values[0])

        chain = CONFIG_MARKETS[market]["chain"]
        
        context.log.info(f"block_height: {block_height}")

        # set up web3 connection
        w3 = Web3(Web3.HTTPProvider(CONFIG_CHAINS[chain]['web3_rpc_url']))

        ctokens = CONFIG_COMPOUND_v2[market]

        def rate_to_apy(rate):
            # as per https://docs.compound.finance/v2/#protocol-math
            decimals = 18
            blocks_per_day = 60 / 12 * 60 * 24 
            days_per_year = 365
            return (((rate / 10 ** decimals * blocks_per_day + 1) ** days_per_year)) - 1
        
        
        for ctoken in ctokens.keys():
            # ic(ctoken)
            ctoken_address = ctokens[ctoken]['address']
            # ic(ctoken_address)
            ctoken_decimals = ctokens[ctoken]['ctoken_decimals']
            underlying_decimals = ctokens[ctoken]['underlying_decimals']
            

            # configure multicall call objects
            ctoken_calls = [
                Call(ctoken_address, ['supplyRatePerBlock()(uint256)'],[['supply_apy', rate_to_apy]]),
                Call(ctoken_address, ['borrowRatePerBlock()(uint256)'],[['borrow_apy', rate_to_apy]]),
                Call(ctoken_address, ['totalSupply()(uint256)'],[['total_supply', None]]),
                Call(ctoken_address, ['totalBorrows()(uint256)'],[['total_borrows', None]]),
                Call(ctoken_address, ['exchangeRateStored()(uint256)'],[['exchange_rate', None]]),
            ]

            # create the multicall object
            ctoken_multicall = Multicall(ctoken_calls, _w3 = w3, block_id = block_height)

            # execute the call
            ctoken_results = ctoken_multicall()
            
            exchange_rate = ctoken_results['exchange_rate'] / 10 ** (18 + underlying_decimals - ctoken_decimals)
            ctoken_deposits = ctoken_results['total_supply'] / 10 ** ctoken_decimals * exchange_rate
            ctoken_borrows = ctoken_results['total_borrows'] / 10 ** underlying_decimals
            data = [ctoken,
                 ctoken_address,
                 ctokens[ctoken]['underlying_symbol'],
                 ctokens[ctoken]['underlying_address'],
                 ctoken_results['supply_apy'],
                 ctoken_results['borrow_apy'],
                 ctoken_deposits,
                 ctoken_borrows,
                 ]
            ctoken_row = pd.DataFrame(
                data=[data],
                 columns=['symbol','address','underlying_symbol','underlying_address','supply_apy','borrow_apy','deposits','borrows'],
                 index=[0]
            )

            # ic(ctoken_row)
            ctoken_data = pd.concat([ctoken_data, ctoken_row], axis=0)

        # add the other metadata
        ctoken_data.insert(0, 'block_hour', partition_datetime)
        ctoken_data.insert(1, 'block_height', block_height)
        ctoken_data.insert(2, 'chain', chain)
        ctoken_data.insert(3, 'compound_version', 'compound_v2')
        
        ctoken_data = standardise_types(ctoken_data)

        # ic(ctoken_data)

    context.add_output_metadata(
        {
            "num_records": len(ctoken_data),
            "preview": MetadataValue.md(ctoken_data.head().to_markdown()),
        }
    )

    return ctoken_data


@asset(
    partitions_def=market_hour_multipartition,
    compute_kind="python",
    code_version="1",
    io_manager_key = 'protocol_data_lake_io_manager'
)
def compound_v3_by_hour(
    context,
    block_numbers_by_hour,
    ) -> pd.DataFrame:
    """
    Table Compound V3 cTokens and APRs and Borrow/Supply data

    Args:
        context: dagster context object
        block_numbers_by_hour: the output of block_numbers_by_hour for a given market

    Returns:
        A dataframe market config & protocol data for each token in a market
    """

    market = context.partition_key.keys_by_dimension['market']
    time = context.partition_key.keys_by_dimension['time']
    
    # testing vals
    # market = 'ethereum_v3'
    # time = '2023-05-13-06:00'
    # block_height = 17249150

    partition_datetime = datetime.strptime(time, '%Y-%m-%d-%H:%M')
    context.log.info(f"market: {market}")
    context.log.info(f"time: {time}")

    ctoken_data = pd.DataFrame()

    if market in CONFIG_COMPOUND_v3.keys():
            
        block_height = int(block_numbers_by_hour.block_height.values[0])

        chain = CONFIG_MARKETS[market]["chain"]
        
        context.log.info(f"block_height: {block_height}")

        # set up web3 connection
        w3 = Web3(Web3.HTTPProvider(CONFIG_CHAINS[chain]['web3_rpc_url']))

        ctokens = CONFIG_COMPOUND_v3[market]

        def rate_to_apy(rate):
            # as per https://docs.compound.finance/interest-rates/
            decimals = 18
            seconds_per_year = 60 * 60 * 24 * 365
            return rate / 10 ** decimals * seconds_per_year 
        
        def from_wei(val):
            return val / 10 ** 18
        
        
        for ctoken in ctokens.keys():
            # ic(ctoken)
            ctoken_address = ctokens[ctoken]['address']
            # ic(ctoken_address)
            ctoken_decimals = ctokens[ctoken]['ctoken_decimals']
            underlying_decimals = ctokens[ctoken]['underlying_decimals']
            
            # get utilisation rate - other calls are based on this:
            util_call = Call(ctoken_address, ['getUtilization()(uint256)'],[['utilisation_rate', None]], _w3 = w3, block_id = block_height)
            util_uint = util_call()['utilisation_rate']

            # ic(util_rate)

            # configure multicall call objects
            ctoken_calls = [
                Call(ctoken_address, ['getSupplyRate(uint256)(uint256)', util_uint],[['supply_apy', rate_to_apy]]),
                Call(ctoken_address, ['getBorrowRate(uint256)(uint256)', util_uint],[['borrow_apy', rate_to_apy]]),
                Call(ctoken_address, ['totalSupply()(uint256)'],[['total_supply', None]]),
                Call(ctoken_address, ['totalBorrow()(uint256)'],[['total_borrows', None]]),
            ]

            # create the multicall object
            ctoken_multicall = Multicall(ctoken_calls, _w3 = w3, block_id = block_height)

            # execute the call
            ctoken_results = ctoken_multicall()

            ctoken_deposits = ctoken_results['total_supply'] / 10 ** ctoken_decimals 
            ctoken_borrows = ctoken_results['total_borrows'] / 10 ** ctoken_decimals
            data = [ctoken,
                 ctoken_address,
                 ctokens[ctoken]['underlying_symbol'],
                 ctokens[ctoken]['underlying_address'],
                 ctoken_results['supply_apy'],
                 ctoken_results['borrow_apy'],
                 ctoken_deposits,
                 ctoken_borrows,
                 ]
            ctoken_row = pd.DataFrame(
                data=[data],
                 columns=['symbol','address','underlying_symbol','underlying_address','supply_apy','borrow_apy','deposits','borrows'],
                 index=[0]
            )

            # ic(ctoken_row)
            ctoken_data = pd.concat([ctoken_data, ctoken_row], axis=0)

        # add the other metadata
        ctoken_data.insert(0, 'block_hour', partition_datetime)
        ctoken_data.insert(1, 'block_height', block_height)
        ctoken_data.insert(2, 'chain', chain)
        ctoken_data.insert(3, 'compound_version', 'compound_v3')
        
        ctoken_data = standardise_types(ctoken_data)


    context.add_output_metadata(
        {
            "num_records": len(ctoken_data),
            "preview": MetadataValue.md(ctoken_data.head().to_markdown()),
        }
    )

    return ctoken_data


if __name__ == "__main__":

    compound_v3_by_hour()