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
                     DailyPartitionsDefinition, ExperimentalWarning,
                     MetadataValue, #MultiPartitionKey,
                     MultiPartitionsDefinition, #Optional, PartitionKeyRange,
                     #PartitionMapping, PartitionsDefinition,
                     StaticPartitionsDefinition, asset, #op,
                     #LastPartitionMapping,
                     IdentityPartitionMapping,
                     FreshnessPolicy,
                     MultiPartitionKey,
                     build_op_context,
                     MultiPartitionMapping,
                     DimensionPartitionMapping,
                     StaticPartitionMapping
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
    get_quote_from_paraswap,
    get_quote_from_paraswap_async,
)




from aave_data.assets.financials.data_lake import (
    market_day_multipartition,
    # block_numbers_by_day,
    # market_tokens_by_day,
    # aave_oracle_prices_by_day
)

INITIAL_RETRY = 0.01 #seconds
MAX_RETRIES = 10
# DAILY_PARTITION_START_DATE=datetime(2022,2,26,0,0,0, tzinfo=timezone.utc)
DAILY_PARTITION_START_DATE=datetime(2022,1,1,0,0,0, tzinfo=timezone.utc)

daily_partitions_def = DailyPartitionsDefinition(start_date=DAILY_PARTITION_START_DATE, end_offset=1, timezone="UTC")

daily_non_offset_partitions_def = DailyPartitionsDefinition(start_date=DAILY_PARTITION_START_DATE, timezone="UTC")

chain_day_multipartition = MultiPartitionsDefinition(
    {
        "date": DailyPartitionsDefinition(start_date='2023-04-06', end_offset=1, timezone="UTC"),
        "chain": StaticPartitionsDefinition(list(BALANCER_BPT_TOKENS.keys())),
    }
)

market_chain_multipartiton_mapping = MultiPartitionMapping(
    {   
        "date": DimensionPartitionMapping(
            dimension_name='date',
            partition_mapping=IdentityPartitionMapping()
        ),
        "market": DimensionPartitionMapping(
            dimension_name='chain',
            partition_mapping=StaticPartitionMapping(
                {
                'ethereum_v3': 'ethereum',
                'polygon_v3': 'polygon',
                # 'harmony_v3': 'harmony',
                # 'fantom_v3': 'fantom',
                # 'avax_v3': 'avalanche',
                'arbitrum_v3': 'arbitrum',
                'optimism_v3': 'optimism',
                }
            )
        )
    }
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
    context.log.info(f"market: {market}")
    context.log.info(f"date: {date}")
    
    protocol_data = pd.DataFrame()

    if not market_tokens_by_day.empty:

        # multicall is not supported on Harmony until block 24185753.  Don't fetch data before this block.
        if (market == 'harmony_v3' and int(market_tokens_by_day.block_height.values[0]) < 24185753):
            protocol_data = pd.DataFrame()
        else:
        
            block_height = int(market_tokens_by_day.block_height.values[0])
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
    partitions_def=market_day_multipartition,
    compute_kind="python",
    code_version="1",
    io_manager_key = 'protocol_data_lake_io_manager',
    ins={
        "block_numbers_by_day": AssetIn(key_prefix="financials_data_lake"),
    }
)            
def raw_incentives_by_day(
    context,
    block_numbers_by_day: pd.DataFrame,
) -> pd.DataFrame:
    """
    Calls UiIncentiveDataProviderV3 contracts at the specified block height
    and returns a dataframe of incentive configuration and state data.

    V3 and V2 share a common ABI

    V3 returns prices, V2 doesn't


    Args:
        context:
        block_numbers_by_day:

    Returns:
        dataframe of incentives data

    """
    date, market = context.partition_key.split("|")
    block_day = datetime.strptime(date, '%Y-%m-%d')
    chain = CONFIG_MARKETS[market]["chain"]
    block_height = int(block_numbers_by_day.end_block.values[0] + 1)

    #initialise Web3 and variables
    w3 = Web3(Web3.HTTPProvider(CONFIG_CHAINS[chain]['web3_rpc_url']))
    
    incentives_ui_provider = CONFIG_MARKETS[market]['incentives_ui_data_provider']
    pool_address_provider = CONFIG_MARKETS[market]['pool_address_provider']

    context.log.info(f"incentives_ui_provider: {incentives_ui_provider}")
    context.log.info(f"pool_address_provider: {pool_address_provider}")
    context.log.info(f"block_height: {block_height}")

    # Minimal ABI covering getReservesIncentivesData() only
    V3_ABI = [
        {
        "constant": "true",
        "inputs": [
            {
                "internalType": "contract IPoolAddressesProvider",
                "name": "provider",
                "type": "address"
            }
        ],
        "name": "getReservesIncentivesData",
        "outputs": [
            {
                "components": [
                    {
                        "internalType": "address",
                        "name": "underlyingAsset",
                        "type": "address"
                    },
                    {
                        "components": [
                            {
                                "internalType": "address",
                                "name": "tokenAddress",
                                "type": "address"
                            },
                            {
                                "internalType": "address",
                                "name": "incentiveControllerAddress",
                                "type": "address"
                            },
                            {
                                "components": [
                                    {
                                        "internalType": "string",
                                        "name": "rewardTokenSymbol",
                                        "type": "string"
                                    },
                                    {
                                        "internalType": "address",
                                        "name": "rewardTokenAddress",
                                        "type": "address"
                                    },
                                    {
                                        "internalType": "address",
                                        "name": "rewardOracleAddress",
                                        "type": "address"
                                    },
                                    {
                                        "internalType": "uint256",
                                        "name": "emissionPerSecond",
                                        "type": "uint256"
                                    },
                                    {
                                        "internalType": "uint256",
                                        "name": "incentivesLastUpdateTimestamp",
                                        "type": "uint256"
                                    },
                                    {
                                        "internalType": "uint256",
                                        "name": "tokenIncentivesIndex",
                                        "type": "uint256"
                                    },
                                    {
                                        "internalType": "uint256",
                                        "name": "emissionEndTimestamp",
                                        "type": "uint256"
                                    },
                                    {
                                        "internalType": "int256",
                                        "name": "rewardPriceFeed",
                                        "type": "int256"
                                    },
                                    {
                                        "internalType": "uint8",
                                        "name": "rewardTokenDecimals",
                                        "type": "uint8"
                                    },
                                    {
                                        "internalType": "uint8",
                                        "name": "precision",
                                        "type": "uint8"
                                    },
                                    {
                                        "internalType": "uint8",
                                        "name": "priceFeedDecimals",
                                        "type": "uint8"
                                    }
                                ],
                                "internalType": "struct IUiIncentiveDataProviderV3.RewardInfo[]",
                                "name": "rewardsTokenInformation",
                                "type": "tuple[]"
                            }
                        ],
                        "internalType": "struct IUiIncentiveDataProviderV3.IncentiveData",
                        "name": "aIncentiveData",
                        "type": "tuple"
                    },
                    {
                        "components": [
                            {
                                "internalType": "address",
                                "name": "tokenAddress",
                                "type": "address"
                            },
                            {
                                "internalType": "address",
                                "name": "incentiveControllerAddress",
                                "type": "address"
                            },
                            {
                                "components": [
                                    {
                                        "internalType": "string",
                                        "name": "rewardTokenSymbol",
                                        "type": "string"
                                    },
                                    {
                                        "internalType": "address",
                                        "name": "rewardTokenAddress",
                                        "type": "address"
                                    },
                                    {
                                        "internalType": "address",
                                        "name": "rewardOracleAddress",
                                        "type": "address"
                                    },
                                    {
                                        "internalType": "uint256",
                                        "name": "emissionPerSecond",
                                        "type": "uint256"
                                    },
                                    {
                                        "internalType": "uint256",
                                        "name": "incentivesLastUpdateTimestamp",
                                        "type": "uint256"
                                    },
                                    {
                                        "internalType": "uint256",
                                        "name": "tokenIncentivesIndex",
                                        "type": "uint256"
                                    },
                                    {
                                        "internalType": "uint256",
                                        "name": "emissionEndTimestamp",
                                        "type": "uint256"
                                    },
                                    {
                                        "internalType": "int256",
                                        "name": "rewardPriceFeed",
                                        "type": "int256"
                                    },
                                    {
                                        "internalType": "uint8",
                                        "name": "rewardTokenDecimals",
                                        "type": "uint8"
                                    },
                                    {
                                        "internalType": "uint8",
                                        "name": "precision",
                                        "type": "uint8"
                                    },
                                    {
                                        "internalType": "uint8",
                                        "name": "priceFeedDecimals",
                                        "type": "uint8"
                                    }
                                ],
                                "internalType": "struct IUiIncentiveDataProviderV3.RewardInfo[]",
                                "name": "rewardsTokenInformation",
                                "type": "tuple[]"
                            }
                        ],
                        "internalType": "struct IUiIncentiveDataProviderV3.IncentiveData",
                        "name": "vIncentiveData",
                        "type": "tuple"
                    },
                    {
                        "components": [
                            {
                                "internalType": "address",
                                "name": "tokenAddress",
                                "type": "address"
                            },
                            {
                                "internalType": "address",
                                "name": "incentiveControllerAddress",
                                "type": "address"
                            },
                            {
                                "components": [
                                    {
                                        "internalType": "string",
                                        "name": "rewardTokenSymbol",
                                        "type": "string"
                                    },
                                    {
                                        "internalType": "address",
                                        "name": "rewardTokenAddress",
                                        "type": "address"
                                    },
                                    {
                                        "internalType": "address",
                                        "name": "rewardOracleAddress",
                                        "type": "address"
                                    },
                                    {
                                        "internalType": "uint256",
                                        "name": "emissionPerSecond",
                                        "type": "uint256"
                                    },
                                    {
                                        "internalType": "uint256",
                                        "name": "incentivesLastUpdateTimestamp",
                                        "type": "uint256"
                                    },
                                    {
                                        "internalType": "uint256",
                                        "name": "tokenIncentivesIndex",
                                        "type": "uint256"
                                    },
                                    {
                                        "internalType": "uint256",
                                        "name": "emissionEndTimestamp",
                                        "type": "uint256"
                                    },
                                    {
                                        "internalType": "int256",
                                        "name": "rewardPriceFeed",
                                        "type": "int256"
                                    },
                                    {
                                        "internalType": "uint8",
                                        "name": "rewardTokenDecimals",
                                        "type": "uint8"
                                    },
                                    {
                                        "internalType": "uint8",
                                        "name": "precision",
                                        "type": "uint8"
                                    },
                                    {
                                        "internalType": "uint8",
                                        "name": "priceFeedDecimals",
                                        "type": "uint8"
                                    }
                                ],
                                "internalType": "struct IUiIncentiveDataProviderV3.RewardInfo[]",
                                "name": "rewardsTokenInformation",
                                "type": "tuple[]"
                            }
                        ],
                        "internalType": "struct IUiIncentiveDataProviderV3.IncentiveData",
                        "name": "sIncentiveData",
                        "type": "tuple"
                    }
                ],
                "internalType": "struct IUiIncentiveDataProviderV3.AggregatedReserveIncentiveData[]",
                "name": "",
                "type": "tuple[]"
            }
        ],
        "stateMutability": "view",
        "type": "function"
    },
    ]
       
    all_rewards = pd.DataFrame()

    if incentives_ui_provider is not None:
        incentives_ui_provider_contract = w3.eth.contract(address=Web3.to_checksum_address(incentives_ui_provider), abi=V3_ABI)
        # contract_return = incentives_ui_provider_contract.functions.getReservesIncentivesData(
        #     Web3.to_checksum_address(pool_address_provider)).call(block_identifier=int(block_height))

        # exponential backoff retries on the function call to deal with transient RPC errors
        if block_height > 0:
            i = 0
            delay = INITIAL_RETRY
            while True:
                try:
                    contract_return = incentives_ui_provider_contract.functions.getReservesIncentivesData(
                        Web3.to_checksum_address(pool_address_provider)).call(block_identifier=int(block_height))
                    break
                # except InsufficientDataBytes as id:
                except BadFunctionCallOutput as b:
                    # This excepts when incentives haven't yet been deployed for the pool
                    contract_return = None
                    break
                except Exception as e:
                    i += 1
                    if i > MAX_RETRIES:
                        raise ValueError(f"RPC error count {i}, last error {str(e)}.  Bailing out.")
                    rand_delay = randint(0, 250) / 1000
                    sleep(delay + rand_delay)
                    delay *= 2
                    print(f"Request Error {str(e)}, retry count {i}")

        if contract_return is not None:
            # process the contract return data (list of tuples) into a dataframe
            raw_rewards = pd.DataFrame(contract_return, columns=['underlying_asset', 'a_incentive_data', 'v_incentive_data', 's_incentive_data'])

            token_cols = ['a_incentive_data', 'v_incentive_data', 's_incentive_data']
            token_types = ['atoken','vtoken','stoken']

            for col, token_type in zip(token_cols, token_types):

                atoken_rewards = pd.DataFrame(raw_rewards[col].tolist(), index=raw_rewards.index, columns=[
                                            'token_address', 'incentive_controller_address', 'rewards_token_information'])
                atoken_rewards = atoken_rewards.explode(column='rewards_token_information').dropna()

                dfa_rewards = pd.DataFrame(atoken_rewards.rewards_token_information.tolist(), index=atoken_rewards.index, columns = [
                                                    'reward_token_symbol',
                                                    'reward_token_address',
                                                    'reward_oracle_address',
                                                    'emission_per_second',
                                                    'incentives_last_update_timestamp',
                                                    'token_incentives_index',
                                                    'emission_end_timestamp',
                                                    'reward_price_feed',
                                                    'reward_token_decimals',
                                                    'precision',
                                                    'price_feed_decimals'])

                atoken_rewards = atoken_rewards[['token_address','incentive_controller_address']].join(dfa_rewards).drop_duplicates()
                atoken_rewards['token_type'] = token_type

                atoken_rewards = raw_rewards[['underlying_asset']].join(atoken_rewards).dropna()
                
                all_rewards = pd.concat([all_rewards, atoken_rewards])

            # force big cols to float to deal with bigints
            for col in ['emission_per_second',
                        'token_incentives_index',
                        'reward_price_feed']:
                all_rewards[col] = all_rewards[col].astype(float)
            
            # force other numerics to int
            for col in ['incentives_last_update_timestamp',
                        'emission_end_timestamp',
                        'reward_token_decimals',
                        'precision',
                        'price_feed_decimals']:
                all_rewards[col] = all_rewards[col].astype(int)

            all_rewards.insert(0, 'block_day', block_day)
            all_rewards.insert(1, 'block_height', block_height)
            all_rewards.insert(2, 'market', market)

    all_rewards = standardise_types(all_rewards)

    context.add_output_metadata(
        {
            "num_records": len(all_rewards),
            "preview": MetadataValue.md(all_rewards.head().to_markdown()),
        }
    )

    return all_rewards.reset_index(drop=True)


@asset(
    partitions_def=market_day_multipartition,
    compute_kind="python",
    code_version="1",
    io_manager_key = 'protocol_data_lake_io_manager',
    ins={
        "protocol_data_by_day": AssetIn(key_prefix="protocol_data_lake"),
    }
)            
def emode_config_by_day(
    context,
    protocol_data_by_day: pd.DataFrame,
) -> pd.DataFrame:
    """
    Calls Pool contracts at the specified block height
    and returns a dataframe of emode data.

    Specific to V3 only.  V1/V2 return empty

    Args:
        context:
        block_numbers_by_day:

    Returns:
        dataframe of incentives data

    """
    date, market = context.partition_key.split("|")
    block_day = datetime.strptime(date, '%Y-%m-%d')
    chain = CONFIG_MARKETS[market]["chain"]
    
    try:
        block_height = int(protocol_data_by_day.block_height.values[0])
    except IndexError:
        # protocol not deployed on this day
        context.add_output_metadata(
            {
                "num_records": 0,
            }
        )
        return pd.DataFrame()

    emode_output = pd.DataFrame()

    if CONFIG_MARKETS[market]['version'] == 3:
        
        lending_pool = CONFIG_MARKETS[market]['pool']

        context.log.info(f"pool: {lending_pool}")
        context.log.info(f"block_height: {block_height}")
        #initialise Web3 and variables
        w3 = Web3(Web3.HTTPProvider(CONFIG_CHAINS[chain]['web3_rpc_url']))

        def emode_handler(value):
            return {
                'emode_ltv': value[0] / 10000,
                'emode_liquidation_threshold': value[1] / 10000,
                'emode_liquidation_bonus': value[2] / 10000,
                'emode_price_address': value[3],
                'emode_category_name': value[4],
            }

        emodes = protocol_data_by_day.query('reserve_emode_category > 0').reserve_emode_category.unique()

        for emode in emodes:
            emode_call = Call(
                lending_pool,
                ['getEModeCategoryData(uint8)((uint16,uint16,uint16,address,string))', int(emode)],
                [['emode_data', emode_handler]],
                _w3 = w3,
                block_id = block_height
            )
            
            # exponential backoff retries on the function call to deal with transient RPC errors
            i = 0
            delay = INITIAL_RETRY
            while True:
                try:
                    call_output = emode_call()
                    break
                except Exception as e:
                    i += 1
                    if i > MAX_RETRIES:
                        raise ValueError(f"RPC error count {i}, last error {str(e)}.  Bailing out.")
                    rand_delay = randint(0, 250) / 1000
                    sleep(delay + rand_delay)
                    delay *= 2
                    print(f"Request Error {str(e)}, retry count {i}")

            call_output['emode_data']['reserve_emode_category'] = int(emode)
            ouput_row = pd.DataFrame(call_output['emode_data'], index=[0])
            emode_output = pd.concat([emode_output, ouput_row])

        if not emode_output.empty:      
            emode_output['block_day'] = block_day
            emode_output['block_height'] = block_height
            emode_output['market'] = market
            emode_output = emode_output[
                [
                    'block_day',
                    'block_height',
                    'market',
                    'reserve_emode_category',
                    'emode_category_name',
                    'emode_ltv',
                    'emode_liquidation_threshold',
                    'emode_liquidation_bonus',
                    'emode_price_address',
                ]
            ].reset_index(drop=True)
            emode_output = standardise_types(emode_output)
        else:
            emode_output = pd.DataFrame()

    context.add_output_metadata(
        {
            "num_records": len(emode_output),
            "preview": MetadataValue.md(emode_output.head().to_markdown()),
        }
    )

    return emode_output



@asset(
    # partitions_def=DailyPartitionsDefinition(start_date=DAILY_PARTITION_START_DATE),
    # partitions_def=DailyPartitionsDefinition(start_date='2022-02-26', end_offset=1),
    partitions_def=daily_partitions_def,
    compute_kind="python",
    code_version="1",
    io_manager_key = 'protocol_data_lake_io_manager',
    ins={
        "blocks_by_day": AssetIn(key_prefix="warehouse"),
    }
)
def matic_lsd_token_supply_by_day(
    context,
    blocks_by_day: pd.DataFrame,
) -> pd.DataFrame:
    """
    Table of the totalsupply of the tokens listed below

    To add a new token to this table either:
    - add it to the TOKENS dict below and backfill the entire table
    - make a new table with the new token and backfill it (saves re-getting existing data)

    Args:
        context: dagster context object
        blocks_by_day: warehouse tables of block numbers by day by chain

    Returns:
        A dataframe of the totalsupply of the tokens listed below

    """
    date = context.partition_key
    block_day = datetime.strptime(date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    prev_block_day = block_day - timedelta(days=1)
    context.log.info(f"date: {date}")
    
    TOKENS = {
        "polygon": {
                "stMATIC": {
                    "address": "0x3a58a54c066fdc0f2d55fc9c89f0415c92ebf3c4",
                    "decimals": 18,
                },
                "MaticX": {
                    "address": "0xfa68fb4628dff1028cfec22b4162fccd0d45efb6",
                    "decimals": 18,
                },
        },
        "ethereum": {
                "stMATIC": {
                        "address": "0x9ee91f9f426fa633d227f7a9b000e28b9dfd8599",
                        "decimals": 18,
                },
                "MaticX": {
                        "address": "0xf03a7eb46d01d9ecaa104558c732cf82f6b6b645",
                        "decimals": 18,
                },

        }
    }
    supply_data = pd.DataFrame()

    for chain in TOKENS:
        context.log.info(f"chain: {chain}")
        block_height = int(blocks_by_day.loc[(blocks_by_day.chain == chain) & (blocks_by_day.block_day == prev_block_day)].end_block.values[0]+1)
        context.log.info(f"block_height: {block_height}")

        # setup the Web3 connection
        w3 = Web3(Web3.HTTPProvider(CONFIG_CHAINS[chain]['web3_rpc_url']))

        # grab the token addresses in an iterable
        addresses = [TOKENS[chain][token]['address'] for token in TOKENS[chain]]

        # set up the multiple call objects (one for each address)
        chain_calls = [Call(address, ['totalSupply()((uint256))'], [[address, None]]) for address in addresses]

        # configure the mulitcall object
        chain_multi = Multicall(chain_calls, _w3 = w3, block_id = block_height)

        # exponential backoff retries on the function call to deal with transient RPC errors
        i = 0
        delay = INITIAL_RETRY
        while True:
            try:
                chain_output = chain_multi()
                break
            except Exception as e:
                i += 1
                if i > MAX_RETRIES:
                    raise ValueError(f"RPC error count {i}, last error {str(e)}.  Bailing out.")
                rand_delay = randint(0, 250) / 1000
                sleep(delay + rand_delay)
                delay *= 2
                print(f"Request Error {str(e)}, retry count {i}")
        
        chain_data = pd.DataFrame(chain_output).T.reset_index()
        chain_data.columns = ['address', 'total_supply']
        chain_data['chain'] = chain
        chain_data['block_height'] = block_height
        chain_data['block_day'] = block_day
        
        # chain_data['symbol'] = chain_data.address.map({TOKENS[chain][token]['address']: token for token in TOKENS[chain]})
        chain_data['symbol'] = pd.Series(TOKENS[chain].keys())
        chain_data['decimals'] = pd.Series([TOKENS[chain][token]['decimals'] for token in TOKENS[chain]])
        chain_data.total_supply = chain_data.total_supply.astype(float) / 10**chain_data.decimals
        chain_data = chain_data[['block_day', 'block_height', 'chain', 'address', 'symbol', 'decimals', 'total_supply']]
        
        supply_data = pd.concat([supply_data, chain_data]).reset_index(drop=True)
    
    supply_data.total_supply = supply_data.total_supply.fillna(0)
    supply_data = standardise_types(supply_data)

    context.add_output_metadata(
        {
            "num_records": len(supply_data),
            "preview": MetadataValue.md(supply_data.head().to_markdown()),
        }
    )

    return supply_data


@asset(
    compute_kind="python",
    code_version="1",
    io_manager_key = 'protocol_data_lake_append_io_manager',
)
def liquidity_depth_raw(context):
    """
    Uses the Paraswap to get the liquidity depth of the configured
    tokens with respect to the to_asset
    
    Used for risk management in looped lsd assets

    This asset is not partitioned and appends to the existing table when run.
    This asset is not idempotent as the 1inch data is ephemeral

    Uses a non-idempoent IO manager to write to the warehouse
    
    Args:
        context: dagster context object
    
    Returns:
        A dataframe of the liquidity depth of the tokens at the time the function is called
    """
    CONCURRENT_REQUESTS = 20
    
    # construct the ouput dataframe
    rows = []
    for market_key, market_data in CONFIG_1INCH.items():
        for from_asset_key, from_asset_data in market_data["from_assets"].items():
            row = {
                "market_key": market_key,
                "market": market_data["market"],
                "chain": market_data["chain"],
                "loop_market": market_data["loop_market"],
                "to_asset": list(market_data["to_asset"].keys())[0],
                "to_asset_address": market_data["to_asset"][list(market_data["to_asset"].keys())[0]]["address"],
                "to_asset_decimals": market_data["to_asset"][list(market_data["to_asset"].keys())[0]]["decimals"],
                "from_asset": from_asset_key,
                "from_asset_address": from_asset_data["address"],
                "from_asset_decimals": from_asset_data["decimals"]
            }
            rows.append(row)
    output = pd.DataFrame(rows)

    # get the from asset chain & assets & grab the oracle price
    from_assets = output[["from_asset", "from_asset_address", "from_asset_decimals", "chain", "market"]].drop_duplicates().reset_index(drop=True)
    from_assets['from_asset_price'] = from_assets.apply(lambda x: get_aave_oracle_price(x.market, x.from_asset_address), axis=1)

    # get the to asset chain & assets & grab the oracle price
    to_assets = output[["to_asset", "to_asset_address", "to_asset_decimals", "chain", "market"]].drop_duplicates().reset_index(drop=True)
    to_assets['to_asset_price'] = to_assets.apply(lambda x: get_aave_oracle_price(x.market, x.to_asset_address), axis=1)

    # join back into the output
    output = output.merge(from_assets, on=["from_asset", "from_asset_address", "from_asset_decimals", "chain", "market"], how="left")
    output = output.merge(to_assets, on=["to_asset", "to_asset_address", "to_asset_decimals", "chain", "market"], how="left")

    # get the chain_ids for use inth 1inch API
    chain_ids = {chain: CONFIG_CHAINS[chain]["chain_id"] for chain in output.chain.unique()}
    output["chain_id"] = output.chain.map(chain_ids)

    # build the list of from amounts to sweep
    sweep_range = [10**i for i in range(3, 10)]
    output['from_amount_usd'] = output.apply(lambda x: sweep_range, axis=1)
    output = output.explode("from_amount_usd").reset_index(drop=True)
    output["from_amount_usd"] = output.from_amount_usd.astype(float)

    # convert to native asset amounts
    output["from_amount_native"] = output.from_amount_usd / output.from_asset_price

    ################################################################
    # 1inch API calls using synchronous requests
    ################################################################
    # import time
    # start = time.time()
    # # run the first sweep sync
    # output["to_amount_native"] = output.apply(
    #     lambda x: get_quote_from_1inch(
    #             x.chain_id,
    #             x.from_asset_address,
    #             x.from_asset_decimals,
    #             x.to_asset_address,
    #             x.to_asset_decimals,
    #             x.from_amount_native
    #     ), axis=1)
    # end = time.time()
    # elapsed = end - start
    # ic(elapsed)
    ################################################################
    
    ################################################################
    # 1inch API calls using async requests
    ################################################################
    # run the first sweep async
    async def sweep():
        semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
        tasks = output.apply(
            lambda x: get_quote_from_paraswap_async(
                    x.chain_id,
                    x.from_asset_address,
                    x.from_asset_decimals,
                    x.to_asset_address,
                    x.to_asset_decimals,
                    x.from_amount_native,
                    semaphore
            ), axis=1)
        results = await asyncio.gather(*tasks)
        return results
    
    # import time
    # start = time.time()
    output["to_amount_native"] = asyncio.run(sweep())
    # end = time.time()
    # elapsed = end - start
    # ic(elapsed)
    ################################################################
    
    output['to_amount_usd'] = output.to_amount_native * output.to_asset_price
    output['price_impact'] = 1 - (output.to_amount_usd / output.from_amount_usd)

    # ic(from_amounts_usd)
    # ic(output)
    # output.to_pickle("output.pkl")
    # output = pd.read_pickle("output.pkl")
    detail_sweep = output.copy()
    

    detail_sweep['dist_from_1'] = 0.01 - detail_sweep.price_impact
    detail_sweep['dist_from_5'] = detail_sweep.price_impact - 0.05

    # filter for lowest non-negative distance values for the 1% price impact
    detail_sweep_low = detail_sweep.loc[(detail_sweep.dist_from_1 >= 0)]
    detail_sweep_low_mins = (detail_sweep_low
                                .groupby(["market_key", "from_asset", "to_asset"])
                                .agg({"dist_from_1": "min"})
                                .rename(columns={"dist_from_1": "dist_from_1_min",})
                                .reset_index()
        )
    # join the mins back in and filter for the lowest distance values
    detail_sweep_low = detail_sweep_low.merge(detail_sweep_low_mins, how="left")
    detail_sweep_low = detail_sweep_low.loc[(detail_sweep_low.dist_from_1 == detail_sweep_low.dist_from_1_min)]
    detail_sweep_low.drop(columns=['to_amount_native','to_amount_usd','price_impact','from_amount_native','dist_from_1', 'dist_from_5', 'dist_from_1_min'], inplace=True)
    detail_sweep_low = detail_sweep_low.rename(columns={"from_amount_usd": "from_amount_usd_low",})
    
    # filter for lowest non-negative distance values for the 5% price impact
    detail_sweep_high = detail_sweep.loc[(detail_sweep.dist_from_5 >= 0)]
    detail_sweep_high_mins = (detail_sweep_high
                                .groupby(["market_key", "from_asset", "to_asset"])
                                .agg({"dist_from_5": "min"})
                                .rename(columns={"dist_from_5": "dist_from_5_min",})
                                .reset_index()
        )
    # join the mins back in and filter for the lowest distance values
    detail_sweep_high = detail_sweep_high.merge(detail_sweep_high_mins, how="left")
    detail_sweep_high = detail_sweep_high.loc[(detail_sweep_high.dist_from_5 == detail_sweep_high.dist_from_5_min)]
    detail_sweep_high.drop(columns=['to_amount_native','to_amount_usd','from_amount_native','price_impact','dist_from_1', 'dist_from_5','dist_from_5_min'], inplace=True)
    detail_sweep_high = detail_sweep_high.rename(columns={"from_amount_usd": "from_amount_usd_high",})

    # join the two dfs together
    detail_sweep = detail_sweep_low.merge(detail_sweep_high, how="left")
    
    # generate the sweep range (non-inclusive)
    SWEEP_STEPS = 20
    
    detail_sweep['sweep_range'] = detail_sweep.apply(lambda x: np.linspace(x.from_amount_usd_low, x.from_amount_usd_high, SWEEP_STEPS, endpoint=False), axis=1)
    detail_sweep = detail_sweep.explode('sweep_range').reset_index(drop=True)
    # don't sweep the values we already have
    detail_sweep = detail_sweep.loc[detail_sweep.sweep_range != detail_sweep.from_amount_usd_low]

    detail_sweep.rename(columns={"sweep_range": "from_amount_usd"}, inplace=True)
    detail_sweep.drop(columns=['from_amount_usd_low', 'from_amount_usd_high'], inplace=True)
    detail_sweep['from_amount_native'] = detail_sweep.from_amount_usd / detail_sweep.from_asset_price

    # detail_sweep.to_csv("detail_sweep.csv", index=False)

    ################################################################
    # 1inch API calls using async requests
    ################################################################
    # run the sweep async
    async def sweep():
        semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
        tasks = detail_sweep.apply(
            lambda x: get_quote_from_paraswap_async(
                    x.chain_id,
                    x.from_asset_address,
                    x.from_asset_decimals,
                    x.to_asset_address,
                    x.to_asset_decimals,
                    x.from_amount_native,
                    semaphore
            ), axis=1)
        results = await asyncio.gather(*tasks)
        return results
    
    # import time
    # start = time.time()
    detail_sweep["to_amount_native"] = asyncio.run(sweep())
    # end = time.time()
    # elapsed = end - start
    # ic(elapsed)
    ################################################################
    
    # calc the results
    detail_sweep['to_amount_usd'] = detail_sweep.to_amount_native * detail_sweep.to_asset_price
    detail_sweep['price_impact'] = 1 - (detail_sweep.to_amount_usd / detail_sweep.from_amount_usd)

    # ic(detail_sweep)

    # join back with the 1st sweep
    output = pd.concat([output, detail_sweep], axis=0)

    # tidy up
    output = output.sort_values(by=['market_key', 'from_asset', 'to_asset', 'from_amount_usd']).reset_index(drop=True)
    output['fetch_time'] = datetime.now(timezone.utc)

    # output.to_csv("detail_sweep.csv", index=False)
    # output.to_pickle("output.pkl")
    # output = pd.read_pickle("output.pkl")
    context.add_output_metadata(
        {
            "num_records": len(output),
        }
    )

    # ic(output)    
    # fix types
    for col in ['from_amount_usd','from_amount_native','to_amount_native','to_amount_usd','price_impact','from_asset_price','to_asset_price']:
        output[col] = output[col].astype('Float64')
    # fix types
    for col in ['to_asset_decimals','from_asset_decimals','chain_id']:
        output[col] = output[col].astype('Int64')

    output = standardise_types(output)

    return output

@asset(
    partitions_def=chain_day_multipartition,
    compute_kind="python",
    code_version="1",
    io_manager_key = 'protocol_data_lake_io_manager',
    ins={
        "block_numbers_by_day": AssetIn(key_prefix="financials_data_lake", partition_mapping=market_chain_multipartiton_mapping)
    }
)
def balancer_bpt_data_by_day(
    context,
    block_numbers_by_day: pd.DataFrame,
) -> pd.DataFrame:
    """
    Table of the each token in a market with configuration and protocol data
    Data is retrieved on-chain using the Aave Protocol Data Provider (or equivalent)

    Requires access to the previous d

    Args:
        context: dagster context object
        market_tokens_by_day: the output of market_tokens_by_day for a given market

    Returns:
        A dataframe market config & protocol data for each token in a market

    """
    context.log.info(f"{context.partition_key}")
    # chain, date = context.partition_key.split("|")
    chain = context.partition_key.keys_by_dimension['chain']
    date = context.partition_key.keys_by_dimension['date']
    block_day = datetime.strptime(date, '%Y-%m-%d')
    context.log.info(f"market: {chain}")
    context.log.info(f"date: {date}")

    # get the blocks for the day
    block_height = int(block_numbers_by_day.end_block.values[0] + 1)

    bal_bpt_data = pd.DataFrame()
    for bpt in BALANCER_BPT_TOKENS[chain]:
        bpt_data = get_balancer_bpt_data(chain, bpt['pool'], bpt['decimals'], block_height)
        if bpt_data['rate'] is not None:
            bpt_row = pd.DataFrame(bpt, index=[0])
            bpt_data_df = pd.DataFrame(bpt_data, index=[0])
            bpt_row = pd.concat([bpt_row, bpt_data_df], axis=1)
            bal_bpt_data = pd.concat([bal_bpt_data, bpt_row], axis=0).reset_index(drop=True)
    
    if not bal_bpt_data.empty:
        bal_bpt_data['block_day'] = block_day
        bal_bpt_data['block_height'] = block_height
        bal_bpt_data['chain'] = chain
        bal_bpt_data = standardise_types(bal_bpt_data)
    
    context.add_output_metadata(
        {
            "num_records": len(bal_bpt_data),
            "preview": MetadataValue.md(bal_bpt_data.head().to_markdown()),
        }
    )

    return bal_bpt_data

# DAILY_PARTITION_START_DATE = '2022-02-26',


@asset(
    partitions_def=daily_partitions_def,
    compute_kind="python",
    code_version="1",
    io_manager_key = 'protocol_data_lake_io_manager',
    ins={
        "blocks_by_day": AssetIn(key_prefix="warehouse"),
    }
)
def safety_module_rpc(
    context,
    blocks_by_day: pd.DataFrame,
)-> pd.DataFrame:
    """
    Gets Safety Module token data from RPC sources for a day
    and stores in a dataframe

    Args:
        context: dagster context object
        blocks_by_day: the output of blocks_by_day 
    
    Returns:
        A dataframe of Safety Module token data for a day

    """
    date = context.partition_key
    block_day = datetime.strptime(date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    prev_block_day = block_day - timedelta(days=1)
    context.log.info(f"date: {date}")
    chain = "ethereum"
    context.log.info(f"chain: {chain}")
    block_height = int(blocks_by_day.loc[(blocks_by_day.chain == chain) & (blocks_by_day.block_day == prev_block_day)].end_block.values[0] + 1)
    context.log.info(f"block_height: {block_height}")

    # block_height = 17072018
    # partition_datetime = datetime(2023, 4, 16, 0, 0, 0, tzinfo=timezone.utc)

    # setup the Web3 connection
    w3 = Web3(Web3.HTTPProvider(CONFIG_CHAINS[chain]['web3_rpc_url']))

    return_val = pd.DataFrame()
    for sm_token in CONFIG_SM_TOKENS.keys():
        
        stoken = CONFIG_SM_TOKENS[sm_token]['stk_token_address']
        token = CONFIG_SM_TOKENS[sm_token]['unstaked_token_address']
        reward_token = CONFIG_SM_TOKENS[sm_token]['reward_token_address']
        decimals = CONFIG_SM_TOKENS[sm_token]['decimals']

        # multicast return handlers are redefined due to decimals
        def _convert_to_float(x):
            return x / 10 ** decimals
        
        def _assets_handler(x):
            return {
                'emission_per_second' : _convert_to_float(x[0]),
                'emission_per_day' : _convert_to_float(x[0]) * 86400,
                'last_update_timestamp' : datetime.utcfromtimestamp(x[1]),
                'index' : _convert_to_float(x[2]),
            }

        # set up the multiple call objects (one for each address)
        calls = [
            Call(stoken, ['totalSupply()(uint256)'], [['stk_token_supply', _convert_to_float]]),
            Call(token, ['totalSupply()(uint256)'], [['unstaked_token_supply', _convert_to_float]]),
            Call(stoken, ['assets(address)((uint128,uint128,uint256))', stoken], [['asset_config', _assets_handler]]),
        ]

        # configure the multicall object
        if block_height is None:
            multi = Multicall(calls, _w3 = w3)
        else:
            multi = Multicall(calls, _w3 = w3, block_id = block_height)

        # exponential backoff retries on the function call to deal with transient RPC errors
        i = 0
        delay = INITIAL_RETRY
        while True:
            try:
                multi_output = multi()
                break
            except Exception as e:
                i += 1
                if i > MAX_RETRIES:
                    raise ValueError(f"RPC error count {i}, last error {str(e)}.  Bailing out.")
                rand_delay = randint(0, 250) / 1000
                sleep(delay + rand_delay)
                delay *= 2
                print(f"Request Error {str(e)}, retry count {i}")

        # add the metadata to the output dataframe
        if multi_output is not None:
            output_row = pd.DataFrame(multi_output['asset_config'], index=[0])
            output_row['stk_token_supply'] = multi_output['stk_token_supply']
            output_row['unstaked_token_supply'] = multi_output['unstaked_token_supply']
            output_row['block_day'] = block_day
            output_row['block_height'] = block_height
            output_row['stk_token_address'] = stoken
            output_row['stk_token_symbol'] = CONFIG_SM_TOKENS[sm_token]['stk_token_symbol']
            output_row['unstaked_token_address'] = token
            output_row['unstaked_token_symbol'] = CONFIG_SM_TOKENS[sm_token]['unstaked_token_symbol']
            output_row['reward_token_address'] = reward_token
            output_row['reward_token_symbol'] = CONFIG_SM_TOKENS[sm_token]['reward_token_symbol']

            output_row.drop(columns=['index'], inplace=True)
        else:
            output_row = pd.DataFrame()
        
        return_val = pd.concat([return_val, output_row], axis=0).reset_index(drop=True)
        return_val = standardise_types(return_val)

    context.add_output_metadata(
        {
            "num_records": len(return_val),
            "preview": MetadataValue.md(return_val.head().to_markdown()),
        }
    )
    return return_val

@asset(
    compute_kind="python",
    code_version="1",
    io_manager_key = 'protocol_data_lake_io_manager',
)
def coingecko_data_by_day(context):
    """
    Gets the price data from coingecko for the tokens in 
    the COINGECKO_TOKENS config object
    
    Gets all data from the given start_date through to the current date

    Args:
        context: dagster context object

    Returns:
        A dataframe of price data for the tokens in COINGECKO_TOKENS

    """

    # get the current date
    today = datetime.now(tz=timezone.utc).date()
    today_dt = datetime.combine(today, datetime.min.time()).replace(tzinfo=timezone.utc)
    end_date = int(today_dt.timestamp())


    # get the price data for each token
    return_val = pd.DataFrame()
    for token in COINGECKO_TOKENS.keys():
        start_date = int(datetime.strptime(COINGECKO_TOKENS[token]['start_date'], '%Y-%m-%d').replace(tzinfo=timezone.utc).timestamp())
        cg_id = COINGECKO_TOKENS[token]['cg_id']

        cg_url = f"https://api.coingecko.com/api/v3/coins/{cg_id}/market_chart/range?vs_currency=usd&from={start_date}&to={end_date}"

        # exponential backoff retries on the function call to deal with transient RPC errors
        i = 0
        delay = INITIAL_RETRY
        while True:
            try:
                cg_return = requests.get(cg_url, timeout=300)
                break
            except Exception as e:
                i += 1
                if i > MAX_RETRIES:
                    raise ValueError(f"RPC error count {i}, last error {str(e)}.  Bailing out.")
                rand_delay = randint(0, 250) / 1000
                sleep(delay + rand_delay)
                delay *= 2
                print(f"Request Error {str(e)}, retry count {i}")

        cg_prices = pd.DataFrame(cg_return.json()['prices'], columns=['block_day', 'price_usd'])
        cg_prices.block_day = pd.to_datetime(cg_prices.block_day, unit = 'ms')
        cg_prices['symbol'] = token
        cg_prices['cg_id'] = cg_id
        cg_prices['address'] = COINGECKO_TOKENS[token]['address']
        cg_prices['chain'] = COINGECKO_TOKENS[token]['chain']
        cg_prices['decimals'] = COINGECKO_TOKENS[token]['decimals']

        return_val = pd.concat([return_val, cg_prices], axis=0).reset_index(drop=True)

    return_val = standardise_types(return_val)

    context.add_output_metadata(
        {
            "num_records": len(return_val),
            "preview": MetadataValue.md(return_val.head().to_markdown()),
        }
    )

    return return_val


@asset(
    partitions_def=daily_partitions_def,
    compute_kind="python",
    code_version="1",
    io_manager_key = 'protocol_data_lake_io_manager',
)
def beacon_chain_staking_returns_by_day(
        context
)-> pd.DataFrame:
    """
    Table of the eth reference staking returns by day
    
    Obtained from the Eth STORE (Ether Staking Offered Rate) from 
    https://beaconcha.in/ethstore

    This job needs to fire after 1200 UTC to ensure the data is available

    Args:
        context: dagster context object

    Returns:
        A dataframe of the eth reference staking returns by day 

    Raises:
        ValueError: If the API doesn't return current data

    """
    date = context.partition_key

    # test data
    # date = '2023-05-13'

    beacon_chain_start_date = datetime.strptime('2020-12-01', '%Y-%m-%d').date()
    partition_date = datetime.strptime(date, '%Y-%m-%d').date()

    beaconchain_day = (partition_date - beacon_chain_start_date).days - 1
    ic(beaconchain_day)

    api_url = f'https://beaconcha.in/api/v1/ethstore/{int(beaconchain_day)}'

     # exponential backoff retries on the function call to deal with transient RPC errors
    i = 0
    delay = INITIAL_RETRY
    while True:
        try:
            api_return = requests.get(api_url, timeout=300)
            break
        except Exception as e:
            i += 1
            if i > MAX_RETRIES:
                raise ValueError(f"RPC error count {i}, last error {str(e)}.  Bailing out.")
            rand_delay = randint(0, 250) / 1000
            sleep(delay + rand_delay)
            delay *= 2
            print(f"Request Error {str(e)}, retry count {i}")
    
    store_data = api_return.json()['data']
    
    if not store_data:
        raise ValueError(f"API data for {date} is empty")
    
    return_val = pd.DataFrame(store_data, index=[0])

    return_val = return_val[
        [
            'day',
            'day_start',
            'day_end',
            'apr',
            'cl_apr',
            'el_apr'
        ]
    ]
    return_val.insert(0, 'partition_date', datetime.strptime(date, '%Y-%m-%d'))
    return_val.rename(columns={'day': 'beaconchain_day'}, inplace=True)
    return_val.day_start = pd.to_datetime(return_val.day_start)
    return_val.day_end = pd.to_datetime(return_val.day_end)

    return_val = standardise_types(return_val)

    # ic(return_val)

    context.add_output_metadata(
        {
            "num_records": len(return_val),
            "preview": MetadataValue.md(return_val.head().to_markdown()),
        }
    )

    return return_val

@asset(
    partitions_def=daily_partitions_def,
    compute_kind="python",
    code_version="1",
    io_manager_key = 'protocol_data_lake_io_manager',
    ins={
        "blocks_by_day": AssetIn(key_prefix="warehouse"),
    }
)
def compound_v2_by_day(
    context,
    blocks_by_day,
    ) -> pd.DataFrame:
    """
    Table Compound V2 cTokens and APRs and Borrow/Supply data

    Args:
        context: dagster context object
        block_numbers_by_hour: the output of block_numbers_by_hour for a given market

    Returns:
        A dataframe market config & protocol data for each token in a market
    """

    date = context.partition_key
    block_day = datetime.strptime(date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    prev_block_day = block_day - timedelta(days=1)
    context.log.info(f"date: {date}")
    chain = "ethereum"
    context.log.info(f"chain: {chain}")

    block_height = int(blocks_by_day.loc[(blocks_by_day.chain == chain) & (blocks_by_day.block_day == prev_block_day)].end_block.values[0] + 1)
    context.log.info(f"block_height: {block_height}")

    partition_datetime = datetime.strptime(date, '%Y-%m-%d')


    ctoken_data = pd.DataFrame()
    market = 'ethereum_v3'

    if market in CONFIG_COMPOUND_v2.keys():

        chain = CONFIG_MARKETS[market]["chain"]

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
        ctoken_data.insert(0, 'block_day', partition_datetime)
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
    partitions_def=daily_partitions_def,
    compute_kind="python",
    code_version="1",
    io_manager_key = 'protocol_data_lake_io_manager',
    ins={
        "blocks_by_day": AssetIn(key_prefix="warehouse"),
    }
)
def compound_v3_by_day(
    context,
    blocks_by_day,
    ) -> pd.DataFrame:
    """
    Table Compound V3 cTokens and APRs and Borrow/Supply data

    Args:
        context: dagster context object
        block_numbers_by_hour: the output of block_numbers_by_hour for a given market

    Returns:
        A dataframe market config & protocol data for each token in a market
    """

    date = context.partition_key
    block_day = datetime.strptime(date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    prev_block_day = block_day - timedelta(days=1)
    context.log.info(f"date: {date}")
    chain = "ethereum"
    context.log.info(f"chain: {chain}")

    block_height = int(blocks_by_day.loc[(blocks_by_day.chain == chain) & (blocks_by_day.block_day == prev_block_day)].end_block.values[0] + 1)
    context.log.info(f"block_height: {block_height}")

    

    partition_datetime = datetime.strptime(date, '%Y-%m-%d')

    ctoken_data = pd.DataFrame()

    market = 'ethereum_v3'
    if market in CONFIG_COMPOUND_v3.keys():

        chain = CONFIG_MARKETS[market]["chain"]

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
            # bail if we're before the contract 
            if block_height < ctokens[ctoken]['first_block']:
                ctoken_row = pd.DataFrame()
            else:
            
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

        if not ctoken_data.empty:
            # add the other metadata
            ctoken_data.insert(0, 'block_day', partition_datetime)
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

@asset(
    partitions_def=daily_partitions_def,
    compute_kind="python",
    code_version="1",
    io_manager_key = 'protocol_data_lake_io_manager',
    ins={
        "blocks_by_day": AssetIn(key_prefix="warehouse"),
    }
)
def safety_module_bal_pool_contents(
    context,
    blocks_by_day: pd.DataFrame,
)-> pd.DataFrame:
    """
    Table Safety Module Balancer Pool Contents

    Args:
        context: dagster context object

    Returns:
        A dataframe of safety module balancer pool contents
    """

    date = context.partition_key
    block_day = datetime.strptime(date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    prev_block_day = block_day - timedelta(days=1)
    context.log.info(f"date: {date}")
    chain = "ethereum"
    context.log.info(f"chain: {chain}")
    block_height = int(blocks_by_day.loc[(blocks_by_day.chain == chain) & (blocks_by_day.block_day == prev_block_day)].end_block.values[0] + 1)
    context.log.info(f"block_height: {block_height}")

    # # dev data
    # date = '2023-05-18'
    # block_day = datetime.strptime(date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    # # prev_block_day = block_day - timedelta(days=1)
    # block_height = 17282745
    
    # set up web3 connection
    w3 = Web3(Web3.HTTPProvider(CONFIG_CHAINS[chain]['web3_rpc_url']))

    sm_contents = pd.DataFrame()
    for sm_token in CONFIG_SM_TOKENS:
        if CONFIG_SM_TOKENS[sm_token]['bal_pool_address'] is not None:
            bal_pool_address = CONFIG_SM_TOKENS[sm_token]['bal_pool_address']
            
            # get the pool tokens from the pool
            tokens_call = Call(bal_pool_address, ['getCurrentTokens()(address[])'],[['tokens', None]], _w3 = w3, block_id = block_height)
            pool_tokens = tokens_call()['tokens']

            for token_address in pool_tokens:
                # Get the decimals, weights and balances from the pool via multicall
                calls = [
                    Call(token_address, ['decimals()(uint8)'],[['decimals', None]]),
                    Call(token_address, ['symbol()(string)'],[['symbol', None]]),
                    Call(bal_pool_address, ['getNormalizedWeight(address)(uint256)', token_address], [['weight', None]]),
                    Call(bal_pool_address, ['getBalance(address)(uint256)', token_address], [['balance', None]])
                ]
                
                # create the multicall object
                multicall = Multicall(calls, _w3 = w3, block_id = block_height)

                # execute the call
                results = multicall()

                # convert to dataframe
                token_data = pd.DataFrame(results, index=[0])
                token_data.insert(0, 'safety_module_token', sm_token)
                token_data.insert(1, 'bal_pool_address', bal_pool_address)
                token_data.insert(2, 'token_address', token_address)
                token_data.weight = token_data.weight / 10 ** 18
                token_data.balance = token_data.balance / 10 ** token_data.decimals
                token_data.balance = token_data.balance.astype(pd.Float64Dtype())
                token_data.drop(columns=['decimals'], inplace=True)

                sm_contents = pd.concat([sm_contents, token_data], axis=0).reset_index(drop=True)
        
    
    if not sm_contents.empty:
        sm_contents.insert(0, 'block_day', block_day)
        sm_contents.insert(1, 'block_height', block_height)
        sm_contents.insert(2, 'chain', chain)
    
    sm_contents = standardise_types(sm_contents)
    
    context.add_output_metadata(
        {
            "num_records": len(sm_contents),
            "preview": MetadataValue.md(sm_contents.head().to_markdown()),
        }
    )

    return sm_contents

    # # configure multicall call objects
    # calls = [
    #     Call(balancer_pool_address, ['getBalance(address)(uint256)', CONFIG_SAFETY_MODULE_TOKEN_ADDRESS],[['safety_module_balance', None]]),
    #     Call(balancer_pool_address, ['getBalance(address)(uint256)', CONFIG_BALANCER_POOL_TOKEN_ADDRESS],[['balancer_pool_balance', None]]),
    #     Call(balancer_pool_address, ['getTotalDenormalizedWeight()(uint256)'],[['total_denormalized_weight', None]]),
    #     Call(balancer_pool_address, ['getNormalizedWeight(address)(uint256)', CONFIG_SAFETY_MODULE_TOKEN_ADDRESS],[['safety_module_denormalized_weight', None]]),
    #     Call(balancer_pool_address, ['getNormalizedWeight(address)(uint256)', CONFIG_BALANCER_POOL_TOKEN_ADDRESS],[['balancer_pool_denormalized_weight', None]]),
    # ]

    # # create the multicall object
    # multicall = Multicall(calls, _w3 = w3, block_id = block_height)

    # # execute the call
    # results = multicall()

    # # ic(results)

    # # convert to dataframe
    # data = [balancer_pool_address,
    #     results['safety_module_balance'],
    #     results['balancer_pool_balance'],
    #     results['total_denormalized_weight'],
    #     results['safety_module_denormalized_weight'],
    #     results['balancer_pool_denormalized_weight'],
    #     ]
    # bal_pool_contents = pd.DataFrame(
    #     data=[data],
    #     columns=['balancer_pool_address','safety_module_balance','balancer_pool_balance','total_denormalized_weight','safety_module_denormalized_weight
                 




if __name__ == "__main__":

    # import time
    # start = time.time()
    # out = liquidity_depth_raw()
    # end = time.time()
    # elapsed = end - start
    # ic(elapsed)
    
    safety_module_bal_pool_contents()
