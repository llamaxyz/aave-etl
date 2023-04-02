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
from eth_abi.exceptions import InsufficientDataBytes
from eth_utils.conversions import to_bytes
from shroomdk import ShroomDK
from time import sleep
from random import randint
from multicall import Call, Multicall

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
    partition_datetime = datetime.strptime(date, '%Y-%m-%d')
    chain = CONFIG_MARKETS[market]["chain"]
    block_height = int(block_numbers_by_day.block_height.values[0])

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

            all_rewards.insert(0, 'block_day', partition_datetime)
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
    partition_datetime = datetime.strptime(date, '%Y-%m-%d')
    chain = CONFIG_MARKETS[market]["chain"]
    
    try:
        block_height = int(protocol_data_by_day.block_height.values[0])
    except IndexError:
        # protocol not deployed on this day
        return pd.DataFrame()

    if CONFIG_MARKETS[market]['version'] == 3:
        
    
        emode_output = pd.DataFrame()
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
        
        emode_output['block_day'] = partition_datetime
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

    context.add_output_metadata(
        {
            "num_records": len(emode_output),
            "preview": MetadataValue.md(emode_output.head().to_markdown()),
        }
    )

    return emode_output
