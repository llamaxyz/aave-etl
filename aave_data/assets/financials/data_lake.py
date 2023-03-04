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

from aave_data.resources.financials_config import * #pylint: disable=wildcard-import, unused-wildcard-import

from aave_data.resources.helpers import (
    get_market_tokens_at_block_messari,
    get_market_tokens_at_block_aave,
    get_token_transfers_from_covalent,
    get_token_transfers_from_alchemy,
    get_erc20_balance_of,
    get_scaled_balance_of,
    get_events_by_topic_hash_from_covalent,
    standardise_types
)

if not sys.warnoptions:
    import warnings
    # warnings.simplefilter("ignore")
    warnings.filterwarnings("ignore", category=ExperimentalWarning)

INITIAL_RETRY = 0.01 #seconds
MAX_RETRIES = 10

market_day_multipartition = MultiPartitionsDefinition(
    {
        "date": DailyPartitionsDefinition(start_date=FINANCIAL_PARTITION_START_DATE),
        "market": StaticPartitionsDefinition(list(CONFIG_MARKETS.keys())),
    }
)

@asset(
    partitions_def=market_day_multipartition,
    compute_kind="python",
    #group_name='data_lake',
    code_version="1",
    io_manager_key = 'data_lake_io_manager'
    # freshness_policy=FreshnessPolicy(maximum_lag_minutes=6*60),
)
def block_numbers_by_day(context) -> pd.DataFrame:
    """Table with the closest block number to the daily partition boundary (0000 UTC) for a chain

    Uses the defillama API at https://coins.llama.fi/block/{chain}/{timestamp}
    Returns the block height along with the target time and actual block time

    Args:
        target_time: a datetime of the time we want the closest block to in UTC
        chain: a chain identifier from https://api.llama.fi/chains (field 'name', case insensitive)

    Returns:
        A dataframe with the target datetime, the actual datetime, the block height and the chain

    Raises:
        HTTPError from requests module on invalid calls or server errors

    """

    # market = context.partition_key.keys_by_dimension['market']
    # date = context.partition_key.keys_by_dimension['date']
    date, market = context.partition_key.split("|")
    partition_datetime = datetime.strptime(date, '%Y-%m-%d')
    context.log.info(f"market: {market}")
    context.log.info(f"date: {date}")
    config_chain = CONFIG_MARKETS[market]['chain']
    llama_chain = CONFIG_CHAINS[config_chain]['defillama_chain']

    unix_timestamp = partition_datetime.timestamp()

    endpoint = f'https://coins.llama.fi/block/{llama_chain}/{unix_timestamp:.0f}'

    response = requests.get(endpoint, timeout=300)
    response.raise_for_status()

    vals = response.json()

    block_height = vals['height']
    block_time = datetime.utcfromtimestamp(vals['timestamp'])

    end_block_day = partition_datetime + timedelta(days=1)
    end_block_day_unix = end_block_day.timestamp()

    endpoint = f'https://coins.llama.fi/block/{llama_chain}/{end_block_day_unix:.0f}'

    response = requests.get(endpoint, timeout=300)
    response.raise_for_status()

    vals = response.json()

    end_block = vals['height'] - 1

    return_val = pd.DataFrame([[partition_datetime, block_time, block_height, end_block]],
                        columns=['block_day', 'block_time', 'block_height', 'end_block'])


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
    partitions_def=market_day_multipartition,
    compute_kind="python",
    #group_name='data_lake',
    code_version="1",
    io_manager_key = 'data_lake_io_manager',
    ins={
        "block_numbers_by_day": AssetIn(key_prefix="financials_data_lake"),
    }
)
def market_tokens_by_day(context, block_numbers_by_day) -> pd.DataFrame: #pylint: disable=W0621
    """Table of the tokens and metadata in a market at a given block height

    Uses either the official Aave subgraph or the Messari subgraph based on config

    Args:
        context: dagster context object
        block_numbers_by_day: the output of block_numbers_by_day for a given market

    Returns:
        A dataframe with market reserve & atoken details

    """
    #pylint: disable=E1137,E1101

    # market = context.partition_key.keys_by_dimension['market']
    date, market = context.partition_key.split("|")
    block_height = int(block_numbers_by_day.block_height.values[0])
    block_day = block_numbers_by_day.block_day.values[0]
    token_source = CONFIG_MARKETS[market]['token_source']

    context.log.info(f"market: {market}")
    context.log.info(f"date: {date}")
    context.log.info(f"block_height: {block_height}")

    if token_source == "aave":
        tokens = get_market_tokens_at_block_aave(market, block_height, CONFIG_MARKETS)
    else:
        tokens = get_market_tokens_at_block_messari(market, block_height, CONFIG_MARKETS)

    if not tokens.empty:
        tokens['block_day'] = block_day
        # overwrite ETH with WETH address
        tokens.loc[tokens.reserve == '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee', 'reserve'] = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'

    tokens = standardise_types(tokens)

    context.add_output_metadata(
        {
            "num_records": len(tokens),
            "preview": MetadataValue.md(tokens.head().to_markdown()),
            "token_data_source": token_source
        }
    )
    return tokens

@asset(
    partitions_def=market_day_multipartition,
    compute_kind="python",
    #group_name='data_lake',
    code_version="1",
    io_manager_key = 'data_lake_io_manager',
    ins={
        "market_tokens_by_day": AssetIn(key_prefix="financials_data_lake"),
    }
)
def aave_oracle_prices_by_day(context, market_tokens_by_day) -> pd.DataFrame:  # type: ignore pylint: disable=W0621
    """Table of the token and aave oracle price foreacharket at each block height

    Uses web3.py to access an RPC node and call the oracle contract directly

    Args:
        context: dagster context object
        market_tokens_by_day: the output of market_tokens_by_day for a given market

    Returns:
        A dataframe market, token and the underlying reserve oracle price at the block height

    """
    #pylint: disable=E1137,E1101

    # market = context.partition_key.keys_by_dimension['market']
    # date = context.partition_key.keys_by_dimension['date']
    date, market = context.partition_key.split("|")
    # market = 'ethereum_v2'#'arbitrum_v3'
    # date = '2022-11-26'

    chain = CONFIG_MARKETS[market]['chain']

    if market_tokens_by_day.empty:
        return_val = pd.DataFrame()
    else:
        block_height = int(market_tokens_by_day.block_height.values[0])
        context.log.info(f"market: {market}")
        context.log.info(f"date: {date}")
        context.log.info(f"block_height: {block_height}")

        # Get the eth price from the chainlink oracle if the Aave oracle price is denominated in eth
        if CONFIG_MARKETS[market]['oracle_base_currency'] == 'wei':
            w3 = Web3(Web3.HTTPProvider(CONFIG_CHAINS['ethereum']['web3_rpc_url']))
            eth_address = '0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE'
            usd_address = '0x0000000000000000000000000000000000000348'
            chainlink_feed_registry = '0x47Fb2585D2C56Fe188D0E6ec628a38b74fCeeeDf'
            feed_registry_abi_url = f"https://api.etherscan.io/api?module=contract&action=getabi&apikey={ETHERSCAN_API_KEY}&address={chainlink_feed_registry}"
            feed_registry_abi = json.loads(requests.get(feed_registry_abi_url, timeout=300).json()['result'])
            feed_registry = w3.eth.contract(address=chainlink_feed_registry, abi=feed_registry_abi)
            # get the block number from the eth_v2 upstream asset.
            pkey = MultiPartitionKey(
                {
                    "date": date,
                    "market": 'ethereum_v2'
                }
            )  # type: ignore
            eth_context = build_op_context(partition_key=pkey)
            eth_block_numbers_by_day = block_numbers_by_day(eth_context)
            eth_block_height = int(eth_block_numbers_by_day.block_height.values[0])
            eth_usd_price = float(feed_registry.functions.latestAnswer(eth_address, usd_address).call(block_identifier = eth_block_height) / 10**8)
        else:
            eth_usd_price = float(0)

        # get the abi for the oracle contract from etherscan/polygonscan
        aave_version = CONFIG_MARKETS[market]['version']
        oracle_abi_url = CONFIG_ABI[aave_version]['abi_url_base'] + CONFIG_ABI[aave_version]['oracle_implementation']
        oracle_abi = json.loads(requests.get(oracle_abi_url, timeout=300).json()['result'])

        # collect the reserve tokens into a list for the web3 contract call
        reserves = list(market_tokens_by_day['reserve'].values)
        reserves = [Web3.toChecksumAddress(reserve) for reserve in reserves]
        # ic(reserves)

        #initialise web3 and the oracle contract
        w3 = Web3(Web3.HTTPProvider(CONFIG_CHAINS[chain]['web3_rpc_url']))
        oracle_address = Web3.toChecksumAddress(CONFIG_MARKETS[market]['oracle'])
        oracle = w3.eth.contract(address=oracle_address, abi=oracle_abi)

        # get the price multiplier for the oracle price
        if CONFIG_MARKETS[market]['oracle_base_currency'] == 'usd':
            try:
                base_currency_unit = oracle.functions.BASE_CURRENCY_UNIT().call(block_identifier = block_height)
            except AttributeError:
                # some markets don't have this function - it fails on call (rwa)
                base_currency_unit = 100000000
            price_multiplier = 1 / base_currency_unit
        elif CONFIG_MARKETS[market]['oracle_base_currency'] == 'wei':
            price_multiplier = eth_usd_price / 1e18
        else:
            price_multiplier = 1

        # ic(price_multiplier)
        
        # use exponential backoff for this call - large return values.  sometimes times out on RPC as ValueError
        i = 0
        delay_time = INITIAL_RETRY
        while True:
            try:
                response = oracle.functions.getAssetsPrices(reserves).call(block_identifier = block_height)
                break
            except ValueError as e:
                if i > MAX_RETRIES:
                    raise e
                print(f"Retry {i} web3 getAssetsPrices after {delay_time} seconds")
                sleep(delay_time)
                i += 1
                delay_time *= 2
                


        # ic(response)

        # create a dataframe with the price
        return_val = market_tokens_by_day[['reserve','symbol','market','block_height','block_day']].copy()
        return_val['usd_price'] = pd.Series(response, name='usd_price').astype('Float64') * price_multiplier # type: ignore

        return_val = standardise_types(return_val)

    context.add_output_metadata(
        {
            "num_records": len(return_val),
            "preview": MetadataValue.md(return_val.head().to_markdown()),
        }
    )
    return return_val
    # pylint: enable=E1137,E1101

@asset(
    partitions_def=market_day_multipartition,
    compute_kind="python",
    #group_name='data_lake',
    code_version="1",
    io_manager_key = 'data_lake_io_manager',
    ins={
        "block_numbers_by_day": AssetIn(key_prefix="financials_data_lake"),
        "market_tokens_by_day": AssetIn(key_prefix="financials_data_lake"),
    }
)
def collector_atoken_transfers_by_day(context, market_tokens_by_day, block_numbers_by_day) -> pd.DataFrame:  # type: ignore pylint: disable=W0621
    """
    Table of the aave market token transfers in & out of the collector contracts for each market

    Uses the Covalent token transfers API

    Args:
        context: dagster context object
        market_tokens_by_day: the output of market_tokens_by_day for a given market

    Returns:
        A dataframe with the token transfers in and out of the collector contracts for each market

    """

    # iterate through the atokens & call the covalent API then assemble the results into a dataframe
    start_block = int(block_numbers_by_day.block_height.values[0])
    end_block = int(block_numbers_by_day.end_block.values[0])
    # market = context.partition_key.keys_by_dimension['market']
    # date = context.partition_key.keys_by_dimension['date']
    date, market = context.partition_key.split("|")
    chain = CONFIG_MARKETS[market]['chain']
    chain_id = CONFIG_CHAINS[chain]['chain_id']

    # special case for V1, collector contract changed
    if market == 'ethereum_v1':
        partition_datetime = datetime.strptime(date, '%Y-%m-%d')
        if partition_datetime > CONFIG_MARKETS[market]['collector_change_date']:
            collectors = [CONFIG_MARKETS[market]['collector'],CONFIG_MARKETS[market]['collector_v2']]
        else:
            collectors = [CONFIG_MARKETS[market]['collector']]
    else:
        collectors = [CONFIG_MARKETS[market]['collector']]
    

    context.log.info(f"market: {market}")
    context.log.info(f"date: {date}")
    block_day = block_numbers_by_day.block_day.values[0]

    transfers = pd.DataFrame()
    for collector in collectors:
        for row in market_tokens_by_day.itertuples():
            # ic(row)
            context.log.info(f"atoken: {row.atoken_symbol}")
            if market == 'ethereum_v1':
                token = row.reserve
            else:
                token = row.atoken
            
            if chain in ['polygon','ethereum','optimism','arbitrum']:
                try:
                    row_transfers = get_token_transfers_from_alchemy(
                        start_block,
                        end_block,
                        block_day,
                        chain,
                        collector,
                        token                    
                    )
                except TypeError:
                    # fall back to covalent if alchemy fails
                    # catches an edge case where a new atoken doesn't have metadata in the alchemy API response yet
                    row_transfers = get_token_transfers_from_covalent(
                    start_block,
                    end_block,
                    chain_id,
                    collector,
                    token
                )
            else:
                row_transfers = get_token_transfers_from_covalent(
                    start_block,
                    end_block,
                    chain_id,
                    collector,
                    token
                )
            if not row_transfers.empty:
                row_transfers['market'] = market
                row_transfers['collector'] = collector

            transfers = pd.concat([transfers, row_transfers]).reset_index(drop=True)

    transfers = standardise_types(transfers)

    context.add_output_metadata(
        {
            "num_records": len(transfers),
            "preview": MetadataValue.md(transfers.head().to_markdown()),
        }
    )

    return transfers

@asset(
    partitions_def=market_day_multipartition,
    compute_kind="python",
    #group_name='data_lake',
    code_version="1",
    io_manager_key = 'data_lake_io_manager',
    ins={
        "block_numbers_by_day": AssetIn(key_prefix="financials_data_lake"),
    }
)
def non_atoken_transfers_by_day(context, block_numbers_by_day) -> pd.DataFrame:  # type: ignore pylint: disable=W0621
    """
    Table of other token transfers relevant to the protocol
    - treasury swaps & actions
    - aave governance token transfers
    - incentives payments

    uses the list of contracts & tokens in CONFIG_TOKENS

    Uses the Covalent token transfers API

    Args:
        context: dagster context object
        market_tokens_by_day: the output of market_tokens_by_day for a given market

    Returns:
        A dataframe with the token transfers in and out of the collector contracts for each market

    """
    # iterate through the atokens & call the covalent API then assemble the results into a dataframe
    start_block = block_numbers_by_day.block_height.values[0]
    end_block = block_numbers_by_day.end_block.values[0]
    # market = context.partition_key.keys_by_dimension['market']
    # date = context.partition_key.keys_by_dimension['date']
    date, market = context.partition_key.split("|")
    chain = CONFIG_MARKETS[market]['chain']
    chain_id = CONFIG_CHAINS[chain]['chain_id']

    context.log.info(f"market: {market}")
    context.log.info(f"date: {date}")

    transfers = pd.DataFrame()
    if market in CONFIG_TOKENS.keys():
        for wallet in CONFIG_TOKENS[market].keys():
            wallet_address = CONFIG_TOKENS[market][wallet]['address']
            for token in CONFIG_TOKENS[market][wallet]['tokens'].keys():
                token_address = CONFIG_TOKENS[market][wallet]['tokens'][token]['address']
                row_transfers = get_token_transfers_from_covalent(
                    start_block,
                    end_block,
                    chain_id,
                    wallet_address,
                    token_address
                )
                row_transfers['market'] = market
                row_transfers['collector'] = wallet_address
                transfers = pd.concat([transfers, row_transfers]).reset_index(drop=True)
                
                context.log.info(f"{wallet}: {token}")
            

    transfers = standardise_types(transfers)

    context.add_output_metadata(
        {
            "num_records": len(transfers),
            "preview": MetadataValue.md(transfers.head().to_markdown()),
        }
    )

    return transfers

@asset(
    partitions_def=market_day_multipartition,
    compute_kind="python",
    #group_name='data_lake',
    code_version="1",
    io_manager_key = 'data_lake_io_manager',
    ins={
        "block_numbers_by_day": AssetIn(key_prefix="financials_data_lake"),
        "market_tokens_by_day": AssetIn(key_prefix="financials_data_lake"),
    }
)
def collector_atoken_balances_by_day(context, market_tokens_by_day, block_numbers_by_day) -> pd.DataFrame:  # type: ignore pylint: disable=W0621
    """
    Table of the aave market token balances in the collector contracts for each market

    Uses RPC calls to balanceOf() via web3.py

    Args:
        context: dagster context object
        market_tokens_by_day: the output of market_tokens_by_day for a given market
        block_numbers_by_day: the output of block_numbers_by_day for a given chain

    Returns:
        A dataframe with the balances of each token for each collector contract

    """
    block_height = int(block_numbers_by_day.block_height.values[0])
    chain = block_numbers_by_day.chain.values[0]
    # market = context.partition_key.keys_by_dimension['market']
    # date = context.partition_key.keys_by_dimension['date']
    date, market = context.partition_key.split("|")
    chain = CONFIG_MARKETS[market]['chain']
    partition_datetime = datetime.strptime(date, '%Y-%m-%d')

    # special case for V1, collector contract changed
    if market == 'ethereum_v1':
        partition_datetime = datetime.strptime(date, '%Y-%m-%d')
        if partition_datetime > CONFIG_MARKETS[market]['collector_change_date']:
            collectors = [CONFIG_MARKETS[market]['collector'],CONFIG_MARKETS[market]['collector_v2']]
        else:
            collectors = [CONFIG_MARKETS[market]['collector']]
    else:
        collectors = [CONFIG_MARKETS[market]['collector']]

    context.log.info(f"market: {market}")
    context.log.info(f"date: {date}")

    balances = pd.DataFrame()
    for collector in collectors:
        for row in market_tokens_by_day.itertuples():
            # ic(row)

            if market == 'ethereum_v1':
                token = row.reserve
                decimals = row.decimals
                symbol = row.symbol
            else:
                token = row.atoken
                decimals = row.atoken_decimals
                symbol = row.atoken_symbol
                

            row_balance = get_erc20_balance_of(
                collector,
                token,
                decimals,
                chain,
                block_height
            )

            if market == 'ethereum_v1':
                row_scaled_balance = row_balance
            else:
                row_scaled_balance = get_scaled_balance_of(
                    collector,
                    token,
                    decimals,
                    chain,
                    block_height
                )
        
            output_row = {
                        'collector': collector, 
                        'market': market,
                        'token': token, 
                        'symbol': symbol,
                        'block_height': block_height, 
                        'block_day': partition_datetime.replace(tzinfo=timezone.utc),
                        'balance': row_balance,
                        'scaled_balance': row_scaled_balance
                    }

            balance_row = pd.DataFrame(output_row, index=[0])
            context.log.info(f"atoken: {row.atoken_symbol}")
            balances = pd.concat([balances, balance_row]).reset_index(drop=True)  # type: ignore

    balances = standardise_types(balances)

    context.add_output_metadata(
        {
            "num_records": len(balances),
            "preview": MetadataValue.md(balances.head().to_markdown()),
        }
    )

    return balances

@asset(
    partitions_def=market_day_multipartition,
    compute_kind="python", 
    #group_name='data_lake',
    code_version="1",
    io_manager_key = 'data_lake_io_manager',
    ins={
        "block_numbers_by_day": AssetIn(key_prefix="financials_data_lake"),
    }
)
def non_atoken_balances_by_day(context, block_numbers_by_day) -> pd.DataFrame:  # type: ignore pylint: disable=W0621
    """
    Table of balances of other tokens relevant to the protocol
    - treasury swaps & actions
    - aave governance token transfers
    - incentives payments

    uses the list of contracts & tokens in CONFIG_TOKENS

    Uses the balanceOf() function via RPC calls

    Args:
        context: dagster context object
        block_numbers_by_day: block numbers at the start and end of the day for the chain

    Returns:
        A dataframe with the balances of each token on each contract

    """
    # iterate through the atokens & call the covalent API then assemble the results into a dataframe
    start_block = block_numbers_by_day.block_height.values[0]
    # market = context.partition_key.keys_by_dimension['market']
    # date = context.partition_key.keys_by_dimension['date']
    date, market = context.partition_key.split("|")
    chain = CONFIG_MARKETS[market]['chain']
    partition_datetime = datetime.strptime(date, '%Y-%m-%d')

    context.log.info(f"market: {market}")
    context.log.info(f"date: {date}")

    balances = pd.DataFrame()
    if market in CONFIG_TOKENS:
        for wallet in CONFIG_TOKENS[market].keys():
            wallet_address = CONFIG_TOKENS[market][wallet]['address']
            for token in CONFIG_TOKENS[market][wallet]['tokens'].keys():
                token_address = CONFIG_TOKENS[market][wallet]['tokens'][token]['address']
                token_decimals = CONFIG_TOKENS[market][wallet]['tokens'][token]['decimals']
                balance = get_erc20_balance_of(
                    wallet_address,
                    token_address,
                    token_decimals,
                    chain,
                    start_block
                    )
                output_row = {
                    'contract_address': wallet_address, 
                    'chain': chain,
                    'market': market,
                    'token': token_address, 
                    'decimals': token_decimals,
                    'symbol': token,
                    'block_height': start_block, 
                    'block_day': partition_datetime.replace(tzinfo=timezone.utc),
                    'balance': balance
                }
                balance_row = pd.DataFrame(output_row, index=[0])
                context.log.info(f"balanceOf: {token} on {chain}")
                balances = pd.concat([balances, balance_row]).reset_index(drop=True)  # type: ignore

    balances = standardise_types(balances)

    context.add_output_metadata(
        {
            "num_records": len(balances),
            "preview": MetadataValue.md(balances.head().to_markdown()),
        }
    )

    return balances

@asset(
    # partitions_def=v3_market_day_multipartition,
    partitions_def=market_day_multipartition,
    # ins={
    #     "market_tokens_by_day":
    #         AssetIn(
    #             key="market_tokens_by_day",
    #             partition_mapping=IdentityPartitionMapping()
    #         )
    # },
    compute_kind="python", 
    #group_name='data_lake',
    code_version="1",
    io_manager_key = 'data_lake_io_manager',
    ins={
        "market_tokens_by_day": AssetIn(key_prefix="financials_data_lake"),
    }

)
def v3_accrued_fees_by_day(context, market_tokens_by_day) -> pd.DataFrame: # type: ignore
    """
    Gets fees which have been earned on Aave v3 but not yet 
    materialised as a token transfer via MintToTransfer()

    Calls the getReserveData() function on the AaveDataProvider contract
    via RPC calls

    Args:
        context: dagster context object
        market_tokens_by_day: list of atokens & block heights

    Returns:
        A dataframe with accrued fees for each atoken

    """
    # market = context.partition_key.keys_by_dimension['market']
    # date = context.partition_key.keys_by_dimension['date']
    date, market = context.partition_key.split("|")
    if market_tokens_by_day.empty:
        fees = pd.DataFrame()
    else:
        block_height = market_tokens_by_day.block_height.values[0]
        chain = CONFIG_MARKETS[market]['chain']
        partition_datetime = datetime.strptime(date, '%Y-%m-%d')

        context.log.info(f"market: {market}")
        context.log.info(f"date: {date}")
        context.log.info(f"block_height: {block_height}")

        fees = pd.DataFrame()

        if CONFIG_MARKETS[market]['version'] == 3:

            # a minimal ABI supporting getReserveData only
            provider_abi = [
                    {
                        "inputs":[
                            {
                                "internalType":"address",
                                "name":"asset",
                                "type":"address"
                            }
                        ],
                        "name":"getReserveData",
                        "outputs":[
                            {
                                "internalType":"uint256",
                                "name":"unbacked",
                                "type":"uint256"
                            },
                            {
                                "internalType":"uint256",
                                "name":"accruedToTreasuryScaled",
                                "type":"uint256"
                            },
                            {
                                "internalType":"uint256",
                                "name":"totalAToken",
                                "type":"uint256"
                            },
                            {
                                "internalType":"uint256",
                                "name":"totalStableDebt",
                                "type":"uint256"
                            },
                            {
                                "internalType":"uint256",
                                "name":"totalVariableDebt",
                                "type":"uint256"
                            },
                            {
                                "internalType":"uint256",
                                "name":"liquidityRate",
                                "type":"uint256"
                            },
                            {
                                "internalType":"uint256",
                                "name":"variableBorrowRate",
                                "type":"uint256"
                            },
                            {
                                "internalType":"uint256",
                                "name":"stableBorrowRate",
                                "type":"uint256"
                            },
                            {
                                "internalType":"uint256",
                                "name":"averageStableBorrowRate",
                                "type":"uint256"
                            },
                            {
                                "internalType":"uint256",
                                "name":"liquidityIndex",
                                "type":"uint256"
                            },
                            {
                                "internalType":"uint256",
                                "name":"variableBorrowIndex",
                                "type":"uint256"
                            },
                            {
                                "internalType":"uint40",
                                "name":"lastUpdateTimestamp",
                                "type":"uint40"
                            }
                        ],
                        "stateMutability":"view",
                        "type":"function"
                    },
                ]
            
            provider_address = Web3.toChecksumAddress(CONFIG_MARKETS[market]['protocol_data_provider'])
            #initialise Web3 and provider contract
            w3 = Web3(Web3.HTTPProvider(CONFIG_CHAINS[chain]['web3_rpc_url']))
            provider = w3.eth.contract(address=provider_address, abi=provider_abi)
            
            for row in market_tokens_by_day.itertuples():
                reserve_data = provider.functions.getReserveData(Web3.toChecksumAddress(row.reserve)).call(block_identifier=int(block_height))
                # ic(row.symbol)
                # ic(reserve_data)
                accrued_fees_scaled = reserve_data[1] / pow(10, row.decimals)
                liquidity_index = reserve_data[9] / pow(10, 27)
                accrued_fees = accrued_fees_scaled * liquidity_index
                # ic(accrued_fees)
                output_row = {
                    'market': market,
                    'reserve': row.reserve,
                    'symbol': row.symbol,
                    'atoken': row.atoken,
                    'atoken_symbol': row.atoken_symbol,
                    'block_height': block_height,
                    'block_day': partition_datetime.replace(tzinfo=timezone.utc),
                    'accrued_fees_scaled': accrued_fees_scaled,
                    'liquidity_index': liquidity_index,
                    'accrued_fees': accrued_fees
                }
                fees_row = pd.DataFrame(output_row, index=[0])
                context.log.info(f"accrued_fees: {row.symbol} on {market}")
                fees = pd.concat([fees, fees_row]).reset_index(drop=True)

    fees = standardise_types(fees)

    context.add_output_metadata(
        {
            "num_records": len(fees),
            "preview": MetadataValue.md(fees.head().to_markdown()),
        }
    )
    return fees

@asset(
    # partitions_def=v3_market_day_multipartition,
    partitions_def=market_day_multipartition,
    # ins={
    #     "market_tokens_by_day":
    #         AssetIn(
    #             key="market_tokens_by_day",
    #             partition_mapping=IdentityPartitionMapping()
    #         ),
    # },
    compute_kind="python",
    #group_name='data_lake',
    code_version="1",
    io_manager_key = 'data_lake_io_manager',
    ins={
        "block_numbers_by_day": AssetIn(key_prefix="financials_data_lake"),
        "market_tokens_by_day": AssetIn(key_prefix="financials_data_lake"),
    }

)
def v3_minted_to_treasury_by_day(context, block_numbers_by_day, market_tokens_by_day) -> pd.DataFrame:
    """
    Gets fees which have been earned on Aave v3 but not yet 
    materialised as a token transfer via MintToTransfer()
    Gets MintedToTreasury events via covalent then looks up the corresponding
    Mint event to get the amount minted/transferred in the transaction.
    The Mint & Transfer events include botht he MintedToTreasury amount
    as well as atoken (deposit) interest accrued since the last on-chain action
    for the treasury account.  This needs to be adjusted for when calculating
    total deposit interest.

    Args:
        context: dagster context object
        market_tokens_by_day: list of atokens & block heights

    Returns:
        A dataframe with the MintedToTreasury and Minted amounts for each reserve/atoken

    """

    # market = context.partition_key.keys_by_dimension['market']
    # date = context.partition_key.keys_by_dimension['date']
    date, market = context.partition_key.split("|")
    start_block = block_numbers_by_day.block_height.values[0]
    end_block = block_numbers_by_day.end_block.values[0]
    pool = CONFIG_MARKETS[market]['pool']
    chain = CONFIG_MARKETS[market]['chain']
    chain_id = CONFIG_CHAINS[chain]['chain_id']
    partition_datetime = datetime.strptime(date, '%Y-%m-%d')

    context.log.info(f"market: {market}")
    context.log.info(f"date: {date}")
    context.log.info(f"block_height: {start_block}")

    if CONFIG_MARKETS[market]['version'] == 3:

        # MintedToTreasury(address,uint256) via https://emn178.github.io/online-tools/keccak_256.html
        minted_to_treasury_topic_hash = '0xbfa21aa5d5f9a1f0120a95e7c0749f389863cbdbfff531aa7339077a5bc919de'

        minted_to_treasury = get_events_by_topic_hash_from_covalent(
            start_block,
            end_block,
            chain_id,
            minted_to_treasury_topic_hash,
            pool
        )
        if not minted_to_treasury.empty:
            # Decode the data & topic1 fields from the log events
            # pylint: disable=E1136
            minted_to_treasury = minted_to_treasury.loc[~minted_to_treasury.raw_log_topics_0.isna()]
            minted_to_treasury['minted_to_treasury_amount'] = minted_to_treasury.apply(
                lambda row: decode(['uint256'], to_bytes(hexstr = row.raw_log_data))[0],
                axis=1)
            minted_to_treasury['minted_reserve'] = minted_to_treasury.apply(
                lambda row: decode(['address'], to_bytes(hexstr = row.raw_log_topics_1))[0],
                axis=1)
            # pylint: enable=E1136

            # Join the token table on the reserve address   
            minted_to_treasury = minted_to_treasury.merge(
                market_tokens_by_day,
                how='inner',
                left_on='minted_reserve',
                right_on='reserve'
            )

            minted_to_treasury.minted_to_treasury_amount = minted_to_treasury.minted_to_treasury_amount / pow(10, minted_to_treasury.decimals)

            # build the output dataframe
            minted_to_treasury['block_day'] = pd.to_datetime(partition_datetime, utc=True)
            minted_to_treasury['market'] = market
            minted_to_treasury['block_height'] = minted_to_treasury.block_height_y.astype('int64')
            minted_to_treasury.minted_to_treasury_amount = minted_to_treasury.minted_to_treasury_amount.astype('float64')

            # minted_to_treasury.info()
            minted_to_treasury = minted_to_treasury[[
                'tx_hash',
                'market',
                'reserve',
                'symbol',
                'decimals',
                'atoken',
                'atoken_symbol',
                'block_height',
                'block_day',
                'minted_to_treasury_amount'
            ]]

            # Mint(address,address,uint256,uint256,uint256) via https://emn178.github.io/online-tools/keccak_256.html
            mint_topic_hash = '0x458f5fa412d0f69b08dd84872b0215675cc67bc1d5b6fd93300a1c3878b86196'

            mints = get_events_by_topic_hash_from_covalent(
                start_block,
                end_block,
                chain_id,
                mint_topic_hash
            )
            # decoded data in covalent is unreliable due to int->float conversions and incorrect ABI
            # filter for the tx_hash in the minted_to_treasury dataframe
            # then grab the raw log data and decode it here
            mints = mints[mints.tx_hash.isin(minted_to_treasury.tx_hash)]
            mints = mints[mints.decoded_params_name.isna()]
            mints['decoded'] = mints.apply(
                lambda row: decode(['uint256','uint256','uint256'], to_bytes(hexstr = row.raw_log_data)),
                axis=1)
            mints['value'], mints['balanceIncrease'], mints['index'] = zip(*mints.decoded)

            mints['minted_atoken'] = mints.sender_address

            mints = mints[[
                'tx_hash',
                'minted_atoken',
                'value',
                'sender_name'
            ]]
            
            minted_to_treasury = minted_to_treasury.merge(
                mints,
                how='inner',
                left_on=['tx_hash','atoken'],
                right_on=['tx_hash','minted_atoken']
            )

            minted_to_treasury['minted_amount'] = minted_to_treasury.value / pow(10, minted_to_treasury.decimals) 
            minted_to_treasury.minted_amount = minted_to_treasury.minted_amount.astype('float64')

            minted_to_treasury.drop(['tx_hash','minted_atoken','value','sender_name','decimals'], axis=1, inplace=True)

            # roll up by day to catch if there are more than one MintToTreasury events in a day
            minted_to_treasury = minted_to_treasury.groupby([
                'market',
                'reserve',
                'symbol',
                'atoken',
                'atoken_symbol',
                'block_height',
                'block_day'
                ]).sum().reset_index()
        else:
            minted_to_treasury = pd.DataFrame()
    else:
        minted_to_treasury = pd.DataFrame()

    minted_to_treasury = standardise_types(minted_to_treasury)

    context.add_output_metadata(
            {
                "num_records": len(minted_to_treasury),
                "preview": MetadataValue.md(minted_to_treasury.head().to_markdown()),
            }
        )

    return(minted_to_treasury)


@asset(
    # partitions_def=v3_market_day_multipartition,
    partitions_def=market_day_multipartition,
    compute_kind="python",
    #group_name='data_lake',
    code_version="1",
    io_manager_key = 'data_lake_io_manager',
    ins={
        "block_numbers_by_day": AssetIn(key_prefix="financials_data_lake"),
    }
)
def treasury_accrued_incentives_by_day(context, block_numbers_by_day) -> pd.DataFrame:
    """
    Gets accrued LM incentives that are owed to the treasury from holding aTokens

    Uses an RPC call to the IncentivesController contract to get the accrued amount

    On Aave V3, it enumerates the available rewards and then calls getRewardsBalance on each token

    Args:
        context: dagster context object
        block_numbers_by_day: block numbers at the start and end of the day for the chain

    Returns:
        A dataframe with the reward token balances of each rewards token on the treasury contract

    """

    date, market = context.partition_key.split("|")
    chain = CONFIG_MARKETS[market]['chain']
    partition_datetime = datetime.strptime(date, '%Y-%m-%d')

    context.log.info(f"market: {market}")
    context.log.info(f"date: {date}")

    # minimal abis supporting just the required functions
    v3_rewards_abi = [
        {
            "inputs":[
                
            ],
            "name":"getRewardsList",
            "outputs":[
                {
                    "internalType":"address[]",
                    "name":"",
                    "type":"address[]"
                }
            ],
            "stateMutability":"view",
            "type":"function"
        },
        {
            "inputs":[
                {
                    "internalType":"address",
                    "name":"user",
                    "type":"address"
                },
                {
                    "internalType":"address",
                    "name":"reward",
                    "type":"address"
                }
            ],
            "name":"getUserAccruedRewards",
            "outputs":[
                {
                    "internalType":"uint256",
                    "name":"",
                    "type":"uint256"
                }
            ],
            "stateMutability":"view",
            "type":"function"
        }
    ]

    v2_rewards_abi = [
        {
            "inputs":[
                {
                    "internalType":"address",
                    "name":"_user",
                    "type":"address"
                }
            ],
            "name":"getUserUnclaimedRewards",
            "outputs":[
                {
                    "internalType":"uint256",
                    "name":"",
                    "type":"uint256"
                }
            ],
            "stateMutability":"view",
            "type":"function"
        }
    ]

    if CONFIG_MARKETS[market]['incentives_controller'] is None:
        rewards = pd.DataFrame()
    else:
        # initialise web3
        web3 = Web3(Web3.HTTPProvider(CONFIG_CHAINS[chain]['web3_rpc_url']))
        collector_contract = Web3.toChecksumAddress(CONFIG_MARKETS[market]['collector'])
        block_height = int(block_numbers_by_day['block_height'].values[0])
        incentives_controller_address = Web3.toChecksumAddress(CONFIG_MARKETS[market]['incentives_controller'])

        

        if CONFIG_MARKETS[market]['version'] == 3:
            # get the list of rewards tokens
            incentives_controller = web3.eth.contract(address=incentives_controller_address, abi=v3_rewards_abi)
            try:
                rewards_list = incentives_controller.functions.getRewardsList().call(block_identifier=block_height)
            except BadFunctionCallOutput:
                # if there are no rewards, the call will fail
                rewards_list = []
            
            rewards = pd.DataFrame()
            for rewards_token in rewards_list:
                # get the symbol and decimals of the rewards token
                rewards_token_contract = web3.eth.contract(address=rewards_token, abi=ERC20_ABI)
                rewards_token_symbol = rewards_token_contract.functions.symbol().call(block_identifier=block_height)
                rewards_token_decimals = rewards_token_contract.functions.decimals().call(block_identifier=block_height)
                # get the balance of the rewards token for the collector contract
                rewards_token_balance = incentives_controller.functions.getUserAccruedRewards(
                    collector_contract, 
                    rewards_token
                ).call(block_identifier=block_height) / pow(10, rewards_token_decimals)
                rewards_token_element = pd.DataFrame(
                    [
                        {
                            'chain': chain,
                            'market': market,
                            'collector_contract': collector_contract.lower(),
                            'block_height': block_height,
                            'block_day': block_numbers_by_day['block_day'].values[0],
                            'rewards_token_address': rewards_token.lower(),
                            'rewards_token_symbol': rewards_token_symbol,
                            'accrued_rewards': rewards_token_balance,
                        }
                    ]
                )
                rewards = pd.concat([rewards, rewards_token_element], axis=0)

        elif CONFIG_MARKETS[market]['version'] == 2:
            incentives_controller = web3.eth.contract(address=incentives_controller_address, abi=v2_rewards_abi)
            rewards_token_decimals = CONFIG_MARKETS[market]['rewards_token_decimals']
            rewards_token_balance = incentives_controller.functions.getUserUnclaimedRewards(
                    collector_contract, 
                ).call(block_identifier=block_height) / pow(10, rewards_token_decimals)
            rewards = pd.DataFrame(
                    [
                        {
                            'chain': chain,
                            'market': market,
                            'collector_contract': collector_contract.lower(),
                            'block_height': block_height,
                            'block_day': block_numbers_by_day['block_day'].values[0],
                            'rewards_token_address': CONFIG_MARKETS[market]['rewards_token'].lower(),
                            'rewards_token_symbol': CONFIG_MARKETS[market]['rewards_token_symbol'],
                            'accrued_rewards': rewards_token_balance,
                        }
                    ]
                )
        else:
            rewards = pd.DataFrame()
    
    rewards = standardise_types(rewards)

    context.add_output_metadata(
        {
            "num_records": len(rewards),
            "preview": MetadataValue.md(rewards.head().to_markdown()),
        }
    )

    return rewards

@asset(
    # partitions_def=v3_market_day_multipartition,
    partitions_def=market_day_multipartition,
    compute_kind="python",
    #group_name='data_lake',
    code_version="1",
    io_manager_key = 'data_lake_io_manager',
    ins={
        "block_numbers_by_day": AssetIn(key_prefix="financials_data_lake"),
    }
)
def user_lm_rewards_claimed(context, block_numbers_by_day):
    """
    Gets the total rewards claimed by users for:
        - The LM distribution contract (incentives controller)
        - The Safety Module 80:20 balancer pool deposits
        - The Safety Module stkAAVE deposits

    Uses an SQL query to sum the event values for each of the above contracts

    Args:
        context: dagster context object
        block_numbers_by_day: block numbers at the start and end of the day for the chain

    Returns:
        A dataframe with the rewards claimed by users foe each of the above contracts
        On markets that are not Aave V2 mainnet, this returns an empty dataframe

    """

    date, market = context.partition_key.split("|")
    chain = CONFIG_MARKETS[market]['chain']
    partition_datetime = datetime.strptime(date, '%Y-%m-%d')

    context.log.info(f"market: {market}")
    context.log.info(f"date: {date}")

    start_block = block_numbers_by_day.block_height.values[0]
    end_block = block_numbers_by_day.end_block.values[0]

    if market == 'ethereum_v2':
        # define the Flipside query
        sql = f"""
            with claims as (
            select 
            '{partition_datetime}' as block_day
            , contract_address as vault_address
            , case 
                when contract_address = '0xd784927ff2f95ba542bfc824c8a8a98f3495f6b5' then 'incentives_controller'
                when contract_address = '0xa1116930326d21fb917d5a27f1e9943a9595fb47' then 'balancer_pool'
                when contract_address = '0x4da27a545c0c5b758a6ba100e3a049001de870f5' then 'stkAAVE'
                end as contract_name
            , case 
                when contract_address = '0xd784927ff2f95ba542bfc824c8a8a98f3495f6b5' then 'incentives_controller'
                when contract_address = '0xa1116930326d21fb917d5a27f1e9943a9595fb47' then 'ecosystem_reserve'
                when contract_address = '0x4da27a545c0c5b758a6ba100e3a049001de870f5' then 'ecosystem_reserve'
                end as reward_vault
            , sum(event_inputs:amount) / 1e18 as amount
            from ethereum.core.fact_event_logs
            where event_name = 'RewardsClaimed'
            and block_number >= {start_block}
            and block_number < {end_block}
            and contract_address in ('0xd784927ff2f95ba542bfc824c8a8a98f3495f6b5', 
                                     '0xa1116930326d21fb917d5a27f1e9943a9595fb47', 
                                     '0x4da27a545c0c5b758a6ba100e3a049001de870f5')
            and tx_status = 'SUCCESS'
            group by block_day, contract_address, contract_name
            )

            , staging as (
            select 
                block_day
                , vault_address
                , reward_vault
                , case when contract_name = 'balancer_pool' then amount else 0 end as balancer_claims
                , case when contract_name = 'incentives_controller' then amount else 0 end as incentives_claims 
                , case when contract_name = 'stkAAVE' then amount else 0 end as stkaave_claims 
                from claims 
            )

            select 
                block_day
                , 'ethereum' as chain
                , '{market}' as market
                , case 
                    when reward_vault = 'incentives_controller' then '0xd784927ff2f95ba542bfc824c8a8a98f3495f6b5'
                    when reward_vault = 'ecosystem_reserve' then '0x25f2226b597e8f9514b3f68f00f494cf4f286491'
                  end as vault_address
                , reward_vault
                , lower('0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9') as token_address
                , sum(stkaave_claims) as sm_stkAAVE_claims
                , sum(balancer_claims) as sm_stkABPT_claims
                , sum(incentives_claims) as lm_aave_v2_claims
                
                from staging
                group by block_day, reward_vault 
                order by block_day, reward_vault
            """

        # initialise the query
        sdk = ShroomDK(FLIPSIDE_API_KEY)
        query_result = sdk.query(sql)
        
        rewards_claimed = pd.DataFrame(data=query_result.rows, columns=[x.lower() for x in query_result.columns])
        rewards_claimed.block_day = pd.to_datetime(rewards_claimed.block_day, utc=True)
        rewards_claimed.rename(columns={
            'sm_stkaave_claims': 'sm_stkAAVE_claims',
            'sm_stkabpt_claims': 'sm_stkABPT_claims',
            'lm_aave_v2_claims': 'lm_aave_v2_claims'
        }, inplace=True)



    else:
        rewards_claimed = pd.DataFrame()

    rewards_claimed = standardise_types(rewards_claimed)
    
    context.add_output_metadata(
        {
            "num_records": len(rewards_claimed),
            "preview": MetadataValue.md(rewards_claimed.head().to_markdown()),
        }
    )

    return rewards_claimed

@asset(
    compute_kind="python",
    #group_name='data_lake',
    io_manager_key = 'data_lake_io_manager',
    code_version="1",
    # key_prefix="financials_data_lake"
)
def internal_external_addresses(context) -> pd.DataFrame:
    """
    Returns a dataframe of internal and external addresses for Aave
    Used in the classification of transactions in the data warehouse
    Data is loaded from the public Google Cloud Storage bucket so
    no authentication is required
    
    Args:
        context: Dagster context object
    Returns:
        A dataframe with the internal and external addresses for Aave
    Raises:
        EnvironmentError: if the DAGSTER_DEPLOYMENT environment variable is not set correctly

    """

    from aave_data import dagster_deployment

    if dagster_deployment in ('local_filesystem','local_cloud'):
        url = 'https://storage.googleapis.com/llama_aave_dev_public/aave_internal_external_addresses.csv'
    elif dagster_deployment == 'prod':
        url = 'https://storage.googleapis.com/llama_aave_prod_public/aave_internal_external_addresses.csv'
    else:
        errmsg = "Environment variable DAGSTER_DEPLOYMENT must be set to either 'local_filesystem', 'local_cloud', or 'prod'"
        raise EnvironmentError(errmsg)


    internal_external = pd.read_csv(url, engine='python', quoting=3)
    internal_external = standardise_types(internal_external)

    context.add_output_metadata(
        {
            "num_records": len(internal_external),
            "preview": MetadataValue.md(internal_external.to_markdown()),
        }
    )

    return internal_external

@asset(
    compute_kind="python",
    #group_name='data_lake',
    io_manager_key = 'data_lake_io_manager',
    code_version="1",
)
def tx_classification(context) -> pd.DataFrame:
    """
    Returns a dataframe of transaction types for Aave
    Used in the classification of transactions in the data warehouse
    Data is loaded from the public Google Cloud Storage bucket so
    no authentication is required
    
    Args:
        context: Dagster context object
    Returns:
        A dataframe transaction classifications for Aave financials
    Raises:
        EnvironmentError: if the DAGSTER_DEPLOYMENT environment variable is not set correctly

    """

    from aave_data import dagster_deployment

    if dagster_deployment in ('local_filesystem','local_cloud'):
        url = 'https://storage.googleapis.com/llama_aave_dev_public/aave_financials_transaction_classification.csv'
    elif dagster_deployment == 'prod':
        url = 'https://storage.googleapis.com/llama_aave_prod_public/aave_financials_transaction_classification.csv'
    else:
        errmsg = "Environment variable DAGSTER_DEPLOYMENT must be set to either 'local_filesystem', 'local_cloud', or 'prod'"
        raise EnvironmentError(errmsg)


    tx = pd.read_csv(url, engine='python', quoting=3)
    tx = standardise_types(tx)

    context.add_output_metadata(
        {
            "num_records": len(tx),
            "preview": MetadataValue.md(tx.to_markdown()),
        }
    )

    return tx

@asset(
    compute_kind="python",
    #group_name='data_lake',
    io_manager_key = 'data_lake_io_manager',
    code_version="1",
)
def display_names(context) -> pd.DataFrame:
    """
    Returns a dataframe of display names for Aave financials
    Used in the classification of transactions in the data warehouse
    Data is loaded from the public Google Cloud Storage bucket so
    no authentication is required
    
    Args:
        context: Dagster context object
    Returns:
        A dataframe of display names for Aave financials
    Raises:
        EnvironmentError: if the DAGSTER_DEPLOYMENT environment variable is not set correctly

    """

    from aave_data import dagster_deployment

    if dagster_deployment in ('local_filesystem','local_cloud'):
        url = 'https://storage.googleapis.com/llama_aave_dev_public/financials_display_names.csv'
    elif dagster_deployment == 'prod':
        url = 'https://storage.googleapis.com/llama_aave_prod_public/financials_display_names.csv'
    else:
        errmsg = "Environment variable DAGSTER_DEPLOYMENT must be set to either 'local_filesystem', 'local_cloud', or 'prod'"
        raise EnvironmentError(errmsg)


    names = pd.read_csv(url, engine='python', quoting=3)
    names = standardise_types(names)

    context.add_output_metadata(
        {
            "num_records": len(names),
            "preview": MetadataValue.md(names.to_markdown()),
        }
    )

    return names


@asset(
    compute_kind="python",
    #group_name='data_lake',
    io_manager_key = 'data_lake_io_manager',
    code_version="1",
)
def balance_group_lists(context) -> pd.DataFrame:
    """
    Returns a dataframe of token groupings used in reports
    
    Args:
        context: Dagster context object
    Returns:
        A dataframe of token groupings used in reports
    Raises:
        EnvironmentError: if the DAGSTER_DEPLOYMENT environment variable is not set correctly

    """

    from aave_data import dagster_deployment

    if dagster_deployment in ('local_filesystem','local_cloud'):
        url = 'https://storage.googleapis.com/llama_aave_dev_public/aave_token_balance_group_lists.csv'
    elif dagster_deployment == 'prod':
        url = 'https://storage.googleapis.com/llama_aave_prod_public/aave_token_balance_group_lists.csv'
    else:
        errmsg = "Environment variable DAGSTER_DEPLOYMENT must be set to either 'local_filesystem', 'local_cloud', or 'prod'"
        raise EnvironmentError(errmsg)


    names = pd.read_csv(url, engine='python', quoting=3)
    names = standardise_types(names)

    context.add_output_metadata(
        {
            "num_records": len(names),
            # "preview": MetadataValue.md(names.to_markdown()),
        }
    )

    return names

#######################################
# Test assets for the io manager


# @asset(
#     partitions_def = DailyPartitionsDefinition(start_date=FINANCIAL_PARTITION_START_DATE),
#     compute_kind="python",
#     group_name='test_group',
#     io_manager_key = 'data_lake_io_manager'
# )
# def daily_asset(context):
#     return pd.DataFrame(
#         [
#             {
#             'string_col': context.partition_key
#             }
#         ]
#     )

# @asset(
#     partitions_def = DailyPartitionsDefinition(start_date=FINANCIAL_PARTITION_START_DATE),
#     compute_kind="python",
#     group_name='test_group',
#     io_manager_key = 'data_lake_io_manager'
# )
# def daily_asset_downstream(context, daily_asset):

#     ic(daily_asset)
#     return_value = daily_asset
#     return_value['additional_col'] = 'additional value'
#     raise ValueError('force crash')
#     return return_value

# @asset(
#     compute_kind="python",
#     group_name='test_group',
#     io_manager_key = 'data_lake_io_manager'
# )
# def no_partition_asset(context):
#     return pd.DataFrame(
#         [
#             {
#             'string_col': 'unpartitioned asset'            }
#         ]
#     )

# @asset(
#     compute_kind="python",
#     group_name='test_group',
#     io_manager_key = 'data_lake_io_manager'
# )
# def no_partition_asset_downstream(context, no_partition_asset):
#     ic(no_partition_asset)
#     return_value = no_partition_asset
#     return_value['additional_col'] = 'additional value'
#     raise ValueError('force crash')
#     return return_value
# 
################################