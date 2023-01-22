import json
# import sys
from datetime import datetime, timedelta, timezone  # , date, time, timedelta, timezone
import numpy as np
import pandas as pd
import requests
from ape import Contract, networks
from ape.types import ContractType
from dagster import (AssetIn,  # SourceAsset,; Output,
                     DailyPartitionsDefinition, ExperimentalWarning,
                     MetadataValue, #MultiPartitionKey,
                     MultiPartitionsDefinition, #Optional, PartitionKeyRange,
                     #PartitionMapping, PartitionsDefinition,
                     StaticPartitionsDefinition, asset, #op,
                     #LastPartitionMapping,
                     IdentityPartitionMapping,
                     FreshnessPolicy
)
from icecream import ic
# from subgrounds.subgrounds import Subgrounds
from eth_abi.abi import decode
from eth_utils.conversions import to_bytes

from financials.financials_config import * #pylint: disable=wildcard-import, unused-wildcard-import

from financials.resources.helpers import (
    get_market_tokens_at_block_messari,
    get_market_tokens_at_block_aave,
    get_token_transfers_from_covalent,
    get_erc20_balance_of,
    get_events_by_topic_hash_from_covalent
)

# # if not sys.warnoptions:
# import warnings
# with warnings.catch_warnings():
#     warnings.filterwarnings("ignore", category=ExperimentalWarning)
#     # warnings.simplefilter("ignore", category=ExperimentalWarning)

market_day_multipartition = MultiPartitionsDefinition(
    {
        "date": DailyPartitionsDefinition(start_date=FINANCIAL_PARTITION_START_DATE),
        "market": StaticPartitionsDefinition(list(CONFIG_MARKETS.keys())),
    }
)


@asset(
    partitions_def=market_day_multipartition,
    compute_kind="python",
    group_name='data_lake',
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
    group_name='data_lake',
    io_manager_key = 'data_lake_io_manager'
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

    tokens['block_day'] = block_day

    # overwrite ETH with WETH address
    tokens.loc[tokens.reserve == '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee', 'reserve'] = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'

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
    group_name='data_lake',
    io_manager_key = 'data_lake_io_manager'
)
def aave_oracle_prices_by_day(context, market_tokens_by_day) -> pd.DataFrame:  # type: ignore pylint: disable=W0621
    """Table of the token and aave oracle price for each market at each block height

    Uses ape to access an RPC node and call the oracle contract directly

    Args:
        context: dagster context object
        market_tokens_by_day: the output of market_tokens_by_day for a given market

    Returns:
        A dataframe market, token and the underlying reserve oracle price at the block height

    Raises:
        NotImplementedError: for markets on a network without ape support

    """
    #pylint: disable=E1137,E1101

    # market = context.partition_key.keys_by_dimension['market']
    # date = context.partition_key.keys_by_dimension['date']
    date, market = context.partition_key.split("|")
    # market = 'ethereum_v2'#'arbitrum_v3'
    # date = '2022-11-26'

    chain = CONFIG_MARKETS[market]['chain']

    if CONFIG_CHAINS[chain]['ape_ok'] is False:
        raise NotImplementedError(f"ape support not implemented for chain: {chain}")


    block_height = int(market_tokens_by_day.block_height.values[0])
    # token_source = CONFIG_MARKETS[market]['token_source']
    context.log.info(f"market: {market}")
    context.log.info(f"date: {date}")
    context.log.info(f"block_height: {block_height}")

    # pass in the reserves as a list to the ape contract


    # get the abi for the oracle contract from etherscan/polygonscan
    #  not using ape magic for this as harmony doesn't have a proper block explorer
    aave_version = CONFIG_MARKETS[market]['version']
    abi_url = CONFIG_ABI[aave_version]['abi_url_base'] + CONFIG_ABI[aave_version]['oracle_implementation']
    abi = json.loads(requests.get(abi_url, timeout=300).json()['result'])

    # collect the reserve tokens into a list for the ape contract call
    reserves = list(market_tokens_by_day['reserve'].values)

    # Get the eth price from the chainlink oracle if the Aave oracle price is denominated in eth
    if CONFIG_MARKETS[market]['oracle_base_currency'] == 'eth':
        with networks.parse_network_choice(CONFIG_CHAINS['ethereum']['ape_network_choice']) as ape_context:
            # get the price from the contract
            eth_address = '0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE'
            usd_address = '0x0000000000000000000000000000000000000348'
            chainlink_feed_registry = '0x47Fb2585D2C56Fe188D0E6ec628a38b74fCeeeDf'
            feed_registry = Contract(chainlink_feed_registry)
            eth_usd_price = feed_registry.latestAnswer(eth_address, usd_address, block_identifier = block_height) / 10**8
    else:
        eth_usd_price = 0

    #initialise ape
    with networks.parse_network_choice(CONFIG_CHAINS[chain]['ape_network_choice']) as ape_context:

        oracle_contract_type = ContractType(abi=abi, contractName='AaveOracle')  # type: ignore
        oracle_contract_address = CONFIG_MARKETS[market]['oracle']
        oracle = Contract(address=oracle_contract_address, contract_type=oracle_contract_type)
        

        if CONFIG_MARKETS[market]['oracle_base_currency'] == 'usd':
            try:
                base_currency_unit = oracle.BASE_CURRENCY_UNIT()
            except AttributeError:
                # some markets don't have this function - it fails on call (rwa)
                base_currency_unit = 100000000
            price_multiplier = 1 / base_currency_unit
        elif CONFIG_MARKETS[market]['oracle_base_currency'] == 'wei':
            price_multiplier = eth_usd_price / 1e18
        else:
            price_multiplier = 1

        # ic(price_multiplier)
        response = oracle.getAssetsPrices(reserves, block_identifier = block_height)
        # ic(response)


    # create a dataframe with the price
    return_val = market_tokens_by_day[['reserve','symbol','market','block_height','block_day']].copy()
    return_val['usd_price'] = pd.Series(response, name='usd_price') * price_multiplier # type: ignore
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
    group_name='data_lake',
    io_manager_key = 'data_lake_io_manager'
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
    start_block = block_numbers_by_day.block_height.values[0]
    end_block = block_numbers_by_day.end_block.values[0]
    # market = context.partition_key.keys_by_dimension['market']
    # date = context.partition_key.keys_by_dimension['date']
    date, market = context.partition_key.split("|")
    chain = CONFIG_MARKETS[market]['chain']
    chain_id = CONFIG_CHAINS[chain]['chain_id']

    # special case for V1, collector contract changed
    if market == 'ethereum_v1':
        partition_datetime = datetime.strptime(date, '%Y-%m-%d')
        if partition_datetime > CONFIG_MARKETS[market]['collector_change_date']:
            collector = CONFIG_MARKETS[market]['collector_v2']
        else:
            collector = CONFIG_MARKETS[market]['collector']
    else:
        collector = CONFIG_MARKETS[market]['collector']
    

    context.log.info(f"market: {market}")
    context.log.info(f"date: {date}")

    transfers = pd.DataFrame()
    for row in market_tokens_by_day.itertuples():
        # ic(row)
        if market == 'ethereum_v1':
            token = row.reserve
        else:
            token = row.atoken
        row_transfers = get_token_transfers_from_covalent(
            start_block,
            end_block,
            chain_id,
            collector,
            token
        )
        context.log.info(f"atoken: {row.atoken_symbol}")
        transfers = pd.concat([transfers, row_transfers]).reset_index(drop=True)

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
    group_name='data_lake',
    io_manager_key = 'data_lake_io_manager'
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
                row_transfers['wallet'] = wallet
                transfers = pd.concat([transfers, row_transfers]).reset_index(drop=True)
                
                context.log.info(f"{wallet}: {token}")
            
        #fix types here, ints getting mangled to floats somewhere
        transfers.transfers_from_address = transfers.transfers_from_address.str.lower() 
        transfers.transfers_to_address = transfers.transfers_to_address.str.lower()
        transfers.transfers_contract_address = transfers.transfers_contract_address.str.lower()

        transfers.transfers_from_address = transfers.transfers_from_address.astype(pd.StringDtype()) # type: ignore
        transfers.transfers_to_address = transfers.transfers_to_address.astype(pd.StringDtype()) # type: ignore
        transfers.transfers_contract_address = transfers.transfers_contract_address.astype(pd.StringDtype()) # type: ignore
        transfers.transfers_contract_decimals = transfers.transfers_contract_decimals.astype('int64') 
        transfers.start_block = transfers.start_block.astype('int64')
        transfers.end_block = transfers.end_block.astype('int64')

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
    group_name='data_lake',
    io_manager_key = 'data_lake_io_manager'
)
def collector_atoken_balances_by_day(context, market_tokens_by_day, block_numbers_by_day) -> pd.DataFrame:  # type: ignore pylint: disable=W0621
    """
    Table of the aave market token balances in the collector contracts for each market

    Uses RPC calls to balanceOf() via ape

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
        if partition_datetime > CONFIG_MARKETS[market]['collector_change_date']:
            collector = CONFIG_MARKETS[market]['collector_v2']
        else:
            collector = CONFIG_MARKETS[market]['collector']
    else:
        collector = CONFIG_MARKETS[market]['collector']

    context.log.info(f"market: {market}")
    context.log.info(f"date: {date}")

    balances = pd.DataFrame()
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
      
        output_row = {
                    'collector': collector, 
                    'market': market,
                    'token': token, 
                    'symbol': symbol,
                    'block_height': block_height, 
                    'block_day': partition_datetime.replace(tzinfo=timezone.utc),
                    'balance': row_balance
                }

        balance_row = pd.DataFrame(output_row, index=[0])
        context.log.info(f"atoken: {row.atoken_symbol}")
        balances = pd.concat([balances, balance_row]).reset_index(drop=True)  # type: ignore

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
    group_name='data_lake',
    io_manager_key = 'data_lake_io_manager'
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
                    'wallet': wallet_address, 
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
    ins={
        "market_tokens_by_day":
            AssetIn(
                key="market_tokens_by_day",
                partition_mapping=IdentityPartitionMapping()
            )
    },
    compute_kind="python", 
    group_name='data_lake',
    io_manager_key = 'data_lake_io_manager'

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
    block_height = market_tokens_by_day.block_height.values[0]
    chain = CONFIG_MARKETS[market]['chain']
    partition_datetime = datetime.strptime(date, '%Y-%m-%d')

    context.log.info(f"market: {market}")
    context.log.info(f"date: {date}")
    context.log.info(f"block_height: {block_height}")

    fees = pd.DataFrame()

    if CONFIG_MARKETS[market]['version'] == 3:

        # initialise Ape
        # a minimal ABI supporting getReserveData only
        abi = [
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
        
        provider_address = CONFIG_MARKETS[market]['protocol_data_provider']
        #initialise ape 
        with networks.parse_network_choice(CONFIG_CHAINS[chain]['ape_network_choice']) as ape_context:

            provider_contract_type = ContractType(abi=abi, contractName = 'AaveProtocolDataProvider') # type: ignore
            provider = Contract(address=provider_address, contract_type=provider_contract_type) # type: ignore
            
            for row in market_tokens_by_day.itertuples():
                reserve_data = provider.getReserveData(row.reserve, block_identifier=int(block_height))
                # ic(row.symbol)
                # ic(reserve_data)
                accrued_fees = reserve_data['accruedToTreasuryScaled'] / pow(10, row.decimals)
                # ic(accrued_fees)
                output_row = {
                    'market': market,
                    'reserve': row.reserve,
                    'symbol': row.symbol,
                    'atoken': row.atoken,
                    'atoken_symbol': row.atoken_symbol,
                    'block_height': block_height,
                    'block_day': partition_datetime.replace(tzinfo=timezone.utc),
                    'accrued_fees': accrued_fees
                }
                fees_row = pd.DataFrame(output_row, index=[0])
                context.log.info(f"accrued_fees: {row.symbol} on {market}")
                fees = pd.concat([fees, fees_row]).reset_index(drop=True)

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
    ins={
        "market_tokens_by_day":
            AssetIn(
                key="market_tokens_by_day",
                partition_mapping=IdentityPartitionMapping()
            ),
    },
    compute_kind="python",
    group_name='data_lake',
    io_manager_key = 'data_lake_io_manager'

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

    context.add_output_metadata(
            {
                "num_records": len(minted_to_treasury),
                "preview": MetadataValue.md(minted_to_treasury.head().to_markdown()),
            }
        )

    return(minted_to_treasury)
