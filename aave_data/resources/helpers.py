from typing import Optional  # , date, time, timedelta, timezone

import pandas as pd
import requests
import httpx
import asyncio
from dagster import op
from icecream import ic
from subgrounds.subgrounds import Subgrounds
from io import StringIO
from web3 import Web3
from time import sleep
from subgrounds.pagination.pagination import PaginationError
from random import randint
from multicall import Call, Multicall


from aave_data.resources.financials_config import * #pylint: disable=wildcard-import, unused-wildcard-import

INITIAL_RETRY = 0.01 #seconds
MAX_RETRIES = 10

def get_market_tokens_at_block_messari(
        market: str,
        block_height: int,
        version_config: dict) -> pd.DataFrame:
    """Gets Aave token details for market at a given block height

    Uses the subgrounds module.
    Uses the Messari subgraph

    Args:
        market: the market name as per the config object key
        block_height: block height for the time travel subgraph call
        version_config: pipeline config dict, with market as key and each value having a
                    subgraph_url and pool_id property

    Returns:
        A dataframe with details from the subgraph

    """
    #pylint: disable=E1137,E1101

    subgraph_url = version_config[market]['messari_subgraph']

    sg = Subgrounds()
    subgraph = sg.load_subgraph(subgraph_url)


    markets = subgraph.Query.markets(  # type: ignore
        block={'number': block_height},
    )
    try:
        subgraph_data = sg.query_df([
            markets.inputToken,
            markets.outputToken
        ])
    except PaginationError as e:
        if 'data starting at block number' in str(e):
            # ic(f'Pagination error: {e}')
            subgraph_data =  pd.DataFrame()
        else:
            raise e
    
    if not subgraph_data.empty:
        subgraph_data.columns = [col.removeprefix( # type: ignore
                'markets_').lower() for col in subgraph_data.columns] # type: ignore
        subgraph_data.rename(columns={  # type: ignore
            'inputtoken_id': 'reserve',
            'inputtoken_name': 'name',
            'inputtoken_symbol': 'symbol',
            'inputtoken_decimals': 'decimals',
            'outputtoken_id': 'atoken',
            'outputtoken_symbol': 'atoken_symbol'
        },
            inplace=True
        )
        subgraph_data = subgraph_data[['reserve','name','symbol','decimals','atoken','atoken_symbol']]
        subgraph_data['pool'] = version_config[market]['pool'] # type: ignore
        subgraph_data['market'] = market # type: ignore
        subgraph_data['atoken_decimals'] = subgraph_data.decimals # type: ignore
        subgraph_data['block_height'] = block_height # type: ignore

    #pylint: enable=E1137, E1101
    return subgraph_data  # type: ignore




def get_market_tokens_at_block_aave(
        market: str,
        block_height: int,
        markets_config: dict) -> pd.DataFrame:
    """Gets Aave token details for market at a given block height

    Uses the subgrounds module.
    Uses official Aave V1/V2/V3 subgraph

    Args:
        market: the market name as per the config object key
        block_height: block height for the time travel subgraph call
        markets_config: pipeline config dict, with market as key and each value having a
                    subgraph_url and pool_id property

    Returns:
        A dataframe with details from the subgraph

    """
    #pylint: disable=E1137,E1101

    subgraph_url = markets_config[market]['subgraph']
    pool_id = markets_config[market]['pool']
    ic(subgraph_url)
    ic(pool_id)

    sg = Subgrounds()
    subgraph = sg.load_subgraph(subgraph_url)

    if markets_config[market]['version'] == 3:
        pools = subgraph.Query.pools(  # type: ignore
                block={'number': block_height},
                where=[
                    subgraph.Pool.id == markets_config[market]['pool_address_provider']  # type: ignore
                ]
            )
    else:
        pools = subgraph.Query.pools(  # type: ignore
                block={'number': block_height},
                where=[
                    subgraph.Pool.lendingPool == pool_id,  # type: ignore
                ]
            )
    try:
        subgraph_data = sg.query_df([
                pools.reserves.underlyingAsset,
                pools.reserves.name,
                pools.reserves.symbol,
                pools.reserves.decimals,
                pools.reserves.aToken.id
            ])
    except PaginationError as e:
        if 'data starting at block number' in str(e):
            # ic(f'Pagination error: {e}')
            subgraph_data =  pd.DataFrame()
        else:
            raise e

    if not subgraph_data.empty:
        subgraph_data.columns = [col.removeprefix( # type: ignore
            'pools_reserves_').lower() for col in subgraph_data.columns] # type: ignore
        subgraph_data['pool'] = markets_config[market]['pool'] # type: ignore
        subgraph_data['market'] = market # type: ignore
        subgraph_data['atoken_decimals'] = subgraph_data.decimals # type: ignore
        subgraph_data['block_height'] = block_height # type: ignore
        subgraph_data.rename(columns={  # type: ignore
            'pools_lendingpool': 'lending_pool',
            'underlyingasset': 'reserve',
            'atoken_id': 'atoken'
        },
            inplace=True
        )
        subgraph_data['atoken_symbol'] = markets_config[market]['atoken_prefix'] + subgraph_data.symbol  # type: ignore

    #pylint: enable=E1137,E1101
    return subgraph_data  # type: ignore


def get_token_transfers_from_covalent(start_block: int,
                             end_block: int,
                             chain_id: int,
                             address: str,
                             token: str) -> pd.DataFrame:
    """Queries the Covalent API
        to get token transfers between the block heights given
    Args:
        start_block: int
        end_block: int 
        chain_id: int 
        address:  str address of wallet/contract 
        token: str address of token transferred
    Returns:
        pd.DataFrame of token transfer amounts
    """    
    covalent_api_url = f'https://api.covalenthq.com/v1/{chain_id}/address/{address}/transfers_v2/?quote-currency=USD&format=CSV'\
                        + f'&starting-block={start_block}&ending-block={end_block}'\
                        + f'&page-size=10000000&contract-address={token}&key={COVALENT_KEY}'

    # print(covalent_api_url)
    # ic(covalent_api_url)
    # response = requests.get(covalent_api_url)#, auth=(API_KEY, API_KEY))
    #pylint: disable=E1137,E1101
    i = 0
    delay = INITIAL_RETRY
    while True:
        response = requests.get(covalent_api_url, timeout=300)#, auth=(API_KEY, API_KEY))
        if response.status_code == requests.codes.ok:
            break
        i += 1
        if i > MAX_RETRIES:
            raise ValueError(f"Covalent token transfers API error count {i}, last error {response.status_code} {response.reason}.  Bailing out.")
        rand_delay = randint(0, 250) / 1000
        sleep(delay + rand_delay)
        delay *= 2
        print(f"Request Error {response.status_code} {response.reason}, retry count {i}")
    #pylint: enable=E1137,E1101
  
    if len(response.text) > 0:

        transfers = pd.read_csv(StringIO(response.text))

        # transfers.info()
        if transfers.empty:
            return transfers        

        transfers_subset = transfers[['block_signed_at'
                                    ,'transfers_transfer_type'
                                    ,'transfers_from_address'
                                    ,'transfers_to_address'
                                    ,'transfers_contract_address'
                                    ,'transfers_contract_name'
                                    ,'transfers_contract_decimals'
                                    ,'transfers_contract_ticker_symbol'
                                    ,'transfers_delta'
                                    ]].copy()

        transfers_subset.block_signed_at = pd.to_datetime(transfers_subset.block_signed_at)
        
        #use float to handle big integers (int64 not big enough)
        transfers_subset.transfers_delta = transfers_subset.transfers_delta.astype('float')

        # change in API behaviour at block 11883402 on eth - from address goes from 0x0 to null
        transfers_subset.transfers_from_address = transfers_subset.transfers_from_address.fillna('0x0000000000000000000000000000000000000000')

        #transform & group the data by day
        transfers_subset = (transfers_subset
                    .assign(amount_transferred=transfers_subset['transfers_delta']/pow(10,transfers_subset['transfers_contract_decimals']))
                    .assign(block_day=transfers_subset.block_signed_at.dt.floor('D'))
                    .drop(['transfers_delta','block_signed_at'], axis=1)
                    .groupby(['transfers_transfer_type'
                                ,'transfers_from_address'
                                ,'transfers_to_address'
                                ,'transfers_contract_address'
                                ,'transfers_contract_name'
                                ,'transfers_contract_decimals'
                                ,'transfers_contract_ticker_symbol'
                                ,'block_day'])
                    .sum()
                    .reset_index()
        )
        transfers_subset['start_block'] = start_block
        transfers_subset['end_block'] = end_block
        transfers_subset.transfers_contract_decimals = transfers_subset.transfers_contract_decimals.astype('Int64')
        transfers_subset.start_block = transfers_subset.start_block.astype('Int64')
        transfers_subset.end_block = transfers_subset.end_block.astype('Int64')
        transfers_subset.transfers_contract_decimals = transfers_subset.transfers_contract_decimals.astype('Int64')
        transfers_subset.rename(columns={'transfers_contract_ticker_symbol': 'transfers_contract_symbol'}, inplace=True)
        transfers_subset = standardise_types(transfers_subset)
    else:
        transfers_subset = pd.DataFrame()

    return transfers_subset


def get_token_transfers_from_alchemy(start_block: int,
                             end_block: int,
                             block_day: datetime,
                             chain: str,
                             address: str,
                             token: str) -> pd.DataFrame:
    """Queries the Covalent API
        to get token transfers between the block heights given
    Args:
        start_block: int
        end_block: int 
        chain: str
        address:  str address of wallet/contract 
        token: str address of token transferred
    Returns:
        pd.DataFrame of token transfer amounts
    """    

    alchemy_api_url = CONFIG_CHAINS[chain]['web3_rpc_url']
    if not 'alchemy.com' in alchemy_api_url:
        raise ValueError(f"Config element web3_rpc_url for this chain is not an alchemy RPC endpoint: {alchemy_api_url}")

    # do the transfers to the address (IN)
    payload = {
        "id": 1,
        "jsonrpc": "2.0",
        "method": "alchemy_getAssetTransfers",
        "params": [
            {
                "fromBlock": Web3.to_hex(start_block),
                "toBlock": Web3.to_hex(end_block),
                "toAddress": address,
                "contractAddresses": [token],
                "category": ["erc20"],
                "withMetadata": False,
                "excludeZeroValue": True,
                "maxCount": '0x3e8' # max is 1000
            }
        ]
    }
    headers = {
        "accept": "application/json",
        "content-type": "application/json"
    }

    #pylint: disable=E1137,E1101
    # get the API response with exponential backoff for retries
    i = 0
    delay = INITIAL_RETRY
    while True:
        response = requests.post(alchemy_api_url, json=payload, headers=headers)
        if response.status_code == requests.codes.ok:
            break
        i += 1
        if i > MAX_RETRIES:
            raise ValueError(f"Alchemy token transfers API error count {i}, last error {response.status_code} {response.reason}.  Bailing out.")
        rand_delay = randint(0, 250) / 1000
        sleep(delay + rand_delay)
        delay *= 2
        print(f"Request Error {response.status_code} {response.reason}, retry count {i}")
    #pylint: enable=E1137,E1101
  
    transfers_in = pd.json_normalize(response.json()['result']['transfers'])

    # handle paginated results if > 1000 returned
    while 'pageKey' in response.json()['result'].keys():
        payload['params'][0]['pageKey'] = response.json()['result']['pageKey']
        i = 0
        while True:
            response = requests.post(alchemy_api_url, json=payload, headers=headers)
            if response.status_code == requests.codes.ok:
                break
            i += 1
            if i > MAX_RETRIES:
                raise ValueError(f"Alchemy token transfers API error count {i}, last error {response.status_code} {response.reason}.  Bailing out.")
            rand_delay = randint(0, 250) / 1000
            sleep(delay + rand_delay)
            delay *= 2
            print(f"Request Error {response.status_code} {response.reason}, retry count {i}")

        next_bit = pd.json_normalize(response.json()['result']['transfers'])
        transfers_in = pd.concat([transfers_in, next_bit])

# do the transfers from the address (OUT)
    payload = {
        "id": 1,
        "jsonrpc": "2.0",
        "method": "alchemy_getAssetTransfers",
        "params": [
            {
                "fromBlock": Web3.to_hex(start_block),
                "toBlock": Web3.to_hex(end_block),
                "fromAddress": address,
                "contractAddresses": [token],
                "category": ["erc20"],
                "withMetadata": False,
                "excludeZeroValue": True,
                "maxCount": '0x3e8' # max is 1000
            }
        ]
    }
    headers = {
        "accept": "application/json",
        "content-type": "application/json"
    }

    #pylint: disable=E1137,E1101
    # get the API response with exponential backoff for retries
    i = 0
    delay = INITIAL_RETRY
    while True:
        response = requests.post(alchemy_api_url, json=payload, headers=headers)
        if response.status_code == requests.codes.ok:
            break
        i += 1
        if i > MAX_RETRIES:
            raise ValueError(f"Alchemy token transfers API error count {i}, last error {response.status_code} {response.reason}.  Bailing out.")
        rand_delay = randint(0, 250) / 1000
        sleep(delay + rand_delay)
        delay *= 2
        print(f"Request Error {response.status_code} {response.reason}, retry count {i}")
    #pylint: enable=E1137,E1101
  
    transfers_out = pd.json_normalize(response.json()['result']['transfers'])
    # ic(response.json()['result']['pageKey'])
    # handle paginated results if > 1000 returned
    while 'pageKey' in response.json()['result'].keys():
        payload['params'][0]['pageKey'] = response.json()['result']['pageKey']
        i = 0
        while True:
            response = requests.post(alchemy_api_url, json=payload, headers=headers)
            if response.status_code == requests.codes.ok:
                break
            i += 1
            if i > MAX_RETRIES:
                raise ValueError(f"Alchemy token transfers API error count {i}, last error {response.status_code} {response.reason}.  Bailing out.")
            rand_delay = randint(0, 250) / 1000
            sleep(delay + rand_delay)
            delay *= 2
            print(f"Request Error {response.status_code} {response.reason}, retry count {i}")

            next_bit = pd.json_normalize(response.json()['result']['transfers'])
            transfers_out = pd.concat([transfers_out, next_bit])

    
    # add the directions and merge
    if not transfers_in.empty:
        transfers_in['transfers_transfer_type'] = 'IN'
    if not transfers_out.empty:
        transfers_out['transfers_transfer_type'] = 'OUT'

    transfers = pd.concat([transfers_in, transfers_out])

    if not transfers.empty:
        # rename the columns & organise the data
        transfers = transfers.rename(
            columns={
                'from': 'transfers_from_address',
                'to': 'transfers_to_address',
                'rawContract.address': 'transfers_contract_address',
                'rawContract.decimal': 'transfers_contract_decimals',
                'asset': 'transfers_contract_symbol',
                'value': 'amount_transferred'
            }
        )
        transfers['transfers_contract_name'] = transfers.transfers_contract_symbol
        try:
            transfers.transfers_contract_decimals = transfers.transfers_contract_decimals.apply(lambda x: Web3.to_int(hexstr=x))
        except TypeError:
            # catch the edge case where a new token doesn't return the metadata from the API.  Raise & handle in the calling function.
            raise
        transfers['block_day'] = block_day
        transfers['start_block'] = start_block
        transfers['end_block'] = end_block

        transfers = transfers[
            [
                'transfers_transfer_type',
                'transfers_from_address',
                'transfers_to_address',
                'transfers_contract_address',
                'transfers_contract_name',
                'transfers_contract_decimals',
                'transfers_contract_symbol',
                'block_day',
                'amount_transferred',
                'start_block',
                'end_block'
            ]
        ]

        # group and sum
        transfers = transfers.groupby([
                'transfers_transfer_type',
                'transfers_from_address',
                'transfers_to_address',
                'transfers_contract_address',
                'transfers_contract_name',
                'transfers_contract_decimals',
                'transfers_contract_symbol',
                'block_day',
                'start_block',
                'end_block'
            ]
        ).sum().reset_index()
        transfers = standardise_types(transfers)
        return transfers
    else:
        return pd.DataFrame()
    

def get_erc20_balance_of(
                address: str,
                token: str,
                token_decimals: int,
                chain: str,
                block_height: int = 0
                ) -> float:
    """
    Uses web3.py to get the balance of an ERC20 token for a given address

    Assumes an ERC20 compliant token with a balanceOf function

    Args:
        address: wallet address to find the balance for
        token: token address to find the balance of
        token_decimals: already in the database so passed in to save an RPC call
        chain: string from CONFIG_CHAINS

    Returns:
        float: balance of the token for the address, decimals adjusted


    """    
       
    #initialise Web3 and token contract
    w3 = Web3(Web3.HTTPProvider(CONFIG_CHAINS[chain]['web3_rpc_url']))
    token_contract = w3.eth.contract(address=Web3.to_checksum_address(token), abi=ERC20_ABI)

    if block_height > 0:
        i = 0
        delay = INITIAL_RETRY
        while True:
            try:
                balance_raw = token_contract.functions.balanceOf(Web3.to_checksum_address(address)).call(block_identifier=int(block_height))
                break
            except Exception as e:
                i += 1
                if i > MAX_RETRIES:
                    raise ValueError(f"RPC error count {i}, last error {str(e)}.  Bailing out.")
                rand_delay = randint(0, 250) / 1000
                sleep(delay + rand_delay)
                delay *= 2
                print(f"Request Error {str(e)}, retry count {i}")
    else:
        i = 0
        delay = INITIAL_RETRY
        while True:
            try:
                balance_raw = token_contract.functions.balanceOf(Web3.to_checksum_address(address)).call()
                break
            except Exception as e:
                i += 1
                if i > MAX_RETRIES:
                    raise ValueError(f"RPC error count {i}, last error {str(e)}.  Bailing out.")
                rand_delay = randint(0, 250) / 1000
                sleep(delay + rand_delay)
                delay *= 2
                print(f"Request Error {str(e)}, retry count {i}")
        

    balance = balance_raw / pow(10, token_decimals)

    return balance

def get_scaled_balance_of(
                address: str,
                token: str,
                token_decimals: int,
                chain: str,
                block_height: int = 0
                ) -> float:
    """
    Uses web3.py to get the balance of an ERC20 token for a given address

    Assumes an aave atoken token with a scaledBalanceOf function

    Args:
        address: wallet address to find the balance for
        token: token address to find the balance of
        token_decimals: already in the database so passed in to save an RPC call
        chain: string from CONFIG_CHAINS

    Returns:
        float: balance of the token for the address, decimals adjusted


    """    
       
    #initialise Web3 and token contract
    w3 = Web3(Web3.HTTPProvider(CONFIG_CHAINS[chain]['web3_rpc_url']))
    token_contract = w3.eth.contract(address=Web3.to_checksum_address(token), abi=ERC20_ABI)

    if block_height > 0:
        i = 0
        delay = INITIAL_RETRY
        while True:
            try:
                balance_raw = token_contract.functions.scaledBalanceOf(Web3.to_checksum_address(address)).call(block_identifier=int(block_height))
                break
            except Exception as e:
                i += 1
                if i > MAX_RETRIES:
                    raise ValueError(f"RPC error count {i}, last error {str(e)}.  Bailing out.")
                rand_delay = randint(0, 250) / 1000
                sleep(delay + rand_delay)
                delay *= 2
                print(f"Request Error {str(e)}, retry count {i}")
    else:
        i = 0
        delay = INITIAL_RETRY
        while True:
            try:
                balance_raw = token_contract.functions.scaledBalanceOf(Web3.to_checksum_address(address)).call()
                break
            except Exception as e:
                i += 1
                if i > MAX_RETRIES:
                    raise ValueError(f"RPC error count {i}, last error {str(e)}.  Bailing out.")
                rand_delay = randint(0, 250) / 1000
                sleep(delay + rand_delay)
                delay *= 2
                print(f"Request Error {str(e)}, retry count {i}")

    balance = balance_raw / pow(10, token_decimals)

    return balance

    
def get_events_by_topic_hash_from_covalent(start_block: int,
                             end_block: int,
                             chain_id: int,
                             topic_hash: str,
                             address: Optional[str] = None
                             ) -> pd.DataFrame:
    """Queries the Covalent API
        to get events by topic_hash for a given address
    Args:
        start_block: int
        end_block: int 
        chain_id: int 
        address:  str address of contract.  Optional.  If not provided, all events for the topic_hash are returned
        topic_hash: keccak_256 hash of the event signature https://emn178.github.io/online-tools/keccak_256.html
    Returns:
        pd.DataFrame of events.  No decoding of output data is done.  Data is filtered for address
    """

    covalent_api_url = f'https://api.covalenthq.com/v1/{chain_id}/events/topics/{topic_hash}/?quote-currency=USD&format=CSV'\
                        + f'&starting-block={start_block}&ending-block={end_block}'\
                        + f'&page-size=10000000&key={COVALENT_KEY}'


    # ic(covalent_api_url)

    #pylint: disable=E1137,E1101
    i = 0
    delay = INITIAL_RETRY
    while True:
        response = requests.get(covalent_api_url, timeout=300)#, auth=(API_KEY, API_KEY))
        if response.status_code == requests.codes.ok:
            break
        i += 1
        if i > MAX_RETRIES:
            raise ValueError(f"Covalent token transfers API error count {i}, last error {response.status_code} {response.reason}.  Bailing out.")
        rand_delay = randint(0, 250) / 1000
        sleep(delay + rand_delay)
        delay *= 2
        print(f"Request Error {response.status_code} {response.reason}, retry count {i}")
    #pylint: enable=E1137,E1101

    events = pd.DataFrame()
    if len(response.text) > 0:

        events = pd.read_csv(StringIO(response.text))

        if events.empty:
            return events

        events.block_signed_at = pd.to_datetime(events.block_signed_at, utc=True)

        # filter for address if provided
        if address is not None:
            address = address.lower()
            events = events[(events['sender_address'] == address)]

        # ic(events)

    return events

def standardise_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Sets the dtypes for a dataframe to match the convention used in the database

    object (string) -> pd.StringDtype()
    int64 -> pd.Int64Dtype()
    float64 -> pd.Float64Dtype() 
    datetimes -> apply tz_localize('UTC')
    addresses -> force to lowercase

    Args:
        df (pd.DataFrame): dataframe to set dtypes for

    Returns:
        pd.DataFrame: identical dataframe with dtypes set
    """

    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].astype(pd.StringDtype())
        elif df[col].dtype == 'int64':
            df[col] = df[col].astype(pd.Int64Dtype())
        elif df[col].dtype == 'float64':
            df[col] = df[col].astype(pd.Float64Dtype())
        elif df[col].dtype == 'datetime64[ns]':
            df[col] = df[col].dt.tz_localize('UTC')

        if df[col].dtype == pd.StringDtype() and df[col].str.startswith('0x').any():
            df[col] = df[col].str.lower()
        
    return df

def get_raw_reserve_data(
    market: str,
    chain: str,
    reserve: str,
    decimals: int,
    block_height: int,
) -> dict:
    """
    Calls protocol data provider contracts at the specified block height
    and returns a dataframe of reserve configuration and state data.
    Uses multicall to minimise RPC hits.
    Uses appropriate call set for the aave version.


    Args:
        market:
        chain:
        reserve:
        decimals:
        block_height:


    Returns:
        pd.DataFrame: identical dataframe with dtypes set

    Raises:
        ValueError: if the market version is not set in config

    """

    #initialise Web3 and variables
    w3 = Web3(Web3.HTTPProvider(CONFIG_CHAINS[chain]['web3_rpc_url']))

    lending_pool = CONFIG_MARKETS[market]['pool']
    core = '0x3dfd23A6c5E8BbcFc9581d2E864a68feb6a076d3' # aave_v1 only

    if CONFIG_MARKETS[market]['version'] == 1:
        data_provider = lending_pool
    else:
        data_provider = CONFIG_MARKETS[market]['protocol_data_provider']

    if (CONFIG_MARKETS[market]['version'] == 1) and (reserve == '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'):
        reserve = '0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE'

    # check if the reserve exists in the pool (skip for aave v1, all reserves are old enough)
    if CONFIG_MARKETS[market]['version'] == 1:
        check_address = '0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE'
    else:
        tokens_check = Call(
            data_provider, ['getReserveTokensAddresses(address)((address,address,address))', reserve],
            [['reserve_tokens_addresses', None]]
            ,_w3 = w3
            ,block_id = block_height)()
        check_address = tokens_check['reserve_tokens_addresses'][0]

    if check_address == '0x0000000000000000000000000000000000000000':
        multicall_output = dict()
    else:

        # return value handler functions
        def reserve_config_handler(value):
            return {
                'decimals': value[0],
                'ltv': value[1] / 1e4,
                'liquidation_threshold': value[2] / 1e4,
                'liquidation_bonus': value[3] / 1e4,
                'reserve_factor': value[4] / 1e4,
                'usage_as_collateral_enabled': value[5],
                'borrowing_enabled': value[6],
                'stable_borrow_rate_enabled': value[7],
                'is_active': value[8],
                'is_frozen': value[9],
            }
        
        def v1_reserve_config_handler(value):
            return {
                'decimals': decimals,
                'ltv': value[0] / 100,
                'liquidation_threshold': value[1] / 100,
                'liquidation_bonus': value[2] / 100,
                'reserve_factor': 0.09,
                'usage_as_collateral_enabled': value[4],
                'borrowing_enabled': value[5],
                'stable_borrow_rate_enabled': value[6],
                'is_active': value[7],
            }
        
        def v3_reserve_data_handler(value):
            return {
                'unbacked_atokens': value[0] / 10 ** decimals,
                'scaled_accrued_to_treasury': value[1] / 10 ** decimals,
                'atoken_supply': value[2] / 10 ** decimals,
                'stable_debt': value[3] / 10 ** decimals,
                'variable_debt': value[4] / 10 ** decimals,
                'liquidity_rate': value[5] / 10 ** 27,
                'variable_borrow_rate': value[6] / 10 ** 27,
                'stable_borrow_rate': value[7] / 10 ** 27,
                'average_stable_rate': value[8] / 10 ** 27,
                'liquidity_index': value[9] / 10 ** 27,
                'variable_borrow_index': value[10] / 10 ** 27,
                'last_update_timestamp': datetime.utcfromtimestamp(value[11]),
                'available_liquidity': (value[2] - value[3] - value[4]) / 10 ** decimals,
            }
        
        def v2_reserve_data_handler(value):
            return {
                'available_liquidity': value[0] / 10 ** decimals,
                'stable_debt': value[1] / 10 ** decimals,
                'variable_debt': value[2] / 10 ** decimals,
                'liquidity_rate': value[3] / 10 ** 27,
                'variable_borrow_rate': value[4] / 10 ** 27,
                'stable_borrow_rate': value[5] / 10 ** 27,
                'average_stable_rate': value[6] / 10 ** 27,
                'liquidity_index': value[7] / 10 ** 27,
                'variable_borrow_index': value[8] / 10 ** 27,
                'last_update_timestamp': datetime.utcfromtimestamp(value[9]),
                'atoken_supply': (value[0] + value[1] + value[2]) / 10 ** decimals,
                'scaled_accrued_to_treasury': 0, #match v3 schema
                'unbacked_atokens': 0,
            }
        
        def v1_reserve_data_handler(value):
            return {
                'atoken_supply': value[0] / 10 ** decimals,
                'available_liquidity': value[1] / 10 ** decimals,
                'stable_debt': value[2] / 10 ** decimals,
                'variable_debt': value[3] / 10 ** decimals,
                'liquidity_rate': value[4] / 10 ** 27,
                'variable_borrow_rate': value[5] / 10 ** 27,
                'stable_borrow_rate': value[6] / 10 ** 27,
                'average_stable_rate': value[7] / 10 ** 27,
                'liquidity_index': value[9] / 10 ** 27,
                'variable_borrow_index': value[10] / 10 ** 27,
                'last_update_timestamp': datetime.utcfromtimestamp(value[12]),
                'scaled_accrued_to_treasury': 0, #match v3 schema
                'unbacked_atokens': 0,
            }
            
        reserve_caps_schema = [['borrow_cap', None], ['supply_cap', None]]

        v3_empty_reserve_data = {
            "reserve_emode_category": 0,
            "borrow_cap": 0,
            "supply_cap": 0,
            "is_paused": False,
            "siloed_borrowing": False,
            "liquidation_protocol_fee": 0,
            "unbacked_mint_cap": 0,
            "debt_ceiling": 0,
            "debt_ceiling_decimals": 2,
        }

        # calls common across v2 and v3 markets
        common_calls = [
            Call(data_provider, ['getReserveConfigurationData(address)((uint256,uint256,uint256,uint256,uint256,bool,bool,bool,bool,bool))', reserve], [['reserve_config', reserve_config_handler]]),
        ]
        
        # calls specific to v3
        v3_calls = [
            Call(data_provider, ['getReserveData(address)((uint256,uint256,uint256,uint256,uint256,uint256,uint256,uint256,uint256,uint256,uint256,uint40))', reserve], [['reserve_data', v3_reserve_data_handler]]),
            Call(data_provider, ['getReserveEModeCategory(address)(uint256)', reserve], [['reserve_emode_category', None]]),
            Call(data_provider, ['getReserveCaps(address)(uint256,uint256)', reserve], reserve_caps_schema),
            Call(data_provider, ['getPaused(address)(bool)', reserve], [['is_paused', None]]),
            Call(data_provider, ['getSiloedBorrowing(address)(bool)', reserve], [['siloed_borrowing', None]]),
            Call(data_provider, ['getLiquidationProtocolFee(address)(uint256)', reserve], [['liquidation_protocol_fee', None]]),
            Call(data_provider, ['getUnbackedMintCap(address)(uint256)', reserve], [['unbacked_mint_cap', None]]),
            Call(data_provider, ['getDebtCeiling(address)(uint256)', reserve], [['debt_ceiling', None]]),
            Call(data_provider, ['getDebtCeilingDecimals()(uint256)'], [['debt_ceiling_decimals', None]]),
            # Call(data_provider, ['getFlashLoanEnabled(address)(bool)', reserve], [['flash_loan_enabled', None]]),
        ]
        
        # calls specific to v2
        v2_calls = [
            Call(data_provider, ['getReserveData(address)((uint256,uint256,uint256,uint256,uint256,uint256,uint256,uint256,uint256,uint40))', reserve], [['reserve_data', v2_reserve_data_handler]]),
            Call(lending_pool, ['paused()(bool)'], [['is_paused', None]]),
        ]


        v1_calls = [
            Call(lending_pool, ['getReserveConfigurationData(address)((uint256,uint256,uint256,address,bool,bool,bool,bool))', reserve], [['reserve_config', v1_reserve_config_handler]]),
            Call(lending_pool, ['getReserveData(address)((uint256,uint256,uint256,uint256,uint256,uint256,uint256,uint256,uint256,uint256,uint256,address,uint40))', reserve], [['reserve_data', v1_reserve_data_handler]]),
            Call(core, ['getReserveIsFreezed(address)(bool)', reserve], [['is_frozen', None]]),
        ]

        # create multicall object and merge v3 fields if on v1 or v
        if CONFIG_MARKETS[market]['version'] == 3:
            multi = Multicall(
                [   
                    *common_calls,
                    *v3_calls
                ],
                _w3 = w3,
                block_id = block_height
            )
        elif CONFIG_MARKETS[market]['version'] == 2:
            multi = Multicall(
                [   
                    *common_calls,
                    *v2_calls
                ],
                _w3 = w3,
                block_id = block_height
            )
        elif CONFIG_MARKETS[market]['version'] == 1:
            multi = Multicall(
                [   
                    *v1_calls
                ],
                _w3 = w3,
                block_id = block_height
            )
        else:
            raise ValueError(f"Unknown aave market version {CONFIG_MARKETS[market]['version']}")

        # execute multicall
        # use retry logic to handle transient RPC errors
        i = 0
        delay = INITIAL_RETRY
        while True:
            try:
                multicall_output = multi()
                break
            except Exception as e:
                i += 1
                if i > MAX_RETRIES:
                    raise ValueError(f"RPC error count {i}, last error {str(e)}.  Bailing out.")
                rand_delay = randint(0, 250) / 1000
                sleep(delay + rand_delay)
                delay *= 2
                print(f"Request Error {str(e)}, retry count {i}")

        # out_df = pd.DataFrame(out)

        # ic(multicall_output)
        if CONFIG_MARKETS[market]['version'] != 3:
            # merge v3 fields which are missing in earlier versions
            multicall_output = multicall_output | v3_empty_reserve_data

        # print(multicall_output)

    return multicall_output

def raw_reserve_to_dataframe(
        raw_reserve: dict,
) -> pd.DataFrame():
    """
    Convert raw reserve data to dataframe.
    Takes the output of get_raw_reserve_data and converts it to a dataframe.


    Args:
        raw_reserve: dict from get_raw_reserve_data
        block_height: block height of the data
        reserve: reserve address

    Returns:
        pd.DataFrame: single row dataframe with reserve data

    """
    if not raw_reserve:
        return_val = pd.DataFrame()
    else:
        reserve_config = pd.DataFrame(raw_reserve['reserve_config'], index=[0])
        reserve_data = pd.DataFrame(raw_reserve['reserve_data'], index=[0])
        del raw_reserve['reserve_config']
        del raw_reserve['reserve_data']
        other_params = pd.DataFrame(raw_reserve, index=[0])

        return_val = pd.concat([reserve_config, reserve_data, other_params], axis=1)

    return return_val


def get_quote_from_1inch(
    chain_id: str,
    from_token: str,
    from_token_decimals: int,
    to_token: str,
    to_token_decimals: int,
    from_token_amount) -> float:
    """
    Get swap rate from 1inch API.
    Uses the decimals vars to convert the from and to amounts to the correct number of decimals.

    Uses requests library to make the API call (synchronous)

    Args:
        chain_id: chain_id of the network
        from_token: from token address
        from_token_decimals: number of decimals of from token
        to_token: to token address
        to_token_decimals: number of decimals of to token
        from_token_amount: amount of from token

    Returns:
        float: amount of to_token received from the swap

    
    """
    # convert from_token_amount to the correct number of decimals
    from_amount_converted = int(from_token_amount * 10 ** from_token_decimals)

    # construct the URL
    oneinch_url = f"https://api.1inch.exchange/v5.0/{chain_id}/quote?fromTokenAddress={from_token}&toTokenAddress={to_token}&amount={from_amount_converted}"

    # use exponential backoff logic to handle transient API errors
    i = 0
    delay = INITIAL_RETRY
    while True:
        response = requests.get(oneinch_url, timeout=300)
        if response.status_code == requests.codes.ok:
            break
        elif response.status_code == requests.codes.bad:
            # request error, don't retry
            print(f"Request Error {response.status_code} {response.reason} {response.json()['description']}")
            response.raise_for_status()
        i += 1
        if i > MAX_RETRIES:
            raise ValueError(f"1Inch Quote API error count {i}, last error {response.status_code} {response.reason}.  Bailing out.")
        rand_delay = randint(0, 250) / 1000
        sleep(delay + rand_delay)
        delay *= 2
        print(f"Request Error {response.status_code} {response.reason}, retry count {i}")

    # parse the response
    response_json = response.json()
    # ic(response_json)

    quote_amount = int(response_json['toTokenAmount']) / 10 ** to_token_decimals

    return quote_amount
    
async def get_quote_from_1inch_async(
    chain_id: str,
    from_token: str,
    from_token_decimals: int,
    to_token: str,
    to_token_decimals: int,
    from_token_amount: float,
    semaphore: asyncio.Semaphore) -> float:
    """
    Get swap rate from 1inch API.
    Uses the decimals vars to convert the from and to amounts to the correct number of decimals.

    Uses httpx library to make the API call (asynchronous)

    Args:
        chain_id: chain_id of the network
        from_token: from token address
        from_token_decimals: number of decimals of from token
        to_token: to token address
        to_token_decimals: number of decimals of to token
        from_token_amount: amount of from token
        semaphore: asyncio.Semaphore to limit the number of concurrent requests

    Returns:
        float: amount of to_token received from the swap

    """
    # convert from_token_amount to the correct number of decimals
    from_amount_converted = int(from_token_amount * 10 ** from_token_decimals)

    # construct the URL
    oneinch_url = f"https://api.1inch.exchange/v5.0/{chain_id}/quote?fromTokenAddress={from_token}&toTokenAddress={to_token}&amount={from_amount_converted}"

    async with semaphore:
        async with httpx.AsyncClient() as client:
            
            # use exponential backoff logic to handle transient API errors
            i = 0
            delay = INITIAL_RETRY
            while True:
                response = await client.get(oneinch_url, timeout=300)
                if response.status_code == requests.codes.ok:
                    break
                elif response.status_code == requests.codes.bad:
                    # request error, don't retry
                    print(f"Request Error {response.status_code} {response.reason_phrase} {response.json()['description']}")
                    response.raise_for_status()
                i += 1
                if i > MAX_RETRIES:
                    raise ValueError(f"1Inch Quote API error count {i}, last error {response.status_code} {response.reason_phrase}.  Bailing out.")
                rand_delay = randint(0, 250) / 1000
                sleep(delay + rand_delay)
                delay *= 2
                print(f"Request Error {response.status_code} {response.reason_phrase}, retry count {i}")

    # parse the response
    response_json = response.json()
    # ic(response_json)

    quote_amount = int(response_json['toTokenAmount']) / 10 ** to_token_decimals

    return quote_amount


def get_aave_oracle_price(
    market: str,
    reserve: str,
    block_height: Optional[int] = None,
) -> float:
    """
    Get the price of a reserve from the Aave oracle.

    Args:
        chain: chain of the reserve
        reserve: reserve address
        block_height: block height of the data (optional, defaults to latest)

    Returns:
        float: price of the reserve in the default oracle return units (CONFIG_MARKETS[market]['oracle_base_currency'])

    """
    # get params from config
    oracle_address = CONFIG_MARKETS[market]['oracle']
    chain = CONFIG_MARKETS[market]['chain']

    # configure the Web3 object
    w3 = Web3(Web3.HTTPProvider(CONFIG_CHAINS[chain]['web3_rpc_url']))

    oracle_calls = [
        Call(oracle_address, ['getAssetPrice(address)(uint256)', reserve], [['oracle_price', None]]),
        Call(oracle_address, ['BASE_CURRENCY_UNIT()(uint256)'], [['base_currency_unit', None]]),
    ]

    if block_height is None:
        multi = Multicall(oracle_calls, _w3=w3)
    else:
        multi = Multicall(oracle_calls, block_id=block_height, _w3=w3,)

    # get the data
    multicall_output = multi()
    # ic(multicall_output)

    return multicall_output['oracle_price'] / multicall_output['base_currency_unit']


def get_balancer_bpt_data(
        chain: str,
        bpt_address: str,
        decimals: int,
        block_height: Optional[int] = None,
) -> dict:
    """
    Gets the rate and actualSupply data for a Balancer BPT token

    Uses multicall to get the data in a single RPC call.

    Args:
        chain: chain of the BPT token
        bpt_address: address of the BPT token
        decimals: number of decimals of the BPT token
        block_height: block height of the data (optional, defaults to latest)
    """

    def _convert_to_float(x):
        return x / 10 ** decimals

    # setup the Web3 connection
    w3 = Web3(Web3.HTTPProvider(CONFIG_CHAINS[chain]['web3_rpc_url']))

    # check if the contract exists
    # Get the contract bytecode at the specified block height
    contract_bytecode = w3.eth.get_code(Web3.to_checksum_address(bpt_address), block_identifier=block_height)

    # Check if the contract exists at the specified block height
    if contract_bytecode != b'' and contract_bytecode != '0x':  

        # set up the multiple call objects (one for each address)
        calls = [
            Call(bpt_address, ['getRate()(uint256)'], [['rate', _convert_to_float]]),
            Call(bpt_address, ['getActualSupply()(uint256)'], [['actual_supply', _convert_to_float]]),
        ]

        # configure the mulitcall object
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

    else:
        multi_output =  {"actual_supply": None, "rate": None}
    
    return multi_output
    
def get_token_holders_from_covalent(
        chain_id: int,
        block_height: int,
        token_address: str,
) -> pd.DataFrame:
    """Queries the Covalent API
        to get events by topic_hash for a given address
    Args:
        chain_id:  chain id of the network
        block_height:  block height to query
        token_address:  str address of contract to get token holders for
    Returns:
        pd.DataFrame of events.  Raw blockchain ints converted to floats with decimals.
    """
    page = 0
    has_more = True

    token_holders = pd.DataFrame()
    while has_more:
        covalent_api_url = f'https://api.covalenthq.com/v1/{chain_id}/tokens/{token_address}/token_holders_v2/?'\
                            + f'block-height={block_height}'\
                            + f'&page-number={page}&key={COVALENT_KEY}'
        
        # ic(covalent_api_url)

        #pylint: disable=E1137,E1101
        i = 0
        delay = INITIAL_RETRY
        while True:
            response = requests.get(covalent_api_url, timeout=300)#, auth=(API_KEY, API_KEY))
            if response.status_code == requests.codes.ok:
                break
            i += 1
            if i > MAX_RETRIES:
                raise ValueError(f"Covalent token balances API error count {i}, last error {response.status_code} {response.reason}.  Bailing out.")
            rand_delay = randint(0, 250) / 1000
            sleep(delay + rand_delay)
            delay *= 2
            print(f"Request Error {response.status_code} {response.reason}, retry count {i}")
        #pylint: enable=E1137,E1101

        api_response_df = pd.DataFrame(response.json()['data']['items'])
        token_holders = pd.concat([token_holders, api_response_df], ignore_index=True)

        has_more = response.json()['data']['pagination']['has_more']
        page += 1
        # if page > 3:
        #     has_more = False

    token_holders.total_supply = token_holders.total_supply.astype(float) / 10 ** token_holders.contract_decimals
    token_holders.balance = token_holders.balance.astype(float) / 10 ** token_holders.contract_decimals

    token_holders = standardise_types(token_holders)

    return token_holders


def get_quote_from_paraswap(
    chain_id: str,
    from_token: str,
    from_token_decimals: int,
    to_token: str,
    to_token_decimals: int,
    from_token_amount) -> float:
    """
    Get swap rate from Paraswap V5 API.
    Uses the decimals vars to convert the from and to amounts to the correct number of decimals.

    Uses requests library to make the API call (synchronous)

    Args:
        chain_id: chain_id of the network
        from_token: from token address
        from_token_decimals: number of decimals of from token
        to_token: to token address
        to_token_decimals: number of decimals of to token
        from_token_amount: amount of from token

    Returns:
        float: amount of to_token received from the swap

    
    """
    # convert from_token_amount to the correct number of decimals
    from_amount_converted = int(from_token_amount * 10 ** from_token_decimals)

    # construct the URL
    # paraswap_url = f"https://api.1inch.exchange/v5.0/{chain_id}/quote?fromTokenAddress={from_token}&toTokenAddress={to_token}&amount={from_amount_converted}"
    paraswap_url = f"https://apiv5.paraswap.io/prices?srcToken={from_token}&srcDecimals={from_token_decimals}"\
                 + f"&destToken={to_token}&destDecimals={to_token_decimals}&amount={from_amount_converted}&side=SELL&network={chain_id}"\
                 + f"&maxImpact=100"

    # use exponential backoff logic to handle transient API errors
    i = 0
    delay = INITIAL_RETRY
    while True:
        response = requests.get(paraswap_url, timeout=300)
        if response.status_code == requests.codes.ok:
            break
        elif response.status_code == requests.codes.bad:
            # request error, don't retry
            print(f"Request Error {response.status_code} {response.reason} {response.json()['description']}")
            response.raise_for_status()
        i += 1
        if i > MAX_RETRIES:
            raise ValueError(f"1Inch Quote API error count {i}, last error {response.status_code} {response.reason}.  Bailing out.")
        rand_delay = randint(0, 250) / 1000
        sleep(delay + rand_delay)
        delay *= 2
        print(f"Request Error {response.status_code} {response.reason}, retry count {i}")

    # parse the response
    response_json = response.json()
    # ic(response_json)

    quote_amount = int(response_json['priceRoute']['destAmount']) / 10 ** to_token_decimals

    return quote_amount

async def get_quote_from_paraswap_async(
    chain_id: str,
    from_token: str,
    from_token_decimals: int,
    to_token: str,
    to_token_decimals: int,
    from_token_amount: float,
    semaphore: asyncio.Semaphore) -> float:
    """
    Get swap rate from Paraswap V5 API.
    Uses the decimals vars to convert the from and to amounts to the correct number of decimals.

    Uses httpx library to make the API call (asynchronous)

    Args:
        chain_id: chain_id of the network
        from_token: from token address
        from_token_decimals: number of decimals of from token
        to_token: to token address
        to_token_decimals: number of decimals of to token
        from_token_amount: amount of from token
        semaphore: asyncio.Semaphore to limit the number of concurrent requests

    Returns:
        float: amount of to_token received from the swap

    """
    # convert from_token_amount to the correct number of decimals
    from_amount_converted = int(from_token_amount * 10 ** from_token_decimals)

    # construct the URL
    # oneinch_url = f"https://api.1inch.exchange/v5.0/{chain_id}/quote?fromTokenAddress={from_token}&toTokenAddress={to_token}&amount={from_amount_converted}"
    paraswap_url = f"https://apiv5.paraswap.io/prices?srcToken={from_token}&srcDecimals={from_token_decimals}"\
                 + f"&destToken={to_token}&destDecimals={to_token_decimals}&amount={from_amount_converted}&side=SELL&network={chain_id}"\
                 + f"&maxImpact=100"

    async with semaphore:
        async with httpx.AsyncClient() as client:
            
            # use exponential backoff logic to handle transient API errors
            i = 0
            delay = INITIAL_RETRY
            while True:
                response = await client.get(paraswap_url, timeout=300)
                if response.status_code == requests.codes.ok:
                    break
                elif response.status_code == requests.codes.bad:
                    # request error, don't retry
                    print(f"Request Error {response.status_code} {response.reason_phrase} {response.json()['description']}")
                    response.raise_for_status()
                i += 1
                if i > MAX_RETRIES:
                    raise ValueError(f"1Inch Quote API error count {i}, last error {response.status_code} {response.reason_phrase}.  Bailing out.")
                rand_delay = randint(0, 250) / 1000
                sleep(delay + rand_delay)
                delay *= 2
                print(f"Request Error {response.status_code} {response.reason_phrase}, retry count {i}")

    # parse the response
    response_json = response.json()
    # ic(response_json)

    quote_amount = int(response_json['priceRoute']['destAmount']) / 10 ** to_token_decimals

    return quote_amount

def get_market_tokens_at_block_rpc(
        market: str,
        block_height: int,
        markets_config: dict) -> pd.DataFrame:
    """Gets Aave token details for market at a given block height

    Uses direct RPC calls to the Pool contract

    Args:
        market: the market name as per the config object key
        block_height: block height for the time travel subgraph call
        markets_config: pipeline config dict, with market as key and each value having a
                    subgraph_url and pool_id property

    Returns:
        A dataframe with details from the subgraph

    """
    #pylint: disable=E1137,E1101
    chain = markets_config[market]['chain']

    # setup the Web3 connection
    w3 = Web3(Web3.HTTPProvider(CONFIG_CHAINS[chain]['web3_rpc_url']))

    # get the pool address
    pool_address = markets_config[market]['pool']
    protocol_data_provider = markets_config[market]['protocol_data_provider']

    # get the token addresses from the pool
    # pool_call = Call(address, ['getReservesList ()((address[]))'], [[address, None]]
    # pool_call = Call(
    #     pool_address,
    #     ['getReservesList()(address[])'],
    #     [['reserves', None]],
    #     _w3 = w3,
    #     block_id = block_height
    # )
    reserves_call = Call(
        protocol_data_provider,
        ['getAllReservesTokens()((string,address)[])'],
        [['reserves', None]],
        _w3 = w3,
        block_id = block_height
    )

    reserves_call = Call(
        protocol_data_provider,
        ['getAllATokens()((string,address)[])'],
        [['reserves', None]],
        _w3 = w3,
        block_id = block_height
    )

    # execute the call
    reserves_call_output = reserves_call()

    ic(reserves_call_output)

    # # grab the token addresses in an iterable
    # addresses = [TOKENS[chain][token]['address'] for token in TOKENS[chain]]

    # # set up the multiple call objects (one for each address)
    # chain_calls = [Call(address, ['totalSupply()((uint256))'], [[address, None]]) for address in addresses]

    # # configure the mulitcall object
    # chain_multi = Multicall(chain_calls, _w3 = w3, block_id = block_height)

    # # exponential backoff retries on the function call to deal with transient RPC errors
    # i = 0
    # delay = INITIAL_RETRY
    # while True:
    #     try:
    #         chain_output = chain_multi()
    #         break
    #     except Exception as e:
    #         i += 1
    #         if i > MAX_RETRIES:
    #             raise ValueError(f"RPC error count {i}, last error {str(e)}.  Bailing out.")
    #         rand_delay = randint(0, 250) / 1000
    #         sleep(delay + rand_delay)
    #         delay *= 2
    #         print(f"Request Error {str(e)}, retry count {i}")
    
    # chain_data = pd.DataFrame(chain_output).T.reset_index()
    # chain_data.columns = ['address', 'total_supply']
    # chain_data['chain'] = chain
    # chain_data['block_height'] = block_height
    # chain_data['block_day'] = block_day
    
    # # chain_data['symbol'] = chain_data.address.map({TOKENS[chain][token]['address']: token for token in TOKENS[chain]})
    # chain_data['symbol'] = pd.Series(TOKENS[chain].keys())
    # chain_data['decimals'] = pd.Series([TOKENS[chain][token]['decimals'] for token in TOKENS[chain]])
    # chain_data.total_supply = chain_data.total_supply.astype(float) / 10**chain_data.decimals
    # chain_data = chain_data[['block_day', 'block_height', 'chain', 'address', 'symbol', 'decimals', 'total_supply']]
    
    # supply_data = pd.concat([supply_data, chain_data]).reset_index(drop=True)
    return pd.DataFrame()

if __name__ == "__main__":

    out = get_market_tokens_at_block_rpc('metis_v3', 5602598, CONFIG_MARKETS)
