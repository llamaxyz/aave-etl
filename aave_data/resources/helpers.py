from typing import Optional  # , date, time, timedelta, timezone

import pandas as pd
import requests
from dagster import op
from icecream import ic
from subgrounds.subgrounds import Subgrounds
from io import StringIO
from web3 import Web3
from time import sleep
from subgrounds.pagination.pagination import PaginationError
from random import randint


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
                    subgraph.Pool.id == markets_config[market]['lending_pool_address_provider']  # type: ignore
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
                "fromBlock": Web3.toHex(start_block),
                "toBlock": Web3.toHex(end_block),
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
                "fromBlock": Web3.toHex(start_block),
                "toBlock": Web3.toHex(end_block),
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
            transfers.transfers_contract_decimals = transfers.transfers_contract_decimals.apply(lambda x: Web3.toInt(hexstr=x))
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
    token_contract = w3.eth.contract(address=Web3.toChecksumAddress(token), abi=ERC20_ABI)

    if block_height > 0:
        i = 0
        delay = INITIAL_RETRY
        while True:
            try:
                balance_raw = token_contract.functions.balanceOf(Web3.toChecksumAddress(address)).call(block_identifier=int(block_height))
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
                balance_raw = token_contract.functions.balanceOf(Web3.toChecksumAddress(address)).call()
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
    token_contract = w3.eth.contract(address=Web3.toChecksumAddress(token), abi=ERC20_ABI)

    if block_height > 0:
        i = 0
        delay = INITIAL_RETRY
        while True:
            try:
                balance_raw = token_contract.functions.scaledBalanceOf(Web3.toChecksumAddress(address)).call(block_identifier=int(block_height))
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
                balance_raw = token_contract.functions.scaledBalanceOf(Web3.toChecksumAddress(address)).call()
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

if __name__ == "__main__":
    # wbtc = get_token_transfers_from_covalent(16050438, 16057596, 1, '0x464C71f6c2F760DdA6093dCB91C24c39e5d6e18c', '0xbcca60bb61934080951369a648fb03df4f96263c')
    # weth = get_token_transfers_from_covalent(16050438, 16057596, 1, '0x464C71f6c2F760DdA6093dCB91C24c39e5d6e18c', '0x030ba81f1c18d280636f32af80b9aad02cf0854e')

    # out = pd.concat([wbtc, weth]).reset_index()

    # print(out.to_dict())
    # ic(out)

    balance = get_erc20_balance_of('0x464C71f6c2F760DdA6093dCB91C24c39e5d6e18c', '0xbcca60bb61934080951369a648fb03df4f96263c', 6, 'ethereum')#, block_height=16057596)

    # balance = get_erc20_balance_of('0x8a020d92d6b119978582be4d3edfdc9f7b28bf31', '0x191c10aa4af7c30e871e70c95db0e4eb77237530', 6, 'harmony', block_height=34443481)
    ic(balance)
    # mtt = get_events_by_topic_hash_from_covalent(15154950, 15154960, 43114, '0x794a61358D6845594F94dc1DB02A252b5b4814aD', '0xbfa21aa5d5f9a1f0120a95e7c0749f389863cbdbfff531aa7339077a5bc919de')

    # ic(mtt)

    # mtt.to_csv('mtt.csv')

    # print(mtt.head(1).to_dict())
