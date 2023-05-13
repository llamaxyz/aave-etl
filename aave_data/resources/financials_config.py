""" Contains config objects for use in financials """
import os
from datetime import datetime, timezone


FINANCIAL_PARTITION_START_DATE = '2022-01-01'

ETHERSCAN_API_KEY =  os.environ['ETHERSCAN_API_KEY']
POLYGONSCAN_API_KEY =  os.environ['POLYGONSCAN_API_KEY']
COVALENT_KEY = os.environ['COVALENT_KEY']
WEB3_ALCHEMY_API_KEY = os.environ['WEB3_ALCHEMY_API_KEY']
POLYGON_ALCHEMY_KEY = os.environ['POLYGON_ALCHEMY_KEY']
OPTIMISM_ALCHEMY_KEY = os.environ['OPTIMISM_ALCHEMY_KEY']
ARBITRUM_ALCHEMY_KEY = os.environ['ARBITRUM_ALCHEMY_KEY']
FLIPSIDE_API_KEY = os.environ['FLIPSIDE_API_KEY']

 # a minimal ERC20 ABI supporting balanceOf, scaledBalanceOf, symbol and decimals
ERC20_ABI = [
        {
            "constant": "true",
            "inputs": [
            {
                "name": "owner",
                "type": "address"
            }
            ],
            "name": "balanceOf",
            "outputs": [
            {
                "name": "balance",
                "type": "uint256"
            }
            ],
            "payable": "false",
            "stateMutability": "view",
            "type": "function"
        },
        {
            "constant": "true",
            "inputs": [
            {
                "name": "owner",
                "type": "address"
            }
            ],
            "name": "scaledBalanceOf",
            "outputs": [
            {
                "name": "scaledBalance",
                "type": "uint256"
            }
            ],
            "payable": "false",
            "stateMutability": "view",
            "type": "function"
        },
        {
            "inputs":[
                
            ],
            "name":"decimals",
            "outputs":[
                {
                    "internalType":"uint8",
                    "name":"",
                    "type":"uint8"
                }
            ],
            "stateMutability":"view",
            "type":"function"
        },
        {
            "inputs":[
                
            ],
            "name":"symbol",
            "outputs":[
                {
                    "internalType":"string",
                    "name":"",
                    "type":"string"
                }
            ],
            "stateMutability":"view",
            "type":"function"
        },
        {
            "inputs":[
                
            ],
            "name":"totalSupply",
            "outputs":[
                {
                    "name":"",
                    "type":"uint256"
                }
            ],
            "stateMutability":"view",
            "type":"function"
        }
    ]

# the config dict for tables specific to a blockchain
CONFIG_CHAINS = {
    "ethereum": {
        "chain_id": 1,
        "defillama_chain": "ethereum",
        "ape_ok": True,
        "ape_network_choice": "ethereum:mainnet:alchemy",
        "wrapped_ether": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
        "gas_token": "ETH",
        "web3_rpc_url": f"https://eth-mainnet.g.alchemy.com/v2/{WEB3_ALCHEMY_API_KEY}"
    },
    "polygon": {
        "chain_id": 137,
        "defillama_chain": "polygon",
        "ape_ok": True,
        "ape_network_choice": "polygon:mainnet:alchemy",
        "wrapped_ether": "0x7ceB23fD6bC0adD59E62ac25578270cFf1b9f619",
        "gas_token": "MATIC",
        "web3_rpc_url": f"https://polygon-mainnet.g.alchemy.com/v2/{POLYGON_ALCHEMY_KEY}"
    },
    "avalanche": {
        "chain_id": 43114,
        "defillama_chain": "avax",
        "ape_ok": True, 
        "ape_network_choice": "avalanche:mainnet:https://rpc.ankr.com/avalanche",
        "wrapped_ether": "0x49D5c2BdFfac6CE2BFdB6640F4F80f226bc10bAB",
        "gas_token": "AVAX",
        "web3_rpc_url": "https://rpc.ankr.com/avalanche"
    },
    "optimism": {
        "chain_id": 10,
        "defillama_chain": "optimism",
        "ape_ok": True,
        "ape_network_choice": "optimism:mainnet:alchemy",
        "wrapped_ether": "0x4200000000000000000000000000000000000006",
        "gas_token": "ETH",
        "web3_rpc_url": f"https://opt-mainnet.g.alchemy.com/v2/{OPTIMISM_ALCHEMY_KEY}"
    },
    "arbitrum": {
        "chain_id": 42161,
        "defillama_chain": "arbitrum",
        "ape_ok": True,
        "ape_network_choice": "arbitrum:mainnet:alchemy",
        "wrapped_ether": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
        "gas_token": "ETH",
        "web3_rpc_url": f"https://arb-mainnet.g.alchemy.com/v2/{ARBITRUM_ALCHEMY_KEY}"
    },
    "fantom": {
        "chain_id": 250,
        "defillama_chain": "fantom",
        "ape_ok": True, 
        "ape_network_choice": "fantom:opera:https://rpc.ankr.com/fantom",
        "wrapped_ether": "0x74b23882a30290451A17c44f4F05243b6b58C76d",
        "gas_token": "FTM",
        "web3_rpc_url": "https://rpc.ankr.com/fantom"
    },
    "harmony": {
        "chain_id": 1666600000,
        "defillama_chain": "harmony",
        "ape_ok": True, 
        "ape_network_choice": "harmony:mainnet:https://a.api.s0.t.hmny.io",
        "wrapped_ether": "0x6983D1E6DEf3690C4d616b13597A09e6193EA013",
        "gas_token": "ONE",
        "web3_rpc_url": "https://a.api.s0.t.hmny.io"
    },
    "metis": {
        "chain_id": 1088,
        "defillama_chain": "metis",
        "ape_ok": False,
        "ape_network_choice": None,
        "wrapped_ether": "0xDeadDeAddeAddEAddeadDEaDDEAdDeaDDeAD0000",
        "gas_token": "METIS",
        "web3_rpc_url": 'https://andromeda.metis.io/?owner=1088'
    }
}

# the config dict for markets based on Aave V2 code
CONFIG_MARKETS = {
    "ethereum_v2": {
        "chain": "ethereum",
        "version": 2,
        "pool": "0x7d2768dE32b0b80b7a3454c06BdAc94A69DDc7A9",
        "collector": "0x464C71f6c2F760DdA6093dCB91C24c39e5d6e18c",
        "protocol_data_provider": "0x057835Ad21a177dbdd3090bB1CAE03EaCF78Fc6d",
        "oracle": "0xA50ba011c48153De246E5192C8f9258A2ba79Ca9",
        "incentives_controller": "0xd784927Ff2f95ba542BfC824c8a8a98F3495f6b5",
        "incentives_ui_data_provider" : "0xD01ab9a6577E1D84F142e44D49380e23A340387d",
        "pool_address_provider" : "0xB53C1a33016B2DC2fF3653530bfF1848a515c8c5",
        "rewards_token": "0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9",
        "rewards_token_symbol": "stkAAVE",
        "rewards_token_decimals": 18,
        "subgraph": 'https://api.thegraph.com/subgraphs/name/aave/protocol-v2',
        "messari_subgraph": "https://api.thegraph.com/subgraphs/name/messari/aave-v2-ethereum",
        "token_source": "rpc",
        "atoken_prefix": "a",
        "oracle_base_currency": 'wei',
        "block_table_master": True,
        "price_rank": 3,
        "paraswap_fee_claimer": '0x9abf798f5314bfd793a9e57a654bed35af4a1d60'
    },
    "aave_amm": {
        "chain": "ethereum",
        "version": 2,
        "pool": "0x7937D4799803FbBe595ed57278Bc4cA21f3bFfCB",
        "collector": "0x464C71f6c2F760DdA6093dCB91C24c39e5d6e18c",
        "protocol_data_provider": "0xc443AD9DDE3cecfB9dfC5736578f447aFE3590ba",
        "oracle": "0xA50ba011c48153De246E5192C8f9258A2ba79Ca9",
        "incentives_controller": None,
        "incentives_ui_data_provider" : "0xD01ab9a6577E1D84F142e44D49380e23A340387d",
        "pool_address_provider" : "0xAcc030EF66f9dFEAE9CbB0cd1B25654b82cFA8d5",
        "rewards_token": None,
        "rewards_token_symbol": None,
        "rewards_token_decimals": None,
        "subgraph": 'https://api.thegraph.com/subgraphs/name/aave/protocol-v2',
        "messari_subgraph": "https://api.thegraph.com/subgraphs/name/messari/aave-amm-ethereum",
        "token_source": "rpc",
        "atoken_prefix": "a",
        "oracle_base_currency": 'wei',
        "block_table_master": False,
        "price_rank": 4
    },
    "aave_arc": {
        "chain": "ethereum",
        "version": 2,
        "pool": "0x37D7306019a38Af123e4b245Eb6C28AF552e0bB0",
        "collector": "0x464C71f6c2F760DdA6093dCB91C24c39e5d6e18c",
        "protocol_data_provider": "0x71B53fC437cCD988b1b89B1D4605c3c3d0C810ea",
        "pool_address_provider": "0x6FdfafB66d39cD72CFE7984D3Bbcc76632faAb00",
        "oracle": "0xB8a7bc0d13B1f5460513040a97F404b4fea7D2f3",
        "incentives_controller": None,
        "incentives_ui_data_provider" : None,
        "rewards_token": None,
        "rewards_token_symbol": None,
        "rewards_token_decimals": None,
        "subgraph": None, #'https://api.thegraph.com/subgraphs/name/aave/aave-arc', buggy
        "messari_subgraph": "https://api.thegraph.com/subgraphs/name/messari/aave-arc-ethereum",
        "token_source": "rpc",
        "atoken_prefix": "a",
        "oracle_base_currency": 'wei',
        "block_table_master": False,
        "price_rank": 5
    },
    "aave_rwa": {
        "chain": "ethereum",
        "version": 2,
        "pool": "0xA1a8c33C9a9a9DE231b13a2271a7C09c11C849F1",
        "collector": "0x464C71f6c2F760DdA6093dCB91C24c39e5d6e18c",
        "protocol_data_provider": "0xe69DE09Cd274242DDD7004794a4b00EbE9B65fEA",
        "oracle": "0xf20E5fe26811f7336a8A637C42b6e8913B291868",
        "incentives_controller": "0x77B59b07b87689a6D27adE063FB1D08C7Fe52F0b",
        "incentives_ui_data_provider" : '0xD01ab9a6577E1D84F142e44D49380e23A340387d',
        "pool_address_provider" : "0xB953a066377176092879a151C07798B3946EEa4b",
        "rewards_token": "0xc221b7E65FfC80DE234bbB6667aBDd46593D34F0",
        "rewards_token_symbol": "wCFG",
        "rewards_token_decimals": 18,
        "subgraph": None,
        "messari_subgraph": "https://api.thegraph.com/subgraphs/name/messari/aave-rwa-ethereum",
        "token_source": "rpc",
        "atoken_prefix": "a",
        "oracle_base_currency": 'usd',
        "block_table_master": False,
        "price_rank": 2
    },
    "ethereum_v3": {
        "chain": "ethereum",
        "version": 3,
        "pool": "0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2",
        "collector": "0x464C71f6c2F760DdA6093dCB91C24c39e5d6e18c",
        "protocol_data_provider": "0x7B4EB56E7CD4b454BA8ff71E4518426369a138a3",
        "pool_address_provider": '0x2f39d218133AFaB8F2B819B1066c7E434Ad94E9e',
        "oracle": "0x54586bE62E3c3580375aE3723C145253060Ca0C2",
        "incentives_controller": "0x8164Cc65827dcFe994AB23944CBC90e0aa80bFcb",
        "incentives_ui_data_provider" : "0x162A7AC02f547ad796CA549f757e2b8d1D9b10a6",
        "rewards_token": None,
        "rewards_token_symbol": None,
        "rewards_token_decimals": None,
        "subgraph": 'https://api.thegraph.com/subgraphs/name/aave/protocol-v3',
        "messari_subgraph": "https://api.thegraph.com/subgraphs/name/messari/aave-v3-ethereum",
        "token_source": "rpc",
        "atoken_prefix": "aEth",
        "oracle_base_currency": 'usd',
        "block_table_master": False,
        "price_rank": 1,
        "paraswap_fee_claimer": '0x9abf798f5314bfd793a9e57a654bed35af4a1d60'
    },
    "polygon_v2": {
        "chain": "polygon",
        "version": 2,
        "pool": "0x8dFf5E27EA6b7AC08EbFdf9eB090F32ee9a30fcf",
        "collector": "0x7734280A4337F37Fbf4651073Db7c28C80B339e9",
        "collector_change_date": datetime(2023,4,23,0,0,0, tzinfo=timezone.utc),
        "collector_v2": "0xe8599f3cc5d38a9ad6f3684cd5cea72f10dbc383", 
        "protocol_data_provider": "0x7551b5D2763519d4e37e8B81929D336De671d46d",
        "oracle": "0x0229F777B0fAb107F9591a41d5F02E4e98dB6f2d",
        "incentives_controller": "0x357D51124f59836DeD84c8a1730D72B749d8BC23",
        "incentives_ui_data_provider" : "0x645654D59A5226CBab969b1f5431aA47CBf64ab8",
        "pool_address_provider" : "0xd05e3E715d945B59290df0ae8eF85c1BdB684744",
        "rewards_token": "0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270",
        "rewards_token_symbol": "WMATIC",
        "rewards_token_decimals": 18,
        "subgraph": 'https://api.thegraph.com/subgraphs/name/aave/aave-v2-matic',
        "messari_subgraph": None, #"https://thegraph.com/hosted-service/subgraph/messari/aave-v2-polygon" not indexed yet,
        "token_source": "rpc",
        "atoken_prefix": "am",
        "oracle_base_currency": 'wei',
        "block_table_master": True,
        "price_rank": 2,
        "paraswap_fee_claimer": '0x9abf798f5314bfd793a9e57a654bed35af4a1d60'
    },
    "avax_v2": {
        "chain": "avalanche",
        "version": 2,
        "pool": "0x4F01AeD16D97E3aB5ab2B501154DC9bb0F1A5A2C",
        "collector": "0x467b92aF281d14cB6809913AD016a607b5ba8A36",
        "collector_change_date": datetime(2023,4,23,0,0,0, tzinfo=timezone.utc),
        "collector_v2": "0x5ba7fd868c40c16f7adfae6cf87121e13fc2f7a0", #collector contract changed on 2022-07-07
        "protocol_data_provider": "0x65285E9dfab318f57051ab2b139ccCf232945451",
        "oracle": "0xdC336Cd4769f4cC7E9d726DA53e6d3fC710cEB89",
        "incentives_controller": "0x01D83Fe6A10D2f2B7AF17034343746188272cAc9",
        "incentives_ui_data_provider" : "0x11979886A6dBAE27D7a72c49fCF3F23240D647bF",
        "pool_address_provider" : "0xb6A86025F0FE1862B372cb0ca18CE3EDe02A318f",
        "rewards_token": "0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7",
        "rewards_token_symbol": "WAVAX",
        "rewards_token_decimals": 18,
        "subgraph": 'https://api.thegraph.com/subgraphs/name/aave/protocol-v2-avalanche',
        "messari_subgraph": "https://api.thegraph.com/subgraphs/name/messari/aave-v2-avalanche",
        "token_source": "rpc",
        "atoken_prefix": "av",
        "oracle_base_currency": 'usd',
        "block_table_master": True,
        "price_rank": 2,
        "paraswap_fee_claimer": '0x9abf798f5314bfd793a9e57a654bed35af4a1d60'
    },
    "polygon_v3": {
        "chain": "polygon",
        "version": 3,
        "pool": "0x794a61358d6845594f94dc1db02a252b5b4814ad",
        "collector": "0xe8599f3cc5d38a9ad6f3684cd5cea72f10dbc383",
        "protocol_data_provider": "0x69fa688f1dc47d4b5d8029d5a35fb7a548310654",
        "pool_address_provider": '0xa97684ead0e402dc232d5a977953df7ecbab3cdb',
        "oracle": "0xb023e699f5a33916ea823a16485e259257ca8bd1",
        "incentives_controller": "0x929ec64c34a17401f460460d4b9390518e5b473e",
        "incentives_ui_data_provider" : "0x874313A46e4957D29FAAC43BF5Eb2B144894f557",
        "rewards_token": None,
        "rewards_token_symbol": None,
        "rewards_token_decimals": None,
        "subgraph": 'https://api.thegraph.com/subgraphs/name/aave/protocol-v3-polygon',
        "messari_subgraph": "https://api.thegraph.com/subgraphs/name/messari/aave-v3-polygon",
        "token_source": "rpc", 
        "atoken_prefix": "aPol",
        "oracle_base_currency": 'usd',
        "block_table_master": False,
        "price_rank": 1,
        "paraswap_fee_claimer": '0x9abf798f5314bfd793a9e57a654bed35af4a1d60'
    },
    "avax_v3": {
        "chain": "avalanche",
        "version": 3,
        "pool": "0x794a61358d6845594f94dc1db02a252b5b4814ad",
        "collector": "0x5ba7fd868c40c16f7adfae6cf87121e13fc2f7a0",
        "protocol_data_provider": "0x69fa688f1dc47d4b5d8029d5a35fb7a548310654",
        "pool_address_provider": '0xa97684ead0e402dc232d5a977953df7ecbab3cdb',
        "oracle": "0xebd36016b3ed09d4693ed4251c67bd858c3c7c9c",
        "incentives_controller": "0x929ec64c34a17401f460460d4b9390518e5b473e",
        "incentives_ui_data_provider" : "0x265d414f80b0fca9505710e6F16dB4b67555D365",
        "rewards_token": None,
        "rewards_token_symbol": None,
        "rewards_token_decimals": None,
        "subgraph": 'https://api.thegraph.com/subgraphs/name/aave/protocol-v3-avalanche',
        "messari_subgraph": "https://api.thegraph.com/subgraphs/name/messari/aave-v3-avalanche",
        "token_source": "rpc", 
        "atoken_prefix": "aAva",
        "oracle_base_currency": 'usd',
        "block_table_master": False,
        "price_rank": 1,
        "paraswap_fee_claimer": '0x9abf798f5314bfd793a9e57a654bed35af4a1d60'
    },
    "optimism_v3": {
        "chain": "optimism",
        "version": 3,
        "pool": "0x794a61358d6845594f94dc1db02a252b5b4814ad",
        "collector": "0xb2289e329d2f85f1ed31adbb30ea345278f21bcf",
        "protocol_data_provider": "0x69fa688f1dc47d4b5d8029d5a35fb7a548310654",
        "pool_address_provider": '0xa97684ead0e402dc232d5a977953df7ecbab3cdb',
        "oracle": "0xd81eb3728a631871a7ebbad631b5f424909f0c77",
        "incentives_controller": "0x929ec64c34a17401f460460d4b9390518e5b473e",
        "incentives_ui_data_provider" : "0x6F143FE2F7B02424ad3CaD1593D6f36c0Aab69d7",
        "rewards_token": None,
        "rewards_token_symbol": None,
        "rewards_token_decimals": None,
        "subgraph": 'https://api.thegraph.com/subgraphs/name/aave/protocol-v3-optimism',
        "messari_subgraph": "https://api.thegraph.com/subgraphs/name/messari/aave-v3-optimism",
        "token_source": "rpc", # messari not indexed yet
        "atoken_prefix": "aOpt",
        "oracle_base_currency": 'usd',
        "block_table_master": True,
        "price_rank": 1,
        "paraswap_fee_claimer": '0x9abf798f5314bfd793a9e57a654bed35af4a1d60'
    },
    "arbitrum_v3": {
        "chain": "arbitrum",
        "version": 3,
        "pool": "0x794a61358d6845594f94dc1db02a252b5b4814ad",
        "collector": "0x053d55f9b5af8694c503eb288a1b7e552f590710",
        "protocol_data_provider": "0x69fa688f1dc47d4b5d8029d5a35fb7a548310654",
        "pool_address_provider": '0xa97684ead0e402dc232d5a977953df7ecbab3cdb',
        "oracle": "0xb56c2f0b653b2e0b10c9b928c8580ac5df02c7c7",
        "incentives_controller": "0x929ec64c34a17401f460460d4b9390518e5b473e",
        "incentives_ui_data_provider" : "0xDA67AF3403555Ce0AE3ffC22fDb7354458277358",
        "rewards_token": None,
        "rewards_token_symbol": None,
        "rewards_token_decimals": None,
        "subgraph": 'https://api.thegraph.com/subgraphs/name/aave/protocol-v3-arbitrum',
        "messari_subgraph": "https://api.thegraph.com/subgraphs/name/messari/aave-v3-arbitrum",
        "token_source": "rpc",
        "atoken_prefix": "aArb",
        "oracle_base_currency": 'usd',
        "block_table_master": True,
        "price_rank": 1,
        "paraswap_fee_claimer": '0x9abf798f5314bfd793a9e57a654bed35af4a1d60'
    },
    "fantom_v3": {
        "chain": "fantom",
        "version": 3,
        "pool": "0x794a61358d6845594f94dc1db02a252b5b4814ad",
        "collector": "0xbe85413851d195fc6341619cd68bfdc26a25b928",
        "protocol_data_provider": "0x69fa688f1dc47d4b5d8029d5a35fb7a548310654",
        "pool_address_provider": '0xa97684ead0e402dc232d5a977953df7ecbab3cdb',
        "oracle": "0xfd6f3c1845604c8ae6c6e402ad17fb9885160754",
        "incentives_controller": "0x929ec64c34a17401f460460d4b9390518e5b473e",
        "incentives_ui_data_provider" : "0x67Da261c14fd94cE7fDd77a0A8476E5b244089A9",
        "rewards_token": None,
        "rewards_token_symbol": None,
        "rewards_token_decimals": None,
        "subgraph": 'https://api.thegraph.com/subgraphs/name/aave/protocol-v3-fantom',
        "messari_subgraph": "https://api.thegraph.com/subgraphs/name/messari/aave-v3-fantom",
        "token_source": "rpc",
        "atoken_prefix": "aFan",
        "oracle_base_currency": 'usd',
        "block_table_master": True,
        "price_rank": 1,
        "paraswap_fee_claimer": '0x9abf798f5314bfd793a9e57a654bed35af4a1d60'
    },
    "harmony_v3": {
        "chain": "harmony",
        "version": 3,
        "pool": "0x794a61358d6845594f94dc1db02a252b5b4814ad",
        "collector": "0x8a020d92d6b119978582be4d3edfdc9f7b28bf31",
        "protocol_data_provider": "0x69fa688f1dc47d4b5d8029d5a35fb7a548310654",
        "pool_address_provider": '0xa97684ead0e402dc232d5a977953df7ecbab3cdb',
        "oracle": "0x3c90887ede8d65ccb2777a5d577beab2548280ad",
        "incentives_controller": "0x929ec64c34a17401f460460d4b9390518e5b473e",
        "incentives_ui_data_provider" : "0xf7a60467aBb8A3240A0382b22E1B03c7d4F59Da5",
        "rewards_token": None,
        "rewards_token_symbol": None,
        "rewards_token_decimals": None,
        "subgraph": 'https://api.thegraph.com/subgraphs/name/aave/protocol-v3-harmony',
        "messari_subgraph": "https://api.thegraph.com/subgraphs/name/messari/aave-v3-harmony",
        "token_source": "rpc",
        "atoken_prefix": "aHar",
        "oracle_base_currency": 'usd',
        "block_table_master": True,
        "price_rank": 1,
    },
    "ethereum_v1": {
        "chain": "ethereum",
        "version": 1,
        "pool": "0x398ec7346dcd622edc5ae82352f02be94c62d119",
        "pool_address_provider": "0x24a42fd28c976a61df5d00d0599c34c4f90748c8",
        "collector": "0xe3d9988f676457123c5fd01297605efdd0cba1ae",
        "collector_change_date": datetime(2022,7,7,0,0,0, tzinfo=timezone.utc),
        "collector_v2": "0x464C71f6c2F760DdA6093dCB91C24c39e5d6e18c", #collector contract changed on 2022-07-07
        "protocol_data_provider": None,
        "oracle": "0x76b47460d7f7c5222cfb6b6a75615ab10895dde4",
        "incentives_controller": None,
        "incentives_ui_data_provider" : None,
        "rewards_token": None,
        "rewards_token_symbol": None,
        "rewards_token_decimals": None,
        "subgraph": 'https://api.thegraph.com/subgraphs/name/aave/protocol-multy-raw',
        "messari_subgraph": None,
        "token_source": "aave",
        "atoken_prefix": "a",
        "oracle_base_currency": 'wei',
        "block_table_master": False,
        "price_rank": 6
    },
    "metis_v3": {
        "chain": "metis",
        "version": 3,
        "pool": "0x90df02551bb792286e8d4f13e0e357b4bf1d6a57",
        "collector": "0xb5b64c7e00374e766272f8b442cd261412d4b118",
        "protocol_data_provider": "0x99411fc17ad1b56f49719e3850b2cdcc0f9bbfd8",
        "pool_address_provider": '0xb9fabd7500b2c6781c35dd48d54f81fc2299d7af',
        "oracle": "0x38d36e85e47ea6ff0d18b0adf12e5fc8984a6f8e",
        "incentives_controller": "0x30c1b8f0490fa0908863d6cbd2e36400b4310a6b",
        "incentives_ui_data_provider" : "0x3e7bc5ece0f22dbb16c3e3eea288a10a57d68927",
        "rewards_token": None,
        "rewards_token_symbol": None,
        "rewards_token_decimals": None,
        "subgraph": "https://andromeda.thegraph.metis.io/subgraphs/name/aave/protocol-v3-metis",
        "messari_subgraph": None,
        "token_source": "rpc",
        "atoken_prefix": "aMet",
        "oracle_base_currency": 'usd',
        "block_table_master": True,
        "price_rank": 1,
    },
}

CONFIG_ABI = {
    1: {
        "abi_url_base": f"https://api.etherscan.io/api?module=contract&action=getabi&apikey={ETHERSCAN_API_KEY}&address=",
        "oracle_implementation": "0x76b47460d7f7c5222cfb6b6a75615ab10895dde4"
    },
    2: {
        "abi_url_base": f"https://api.etherscan.io/api?module=contract&action=getabi&apikey={ETHERSCAN_API_KEY}&address=",
        "oracle_implementation": "0xA50ba011c48153De246E5192C8f9258A2ba79Ca9"
    },
    3: {
        "abi_url_base": f"https://api.polygonscan.com/api?module=contract&action=getabi&apikey={POLYGONSCAN_API_KEY}&address=",
        "oracle_implementation": "0xb023e699f5a33916ea823a16485e259257ca8bd1"
    },
}

CONFIG_TOKENS = {
    "ethereum_v2": {
        "ecosystem_reserve": {
            "address": "0x25f2226b597e8f9514b3f68f00f494cf4f286491",
            "tokens": {
                "AAVE": {
                    "address": "0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9",
                    "decimals": 18,
                }
            }
        },
        "ethereum_v2_incentives_controller": {
            "address": "0xd784927ff2f95ba542bfc824c8a8a98f3495f6b5",
            "tokens": {
                "AAVE": {
                    "address": "0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9",
                    "decimals": 18,
                }
            }
        },
        "ethereum_v2_treasury": {
            "address": "0x464c71f6c2f760dda6093dcb91c24c39e5d6e18c",
            "tokens": {
                "BAL": {
                    "address": "0xba100000625a3754423978a60c9317c58a424e3D",
                    "decimals": 18,
                },
                "CRV": {
                    "address": "0xD533a949740bb3306d119CC777fa900bA034cd52",
                    "decimals": 18,
                }
            }
        },
    },
    "arbitrum_v3": {
        "arbitrum_v3_treasury": {
            "address": "0x053d55f9b5af8694c503eb288a1b7e552f590710",
            "tokens": {
                "ARB": {
                    "address": "0x912CE59144191C1204E64559FE8253a0e49E6548",
                    "decimals": 18,
                }
            }
        },
    }
}

CONFIG_1INCH = {
    "polygon_matic": {
        "market": "polygon_v3",
        "chain": "polygon",
        "loop_market": "MATIC",
        "to_asset": {
            "WMATIC": {
                    "address": "0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270",
                    "decimals": 18,
                }
            },
        "from_assets": {
            "MaticX": {
                    "address": "0xfa68fb4628dff1028cfec22b4162fccd0d45efb6",
                    "decimals": 18,
            },
            "stMATIC": {
                    "address": "0x3a58a54c066fdc0f2d55fc9c89f0415c92ebf3c4",
                    "decimals": 18,
            }
        },
    },
    "polygon_usdc": {
        "market": "polygon_v3",
        "chain": "polygon",
        "loop_market": "MATIC",
        "to_asset": {
            "USDC": {
                    "address": "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",
                    "decimals": 6,
                }
            },
        "from_assets": {
            "WMATIC": {
                    "address": "0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270",
                    "decimals": 18,
                },
        },
    },
    "ethereum_eth": {
        "market": "ethereum_v3",
        "chain": "ethereum",
        "loop_market": "ETH",
        "to_asset": {
            "WETH": {
                    "address": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
                    "decimals": 18,
                }
            },
        "from_assets": {
            "wstETH": {
                    "address": "0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0",
                    "decimals": 18,
            },
            "rETH": {
                    "address": "0xae78736cd615f374d3085123a210448e74fc6393",
                    "decimals": 18,
            },
            "cbETH": {
                    "address": "0xbe9895146f7af43049ca1c1ae358b0541ea49704",
                    "decimals": 18,
            }
        },
    },
    "arbitrum_eth": {
        "market": "arbitrum_v3",
        "chain": "arbitrum",
        "loop_market": "ETH",
        "to_asset": {
            "WETH": {
                    "address": "0x82af49447d8a07e3bd95bd0d56f35241523fbab1",
                    "decimals": 18,
                }
            },
        "from_assets": {
            "wstETH": {
                    "address": "0x5979d7b546e38e414f7e9822514be443a4800529",
                    "decimals": 18,
            },
        },
    },
    "optimism_eth": {
        "market": "optimism_v3",
        "chain": "optimism",
        "loop_market": "ETH",
        "to_asset": {
            "WETH": {
                    "address": "0x4200000000000000000000000000000000000006",
                    "decimals": 18,
                }
            },
        "from_assets": {
            "wstETH": {
                    "address": "0x1f32b1c2345538c0c6f582fcb022739c4a194ebb",
                    "decimals": 18,
            },
        },
    },
    "avalanche_avax": {
        "market": "avax_v3",
        "chain": "avalanche",
        "loop_market": "WAVAX",
        "to_asset": {
            "WAVAX": {
                    "address": "0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7",
                    "decimals": 18,
                }
            },
        "from_assets": {
            "sAVAX": {
                    "address": "0x2b2c81e08f1af8835a78bb2a90ae924ace0ea4be",
                    "decimals": 18,
            },
        },
    },
}

BALANCER_BPT_TOKENS ={
   "arbitrum":
      [
         {
            "pool":"0x5a7f39435fd9c381e4932fa2047c9a5136a5e3e7",
            "symbol":"wstETH-bb-a-WETH-BPT",
            "name":"Balancer wstETH-Boosted Aave WETH StablePool",
            "decimals":18,
            "denom":"ETH",
            "price_token":"0x82af49447d8a07e3bd95bd0d56f35241523fbab1",
            "price_symbol":"WETH"
         },
         {
            "pool":"0xEE02583596AEE94ccCB7e8ccd3921d955f17982A",
            "symbol":"bb-a-USD",
            "name":"Balancer Aave v3 Boosted StablePool",
            "decimals":18,
            "denom":"USD",
            "price_token": None,
            "price_symbol":"USD"
         },
         {
            "pool":"0xCba9Ff45cfB9cE238AfDE32b0148Eb82CbE63562",
            "symbol":"rETH-bb-a-WETH-BPT",
            "name":"Balancer rETH-Boosted Aave WETH StablePool",
            "decimals":18,
            "denom":"ETH",
            "price_token":"0x82af49447d8a07e3bd95bd0d56f35241523fbab1",
            "price_symbol":"WETH"
         }
      ],
   "ethereum":
      [
         {
            "pool":"0x9001cbbd96f54a658ff4e6e65ab564ded76a5431",
            "symbol":"cbETH-bb-a-WETH-BPT",
            "name":"Balancer cbETH-Boosted Aave WETH StablePool",
            "decimals":18,
            "denom":"ETH",
            "price_token":"0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
            "price_symbol":"WETH"
         },
         {
            "pool":"0xe0fcbf4d98f0ad982db260f86cf28b49845403c5",
            "symbol":"wstETH-bb-a-WETH-BPT",
            "name":"Balancer Boosted Aave WETH StablePool",
            "decimals":18,
            "denom":"ETH",
            "price_token":"0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
            "price_symbol":"WETH"
         },
        {
            "pool":"0xfeBb0bbf162E64fb9D0dfe186E517d84C395f016",
            "symbol":"bb-a-USD",
            "name":"Balancer Aave v3 Boosted StablePool",
            "decimals":18,
            "denom":"USD",
            "price_token":None,
            "price_symbol":"USD"
         },
      ],
   "polygon":
      [
         {
            "pool":"0x4a77ef015ddcd972fd9ba2c7d5d658689d090f1a",
            "symbol":"wstETH-bb-a-WETH-BPT",
            "name":"Balancer wstETH-Boosted Aave WETH StablePool",
            "decimals":18,
            "denom":"ETH",
            "price_token":"0x7ceb23fd6bc0add59e62ac25578270cff1b9f619",
            "price_symbol":"WETH"
         },
         {
            "pool":"0xb371aA09F5a110AB69b39A84B5469d29f9b22B76",
            "symbol":"bb-am-USD",
            "name":"Balancer Aave v3 Boosted StablePool",
            "decimals":18,
            "denom":"USD",
            "price_token": None,
            "price_symbol":"USD"
         },
         {
            "pool":"0xe78b25c06db117fdf8f98583cdaaa6c92b79e917",
            "symbol":"MaticX-bb-a-WMATIC-BPT",
            "name":"Balancer MaticX Boosted Aave WMATIC StablePool",
            "decimals":18,
            "denom":"MATIC",
            "price_token":"0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270",
            "price_symbol":"WMATIC"
         },
         {
            "pool":"0x216690738aac4aa0c4770253ca26a28f0115c595",
            "symbol":"stMATIC-bb-a-WMATIC-BPT",
            "name":"Balancer stMATIC-Boosted Aave WMATIC StablePool",
            "decimals":18,
            "denom":"MATIC",
            "price_token":"0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270",
            "price_symbol":"WMATIC"
         }
      ],
   "optimism":
      [
        #  {
        #     "pool":"not yet",
        #     "symbol":"not yet",
        #     "name":"not yet",
        #     "decimals":18,
        #     "denom":"ETH",
        #     "price_token":"0x4200000000000000000000000000000000000006",
        #     "price_symbol":"WETH"
        #  }
      ],
}

CONFIG_SM_TOKENS = {
    "stkAAVE": {
        "stk_token_address": "0x4da27a545c0c5b758a6ba100e3a049001de870f5",
        "stk_token_symbol": "stkAAVE",
        "decimals": 18,
        "unstaked_token_address": "0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9",
        "unstaked_token_symbol": "AAVE",
        "reward_token_address": "0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9",
        "reward_token_symbol": "AAVE",
        "bal_pool_address": None
    },
    "stkABPT": {
        "stk_token_address": "0xa1116930326d21fb917d5a27f1e9943a9595fb47",
        "stk_token_symbol": "stkABPT",
        "decimals": 18,
        "unstaked_token_address": "0x41a08648c3766f9f9d85598ff102a08f4ef84f84",
        "unstaked_token_symbol": "ABPT",
        "reward_token_address": "0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9",
        "reward_token_symbol": "AAVE",
        "bal_pool_address": '0xc697051d1c6296c24ae3bcef39aca743861d9a81'
    },
}

COINGECKO_TOKENS = {
    "stkABPT": {
        "cg_id": "staked-aave-balancer-pool-token",
        "symbol": "stkABPT",
        "address": "0xa1116930326d21fb917d5a27f1e9943a9595fb47",
        "chain": "ethereum",
        "decimals": 18,
        "start_date": "2022-01-01",
    },
    "ARB": {
        "cg_id": "arbitrum",
        "symbol": "ARB",
        "address": "0x912ce59144191c1204e64559fe8253a0e49e6548",
        "chain": "arbitrum",
        "decimals": 18,
        "start_date": "2023-01-20",
    },
}

CONFIG_COMPOUND_v2 = {
    # using aave market to select because that's the block source.  data stored by chain
    "ethereum_v3": {
        "cDAI": {
            "address": "0x5d3a536e4d6dbd6114cc1ead35777bab948e3643",
            "ctoken_decimals": 8,
            "underlying_address": "0x6b175474e89094c44da98b954eedeac495271d0f",
            "underlying_symbol": "DAI",
            "underlying_decimals": 18,
        },
        "cUSDC": {
            "address": "0x39aa39c021dfbae8fac545936693ac917d5e7563",
            "ctoken_decimals": 8,
            "underlying_address": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
            "underlying_symbol": "USDC",
            "underlying_decimals": 6,
        },
        "cUSDT": {
            "address": "0xf650c3d88d12db855b8bf7d11be6c55a4e07dcc9",
            "ctoken_decimals": 8,
            "underlying_address": "0xdac17f958d2ee523a2206206994597c13d831ec7",
            "underlying_symbol": "USDT",
            "underlying_decimals": 6,
        },
    }
}

CONFIG_COMPOUND_v3 = {
    # using aave market to select because that's the block source.  data stored by chain
    "ethereum_v3": {
        "cUSDC": "0xc3d688b66703497daa19211eedff47f25384cdc3",
    }
}


if __name__ == "__main__":
    
    # print(list(CONFIG_TOKENS.keys()))
    # for key in CONFIG_TOKENS.keys():
    #     print(key)
    #     print(CONFIG_TOKENS[key]["address"])
    #     print(CONFIG_TOKENS[key]["tokens"])
    #     for token in CONFIG_TOKENS[key]["tokens"].keys():
    #         print(token)
    #         print(CONFIG_TOKENS[key]["tokens"][token]["address"])
    #         print(CONFIG_TOKENS[key]["tokens"][token]["decimals"])
    pass
