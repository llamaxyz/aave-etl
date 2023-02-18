""" Contains config objects for use in financials """
import os
from datetime import datetime


FINANCIAL_PARTITION_START_DATE = '2023-02-15'

ETHERSCAN_API_KEY =  os.environ['ETHERSCAN_API_KEY']
POLYGONSCAN_API_KEY =  os.environ['POLYGONSCAN_API_KEY']
COVALENT_KEY = os.environ['COVALENT_KEY']
WEB3_ALCHEMY_API_KEY = os.environ['WEB3_ALCHEMY_API_KEY']
POLYGON_ALCHEMY_KEY = os.environ['POLYGON_ALCHEMY_KEY']
OPTIMISM_ALCHEMY_KEY = os.environ['OPTIMISM_ALCHEMY_KEY']
ARBITRUM_ALCHEMY_KEY = os.environ['ARBITRUM_ALCHEMY_KEY']
FLIPSIDE_API_KEY = os.environ['FLIPSIDE_API_KEY']

 # a minimal ERC20 ABI supporting balanceOf and decimals
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
        "web3_rpc_url": f"https://eth-mainnet.g.alchemy.com/v2/{WEB3_ALCHEMY_API_KEY}"
    },
    "polygon": {
        "chain_id": 137,
        "defillama_chain": "polygon",
        "ape_ok": True,
        "ape_network_choice": "polygon:mainnet:alchemy",
        "wrapped_ether": "0x7ceB23fD6bC0adD59E62ac25578270cFf1b9f619",
        "web3_rpc_url": f"https://polygon-mainnet.g.alchemy.com/v2/{POLYGON_ALCHEMY_KEY}"
    },
    "avalanche": {
        "chain_id": 43114,
        "defillama_chain": "avax",
        "ape_ok": True, 
        "ape_network_choice": "avalanche:mainnet:https://rpc.ankr.com/avalanche",
        "wrapped_ether": "0x49D5c2BdFfac6CE2BFdB6640F4F80f226bc10bAB",
        "web3_rpc_url": "https://rpc.ankr.com/avalanche"
    },
    "optimism": {
        "chain_id": 10,
        "defillama_chain": "optimism",
        "ape_ok": True,
        "ape_network_choice": "optimism:mainnet:alchemy",
        "wrapped_ether": "0x4200000000000000000000000000000000000006",
        "web3_rpc_url": f"https://opt-mainnet.g.alchemy.com/v2/{OPTIMISM_ALCHEMY_KEY}"
    },
    "arbitrum": {
        "chain_id": 42161,
        "defillama_chain": "arbitrum",
        "ape_ok": True,
        "ape_network_choice": "arbitrum:mainnet:alchemy",
        "wrapped_ether": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
        "web3_rpc_url": f"https://arb-mainnet.g.alchemy.com/v2/{ARBITRUM_ALCHEMY_KEY}"
    },
    "fantom": {
        "chain_id": 250,
        "defillama_chain": "fantom",
        "ape_ok": True, 
        "ape_network_choice": "fantom:opera:https://rpc.ankr.com/fantom",
        "wrapped_ether": "0x74b23882a30290451A17c44f4F05243b6b58C76d",
        "web3_rpc_url": "https://rpc.ankr.com/fantom"
    },
    "harmony": {
        "chain_id": 1666600000,
        "defillama_chain": "harmony",
        "ape_ok": True, 
        "ape_network_choice": "harmony:mainnet:https://a.api.s0.t.hmny.io",
        "wrapped_ether": "0x6983D1E6DEf3690C4d616b13597A09e6193EA013",
        "web3_rpc_url": "https://a.api.s0.t.hmny.io"
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
        "rewards_token": "0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9",
        "rewards_token_symbol": "stkAAVE",
        "rewards_token_decimals": 18,
        "subgraph": 'https://api.thegraph.com/subgraphs/name/aave/protocol-v2',
        "messari_subgraph": "https://api.thegraph.com/subgraphs/name/messari/aave-v2-ethereum",
        "token_source": "messari",
        "atoken_prefix": "a",
        "oracle_base_currency": 'wei',
        "block_table_master": True,
    },
    "aave_amm": {
        "chain": "ethereum",
        "version": 2,
        "pool": "0x7937D4799803FbBe595ed57278Bc4cA21f3bFfCB",
        "collector": "0x464C71f6c2F760DdA6093dCB91C24c39e5d6e18c",
        "protocol_data_provider": "0xc443AD9DDE3cecfB9dfC5736578f447aFE3590ba",
        "oracle": "0xA50ba011c48153De246E5192C8f9258A2ba79Ca9",
        "incentives_controller": None,
        "rewards_token": None,
        "rewards_token_symbol": None,
        "rewards_token_decimals": None,
        "subgraph": 'https://api.thegraph.com/subgraphs/name/aave/protocol-v2',
        "messari_subgraph": "https://api.thegraph.com/subgraphs/name/messari/aave-amm-ethereum",
        "token_source": "messari",
        "atoken_prefix": "a",
        "oracle_base_currency": 'wei',
        "block_table_master": False,
    },
    "aave_arc": {
        "chain": "ethereum",
        "version": 2,
        "pool": "0x37D7306019a38Af123e4b245Eb6C28AF552e0bB0",
        "collector": "0x464C71f6c2F760DdA6093dCB91C24c39e5d6e18c",
        "protocol_data_provider": "0x71B53fC437cCD988b1b89B1D4605c3c3d0C810ea",
        "lending_pool_address_provider": "0x6FdfafB66d39cD72CFE7984D3Bbcc76632faAb00",
        "oracle": "0xB8a7bc0d13B1f5460513040a97F404b4fea7D2f3",
        "incentives_controller": None,
        "rewards_token": None,
        "rewards_token_symbol": None,
        "rewards_token_decimals": None,
        "subgraph": None, #'https://api.thegraph.com/subgraphs/name/aave/aave-arc', buggy
        "messari_subgraph": "https://api.thegraph.com/subgraphs/name/messari/aave-arc-ethereum",
        "token_source": "messari",
        "atoken_prefix": "a",
        "oracle_base_currency": 'wei',
        "block_table_master": False,
    },
    "aave_rwa": {
        "chain": "ethereum",
        "version": 2,
        "pool": "0xA1a8c33C9a9a9DE231b13a2271a7C09c11C849F1",
        "collector": "0x464C71f6c2F760DdA6093dCB91C24c39e5d6e18c",
        "protocol_data_provider": "0xe69DE09Cd274242DDD7004794a4b00EbE9B65fEA",
        "oracle": "0xf20E5fe26811f7336a8A637C42b6e8913B291868",
        "incentives_controller": "0x77B59b07b87689a6D27adE063FB1D08C7Fe52F0b",
        "rewards_token": "0xc221b7E65FfC80DE234bbB6667aBDd46593D34F0",
        "rewards_token_symbol": "wCFG",
        "rewards_token_decimals": 18,
        "subgraph": None,
        "messari_subgraph": "https://api.thegraph.com/subgraphs/name/messari/aave-rwa-ethereum",
        "token_source": "messari",
        "atoken_prefix": "a",
        "oracle_base_currency": 'usd',
        "block_table_master": False,
    },
    "ethereum_v3": {
        "chain": "ethereum",
        "version": 3,
        "pool": "0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2",
        "collector": "0x464C71f6c2F760DdA6093dCB91C24c39e5d6e18c",
        "protocol_data_provider": "0x7B4EB56E7CD4b454BA8ff71E4518426369a138a3",
        "lending_pool_address_provider": '0x2f39d218133AFaB8F2B819B1066c7E434Ad94E9e',
        "oracle": "0x54586bE62E3c3580375aE3723C145253060Ca0C2",
        "incentives_controller": "0x8164Cc65827dcFe994AB23944CBC90e0aa80bFcb",
        "rewards_token": None,
        "rewards_token_symbol": None,
        "rewards_token_decimals": None,
        "subgraph": 'https://api.thegraph.com/subgraphs/name/aave/protocol-v3',
        "messari_subgraph": "https://api.thegraph.com/subgraphs/name/messari/aave-v3-ethereum",
        "token_source": "messari",
        "atoken_prefix": "aEth",
        "oracle_base_currency": 'usd',
        "block_table_master": False,
    },
    "polygon_v2": {
        "chain": "polygon",
        "version": 2,
        "pool": "0x8dFf5E27EA6b7AC08EbFdf9eB090F32ee9a30fcf",
        "collector": "0x7734280A4337F37Fbf4651073Db7c28C80B339e9",
        "protocol_data_provider": "0x7551b5D2763519d4e37e8B81929D336De671d46d",
        "oracle": "0x0229F777B0fAb107F9591a41d5F02E4e98dB6f2d",
        "incentives_controller": "0x357D51124f59836DeD84c8a1730D72B749d8BC23",
        "rewards_token": "0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270",
        "rewards_token_symbol": "WMATIC",
        "rewards_token_decimals": 18,
        "subgraph": 'https://api.thegraph.com/subgraphs/name/aave/aave-v2-matic',
        "messari_subgraph": None, #"https://thegraph.com/hosted-service/subgraph/messari/aave-v2-polygon" not indexed yet,
        "token_source": "aave",
        "atoken_prefix": "am",
        "oracle_base_currency": 'wei',
        "block_table_master": True,
    },
    "avax_v2": {
        "chain": "avalanche",
        "version": 2,
        "pool": "0x4F01AeD16D97E3aB5ab2B501154DC9bb0F1A5A2C",
        "collector": "0x467b92aF281d14cB6809913AD016a607b5ba8A36",
        "protocol_data_provider": "0x65285E9dfab318f57051ab2b139ccCf232945451",
        "oracle": "0xdC336Cd4769f4cC7E9d726DA53e6d3fC710cEB89",
        "incentives_controller": "0x01D83Fe6A10D2f2B7AF17034343746188272cAc9",
        "rewards_token": "0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7",
        "rewards_token_symbol": "WAVAX",
        "rewards_token_decimals": 18,
        "subgraph": 'https://api.thegraph.com/subgraphs/name/aave/protocol-v2-avalanche',
        "messari_subgraph": "https://api.thegraph.com/subgraphs/name/messari/aave-v2-avalanche",
        "token_source": "messari",
        "atoken_prefix": "av",
        "oracle_base_currency": 'usd',
        "block_table_master": True,
    },
    "polygon_v3": {
        "chain": "polygon",
        "version": 3,
        "pool": "0x794a61358d6845594f94dc1db02a252b5b4814ad",
        "collector": "0xe8599f3cc5d38a9ad6f3684cd5cea72f10dbc383",
        "protocol_data_provider": "0x69fa688f1dc47d4b5d8029d5a35fb7a548310654",
        "lending_pool_address_provider": '0xa97684ead0e402dc232d5a977953df7ecbab3cdb',
        "oracle": "0xb023e699f5a33916ea823a16485e259257ca8bd1",
        "incentives_controller": "0x929ec64c34a17401f460460d4b9390518e5b473e",
        "rewards_token": None,
        "rewards_token_symbol": None,
        "rewards_token_decimals": None,
        "subgraph": 'https://api.thegraph.com/subgraphs/name/aave/protocol-v3-polygon',
        "messari_subgraph": "https://api.thegraph.com/subgraphs/name/messari/aave-v3-polygon",
        "token_source": "messari", 
        "atoken_prefix": "aPol",
        "oracle_base_currency": 'usd',
        "block_table_master": False,
    },
    "avax_v3": {
        "chain": "avalanche",
        "version": 3,
        "pool": "0x794a61358d6845594f94dc1db02a252b5b4814ad",
        "collector": "0x5ba7fd868c40c16f7adfae6cf87121e13fc2f7a0",
        "protocol_data_provider": "0x69fa688f1dc47d4b5d8029d5a35fb7a548310654",
        "lending_pool_address_provider": '0xa97684ead0e402dc232d5a977953df7ecbab3cdb',
        "oracle": "0xebd36016b3ed09d4693ed4251c67bd858c3c7c9c",
        "incentives_controller": "0x929ec64c34a17401f460460d4b9390518e5b473e",
        "rewards_token": None,
        "rewards_token_symbol": None,
        "rewards_token_decimals": None,
        "subgraph": 'https://api.thegraph.com/subgraphs/name/aave/protocol-v3-avalanche',
        "messari_subgraph": "https://api.thegraph.com/subgraphs/name/messari/aave-v3-avalanche",
        "token_source": "messari", 
        "atoken_prefix": "aAva",
        "oracle_base_currency": 'usd',
        "block_table_master": False,
    },
    "optimism_v3": {
        "chain": "optimism",
        "version": 3,
        "pool": "0x794a61358d6845594f94dc1db02a252b5b4814ad",
        "collector": "0xb2289e329d2f85f1ed31adbb30ea345278f21bcf",
        "protocol_data_provider": "0x69fa688f1dc47d4b5d8029d5a35fb7a548310654",
        "lending_pool_address_provider": '0xa97684ead0e402dc232d5a977953df7ecbab3cdb',
        "oracle": "0xd81eb3728a631871a7ebbad631b5f424909f0c77",
        "incentives_controller": "0x929ec64c34a17401f460460d4b9390518e5b473e",
        "rewards_token": None,
        "rewards_token_symbol": None,
        "rewards_token_decimals": None,
        "subgraph": 'https://api.thegraph.com/subgraphs/name/aave/protocol-v3-optimism',
        "messari_subgraph": "https://api.thegraph.com/subgraphs/name/messari/aave-v3-optimism",
        "token_source": "aave", # messari not indexed yet
        "atoken_prefix": "aOpt",
        "oracle_base_currency": 'usd',
        "block_table_master": True,
    },
    "arbitrum_v3": {
        "chain": "arbitrum",
        "version": 3,
        "pool": "0x794a61358d6845594f94dc1db02a252b5b4814ad",
        "collector": "0x053d55f9b5af8694c503eb288a1b7e552f590710",
        "protocol_data_provider": "0x69fa688f1dc47d4b5d8029d5a35fb7a548310654",
        "lending_pool_address_provider": '0xa97684ead0e402dc232d5a977953df7ecbab3cdb',
        "oracle": "0xb56c2f0b653b2e0b10c9b928c8580ac5df02c7c7",
        "incentives_controller": "0x929ec64c34a17401f460460d4b9390518e5b473e",
        "rewards_token": None,
        "rewards_token_symbol": None,
        "rewards_token_decimals": None,
        "subgraph": 'https://api.thegraph.com/subgraphs/name/aave/protocol-v3-arbitrum',
        "messari_subgraph": "https://api.thegraph.com/subgraphs/name/messari/aave-v3-arbitrum",
        "token_source": "messari",
        "atoken_prefix": "aArb",
        "oracle_base_currency": 'usd',
        "block_table_master": True,
    },
    "fantom_v3": {
        "chain": "fantom",
        "version": 3,
        "pool": "0x794a61358d6845594f94dc1db02a252b5b4814ad",
        "collector": "0xbe85413851d195fc6341619cd68bfdc26a25b928",
        "protocol_data_provider": "0x69fa688f1dc47d4b5d8029d5a35fb7a548310654",
        "lending_pool_address_provider": '0xa97684ead0e402dc232d5a977953df7ecbab3cdb',
        "oracle": "0xfd6f3c1845604c8ae6c6e402ad17fb9885160754",
        "incentives_controller": "0x929ec64c34a17401f460460d4b9390518e5b473e",
        "rewards_token": None,
        "rewards_token_symbol": None,
        "rewards_token_decimals": None,
        "subgraph": 'https://api.thegraph.com/subgraphs/name/aave/protocol-v3-fantom',
        "messari_subgraph": "https://api.thegraph.com/subgraphs/name/messari/aave-v3-fantom",
        "token_source": "messari",
        "atoken_prefix": "aFan",
        "oracle_base_currency": 'usd',
        "block_table_master": True,
    },
    "harmony_v3": {
        "chain": "harmony",
        "version": 3,
        "pool": "0x794a61358d6845594f94dc1db02a252b5b4814ad",
        "collector": "0x8a020d92d6b119978582be4d3edfdc9f7b28bf31",
        "protocol_data_provider": "0x69fa688f1dc47d4b5d8029d5a35fb7a548310654",
        "lending_pool_address_provider": '0xa97684ead0e402dc232d5a977953df7ecbab3cdb',
        "oracle": "0x3c90887ede8d65ccb2777a5d577beab2548280ad",
        "incentives_controller": "0x929ec64c34a17401f460460d4b9390518e5b473e",
        "rewards_token": None,
        "rewards_token_symbol": None,
        "rewards_token_decimals": None,
        "subgraph": 'https://api.thegraph.com/subgraphs/name/aave/protocol-v3-harmony',
        "messari_subgraph": "https://api.thegraph.com/subgraphs/name/messari/aave-v3-harmony",
        "token_source": "messari",
        "atoken_prefix": "aHar",
        "oracle_base_currency": 'usd',
        "block_table_master": True,
    },
    "ethereum_v1": {
        "chain": "ethereum",
        "version": 1,
        "pool": "0x398ec7346dcd622edc5ae82352f02be94c62d119",
        "pool_address_provider": "0x24a42fd28c976a61df5d00d0599c34c4f90748c8",
        "collector": "0xe3d9988f676457123c5fd01297605efdd0cba1ae",
        "collector_change_date": datetime(2022,7,7,0,0,0),
        "collector_v2": "0x464C71f6c2F760DdA6093dCB91C24c39e5d6e18c", #collector contract changed on 2022-07-07
        "protocol_data_provider": None,
        "oracle": "0x76b47460d7f7c5222cfb6b6a75615ab10895dde4",
        "incentives_controller": None,
        "rewards_token": None,
        "rewards_token_symbol": None,
        "rewards_token_decimals": None,
        "subgraph": 'https://api.thegraph.com/subgraphs/name/aave/protocol-multy-raw',
        "messari_subgraph": None,
        "token_source": "aave",
        "atoken_prefix": "a",
        "oracle_base_currency": 'wei',
        "block_table_master": False,
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
