from datetime import datetime, timezone

import pandas as pd
import numpy as np
from icecream import ic
from pandas.testing import assert_frame_equal
# pylint: disable=import-error
from aave_data.resources.financials_config import CONFIG_MARKETS 
from aave_data.resources.helpers import (
                            get_erc20_balance_of,
                            get_scaled_balance_of,
                            get_market_tokens_at_block_aave,
                            get_market_tokens_at_block_messari,
                            get_token_transfers_from_covalent,
                            get_events_by_topic_hash_from_covalent,
                            standardise_types
                              )
 # pylint: enable=import-error

def test_get_market_tokens_at_block_messari():
    """
    Tests the Messari subgraph agains a reference response
    """
    market = "ethereum_v2"
    block_height = 16050438
    expected = pd.DataFrame(
        [
            {
                "atoken": "0x101cc05f4a51c0319f570d5e146a8c625198e636",
                "atoken_decimals": 18,
                "atoken_symbol": "aTUSD",
                "block_height": 16050438,
                "decimals": 18,
                "market": "ethereum_v2",
                "name": "TrueUSD",
                "pool": "0x7d2768dE32b0b80b7a3454c06BdAc94A69DDc7A9",
                "reserve": "0x0000000000085d4780b73119b644ae5ecd22b376",
                "symbol": "TUSD",
            },
            {
                "atoken": "0xc9bc48c72154ef3e5425641a3c747242112a46af",
                "atoken_decimals": 18,
                "atoken_symbol": "aRAI",
                "block_height": 16050438,
                "decimals": 18,
                "market": "ethereum_v2",
                "name": "Rai Reflex Index",
                "pool": "0x7d2768dE32b0b80b7a3454c06BdAc94A69DDc7A9",
                "reserve": "0x03ab458634910aad20ef5f1c8ee96f1d6ac54919",
                "symbol": "RAI",
            },
        ]
    )
    expected_length = 37
    result = get_market_tokens_at_block_messari(market, block_height, CONFIG_MARKETS)
    result_first_2 = result.head(2)
    # result_first_2.info()
    # expected.info()
    # ic(result_first_2)
    # ic(expected)
    # assert result_first_2.equals(expected), str(result_first_2)
    assert_frame_equal(result_first_2, expected, check_exact=True, check_like=True)
    assert len(result) == expected_length, str(result)


def test_get_market_tokens_at_block_aave():
    """
    Tests the Aave subgraph agains a reference response
    """
    # Check the V2 subgraph
    market = "ethereum_v2"
    block_height = 16050438
    expected = pd.DataFrame(
        [
            {
                "atoken": "0x101cc05f4a51c0319f570d5e146a8c625198e636",
                "atoken_decimals": 18,
                "atoken_symbol": "aTUSD",
                "block_height": 16050438,
                "decimals": 18,
                "market": "ethereum_v2",
                "name": "TrueUSD",
                "pool": "0x7d2768dE32b0b80b7a3454c06BdAc94A69DDc7A9",
                "reserve": "0x0000000000085d4780b73119b644ae5ecd22b376",
                "symbol": "TUSD",
            },
            {
                "atoken": "0xc9bc48c72154ef3e5425641a3c747242112a46af",
                "atoken_decimals": 18,
                "atoken_symbol": "aRAI",
                "block_height": 16050438,
                "decimals": 18,
                "market": "ethereum_v2",
                "name": "Rai Reflex Index",
                "pool": "0x7d2768dE32b0b80b7a3454c06BdAc94A69DDc7A9",
                "reserve": "0x03ab458634910aad20ef5f1c8ee96f1d6ac54919",
                "symbol": "RAI",
            },
        ]
    )
    expected_length = 37
    result = get_market_tokens_at_block_aave(market, block_height, CONFIG_MARKETS)
    result_first_2 = result.head(2)
    assert_frame_equal(result_first_2, expected, check_exact=True, check_like=True)
    assert len(result) == expected_length, str(result)

    # check the V1 subgraph
    market = "ethereum_v1"
    block_height = 16050438
    expected = pd.DataFrame(
        {
            "atoken": {
                0: "0x4da9b813057d04baef4e5800e36083717b4a0341",
                1: "0x12e51e77daaa58aa0e9247db7510ea4b46f9bead",
            },
            "atoken_decimals": {0: 18, 1: 18},
            "atoken_symbol": {0: "aTUSD", 1: "aYFI"},
            "block_height": {0: 16050438, 1: 16050438},
            "decimals": {0: 18, 1: 18},
            "market": {0: "ethereum_v1", 1: "ethereum_v1"},
            "name": {0: "TrueUSD", 1: "yearn.finance"},
            "pool": {
                0: "0x398ec7346dcd622edc5ae82352f02be94c62d119",
                1: "0x398ec7346dcd622edc5ae82352f02be94c62d119",
            },
            "reserve": {
                0: "0x0000000000085d4780b73119b644ae5ecd22b376",
                1: "0x0bc529c00c6401aef6d220be8c6ea1667f6ad93e",
            },
            "symbol": {0: "TUSD", 1: "YFI"},
        }
    )

    expected_length = 22
    result = get_market_tokens_at_block_aave(market, block_height, CONFIG_MARKETS)
    result_first_2 = result.head(2)
    assert_frame_equal(result_first_2, expected, check_like=True)
    assert len(result) == expected_length, str(result)

    # Check the V3 subgraph
    market = "polygon_v3"
    block_height = 36068925

    expected = pd.DataFrame(
        {
            "atoken": {
                0: "0xc45a479877e1e9dfe9fcd4056c699575a1045daa",
                1: "0x6d80113e533a2c0fe82eabd35f1875dcea89ea97",
            },
            "atoken_decimals": {0: 18, 1: 18},
            "atoken_symbol": {0: "aPolSUSHI", 1: "aPolWMATIC"},
            "block_height": {0: 36068925, 1: 36068925},
            "decimals": {0: 18, 1: 18},
            "market": {0: "polygon_v3", 1: "polygon_v3"},
            "name": {0: "SushiToken (PoS)", 1: "Wrapped Matic"},
            "pool": {
                0: "0x794a61358d6845594f94dc1db02a252b5b4814ad",
                1: "0x794a61358d6845594f94dc1db02a252b5b4814ad",
            },
            "reserve": {
                0: "0x0b3f868e0be5597d5db7feb59e1cadbb0fdda50a",
                1: "0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270",
            },
            "symbol": {0: "SUSHI", 1: "WMATIC"},
        }
    )

    expected_length = 19
    result = get_market_tokens_at_block_aave(market, block_height, CONFIG_MARKETS)
    result_first_2 = result.head(2)
    assert_frame_equal(result_first_2, expected, check_exact=True, check_like=True)
    assert len(result) == expected_length, str(result)




def test_get_token_transfers_from_covalent():
    """Tests the covalent token transfers function against a reference response"""
    
    expected = pd.DataFrame(
        {
            "transfers_transfer_type":{
                0:"IN",
                1:"OUT"
            },
            "transfers_from_address":{
                0:"0x0000000000000000000000000000000000000000",
                1:"0x464c71f6c2f760dda6093dcb91c24c39e5d6e18c"
            },
            "transfers_to_address":{
                0:"0x464c71f6c2f760dda6093dcb91c24c39e5d6e18c",
                1:"0x04f90d449d4f8316edd6ef4f963b657f8444a4ca"
            },
            "transfers_contract_address":{
                0:"0xbcca60bb61934080951369a648fb03df4f96263c",
                1:"0xbcca60bb61934080951369a648fb03df4f96263c"
            },
            "transfers_contract_name":{
                0:"Aave interest bearing USDC",
                1:"Aave interest bearing USDC"
            },
            "transfers_contract_decimals":{
                0:6,
                1:6
            },
            "transfers_contract_symbol":{
                0:"aUSDC",
                1:"aUSDC"
            },
            "block_day":{
                0: datetime(2022, 11, 26, 0, 0, 0, tzinfo=timezone.utc),
                1: datetime(2022, 11, 26, 0, 0, 0, tzinfo=timezone.utc)
            },
            "amount_transferred":{
                0:2924.196349,
                1:25077.808782
            },
            "start_block":{
                0:16050438,
                1:16050438
            },
            "end_block":{
                0:16057596,
                1:16057596
            }
        }
    )
    expected = standardise_types(expected)

    result = get_token_transfers_from_covalent(16050438, 16057596, 1, '0x464C71f6c2F760DdA6093dCB91C24c39e5d6e18c', '0xbcca60bb61934080951369a648fb03df4f96263c')
    
    assert_frame_equal(result, expected, check_exact=True, check_like=True)

def test_get_erc20_balance_of():
    """
    Tests the erc20 balance of function against a reference response

    Tests harmony chain as there is no block explorer support for contract ABIs

    """
    
    expected_ethereum = 6361548.379717
    expected_harmony = 765.0733904749163

    result_ethereum  = get_erc20_balance_of('0x464C71f6c2F760DdA6093dCB91C24c39e5d6e18c', '0xbcca60bb61934080951369a648fb03df4f96263c', 6, 'ethereum', block_height=16057596)
    result_harmony  = get_erc20_balance_of('0x8a020d92d6b119978582be4d3edfdc9f7b28bf31', '0x191c10Aa4AF7C30e871E70C95dB0E4eb77237530', 18, 'harmony', block_height=34443481)

    assert result_ethereum == expected_ethereum, str(result_ethereum)  
    assert isinstance(result_ethereum, float), str(type(result_ethereum))

    assert result_harmony == expected_harmony, str(result_harmony)  
    assert isinstance(result_harmony, float), str(type(result_ethereum))

def test_get_scaled_balance_of():
    """
    Tests the scaled balance of function against a reference response

    Tests harmony chain as there is no block explorer support for contract ABIs

    """
    
    expected_ethereum = 5893907.247913
    expected_harmony = 341.926480873084

    result_ethereum  = get_scaled_balance_of('0x464C71f6c2F760DdA6093dCB91C24c39e5d6e18c', '0xbcca60bb61934080951369a648fb03df4f96263c', 6, 'ethereum', block_height=16057596)
    result_harmony  = get_scaled_balance_of('0x8a020d92d6b119978582be4d3edfdc9f7b28bf31', '0x191c10Aa4AF7C30e871E70C95dB0E4eb77237530', 18, 'harmony', block_height=34443481)

    assert result_ethereum == expected_ethereum, str(result_ethereum)  
    assert isinstance(result_ethereum, float), str(type(result_ethereum))

    assert result_harmony == expected_harmony, str(result_harmony)  
    assert isinstance(result_harmony, float), str(type(result_ethereum))


def test_get_events_by_topic_hash_from_covalent():
    """
    Tests the covalent events by topic hash function against a reference response
    Uses Avalanche tx 0xde2b36439ea0b64acaad3bfbcd05cd256a0021977fdb137e0e20c39be5e5f17a
    """

    expected = pd.DataFrame(
                {
                    "block_signed_at":{
                        0:datetime(2022, 5, 25, 10, 52, 31, tzinfo=timezone.utc)
                    },
                    "block_height":{
                        0:15154957
                    },
                    "tx_offset":{
                        0:2
                    },
                    "log_offset":{
                        0:10
                    },
                    "tx_hash":{
                        0:"0xde2b36439ea0b64acaad3bfbcd05cd256a0021977fdb137e0e20c39be5e5f17a"
                    },
                    "sender_contract_decimals":{
                        0:0
                    },
                    "sender_name":{
                        0:np.nan
                    },
                    "sender_contract_ticker_symbol":{
                        0:np.nan
                    },
                    "sender_address":{
                        0:"0x794a61358d6845594f94dc1db02a252b5b4814ad"
                    },
                    "sender_address_label":{
                        0:np.nan
                    },
                    "sender_logo_url":{
                        0:"https://logos.covalenthq.com/tokens/43114/0x794a61358d6845594f94dc1db02a252b5b4814ad.png"
                    },
                    "raw_log_data":{
                        0:"0x000000000000000000000000000000000000000000002f93360435b59609ee68"
                    },
                    "decoded":{
                        0:np.nan
                    },
                    "raw_log_topics_0":{
                        0:"0xbfa21aa5d5f9a1f0120a95e7c0749f389863cbdbfff531aa7339077a5bc919de"
                    },
                    "raw_log_topics_1":{
                        0:"0x000000000000000000000000d586e7f844cea2f87f50152665bcbc2c279d8d70"
                    }
                }
    )

    expected_length = 8

    result = get_events_by_topic_hash_from_covalent(
                15154950,
                15154960,
                43114,
                '0xbfa21aa5d5f9a1f0120a95e7c0749f389863cbdbfff531aa7339077a5bc919de',
                '0x794a61358D6845594F94dc1DB02A252b5b4814aD'
            )

    assert len(result) == expected_length, str(len(result))
    assert_frame_equal(result.head(1), expected, check_exact=True, check_like=True)

def test_standarise_types():
    """
    Tests the standarise types function against a reference response
    """

    input = pd.DataFrame(
            [
                {
                    'block_day': datetime(2022,11,26,0,0,0),
                    'chain': 'ethereum',
                    'token_address': '0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9',
                    'float_field': 1178.178995987,
                    'int_field': 15154960,
                }
            ]
        )

    expected = pd.DataFrame(
            [
                {
                    'block_day': datetime(2022,11,26,0,0,0, tzinfo=timezone.utc),
                    'chain': 'ethereum',
                    'token_address': '0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9'.lower(),
                    'float_field': 1178.178995987,
                    'int_field': 15154960,
                }
            ]
        )

    expected.chain = expected.chain.astype(pd.StringDtype())
    expected.token_address = expected.token_address.astype(pd.StringDtype())
    expected.float_field = expected.float_field.astype('Float64')
    expected.int_field = expected.int_field.astype('Int64')

    ic(input)
    ic(expected)
    input.info()
    expected.info()

    result = standardise_types(input)

    assert_frame_equal(result, expected, check_exact=True)

    

if __name__ == "__main__":
    # test_get_market_tokens_at_block_aave()
    # test_get_erc20_balance_of()
    # test_get_token_transfers_from_covalent()
    # test_get_events_by_topic_hash_from_covalent()
    # test_standarise_types()
    test_get_scaled_balance_of()