from datetime import datetime, timezone

import pandas as pd
import numpy as np
import asyncio
import httpx
from icecream import ic
from pandas.testing import assert_frame_equal
# pylint: disable=import-error
from aave_data.resources.financials_config import CONFIG_MARKETS 
from aave_data.resources.helpers import (
                            get_erc20_balance_of,
                            get_scaled_balance_of,
                            get_market_tokens_at_block_aave,
                            get_market_tokens_at_block_messari,
                            get_market_tokens_at_block_rpc,
                            get_token_transfers_from_covalent,
                            get_token_transfers_from_alchemy,
                            get_events_by_topic_hash_from_covalent,
                            standardise_types,
                            get_raw_reserve_data,
                            raw_reserve_to_dataframe,
                            get_quote_from_1inch,
                            get_aave_oracle_price,
                            get_quote_from_1inch_async,
                            get_balancer_bpt_data
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

    market_v3 = "ethereum_v3"
    block_height_v3 = 16286697
    expected_v3 = pd.DataFrame()
    expected_length_v3 = 0
    result_v3 = get_market_tokens_at_block_messari(market_v3, block_height_v3, CONFIG_MARKETS)

    # result_first_2.info()
    # expected.info()
    # ic(result_first_2)
    # ic(expected)
    # assert result_first_2.equals(expected), str(result_first_2)
    assert_frame_equal(result_first_2, expected, check_exact=True, check_like=True)
    assert_frame_equal(result_v3, expected_v3, check_exact=True, check_like=True)
    assert len(result) == expected_length, str(result)
    assert len(result_v3) == expected_length_v3, str(result_v3)


def test_get_market_tokens_at_block_rpc():
    """
    Tests the market tokens function via RPC agains a reference response
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
    result = get_market_tokens_at_block_rpc(market, block_height, CONFIG_MARKETS)
    result_first_2 = result.head(2)

    market_v3 = "ethereum_v3"
    block_height_v3 = 16286697
    expected_v3 = pd.DataFrame()
    expected_length_v3 = 0
    result_v3 = get_market_tokens_at_block_rpc(market_v3, block_height_v3, CONFIG_MARKETS)

    # result_first_2.info()
    # expected.info()
    # ic(result_first_2)
    # ic(expected)
    # assert result_first_2.equals(expected), str(result_first_2)
    assert_frame_equal(result_first_2, expected, check_exact=True, check_like=True)
    assert_frame_equal(result_v3, expected_v3, check_exact=True, check_like=True)
    assert len(result) == expected_length, str(result)
    assert len(result_v3) == expected_length_v3, str(result_v3)

def test_get_market_tokens_at_block_aave():
    """
    Tests the Aave subgraph agains a reference response
    """
    # # Check the V2 subgraph
    # market = "ethereum_v2"
    # block_height = 16050438
    # expected = pd.DataFrame(
    #     [
    #         {
    #             "atoken": "0x101cc05f4a51c0319f570d5e146a8c625198e636",
    #             "atoken_decimals": 18,
    #             "atoken_symbol": "aTUSD",
    #             "block_height": 16050438,
    #             "decimals": 18,
    #             "market": "ethereum_v2",
    #             "name": "TrueUSD",
    #             "pool": "0x7d2768dE32b0b80b7a3454c06BdAc94A69DDc7A9",
    #             "reserve": "0x0000000000085d4780b73119b644ae5ecd22b376",
    #             "symbol": "TUSD",
    #         },
    #         {
    #             "atoken": "0xc9bc48c72154ef3e5425641a3c747242112a46af",
    #             "atoken_decimals": 18,
    #             "atoken_symbol": "aRAI",
    #             "block_height": 16050438,
    #             "decimals": 18,
    #             "market": "ethereum_v2",
    #             "name": "Rai Reflex Index",
    #             "pool": "0x7d2768dE32b0b80b7a3454c06BdAc94A69DDc7A9",
    #             "reserve": "0x03ab458634910aad20ef5f1c8ee96f1d6ac54919",
    #             "symbol": "RAI",
    #         },
    #     ]
    # )
    # expected_length = 37
    # result = get_market_tokens_at_block_aave(market, block_height, CONFIG_MARKETS)
    # result_first_2 = result.head(2)
    # assert_frame_equal(result_first_2, expected, check_exact=True, check_like=True)
    # assert len(result) == expected_length, str(result)

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

    market_v3 = "ethereum_v3"
    block_height_v3 = 16286697
    expected_v3 = pd.DataFrame()
    expected_length_v3 = 0
    result_v3 = get_market_tokens_at_block_aave(market_v3, block_height_v3, CONFIG_MARKETS)
    assert_frame_equal(result_v3, expected_v3, check_exact=True, check_like=True)
    assert len(result_v3) == expected_length_v3, str(result_v3)


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

def test_get_token_transfers_from_alchemy():
    """Tests the alchemy token transfers function against a reference response"""
    
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
                0:"aUSDC",
                1:"aUSDC"
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

    # tests the case where > 1000 rows are returned from the API and pagination is required
    expected_polygon_usd = pd.DataFrame(
        {
            "transfers_transfer_type":{
                0:"IN",
            },
            "transfers_from_address":{
                0:"0x0000000000000000000000000000000000000000",
            },
            "transfers_to_address":{
                0:"0x7734280a4337f37fbf4651073db7c28c80b339e9",
            },
            "transfers_contract_address":{
                0:"0x1a13f4ca1d028320a707d99520abfefca3998b7f",
            },
            "transfers_contract_name":{
                0:"amUSDC",
            },
            "transfers_contract_decimals":{
                0:6,
            },
            "transfers_contract_symbol":{
                0:"amUSDC",
            },
            "block_day":{
                0: datetime(2023, 1, 24, 0, 0, 0, tzinfo=timezone.utc),
            },
            "amount_transferred":{
                0:181.969631,
            },
            "start_block":{
                0:38448179,
            },
            "end_block":{
                0:38487931,
            }
        }
    )
    expected_polygon_usd = standardise_types(expected_polygon_usd)
    # ic(expected)

    result = get_token_transfers_from_alchemy(
        16050438,
        16057596,
        datetime(2022, 11, 26, 0, 0, 0, tzinfo=timezone.utc),
        "ethereum",
        '0x464C71f6c2F760DdA6093dCB91C24c39e5d6e18c',
        '0xbcca60bb61934080951369a648fb03df4f96263c'
        )
    result_polygon_usd = get_token_transfers_from_alchemy(
        38448179,
        38487931,
        datetime(2023, 1, 24, 0, 0, 0, tzinfo=timezone.utc),
        "polygon",
        '0x7734280a4337f37fbf4651073db7c28c80b339e9',
        '0x1a13f4ca1d028320a707d99520abfefca3998b7f'
        ) 
    # try:
    #     result_arb_usd = get_token_transfers_from_alchemy(
    #         65886486,
    #         66206359,
    #         datetime(2023, 3, 2, 0, 0, 0, tzinfo=timezone.utc),
    #         "arbitrum",
    #         '0x053d55f9b5af8694c503eb288a1b7e552f590710',
    #         '0x513c7e3a9c69ca3e22550ef58ac1c0088e918fff'
    #         )
    # except TypeError:
    #     result_arb_usd = pd.DataFrame()
    # ic(result_arb_usd)
    assert_frame_equal(result, expected, check_exact=True, check_like=True)
    assert_frame_equal(result_polygon_usd, expected_polygon_usd, check_exact=True, check_like=True)

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

def test_get_raw_reserve_data():
    """
    Tests the get raw reserve data function against a reference response

    Tests v1, v2, v3 markets

    """

    v3_expected = {
    "reserve_config": {
        "decimals": 18,
        "ltv": 0.8,
        "liquidation_threshold": 0.825,
        "liquidation_bonus": 1.05,
        "reserve_factor": 0.15,
        "usage_as_collateral_enabled": True,
        "borrowing_enabled": True,
        "stable_borrow_rate_enabled": False,
        "is_active": True,
        "is_frozen": False,
    },
    "reserve_data": {
        "unbacked_atokens": 0.0,
        "scaled_accrued_to_treasury": 1.9250904417920802,
        "atoken_supply": 172763.04842357736,
        "stable_debt": 0.0,
        "variable_debt": 104500.09512616771,
        "liquidity_rate": 0.019913150887283015,
        "variable_borrow_rate": 0.038731198111748875,
        "stable_borrow_rate": 0.09824336643341987,
        "average_stable_rate": 0.0,
        "liquidity_index": 1.0029367439843098,
        "variable_borrow_index": 1.0058526832488903,
        "last_update_timestamp": datetime(2023, 3, 25, 3, 35, 47),
        "available_liquidity": 68262.95329740965,
    },
    "reserve_emode_category": 1,
    "borrow_cap": 1400000,
    "supply_cap": 1800000,
    "is_paused": False,
    "siloed_borrowing": False,
    "liquidation_protocol_fee": 1000,
    "unbacked_mint_cap": 0,
    "debt_ceiling": 0,
    "debt_ceiling_decimals": 2,
    }

    v2_expected = {
    "reserve_config": {
        "decimals": 18,
        "ltv": 0.825,
        "liquidation_threshold": 0.86,
        "liquidation_bonus": 1.05,
        "reserve_factor": 0.15,
        "usage_as_collateral_enabled": True,
        "borrowing_enabled": True,
        "stable_borrow_rate_enabled": True,
        "is_active": True,
        "is_frozen": False,
    },
    "reserve_data": {
        "available_liquidity": 315717.61882480345,
        "stable_debt": 0.23277622824191513,
        "variable_debt": 375932.9838717904,
        "liquidity_rate": 0.01654780296335687,
        "variable_borrow_rate": 0.03581768685055707,
        "stable_borrow_rate": 0.0571765124742706,
        "average_stable_rate": 0.05725995735046088,
        "liquidity_index": 1.022404089914088,
        "variable_borrow_index": 1.0421270320025577,
        "last_update_timestamp": datetime(2023, 3, 25, 4, 24, 35),
        "atoken_supply": 691650.8354728221,
        "scaled_accrued_to_treasury": 0,
        "unbacked_atokens": 0,
    },
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

    v1_expected = {
    "reserve_config": {
        "decimals": 18,
        "ltv": 0.75,
        "liquidation_threshold": 0.8,
        "liquidation_bonus": 1.05,
        "reserve_factor": 0.09,
        "usage_as_collateral_enabled": True,
        "borrowing_enabled": True,
        "stable_borrow_rate_enabled": False,
        "is_active": True,
    },
    "reserve_data": {
        "atoken_supply": 3797.823724736217,
        "available_liquidity": 3665.948707828322,
        "stable_debt": 26.081562023648097,
        "variable_debt": 105.79345488424704,
        "liquidity_rate": 0.00035776439641455103,
        "variable_borrow_rate": 0.0042737031753279356,
        "stable_borrow_rate": 0.03534212896915992,
        "average_stable_rate": 0.03476004572423179,
        "liquidity_index": 1.0069932511657278,
        "variable_borrow_index": 1.0286044570334731,
        "last_update_timestamp": datetime(2023, 3, 24, 11, 1, 23),
        "scaled_accrued_to_treasury": 0,
        "unbacked_atokens": 0,
    },
    "is_frozen": True,
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

    v3_result = get_raw_reserve_data('ethereum_v3','ethereum','0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2', 18, 16902116)
    v2_result = get_raw_reserve_data('ethereum_v2','ethereum','0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2', 18, 16902116)
    v1_result = get_raw_reserve_data('ethereum_v1','ethereum','0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2', 18, 16902116)

    assert v3_result == v3_expected
    assert v2_result == v2_expected
    assert v1_result == v1_expected

def test_get_quote_from_1inch():
    """
    Tests the get_quote_from_1inch() helper function
    Test both sync and async versions of the function
    
    Verifies the API call using a swap quote of 1 ETH to WETH

    Assuming the answer to this is always 1

    """

    result = get_quote_from_1inch(1, '0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE', 18, '0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2', 18, 1)

    assert type(result) == float
    assert result == 1.0

    async def async_get():
            async_result= await get_quote_from_1inch_async(1, '0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE', 18, '0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2', 18, 1)
            return async_result

    result = asyncio.run(async_get())
    
    assert type(result) == float
    assert result == 1.0


def test_get_aave_oracle_price():
    """
    Tests the get_aave_oracle_price() helper function
    
    Calls the ethereum oracle with the WETH address at a known block

    """

    result = get_aave_oracle_price('ethereum_v3', '0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2', 16902116)

    assert type(result) == float
    assert result == 1758.82507

def test_get_balancer_bpt_data():
    """
    Tests the get_balancer_bpt_data() helper function
    
    Calls a balancer pool at a known block

    """
    # check known data
    result = get_balancer_bpt_data('polygon', '0xb371aA09F5a110AB69b39A84B5469d29f9b22B76', 18, 41588560)

    assert type(result) == dict
    assert result == {'actual_supply': 39.41060936959508, 'rate': 1.0000269779494702}

    # check contract not deployed at block height
    result = get_balancer_bpt_data('ethereum', '0x9001cbbd96f54a658ff4e6e65ab564ded76a5431', 18, 16992952)

    assert type(result) == dict
    assert result == {"actual_supply": None, "rate": None}

if __name__ == "__main__":
    # test_get_market_tokens_at_block_messari()
    # test_get_market_tokens_at_block_aave()
    # test_get_erc20_balance_of()
    # test_get_token_transfers_from_covalent()
    # test_get_events_by_topic_hash_from_covalent()
    # test_standarise_types()
    # test_get_scaled_balance_of()
    # test_get_token_transfers_from_alchemy()
    # test_get_raw_reserve_data()
    # test_get_quote_from_1inch()
    # test_get_aave_oracle_price()
    # test_get_balancer_bpt_data()
    test_get_market_tokens_at_block_rpc()