"""Tests for assets & ops in the financials module"""
from datetime import datetime, timezone
# import os

# import pytest
import pandas as pd
from dagster import MultiPartitionKey, build_op_context
# pylint: disable=import-error
from icecream import ic
from pandas.testing import assert_frame_equal
# ic(os.getcwd())
# from aave.financials.
from financials.assets.data_lake import (aave_oracle_prices_by_day,
                                    block_numbers_by_day,
                                    market_tokens_by_day,
                                    collector_atoken_balances_by_day,
                                    collector_atoken_transfers_by_day,
                                    non_atoken_transfers_by_day,
                                    non_atoken_balances_by_day,
                                    v3_accrued_fees_by_day,
                                    v3_minted_to_treasury_by_day,
                                    treasury_accrued_incentives_by_day,
                                    user_lm_rewards_claimed
                                    )
# from financials.assets import data_lake
# from financials.
from financials.financials_config import *  # pylint: disable=wildcard-import, unused-wildcard-import

# from aave.ops.helpers import (
#     get_market_tokens_at_block_aave,
#     get_market_tokens_at_block_messari
# )


# pylint: enable=import-error

def test_block_numbers_by_day():
    """
    Tests the block numbers by day & chain asset

    """
    pkey = MultiPartitionKey(
        {
            "date": '2022-01-01',
            "market": 'ethereum_v2'
        }
    )  # type: ignore

    context = build_op_context(partition_key=pkey)

    expected = pd.DataFrame(
        [
            {
                'block_day': datetime(2022,1,1,0,0,0),
                'block_time': datetime(2022,1,1,0,0,3),
                'block_height': 13916166,
                'end_block': 13922671,
                'chain': 'ethereum',
                'market': 'ethereum_v2'
            }
        ]
    )
    result = block_numbers_by_day(context)

    assert_frame_equal(result, expected, check_exact=True)  # type: ignore

def test_market_tokens_by_day():
    """
    Tests the market tokens by day asset

    """
    pkey = MultiPartitionKey(
        {
            "date": '2022-11-26',
            "market": 'arbitrum_v3'
        }
    )  # type: ignore

    context = build_op_context(partition_key=pkey)

    block_numbers_by_day_sample = pd.DataFrame(
        [
            {
                'block_day': datetime(2022, 11, 26, 0, 0, 0),
                'block_time': datetime(2022, 11, 26, 0, 0, 0),
                'block_height': 41220510,
                'end_block': 41485201,
                'chain': 'arbitrum',
                'market': 'arbitrum_v3'
            }
        ]
    )

    expected = pd.DataFrame(
        {
            "reserve": {
                0: "0x2f2a2543b76a4166549f7aab2e75bef0aefc5b0f",
                1: "0x82af49447d8a07e3bd95bd0d56f35241523fbab1",
            },
            "name": {0: "Wrapped BTC", 1: "Wrapped Ether"},
            "symbol": {0: "WBTC", 1: "WETH"},
            "decimals": {0: 8, 1: 18},
            "atoken": {
                0: "0x078f358208685046a11c85e8ad32895ded33a249",
                1: "0xe50fa9b3c56ffb159cb0fca61f5c9d750e8128c8",
            },
            "atoken_symbol": {0: "aArbWBTC", 1: "aArbWETH"},
            "pool": {
                0: "0x794a61358d6845594f94dc1db02a252b5b4814ad",
                1: "0x794a61358d6845594f94dc1db02a252b5b4814ad",
            },
            "market": {0: "arbitrum_v3", 1: "arbitrum_v3"},
            "atoken_decimals": {0: 8, 1: 18},
            "block_height": {0: 41220510, 1: 41220510},
            "block_day": {
                0: datetime(2022, 11, 26, 0, 0, 0),
                1: datetime(2022, 11, 26, 0, 0, 0),
            },
        }
    )

    result = market_tokens_by_day(context, block_numbers_by_day_sample)
    result = result[result.symbol.isin(['WBTC', 'WETH'])]  # type: ignore 
    # ic(result)
    # ic(expected)

    assert_frame_equal(result, expected, check_exact=True, check_like=True)  # type: ignore

def test_aave_oracle_prices_by_day():
    """
    Tests the aave oracle prices by day asset

    Tests a single network and day
    Materialize assets in dagster to test other networks

    """

    pkey = MultiPartitionKey(
        {
            "date": '2022-11-26',
            "market": 'arbitrum_v3'
        }
    )  # type: ignore

    context = build_op_context(partition_key=pkey)

    # dummy data below for 2022-11-26
    market_tokens_by_day_sample_output = pd.DataFrame(
        {
            "reserve": {
                0: "0x2f2a2543b76a4166549f7aab2e75bef0aefc5b0f",
                1: "0x82af49447d8a07e3bd95bd0d56f35241523fbab1",
            },
            "name": {0: "Wrapped BTC", 1: "Wrapped Ether"},
            "symbol": {0: "WBTC", 1: "WETH"},
            "decimals": {0: 8, 1: 18},
            "atoken": {
                0: "0x078f358208685046a11c85e8ad32895ded33a249",
                1: "0xe50fa9b3c56ffb159cb0fca61f5c9d750e8128c8",
            },
            "pool": {
                0: "0x794a61358d6845594f94dc1db02a252b5b4814ad",
                1: "0x794a61358d6845594f94dc1db02a252b5b4814ad",
            },
            "market": {0: "arbitrum_v3", 1: "arbitrum_v3"},
            "atoken_decimals": {0: 8, 1: 18},
            "block_height": {0: 41220510, 1: 41220510},
            "block_day": {
                0: datetime(2022, 11, 26, 0, 0, 0),
                1: datetime(2022, 11, 26, 0, 0, 0),
            },
        }
    )

    expected = pd.DataFrame(
        {
            "reserve": {
                0: "0x2f2a2543b76a4166549f7aab2e75bef0aefc5b0f",
                1: "0x82af49447d8a07e3bd95bd0d56f35241523fbab1",
            },
            "symbol": {0: "WBTC", 1: "WETH"},
            "market": {0: "arbitrum_v3", 1: "arbitrum_v3"},
            "block_height": {0: 41220510, 1: 41220510},
            "block_day": {
                0: datetime(2022, 11, 26, 0, 0, 0),
                1: datetime(2022, 11, 26, 0, 0, 0),
            },
            "usd_price": {0: 16505.23772028, 1: 1197.52},
        }
    )

    result = aave_oracle_prices_by_day(context, market_tokens_by_day_sample_output)

    assert_frame_equal(result, expected, check_like=True, check_exact=True)  # type: ignore


def test_collector_atoken_transfers_by_day():
    """
    Tests the collector token transfers by day asset
    """

    pkey = MultiPartitionKey(
        {
            "date": '2022-11-26',
            "market": 'ethereum_v2'
        }
    )  # type: ignore

    context = build_op_context(partition_key=pkey)

    market_tokens_by_day_sample_output = pd.DataFrame(
        {
            "reserve": {
                0: "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
                1: "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
            },
            "name": {0: "USD Coin", 1: "Wrapped Ether"},
            "symbol": {0: "USDC", 1: "WETH"},
            "decimals": {0: 6, 1: 18},
            "atoken": {
                0: "0xbcca60bb61934080951369a648fb03df4f96263c",
                1: "0x030ba81f1c18d280636f32af80b9aad02cf0854e",
            },
            "atoken_symbol": {0: "aUSDC", 1: "aWETH"},
            "pool": {
                0: "0x7d2768dE32b0b80b7a3454c06BdAc94A69DDc7A9",
                1: "0x7d2768dE32b0b80b7a3454c06BdAc94A69DDc7A9",
            },
            "market": {0: "ethereum_v2", 1: "ethereum_v2"},
            "atoken_decimals": {0: 6, 1: 18},
            "block_height": {0: 16050438, 1: 16050438},
            "block_day": {
                0: datetime(2022, 11, 26, 0, 0, 0),
                1: datetime(2022, 11, 26, 0, 0, 0),
            },
        }
    )

    block_numbers_by_day_sample_output = pd.DataFrame(
        [
            {
                'block_day': datetime(2022,11,26,0,0,0),
                'block_time': datetime(2022,11,26,0,0,0),
                'block_height': 16050438,
                'end_block': 16057596,
                'chain': 'ethereum'
            }
        ]
    )

    expected = pd.DataFrame(
        {
            "transfers_transfer_type":{
                0:"IN",
                1:"OUT",
                2:"IN"
            },
            "transfers_from_address":{
                0:"0x0000000000000000000000000000000000000000",
                1:"0x464c71f6c2f760dda6093dcb91c24c39e5d6e18c",
                2:"0x0000000000000000000000000000000000000000"
            },
            "transfers_to_address":{
                0:"0x464c71f6c2f760dda6093dcb91c24c39e5d6e18c",
                1:"0x04f90d449d4f8316edd6ef4f963b657f8444a4ca",
                2:"0x464c71f6c2f760dda6093dcb91c24c39e5d6e18c"
            },
            "transfers_contract_address":{
                0:"0xbcca60bb61934080951369a648fb03df4f96263c",
                1:"0xbcca60bb61934080951369a648fb03df4f96263c",
                2:"0x030ba81f1c18d280636f32af80b9aad02cf0854e"
            },
            "transfers_contract_name":{
                0:"Aave interest bearing USDC",
                1:"Aave interest bearing USDC",
                2:"Aave interest bearing WETH"
            },
            "transfers_contract_decimals":{
                0:6,
                1:6,
                2:18
            },
            "transfers_contract_symbol":{
                0:"aUSDC",
                1:"aUSDC",
                2:"aWETH"
            },
            "block_day":{
                0: datetime(2022,11,26,0,0,0, tzinfo=timezone.utc),
                1: datetime(2022,11,26,0,0,0, tzinfo=timezone.utc),
                2: datetime(2022,11,26,0,0,0, tzinfo=timezone.utc)
            },
            "amount_transferred":{
                0:2924.196349,
                1:25077.808782,
                2:3.6074557743338818
            },
            "start_block":{
                0:16050438,
                1:16050438,
                2:16050438
            },
            "end_block":{
                0:16057596,
                1:16057596,
                2:16057596
            }
            }
    )
    ic(expected)
    result = collector_atoken_transfers_by_day(context, market_tokens_by_day_sample_output, block_numbers_by_day_sample_output)

    assert_frame_equal(result, expected, check_exact=True)  # type: ignore

def test_non_atoken_transfers_by_day():
    """
    Tests the non atoken transfers by day asset
    """

    pkey = MultiPartitionKey(
        {
            "date": '2022-11-26',
            "market": 'ethereum_v2'
        }
    )  # type: ignore

    context = build_op_context(partition_key=pkey)

    block_numbers_by_day_sample_output = pd.DataFrame(
        [
            {
                'block_day': datetime(2022,11,26,0,0,0),
                'block_time': datetime(2022,11,26,0,0,0),
                'block_height': 16050438,
                'end_block': 16057596,
                'chain': 'ethereum',
                'market': 'ethereum_v2'
            }
        ]
    )

    expected = pd.DataFrame(
        {
            "transfers_transfer_type":{
                35:"OUT",
                36:"OUT"
            },
            "transfers_from_address":{
                35:"0x25f2226b597e8f9514b3f68f00f494cf4f286491",
                36:"0xd784927ff2f95ba542bfc824c8a8a98f3495f6b5"
            },
            "transfers_to_address":{
                35:"0xfcf150072a21c9a66bf5a103a066746e2f5c7932",
                36:"0x4da27a545c0c5b758a6ba100e3a049001de870f5"
            },
            "transfers_contract_address":{
                35:"0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9",
                36:"0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9"
            },
            "transfers_contract_name":{
                35:"Aave Token",
                36:"Aave Token"
            },
            "transfers_contract_decimals":{
                35:18,
                36:18
            },
            "transfers_contract_symbol":{
                35:"AAVE",
                36:"AAVE"
            },
            "block_day":{
                35: datetime(2022,11,26,0,0,0, tzinfo=timezone.utc),
                36: datetime(2022,11,26,0,0,0, tzinfo=timezone.utc)
            },
            "amount_transferred":{
                35:6.633418347529054,
                36:83.2403840096129
            },
            "start_block":{
                35:16050438,
                36:16050438
            },
            "end_block":{
                35:16057596,
                36:16057596
            },
            "wallet":{
                35:"ecosystem_reserve",
                36:"ethereum_v2_incentives_controller"
            }
        }
    )

    expected.transfers_from_address = expected.transfers_from_address.str.lower() 
    expected.transfers_to_address = expected.transfers_to_address.str.lower()
    expected.transfers_contract_address = expected.transfers_contract_address.str.lower()

    expected.transfers_from_address = expected.transfers_from_address.astype(pd.StringDtype()) # type: ignore
    expected.transfers_to_address = expected.transfers_to_address.astype(pd.StringDtype()) # type: ignore
    expected.transfers_contract_address = expected.transfers_contract_address.astype(pd.StringDtype()) # type: ignore
    expected.transfers_contract_decimals = expected.transfers_contract_decimals.astype('int64') 
    expected.start_block = expected.start_block.astype('int64')
    expected.end_block = expected.end_block.astype('int64')


    ic(expected)
    result = non_atoken_transfers_by_day(context, block_numbers_by_day_sample_output).tail(2) # type: ignore
    ic(result)
    # print(result.tail(2).to_dict())

    assert_frame_equal(result, expected, check_exact=True)  # type: ignore

def test_collector_atoken_balances_by_day():
    """
    Tests the collector token transfers by day asset
    """

    pkey = MultiPartitionKey(
        {
            "date": '2022-11-26',
            "market": 'ethereum_v2'
        }
    )  # type: ignore

    context = build_op_context(partition_key=pkey)

    market_tokens_by_day_sample_output = pd.DataFrame(
        {
            "reserve": {
                0: "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
                1: "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
            },
            "name": {0: "USD Coin", 1: "Wrapped Ether"},
            "symbol": {0: "USDC", 1: "WETH"},
            "decimals": {0: 6, 1: 18},
            "atoken": {
                0: "0xbcca60bb61934080951369a648fb03df4f96263c",
                1: "0x030ba81f1c18d280636f32af80b9aad02cf0854e",
            },
            "atoken_symbol": {0: "aUSDC", 1: "aWETH"},
            "pool": {
                0: "0x7d2768dE32b0b80b7a3454c06BdAc94A69DDc7A9",
                1: "0x7d2768dE32b0b80b7a3454c06BdAc94A69DDc7A9",
            },
            "market": {0: "ethereum_v2", 1: "ethereum_v2"},
            "atoken_decimals": {0: 6, 1: 18},
            "block_height": {0: 16050438, 1: 16050438},
            "block_day": {
                0: datetime(2022, 11, 26, 0, 0, 0),
                1: datetime(2022, 11, 26, 0, 0, 0),
            },
        }
    )

    block_numbers_by_day_sample_output = pd.DataFrame(
        [
            {
                'block_day': datetime(2022,11,26,0,0,0),
                'block_time': datetime(2022,11,26,0,0,0),
                'block_height': 16050438,
                'end_block': 16057596,
                'chain': 'ethereum',
                'market': 'ethereum_v2',
            }
        ]
    )

    expected = pd.DataFrame(
        {
            "collector":{
                0:"0x464C71f6c2F760DdA6093dCB91C24c39e5d6e18c",
                1:"0x464C71f6c2F760DdA6093dCB91C24c39e5d6e18c"
            },
            "market":{
                0:"ethereum_v2",
                1:"ethereum_v2"
            },
            "token":{
                0:"0xbcca60bb61934080951369a648fb03df4f96263c",
                1:"0x030ba81f1c18d280636f32af80b9aad02cf0854e"
            },
            "symbol":{
                0:"aUSDC",
                1:"aWETH"
            },
            "block_height":{
                0:16050438,
                1:16050438
            },
            "block_day":{
                0: datetime(2022,11,26,0,0,0, tzinfo=timezone.utc),
                1: datetime(2022,11,26,0,0,0, tzinfo=timezone.utc)
            },
            "balance":{
                0:6383512.575754,
                1:641.9904367553161
            }
        }
    )
    ic(expected)

    result = collector_atoken_balances_by_day(context, market_tokens_by_day_sample_output, block_numbers_by_day_sample_output)
    ic(result)
    
    assert_frame_equal(result, expected, check_exact=True)  # type: ignore
    
def test_non_atoken_balances_by_day():
    """
    Tests the non atoken balances by day asset
    """
    pkey = MultiPartitionKey(
        {
            "date": '2022-11-26',
            "market": 'ethereum_v2'
        }
    )  # type: ignore

    context = build_op_context(partition_key=pkey)

    block_numbers_by_day_sample_output = pd.DataFrame(
        [
            {
                'block_day': datetime(2022,11,26,0,0,0),
                'block_time': datetime(2022,11,26,0,0,0),
                'block_height': 16050438,
                'end_block': 16057596,
                'chain': 'ethereum',
                'market': 'ethereum_v2',
            }
        ]
    )

    expected = pd.DataFrame(
        {
            "wallet":{
                0:"0x25f2226b597e8f9514b3f68f00f494cf4f286491",
                1:"0xd784927ff2f95ba542bfc824c8a8a98f3495f6b5",
                2:"0x464c71f6c2f760dda6093dcb91c24c39e5d6e18c"
            },
            "chain":{
                0:"ethereum",
                1:"ethereum",
                2:"ethereum"
            },
            "market":{
                0:"ethereum_v2",
                1:"ethereum_v2",
                2:"ethereum_v2"
            },
            "token":{
                0:"0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9",
                1:"0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9",
                2:"0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9"
            },
            "decimals":{
                0:18,
                1:18,
                2:18
            },
            "symbol":{
                0:"AAVE",
                1:"AAVE",
                2:"BAL"
            },
            "block_height":{
                0:16050438,
                1:16050438,
                2:16050438
            },
            "block_day":{
                0: datetime(2022,11,26,0,0,0, tzinfo=timezone.utc),
                1: datetime(2022,11,26,0,0,0, tzinfo=timezone.utc),
                2: datetime(2022,11,26,0,0,0, tzinfo=timezone.utc)
            },
            "balance":{
                0:1458317.7012564517,
                1:59331.77495077029,
                2:4.899669464115677
            }
        }
    )

    ic(expected)
    result = non_atoken_balances_by_day(context, block_numbers_by_day_sample_output).head(3) # type: ignore
    ic(result)
    assert_frame_equal(result, expected, check_exact=True)  # type: ignore

def test_v3_accrued_fees_by_day():
    """
    Tests the collector token transfers by day asset
    """

    pkey = MultiPartitionKey(
        {
            "date": '2022-11-26',
            "market": 'polygon_v3'
        }
    )  # type: ignore

    context = build_op_context(partition_key=pkey)

    market_tokens_by_day_sample_output = pd.DataFrame(
        {
            "reserve": {
                0: "0x2791bca1f2de4661ed88a30c99a7a9449aa84174",
            },
            "name": {0: "USD Coin (PoS)"},
            "symbol": {0: "USDC",},
            "decimals": {0: 6,},
            "atoken": {
                0: "0x625e7708f30ca75bfd92586e17077590c60eb4cd",
            },
            "atoken_symbol": {0: "aPolUSDC",},
            "pool": {
                0: "0x794a61358d6845594f94dc1db02a252b5b4814ad",
            },
            "market": {0: "polygon_v3",},
            "atoken_decimals": {0: 6,},
            "block_height": {0: 36068925,},
            "block_day": {
                0: datetime(2022, 11, 26, 0, 0, 0),
            },
        }
    )

    expected = pd.DataFrame(
        {
            "market":{
                0:"polygon_v3",
            },
            "reserve":{
                0:"0x2791bca1f2de4661ed88a30c99a7a9449aa84174",
            },
            "symbol":{
                0:"USDC",
            },
            "atoken":{
                0:"0x625e7708f30ca75bfd92586e17077590c60eb4cd",
            },
            "atoken_symbol":{
                0:"aPolUSDC",
            },
            "block_height":{
                0:36068925,
            },
            "block_day":{
                0: datetime(2022,11,26,0,0,0, tzinfo=timezone.utc),
            },
            "accrued_fees":{
                0:43.39621,
            }
        }
    )
    ic(expected)

    result = v3_accrued_fees_by_day(context, market_tokens_by_day_sample_output)
    ic(result)
    
    assert_frame_equal(result, expected, check_exact=True)  # type: ignore

def test_v3_minted_to_treasury_by_day():
    """
    Tests the minted_to_treasuries_by_day asset
    """

    pkey = MultiPartitionKey(
        {
            "date": '2022-12-15',
            "market": 'polygon_v3'
        }
    )  # type: ignore

    context = build_op_context(partition_key=pkey)

    market_tokens_by_day_sample_output = pd.DataFrame(
        {
            "reserve": {
                0: "0x2791bca1f2de4661ed88a30c99a7a9449aa84174",
            },
            "name": {0: "USD Coin (PoS)"},
            "symbol": {0: "USDC",},
            "decimals": {0: 6,},
            "atoken": {
                0: "0x625e7708f30ca75bfd92586e17077590c60eb4cd",
            },
            "atoken_symbol": {0: "aPolUSDC",},
            "pool": {
                0: "0x794a61358d6845594f94dc1db02a252b5b4814ad",
            },
            "market": {0: "polygon_v3",},
            "atoken_decimals": {0: 6,},
            "block_height": {0: 36839447,},
            "block_day": {
                0: datetime(2022, 12, 15, 0, 0, 0),
            },
        }
    )

    block_numbers_by_day_sample_output = pd.DataFrame(
        {
            "block_day": {
                0: datetime(2022, 12, 15, 0, 0, 0),
            },
            "block_time": {
                0: datetime(2022, 12, 15, 0, 0, 0),
            },
            "block_height": {
                0: 36839447,
            },
            "end_block": {
                0: 36879843,
            },
            "chain": {
                0: "polygon",
            },
            "market": {
                0: "polygon_v3",
            },
        }
    )

    expected = pd.DataFrame(
        {
            "market":{
                0:"polygon_v3",
            },
            "reserve":{
                0:"0x2791bca1f2de4661ed88a30c99a7a9449aa84174",
            },
            "symbol":{
                0:"USDC",
            },
            "atoken":{
                0:"0x625e7708f30ca75bfd92586e17077590c60eb4cd",
            },
            "atoken_symbol":{
                0:"aPolUSDC",
            },
            "block_height":{
                0:36839447,
            },
            "block_day":{
                0: datetime(2022,12,15,0,0,0, tzinfo=timezone.utc),
            },
            "minted_to_treasury_amount":{
                0:191.977829,
            },
            "minted_amount":{
                0:193.399934
            }
        }
    )
    # expected.minted_to_treasury_amount = expected.minted_to_treasury_amount.astype('float64')

    result = v3_minted_to_treasury_by_day(context, block_numbers_by_day_sample_output, market_tokens_by_day_sample_output)
    ic(expected)
    ic(result)
    assert_frame_equal(result, expected, check_exact=True, check_like=True)  # type: ignore

def test_treasury_accrued_incentives():
    """
    Tests the treasury_accrued_incentives asset on both aave_v3 and aave_v2 (including null returns)

    """

    avax_v2_key = MultiPartitionKey(
        {
            "date": '2022-12-15',
            "market": 'avax_v2'
        }
    )  # type: ignore

    avax_v3_key = MultiPartitionKey(
        {
            "date": '2022-12-15',
            "market": 'avax_v3'
        }
    )  # type: ignore

    eth_arc_key = MultiPartitionKey(
        {
            "date": '2023-01-29',
            "market": 'aave_arc'
        }
    )  # type: ignore

    
    eth_v3_key = MultiPartitionKey(
        {
            "date": '2023-01-29',
            "market": 'ethereum_v3'
        }
    )  # type: ignore

    context_avax_v2 = build_op_context(partition_key=avax_v2_key)
    context_avax_v3 = build_op_context(partition_key=avax_v3_key)
    context_eth_arc = build_op_context(partition_key=eth_arc_key)
    context_eth_v3 = build_op_context(partition_key=eth_v3_key)


    block_numbers_by_day_sample_output_avax = pd.DataFrame(
        [
            {
                'block_day': datetime(2022,12,15,0,0,0),
                'block_time': datetime(2022,12,15,0,0,0),
                'block_height': 23644293,
                'end_block': 23686857,
                'chain': 'avalanche',
                'market': 'avax_v2',
            }
        ]
    )
    
    block_numbers_by_day_sample_output_eth = pd.DataFrame(
        [
            {
                'block_day': datetime(2023,1,29,0,0,0),
                'block_time': datetime(2023,1,29,0,0,0),
                'block_height': 16186378,
                'end_block': 16193533,
                'chain': 'ethereum',
                'market': 'ethereum_v2',
            }
        ]
    )
    
    treasury_accrued_incentives_avax_v2_expected = pd.DataFrame(
        [
            {
                'network': 'avalanche',
                'market': 'avax_v2',
                'collector_contract': '0x467b92aF281d14cB6809913AD016a607b5ba8A36'.lower(),
                'block_height': 23644293,
                'block_day': datetime(2022,12,15,0,0,0),
                'rewards_token_address': '0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7'.lower(),
                'rewards_token_symbol': 'WAVAX',
                'accrued_rewards': 724.8002888143623,
            }
        ]
    )

    treasury_accrued_incentives_avax_v3_expected = pd.DataFrame(
        [
            {
                'network': 'avalanche',
                'market': 'avax_v3',
                'collector_contract': '0x5ba7fd868c40c16f7aDfAe6CF87121E13FC2F7a0'.lower(),
                'block_height': 23644293,
                'block_day': datetime(2022,12,15,0,0,0),
                'rewards_token_address': '0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7'.lower(),
                'rewards_token_symbol': 'WAVAX',
                'accrued_rewards': 584.4829744983532,
            }
        ]
    )

    treasury_accrued_incentives_eth_v3_expected  = pd.DataFrame()
    treasury_accrued_incentives_eth_arc_expected = pd.DataFrame()

    # test v2 with an expected result
    treasury_accrued_incentives_avax_v2_result = treasury_accrued_incentives_by_day(context_avax_v2, block_numbers_by_day_sample_output_avax)
    # test v3 with an expected result
    treasury_accrued_incentives_avax_v3_result = treasury_accrued_incentives_by_day(context_avax_v3, block_numbers_by_day_sample_output_avax)
    # test v3 with an expected null response
    treasury_accrued_incentives_eth_v3_result  = treasury_accrued_incentives_by_day(context_eth_v3, block_numbers_by_day_sample_output_eth)
    # test v1/v2 with an expected null response
    treasury_accrued_incentives_eth_arc_result = treasury_accrued_incentives_by_day(context_eth_arc, block_numbers_by_day_sample_output_eth)

    assert treasury_accrued_incentives_avax_v2_result.equals(treasury_accrued_incentives_avax_v2_expected)
    assert treasury_accrued_incentives_avax_v3_result.equals(treasury_accrued_incentives_avax_v3_expected)
    assert treasury_accrued_incentives_eth_v3_result.equals(treasury_accrued_incentives_eth_v3_expected)
    assert treasury_accrued_incentives_eth_arc_result.equals(treasury_accrued_incentives_eth_arc_expected)


def test_user_lm_rewards_claimed():
    """
    Tests the user lm rewards claimed asset
    """
    pkey_eth = MultiPartitionKey(
        {
            "date": '2022-11-26',
            "market": 'ethereum_v2'
        }
    )  # type: ignore

    pkey_arb = MultiPartitionKey(
        {
            "date": '2022-11-26',
            "market": 'arbitrum_v3'
        }
    )  # type: ignore

    context_eth = build_op_context(partition_key=pkey_eth)
    context_arb = build_op_context(partition_key=pkey_arb)

    block_numbers_by_day_sample_output = pd.DataFrame(
        [
            {
                'block_day': datetime(2022,11,26,0,0,0),
                'block_time': datetime(2022,11,26,0,0,0),
                'block_height': 16050438,
                'end_block': 16057596,
                'chain': 'ethereum',
                'market': 'ethereum_v2',
            }
        ]
    )

    expected_eth = pd.DataFrame(
        [
            {
                'block_day': datetime(2022,11,26,0,0,0, tzinfo=timezone.utc),
                'chain': 'ethereum',
                'market': 'ethereum_v2',
                'reward_vault': 'ecosystem_reserve',
                'token_address': '0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9'.lower(),
                'balancer_claims': 1178.178995987,
                'incentives_claims': 0,
                'stkaave_claims': 103.964332841,
            },
            {
                'block_day': datetime(2022,11,26,0,0,0, tzinfo=timezone.utc),
                'chain': 'ethereum',
                'market': 'ethereum_v2',
                'reward_vault': 'incentives_controller',
                'token_address': '0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9'.lower(),
                'balancer_claims': 0,
                'incentives_claims': 83.24038401,
                'stkaave_claims': 0,
            }
        ]
    )
    ic(expected_eth)
    # the function should handle markets that are not aave_v2 on mainnet gracefully
    expected_non_eth = pd.DataFrame()
    

    result_eth = user_lm_rewards_claimed(context_eth, block_numbers_by_day_sample_output)
    result_non_eth = user_lm_rewards_claimed(context_arb, block_numbers_by_day_sample_output)
    ic(result_eth)
    
    assert_frame_equal(result_eth, expected_eth)
    assert_frame_equal(result_non_eth, expected_non_eth)




if __name__ == "__main__":
    # ic(list(CONFIG_CHAINS.keys()))
    # ic(get_block_number_at_datetime('ethereum', datetime(2022, 11, 26, 0, 0, 0)))
    # ic(get_v2_market_tokens_at_block('ethereum_v2', 16000338, CONFIG_V2_MARKETS).shape)
    # test_aave_oracle_prices_by_day()
    # test_eth_oracle_prices_by_day()
    # test_get_market_tokens_at_block_messari()
    # test_block_numbers_by_day()
    # test_market_tokens_by_day()
    # test_aave_oracle_prices_table()
    # test_market_tokens_table()
    # test_non_atoken_transfers_by_day()
    # test_collector_atoken_balances_by_day()
    # test_non_atoken_balances_table()
    # test_v3_accrued_fees_by_day()
    # test_v3_minted_to_treasury_by_day()
    # test_treasury_accrued_incentives()
    test_user_lm_rewards_claimed()
    # pass


