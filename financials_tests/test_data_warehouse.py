"""Tests for assets & ops in the financials module"""
from datetime import datetime, timezone

# import pytest
import pandas as pd
from dagster import MultiPartitionKey, build_op_context
# pylint: disable=import-error
from icecream import ic
from pandas.testing import assert_frame_equal

from financials.assets.data_warehouse import (
    blocks_by_day,
    atoken_measures_by_day,
    )
from financials.financials_config import *  # pylint: disable=wildcard-import, unused-wildcard-import

from financials.resources.helpers import (
    standardise_types
)

def test_blocks_by_day():
    """
    tests the blocks by day data warehouse table asset
    etherum_v2 is block_table_master
    polygon_v3 is not block_table_master

    """

    pkey_eth = MultiPartitionKey(
        {
            "date": '2022-11-26',
            "market": 'ethereum_v2'
        }
    )  # type: ignore

    pkey_pol = MultiPartitionKey(
        {
            "date": '2022-11-26',
            "market": 'polygon_v3'
        }
    )  # type: ignore
    context_eth = build_op_context(partition_key=pkey_eth)
    context_pol = build_op_context(partition_key=pkey_pol)

    block_numbers_by_day_sample_output_eth = pd.DataFrame(
        [
            {
                'block_day': datetime(2022,11,26,0,0,0),
                'block_time': datetime(2022,11,26,0,0,11),
                'block_height': 16050438,
                'end_block': 16057596,
                'chain': 'ethereum',
                'market': 'ethereum_v2',
            }
        ]
    )

    block_numbers_by_day_sample_output_pol = pd.DataFrame(
        [
            {
                'block_day': datetime(2022,11,26,0,0,0),
                'block_time': datetime(2022,11,26,0,0,1),
                'block_height': 38527699,
                'end_block': 38567207,
                'chain': 'polygon',
                'market': 'polygon_v3',
            }
        ]
    )

    block_numbers_by_day_sample_output_eth = standardise_types(block_numbers_by_day_sample_output_eth)
    block_numbers_by_day_sample_output_pol = standardise_types(block_numbers_by_day_sample_output_pol)

    expected_eth = pd.DataFrame(
        [
            {
                'block_day': datetime(2022,11,26,0,0,0),
                'block_time': datetime(2022,11,26,0,0,11),
                'block_height': 16050438,
                'end_block': 16057596,
                'chain': 'ethereum',
            }
        ]
    )
    expected_eth = standardise_types(expected_eth)
    expected_pol = pd.DataFrame()

    result_eth = blocks_by_day(context_eth, block_numbers_by_day_sample_output_eth)
    result_pol = blocks_by_day(context_pol, block_numbers_by_day_sample_output_pol)

    assert_frame_equal(result_eth, expected_eth, check_exact=True)
    assert_frame_equal(result_pol, expected_pol, check_exact=True)


def test_atoken_measures_by_day():
    """
    
    """

    pkey_pol = MultiPartitionKey(
        {
            "date": '2023-01-19',
            "market": 'polygon_v3'
        }
    )  # type: ignore

    pkey_eth = MultiPartitionKey(
        {
            "date": '2023-01-30',
            "market": 'ethereum_v2'
        }
    )  # type: ignore

    context_pol = build_op_context(partition_key=pkey_pol)
    context_eth = build_op_context(partition_key=pkey_eth)
    
    collector_atoken_balances_by_day_sample_output = pd.DataFrame(
        [
            {
                'collector': "0xe8599f3cc5d38a9ad6f3684cd5cea72f10dbc383",
                'market': 'polygon_v3',
                'token': '0x078f358208685046a11c85e8ad32895ded33a249',
                'symbol': 'aPolWBTC',
                'block_height': 38249632,
                'block_day': datetime(2023,1,19,0,0,0),
                'balance': 0.438996
            }
        ]
    )

    collector_atoken_balances_by_day_sample_output_eth = pd.DataFrame(
        [
            {
                'collector': "0x464c71f6c2f760dda6093dcb91c24c39e5d6e18c",
                'market': 'ethereum_v2',
                'token': '0xc9bc48c72154ef3e5425641a3c747242112a46af',
                'symbol': 'aRAI',
                'block_height': 16515917,
                'block_day': datetime(2023,1,30,0,0,0),
                'balance': 97.5636
            }
        ]
    )

    collector_atoken_balances_by_day_sample_output = standardise_types(collector_atoken_balances_by_day_sample_output)
    collector_atoken_balances_by_day_sample_output_eth = standardise_types(collector_atoken_balances_by_day_sample_output_eth)

    collector_atoken_transfers_by_day_sample_output = pd.DataFrame(
        {
            'transfers_transfer_type': {0: 'IN', 1: 'IN'},
            'transfers_from_address': {0: '0x0000000000000000000000000000000000000000', 1: '0x29a088f8b4c33e149e67a0431e377ab84505c243'}, 
            'transfers_to_address': {0: '0xe8599f3cc5d38a9ad6f3684cd5cea72f10dbc383', 1: '0xe8599f3cc5d38a9ad6f3684cd5cea72f10dbc383'},
            'transfers_contract_address': {0: '0x078f358208685046a11c85e8ad32895ded33a249', 1: '0x078f358208685046a11c85e8ad32895ded33a249'},
            'transfers_contract_name': {0: 'Aave Polygon WBTC', 1: 'Aave Polygon WBTC'},
            'transfers_contract_decimals': {0: 8, 1: 8},
            'transfers_contract_symbol': {0: 'aPolWBTC', 1: 'aPolWBTC'},
            'block_day': {0: datetime(2023,1,19,0,0,0), 1: datetime(2023,1,19,0,0,0)},
            'amount_transferred': {0: 0.0010951, 1: 2.72e-06},
            'start_block': {0: 38249632, 1: 38249632},
            'end_block': {0: 38288978, 1: 38288978}
            }
    )
    
    collector_atoken_transfers_by_day_sample_output_eth = pd.DataFrame()
    collector_atoken_transfers_by_day_sample_output_eth = standardise_types(collector_atoken_transfers_by_day_sample_output_eth)

    v3_accrued_fees_by_day_sample_output = pd.DataFrame(
        [
            {
                'market': 'polygon_v3',
                'reserve': '0x1bfd67037b42cf73acf2047067bd4f2c47d9bfd6',
                'symbol': 'WBTC',
                'atoken': '0x078f358208685046a11c85e8ad32895ded33a249',
                'atoken_symbol': 'aPolWBTC',
                'block_height': 38249632,
                'block_day': datetime(2023,1,19,0,0,0),
                'accrued_fees': 0.00071708,
            }
        ]
    )
    v3_accrued_fees_by_day_sample_output = standardise_types(v3_accrued_fees_by_day_sample_output)

    v3_minted_to_treasury_by_day_sample_output = pd.DataFrame(
        [
            {
                'market': 'polygon_v3',
                'reserve': '0x1bfd67037b42cf73acf2047067bd4f2c47d9bfd6',
                'symbol': 'WBTC',
                'atoken': '0x078f358208685046a11c85e8ad32895ded33a249',
                'atoken_symbol': 'aPolWBTC',
                'block_height': 38249632,
                'block_day': datetime(2023,1,19,0,0,0),
                'minted_to_treasury_amount': 0.00109192,
                'minted_amount': 0.0010951
            }
        ]
    )
    v3_minted_to_treasury_by_day_sample_output = standardise_types(v3_minted_to_treasury_by_day_sample_output)
    
    internal_external_addresses_sample = pd.DataFrame(
        [
            {
                'chain': 'polygon',
                'description': 'V3 collector',
                'contract_address': '0xe8599f3cc5d38a9ad6f3684cd5cea72f10dbc383',
                'internal_external': 'aave_internal',
            }
        ]
    ) 


    expected_pol = pd.DataFrame(
        [
            {
                'collector': "0xe8599f3cc5d38a9ad6f3684cd5cea72f10dbc383",
                'market': 'polygon_v3',
                'token': '0x078f358208685046a11c85e8ad32895ded33a249',
                'symbol': 'aPolWBTC',
                'block_height': 38249632,
                'block_day': datetime(2023,1,19,0,0,0),
                'balance': 0.438996,
                'accrued_fees': 0.00071708,
                'minted_to_treasury_amount': 0.00109192,
                'minted_amount': 0.0010951,
                'tokens_in_external': 0.0010978199999999998,
                'tokens_in_internal': float(0),
                'tokens_out_external': float(0),
                'tokens_out_internal': float(0),
            }
        ]
    )
    expected_pol = standardise_types(expected_pol)

    expected_eth = pd.DataFrame(
        [
            {
                'collector': "0x464c71f6c2f760dda6093dcb91c24c39e5d6e18c",
                'market': 'ethereum_v2',
                'token': '0xc9bc48c72154ef3e5425641a3c747242112a46af',
                'symbol': 'aRAI',
                'block_height': 16515917,
                'block_day': datetime(2023,1,30,0,0,0),
                'balance': 97.5636,
                'accrued_fees': float(0),
                'minted_to_treasury_amount': float(0),
                'minted_amount': float(0),
                'tokens_in_external': float(0),
                'tokens_in_internal': float(0),
                'tokens_out_external': float(0),
                'tokens_out_internal': float(0),
            }
        ]
    )
    expected_eth = standardise_types(expected_eth)
    
    result_pol = atoken_measures_by_day(
                    context_pol,
                    collector_atoken_balances_by_day_sample_output,
                    collector_atoken_transfers_by_day_sample_output,
                    v3_accrued_fees_by_day_sample_output,
                    v3_minted_to_treasury_by_day_sample_output,
                    internal_external_addresses_sample
                    )
    # result_eth = atoken_measures_by_day(
                    # context_eth,
                    # collector_atoken_balances_by_day_sample_output_eth,
                    # collector_atoken_transfers_by_day_sample_output_eth,
                    # pd.DataFrame(),
                    # pd.DataFrame(),
                    # internal_external_addresses_sample
                    # )

    ic(expected_pol)
    ic(result_pol)
    assert_frame_equal(result_pol, expected_pol, check_exact=True)
    # assert_frame_equal(result_eth, expected_eth, check_exact=True)

# pylint: enable=import-error

# def test_market_tokens_table():
#     """
#     Tests the market tokens table

#     Tests for concatenation of the input dataframes
#       and for the first_seen_block calculated correctly
#       when a new asset is added

#     Args: None

#     """
#     context = build_op_context()

#     # dummy data below
#     market_tokens_by_day_sample_output = {
#         "2022-11-25|aave_arc": pd.DataFrame(
#             {
#                 "reserve": {
#                     0: "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599",
#                     1: "0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9",
#                 },
#                 "name": {0: "Wrapped BTC", 1: "Aave Token"},
#                 "symbol": {0: "WBTC", 1: "AAVE"},
#                 "decimals": {0: 8, 1: 18},
#                 "atoken": {
#                     0: "0xe6d6e7da65a2c18109ff56b7cbbdc7b706fc13f8",
#                     1: "0x89efac495c65d43619c661df654ec64fc10c0a75",
#                 },
#                 "atoken_symbol": {0: "aWBTC", 1: "aAAVE"},
#                 "pool": {
#                     0: "0x37D7306019a38Af123e4b245Eb6C28AF552e0bB0",
#                     1: "0x37D7306019a38Af123e4b245Eb6C28AF552e0bB0",
#                 },
#                 "market": {0: "aave_arc", 1: "aave_arc"},
#                 "atoken_decimals": {0: 8, 1: 18},
#                 "block_height": {0: 16040000, 1: 16040000},
#                 "block_day":{
#                     0: datetime(2022, 11, 25, 0, 0, 0),
#                     1: datetime(2022, 11, 25, 0, 0, 0)
#                 },
#             }
#         ),
#         "2022-11-26|aave_arc": pd.DataFrame(
#             {
#                 "reserve": {
#                     0: "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599",
#                     1: "0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9",
#                     2: "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
#                 },
#                 "name": {0: "Wrapped BTC", 1: "Aave Token", 2: "USD Coin"},
#                 "symbol": {0: "WBTC", 1: "AAVE", 2: "USDC"},
#                 "decimals": {0: 8, 1: 18, 2: 6},
#                 "atoken": {
#                     0: "0xe6d6e7da65a2c18109ff56b7cbbdc7b706fc13f8",
#                     1: "0x89efac495c65d43619c661df654ec64fc10c0a75",
#                     2: "0xd35f648c3c7f17cd1ba92e5eac991e3efcd4566d",
#                 },
#                 "atoken_symbol": {0: "aWBTC", 1: "aAAVE", 2: "aUSDC"},
#                 "pool": {
#                     0: "0x37D7306019a38Af123e4b245Eb6C28AF552e0bB0",
#                     1: "0x37D7306019a38Af123e4b245Eb6C28AF552e0bB0",
#                     2: "0x37D7306019a38Af123e4b245Eb6C28AF552e0bB0",
#                 },
#                 "market": {0: "aave_arc", 1: "aave_arc", 2: "aave_arc"},
#                 "atoken_decimals": {0: 8, 1: 18, 2: 6},
#                 "block_height": {0: 16050438, 1: 16050438, 2: 16050438},
#                 "block_day":{
#                     0: datetime(2022, 11, 26, 0, 0, 0),
#                     1: datetime(2022, 11, 26, 0, 0, 0),
#                     2: datetime(2022, 11, 26, 0, 0, 0)
#                 },
#             }
#         ),
#         "2022-11-26|arbitrum_v3": pd.DataFrame(
#             {
#                 "reserve": {
#                     0: "0x2f2a2543b76a4166549f7aab2e75bef0aefc5b0f",
#                     1: "0x82af49447d8a07e3bd95bd0d56f35241523fbab1",
#                 },
#                 "name": {0: "Wrapped BTC", 1: "Wrapped Ether"},
#                 "symbol": {0: "WBTC", 1: "WETH"},
#                 "decimals": {0: 8, 1: 18},
#                 "atoken": {
#                     0: "0x078f358208685046a11c85e8ad32895ded33a249",
#                     1: "0xe50fa9b3c56ffb159cb0fca61f5c9d750e8128c8",
#                 },
#                 "atoken_symbol": {0: "aArbWBTC", 1: "aArbWETH"},
#                 "pool": {
#                     0: "0x794a61358d6845594f94dc1db02a252b5b4814ad",
#                     1: "0x794a61358d6845594f94dc1db02a252b5b4814ad",
#                 },
#                 "market": {0: "arbitrum_v3", 1: "arbitrum_v3"},
#                 "atoken_decimals": {0: 8, 1: 18},
#                 "block_height": {0: 41220510, 1: 41220510},
#                 "block_day":{
#                     0: datetime(2022, 11, 26, 0, 0, 0),
#                     1: datetime(2022, 11, 26, 0, 0, 0)
#                 },
#             }
#         ),
#     }

#     expected = pd.DataFrame(
#         {
#             "atoken": {
#                 0: "0xe6d6e7da65a2c18109ff56b7cbbdc7b706fc13f8",
#                 1: "0x078f358208685046a11c85e8ad32895ded33a249",
#                 2: "0x89efac495c65d43619c661df654ec64fc10c0a75",
#                 3: "0xe50fa9b3c56ffb159cb0fca61f5c9d750e8128c8",
#                 4: "0xd35f648c3c7f17cd1ba92e5eac991e3efcd4566d",
#             },
#             "atoken_decimals": {0: 8, 1: 8, 2: 18, 3: 18, 4: 6},
#             "decimals": {0: 8, 1: 8, 2: 18, 3: 18, 4: 6},
#             "first_seen_block": {
#                 0: 16040000,
#                 1: 41220510,
#                 2: 16040000,
#                 3: 41220510,
#                 4: 16050438,
#             },
#             "first_seen_day": {
#                 0: datetime(2022, 11, 25, 0, 0, 0),
#                 1: datetime(2022, 11, 26, 0, 0, 0),
#                 2: datetime(2022, 11, 25, 0, 0, 0),
#                 3: datetime(2022, 11, 26, 0, 0, 0),
#                 4: datetime(2022, 11, 26, 0, 0, 0),
#             },
#             "market": {
#                 0: "aave_arc",
#                 1: "arbitrum_v3",
#                 2: "aave_arc",
#                 3: "arbitrum_v3",
#                 4: "aave_arc",
#             },
#             "name": {
#                 0: "Wrapped BTC",
#                 1: "Wrapped BTC",
#                 2: "Aave Token",
#                 3: "Wrapped Ether",
#                 4: "USD Coin",
#             },
#             "pool": {
#                 0: "0x37d7306019a38af123e4b245eb6c28af552e0bb0",
#                 1: "0x794a61358d6845594f94dc1db02a252b5b4814ad",
#                 2: "0x37d7306019a38af123e4b245eb6c28af552e0bb0",
#                 3: "0x794a61358d6845594f94dc1db02a252b5b4814ad",
#                 4: "0x37d7306019a38af123e4b245eb6c28af552e0bb0",
#             },
#             "reserve": {
#                 0: "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599",
#                 1: "0x2f2a2543b76a4166549f7aab2e75bef0aefc5b0f",
#                 2: "0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9",
#                 3: "0x82af49447d8a07e3bd95bd0d56f35241523fbab1",
#                 4: "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
#             },
#             "symbol": {0: "WBTC", 1: "WBTC", 2: "AAVE", 3: "WETH", 4: "USDC"},
#             "atoken_symbol": {0: "aWBTC", 1: "aArbWBTC", 2: "aAAVE", 3: "aArbWETH", 4: "aUSDC"},
#         }
#     )

#     # check for bigquery compatible types in the output
#     expected.atoken = expected.atoken.astype(pd.StringDtype()) # type: ignore
#     expected.atoken_decimals = expected.atoken_decimals.astype('int64')
#     expected.decimals = expected.decimals.astype('int64')
#     expected.first_seen_block = expected.first_seen_block.astype('int64')
#     expected.first_seen_day = expected.first_seen_day.dt.tz_localize('UTC')
#     expected.market = expected.market.astype(pd.StringDtype()) # type: ignore
#     expected.name = expected.name.astype(pd.StringDtype()) # type: ignore
#     expected.pool = expected.pool.astype(pd.StringDtype()) # type: ignore
#     expected.reserve = expected.reserve.astype(pd.StringDtype()) # type: ignore
#     expected.symbol = expected.symbol.astype(pd.StringDtype()) # type: ignore
#     expected.atoken_symbol = expected.atoken_symbol.astype(pd.StringDtype()) # type: ignore


#     result = market_tokens_table(context, market_tokens_by_day_sample_output)

#     assert_frame_equal(result, expected, check_like=True, check_exact=True)  # type: ignore

# def test_aave_oracle_prices_table():
#     """
#     Tests the aave oracle prices table asset
#     """
    
#     pkey = MultiPartitionKey(
#         {
#             "date": '2022-11-26',
#             "market": 'arbitrum_v3'
#         }
#     )  # type: ignore

#     context = build_op_context(partition_key=pkey)

#     aave_oracle_prices_by_day_sample_output = pd.DataFrame(
#         {
#             "reserve": {
#                 0: "0x2f2a2543b76a4166549f7aab2e75bef0aefc5b0f",
#                 1: "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
#             },
#             "symbol": {0: "WBTC", 1: "WETH"},
#             "market": {0: "arbitrum_v3", 1: "arbitrum_v3"},
#             "block_height": {0: "41220510", 1: 41220510},
#             "block_day": {
#                 0: datetime(2022, 11, 26, 0, 0, 0),
#                 1: datetime(2022, 11, 26, 0, 0, 0),
#             },
#             "usd_price": {0: 16505.23772028, 1: "1197.52"},
#         }
#     )

#     expected = pd.DataFrame(
#         {
#             "reserve": {
#                 0: "0x2f2a2543b76a4166549f7aab2e75bef0aefc5b0f",
#                 1: "0x82af49447d8a07e3bd95bd0d56f35241523fbab1",
#             },
#             "symbol": {0: "WBTC", 1: "WETH"},
#             "market": {0: "arbitrum_v3", 1: "arbitrum_v3"},
#             "block_height": {0: 41220510, 1: 41220510},
#             "block_day": {
#                 0: datetime(2022, 11, 26, 0, 0, 0),
#                 1: datetime(2022, 11, 26, 0, 0, 0),
#             },
#             "usd_price": {0: 16505.23772028, 1: 1197.52},
#         }

#     )
#     expected.reserve = expected.reserve.astype(pd.StringDtype()) # type: ignore
#     expected.symbol = expected.symbol.astype(pd.StringDtype()) # type: ignore
#     expected.market = expected.market.astype(pd.StringDtype()) # type: ignore
#     expected.block_height = expected.block_height.astype('int64')
#     expected.block_day = expected.block_day.dt.tz_localize('UTC')
#     expected.usd_price = expected.usd_price.astype('float64')

#     result = aave_oracle_prices_table(context, aave_oracle_prices_by_day_sample_output)

#     assert_frame_equal(result, expected, check_exact=True)  # type: ignore

# def test_collector_atoken_transfers_table():
#     """
#     Tests the collector atoken transfers table

#     This table is intended to be materialised to database
#     checks for types by supplying faulty type input

#     """
#     pkey = MultiPartitionKey(
#         {
#             "date": '2022-11-26',
#             "market": 'ethereum_v2'
#         }
#     )  # type: ignore

#     context = build_op_context(partition_key=pkey)

#     collector_atoken_transfers_by_day_sample = pd.DataFrame(
#         {
#             "transfers_transfer_type":{
#                 0:"IN",
#             },
#             "transfers_from_address":{
#                 0:"0x0000000000000000000000000000000000000000",
#             },
#             "transfers_to_address":{
#                 0:"0x464C71f6c2F760DdA6093dCB91C24c39e5d6e18c",
#             },
#             "transfers_contract_address":{
#                 0:"0xbcca60bb61934080951369a648fb03df4f96263c",
#             },
#             "transfers_contract_name":{
#                 0:"Aave interest bearing USDC",
#             },
#             "transfers_contract_decimals":{
#                 0:'6',
#             },
#             "transfers_contract_symbol":{
#                 0:"aUSDC",
#             },
#             "block_day":{
#                 0: datetime(2022,11,26,0,0,0),
#             },
#             "amount_transferred":{
#                 0:2924.196349,
#             },
#             "start_block":{
#                 0:16050438.0,
#             },
#             "end_block":{
#                 0:16057596,
#             }
#         }
#     )

#     expected = pd.DataFrame(
#         {
#             "transfers_transfer_type":{
#                 0:"IN",
#             },
#             "transfers_from_address":{
#                 0:"0x0000000000000000000000000000000000000000",
#             },
#             "transfers_to_address":{
#                 0:"0x464c71f6c2f760dda6093dcb91c24c39e5d6e18c",
#             },
#             "transfers_contract_address":{
#                 0:"0xbcca60bb61934080951369a648fb03df4f96263c",
#             },
#             "transfers_contract_name":{
#                 0:"Aave interest bearing USDC",
#             },
#             "transfers_contract_decimals":{
#                 0:6,
#             },
#             "transfers_contract_symbol":{
#                 0:"aUSDC",
#             },
#             "block_day":{
#                 0: datetime(2022,11,26,0,0,0, tzinfo=timezone.utc),
#             },
#             "amount_transferred":{
#                 0:2924.196349,
#             },
#             "start_block":{
#                 0:16050438,
#             },
#             "end_block":{
#                 0:16057596,
#             }
#         }
#     )

#     # set the types explicitly
#     expected.transfers_transfer_type = expected.transfers_transfer_type.astype(pd.StringDtype()) # type: ignore
#     expected.transfers_from_address = expected.transfers_from_address.astype(pd.StringDtype()) # type: ignore
#     expected.transfers_to_address = expected.transfers_to_address.astype(pd.StringDtype()) # type: ignore
#     expected.transfers_contract_address = expected.transfers_contract_address.astype(pd.StringDtype()) # type: ignore
#     expected.transfers_contract_name = expected.transfers_contract_name.astype(pd.StringDtype()) # type: ignore
#     expected.transfers_contract_decimals = expected.transfers_contract_decimals.astype('int64')
#     expected.transfers_contract_symbol = expected.transfers_contract_symbol.astype(pd.StringDtype()) # type: ignore
#     expected.block_day = pd.to_datetime(expected.block_day, utc=True)
#     expected.amount_transferred = expected.amount_transferred.astype('float64')
#     expected.start_block = expected.start_block.astype('int64')
#     expected.end_block = expected.end_block.astype('int64')

#     result = collector_atoken_transfers_table(context, collector_atoken_transfers_by_day_sample)
#     ic(result)
#     ic(expected)
#     assert_frame_equal(result, expected, check_exact=True)  # type: ignore

# def test_non_atoken_transfers_table():
#     """
#     Tests the collector atoken transfers table

#     This table is intended to be materialised to database
#     checks for types by supplying faulty type input

#     """
#     pkey = MultiPartitionKey(
#         {
#             "date": '2022-11-26',
#             "market": 'ethereum_v2'
#         }
#     )  # type: ignore

#     context = build_op_context(partition_key=pkey)

#     non_atoken_transfers_by_day_sample = pd.DataFrame(
#         {
#             "transfers_transfer_type":{
#                 35:"OUT",
#             },
#             "transfers_from_address":{
#                 35:"0x25F2226B597E8F9514B3F68F00f494cF4f286491",
#             },
#             "transfers_to_address":{
#                 35:"0xfcf150072a21c9a66bf5a103a066746e2f5c7932",
#             },
#             "transfers_contract_address":{
#                 35:"0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9",
#             },
#             "transfers_contract_name":{
#                 35:"Aave Token",
#             },
#             "transfers_contract_decimals":{
#                 35:'18',
#             },
#             "transfers_contract_symbol":{
#                 35:"AAVE"
#             },
#             "block_day":{
#                 35: datetime(2022,11,26,0,0,0, tzinfo=timezone.utc)
#             },
#             "amount_transferred":{
#                 35:6.633418347529054
#             },
#             "start_block":{
#                 35:16050438.0
#             },
#             "end_block":{
#                 35:16057596
#             },
#             "wallet":{
#                 35:"ecosystem_reserve"
#             }
#         }
#     )

#     expected = pd.DataFrame(
#         {
#             "transfers_transfer_type":{
#                 35:"OUT",
#             },
#             "transfers_from_address":{
#                 35:"0x25f2226b597e8f9514b3f68f00f494cf4f286491",
#             },
#             "transfers_to_address":{
#                 35:"0xfcf150072a21c9a66bf5a103a066746e2f5c7932",
#             },
#             "transfers_contract_address":{
#                 35:"0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9",
#             },
#             "transfers_contract_name":{
#                 35:"Aave Token",
#             },
#             "transfers_contract_decimals":{
#                 35:18,
#             },
#             "transfers_contract_symbol":{
#                 35:"AAVE"
#             },
#             "block_day":{
#                 35: datetime(2022,11,26,0,0,0, tzinfo=timezone.utc)
#             },
#             "amount_transferred":{
#                 35:6.633418347529054
#             },
#             "start_block":{
#                 35:16050438
#             },
#             "end_block":{
#                 35:16057596
#             },
#             "wallet":{
#                 35:"ecosystem_reserve"
#             }
#         }
#     )

#     # set the types explicitly
#     expected.transfers_transfer_type = expected.transfers_transfer_type.astype(pd.StringDtype()) # type: ignore
#     expected.transfers_from_address = expected.transfers_from_address.astype(pd.StringDtype()) # type: ignore
#     expected.transfers_to_address = expected.transfers_to_address.astype(pd.StringDtype()) # type: ignore
#     expected.transfers_contract_address = expected.transfers_contract_address.astype(pd.StringDtype()) # type: ignore
#     expected.transfers_contract_name = expected.transfers_contract_name.astype(pd.StringDtype()) # type: ignore
#     expected.transfers_contract_decimals = expected.transfers_contract_decimals.astype('int64')
#     expected.transfers_contract_symbol = expected.transfers_contract_symbol.astype(pd.StringDtype()) # type: ignore
#     expected.block_day = pd.to_datetime(expected.block_day, utc=True)
#     expected.amount_transferred = expected.amount_transferred.astype('float64')
#     expected.start_block = expected.start_block.astype('int64')
#     expected.end_block = expected.end_block.astype('int64')
#     expected.wallet = expected.wallet.astype(pd.StringDtype()) # type: ignore

#     result = non_atoken_transfers_table(context, non_atoken_transfers_by_day_sample)
#     # ic(result)
#     # ic(expected)
#     assert_frame_equal(result, expected, check_exact=True)  # type: ignore
  
# def test_collector_atoken_balances_table():
#     """
#     Tests the collector atoken balances table

#     This table is intended to be materialised to database
#     checks for types by supplying faulty type input

#     """

#     pkey = MultiPartitionKey(
#         {
#             "date": '2022-11-26',
#             "market": 'ethereum_v2'
#         }
#     )  # type: ignore

#     context = build_op_context(partition_key=pkey)

#     collector_atoken_balances_by_day_sample = pd.DataFrame(
#         {
#             "collector":{
#                 0:"0x464C71f6c2F760DdA6093dCB91C24c39e5d6e18c",
#             },
#             "market":{
#                 0:"ethereum_v2",
#             },
#             "token":{
#                 0:"0xbcca60bb61934080951369a648fb03df4f96263c",
#             },
#             "symbol":{
#                 0:"aUSDC",
#             },
#             "block_height":{
#                 0:16050438,
#             },
#             "block_day":{
#                 0: datetime(2022,11,26,0,0,0, tzinfo=timezone.utc),
#             },
#             "balance":{
#                 0:6383512.575754,
#             }
#         }
#     )
    
#     expected = pd.DataFrame(
#         {
#             "collector":{
#                 0:"0x464C71f6c2F760DdA6093dCB91C24c39e5d6e18c",
#             },
#             "market":{
#                 0:"ethereum_v2",
#             },
#             "token":{
#                 0:"0xbcca60bb61934080951369a648fb03df4f96263c",
#             },
#             "symbol":{
#                 0:"aUSDC",
#             },
#             "block_height":{
#                 0:16050438,
#             },
#             "block_day":{
#                 0: datetime(2022,11,26,0,0,0, tzinfo=timezone.utc),
#             },
#             "balance":{
#                 0:6383512.575754,
#             }
#         }
#     )

#     # set the types explicitly
#     expected.collector = expected.collector.astype(pd.StringDtype()) # type: ignore
#     expected.market = expected.market.astype(pd.StringDtype()) # type: ignore
#     expected.token = expected.token.astype(pd.StringDtype()) # type: ignore
#     expected.symbol = expected.symbol.astype(pd.StringDtype()) # type: ignore
#     expected.block_height = expected.block_height.astype('int64')
#     expected.block_day = pd.to_datetime(expected.block_day, utc=True)
#     expected.balance = expected.balance.astype('float64')

#     # force checksum addresses to lowercase
#     expected.collector = expected.collector.str.lower()
#     expected.token = expected.token.str.lower()

#     ic(expected)
#     result = collector_atoken_balances_table(context, collector_atoken_balances_by_day_sample)
#     ic(result)
#     assert_frame_equal(result, expected, check_exact=True)  # type: ignore

# def test_non_atoken_balances_table():
#     """
#     Tests the non atoken balances table asset
#     """
#     pkey = MultiPartitionKey(
#         {
#             "date": '2022-11-26',
#             "market": 'ethereum_v2'
#         }
#     )  # type: ignore

#     context = build_op_context(partition_key=pkey)

#     non_atoken_balances_by_day_sample = pd.DataFrame(
#         {
#             "wallet":{
#                 0:"0x25F2226B597E8F9514B3F68F00f494cF4f286491",
#             },
#             "chain":{
#                 0:"ethereum",
#             },
#             "market":{
#                 0:"ethereum_v2",
#             },
#             "token":{
#                 0:"0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9",
#             },
#             "decimals":{
#                 0:'18',
#             },
#             "symbol":{
#                 0:"AAVE",
#             },
#             "block_height":{
#                 0:16050438.0,
#             },
#             "block_day":{
#                 0: datetime(2022,11,26,0,0,0),
#             },
#             "balance":{
#                 0:1458317.7012564517,
#             }
#         }
#     )

#     expected = pd.DataFrame(
#         {
#             "wallet":{
#                 0:"0x25f2226b597e8f9514b3f68f00f494cf4f286491",
#             },
#             "chain":{
#                 0:"ethereum",
#             },
#             "market":{
#                 0:"ethereum_v2",
#             },
#             "token":{
#                 0:"0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9",
#             },
#             "decimals":{
#                 0:18,
#             },
#             "symbol":{
#                 0:"AAVE",
#             },
#             "block_height":{
#                 0:16050438,
#             },
#             "block_day":{
#                 0: datetime(2022,11,26,0,0,0, tzinfo=timezone.utc),
#             },
#             "balance":{
#                 0:1458317.7012564517,
#             }
#         }
#     )

#     expected.wallet = expected.wallet.astype(pd.StringDtype()) # type: ignore
#     expected.chain = expected.chain.astype(pd.StringDtype()) # type: ignore
#     expected.market = expected.market.astype(pd.StringDtype()) # type: ignore
#     expected.token = expected.token.astype(pd.StringDtype()) # type: ignore
#     expected.decimals = expected.decimals.astype('int')
#     expected.symbol = expected.symbol.astype(pd.StringDtype()) # type: ignore
#     expected.block_height = expected.block_height.astype('int')
#     expected.balance = expected.balance.astype('float')

#     result = non_atoken_balances_table(context, non_atoken_balances_by_day_sample)
#     ic(expected)
#     ic(result)
#     assert_frame_equal(result, expected, check_exact=True)  # type: ignore


# def test_v3_accrued_fees_table():
#     """
#     Tests the aave v3 accrued fees table asset
#     """
#     pkey = MultiPartitionKey(
#         {
#             "date": '2022-11-26',
#             "market": 'polygon_v3'
#         }
#     )  # type: ignore

#     context = build_op_context(partition_key=pkey)

#     non_atoken_balances_by_day_sample = pd.DataFrame(
#         {
#             "market":{
#                 0:"polygon_v3",
#             },
#             "reserve":{
#                 0:"0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",
#             },
#             "symbol":{
#                 0:"USDC",
#             },
#             "atoken":{
#                 0:"0x625e7708f30ca75bfd92586e17077590c60eb4cd",
#             },
#             "atoken_symbol":{
#                 0:"aPolUSDC",
#             },
#             "block_height":{
#                 0:"36068925",
#             },
#             "block_day":{
#                 0: datetime(2022,11,26,0,0,0),
#             },
#             "accrued_fees":{
#                 0:43.39621,
#             }
#         }
#     )

#     expected = pd.DataFrame(
#         {
#             "market":{
#                 0:"polygon_v3",
#             },
#             "reserve":{
#                 0:"0x2791bca1f2de4661ed88a30c99a7a9449aa84174",
#             },
#             "symbol":{
#                 0:"USDC",
#             },
#             "atoken":{
#                 0:"0x625e7708f30ca75bfd92586e17077590c60eb4cd",
#             },
#             "atoken_symbol":{
#                 0:"aPolUSDC",
#             },
#             "block_height":{
#                 0:36068925,
#             },
#             "block_day":{
#                 0: datetime(2022,11,26,0,0,0, tzinfo=timezone.utc),
#             },
#             "accrued_fees":{
#                 0:43.39621,
#             }
#         }
#     )

#     expected.market = expected.market.astype(pd.StringDtype()) # type: ignore
#     expected.reserve = expected.reserve.astype(pd.StringDtype()) # type: ignore
#     expected.symbol = expected.symbol.astype(pd.StringDtype()) # type: ignore
#     expected.atoken = expected.atoken.astype(pd.StringDtype()) # type: ignore
#     expected.atoken_symbol = expected.atoken_symbol.astype(pd.StringDtype()) # type: ignore
#     expected.block_height = expected.block_height.astype('int')
#     expected.accrued_fees = expected.accrued_fees.astype('float')

#     result = v3_accrued_fees_table(context, non_atoken_balances_by_day_sample)
#     ic(expected)
#     ic(result)
#     assert_frame_equal(result, expected, check_exact=True)  # type: ignore


# def test_v3_minted_to_treasury_table():
#     """
#     Tests the aave v3 minted_to_treasury table asset
#     """
#     pkey = MultiPartitionKey(
#         {
#             "date": '2022-12-15',
#             "market": 'polygon_v3'
#         }
#     )  # type: ignore

#     context = build_op_context(partition_key=pkey)

#     v3_minted_to_treasury_table_sample = pd.DataFrame(
#         {
#             "market":{
#                 0:"polygon_v3",
#             },
#             "reserve":{
#                 0:"0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",
#             },
#             "symbol":{
#                 0:"USDC",
#             },
#             "atoken":{
#                 0:"0x625e7708f30ca75bfd92586e17077590c60eb4cd",
#             },
#             "atoken_symbol":{
#                 0:"aPolUSDC",
#             },
#             "block_height":{
#                 0:36839447.0,
#             },
#             "block_day":{
#                 0: datetime(2022,12,15,0,0,0),
#             },
#             "minted_to_treasury_amount":{
#                 0:191.977829,
#             },
#             "minted_amount":{
#                 0:193.399934
#             }
#         }
#     )

#     expected = pd.DataFrame(
#         {
#             "market":{
#                 0:"polygon_v3",
#             },
#             "reserve":{
#                 0:"0x2791bca1f2de4661ed88a30c99a7a9449aa84174",
#             },
#             "symbol":{
#                 0:"USDC",
#             },
#             "atoken":{
#                 0:"0x625e7708f30ca75bfd92586e17077590c60eb4cd",
#             },
#             "atoken_symbol":{
#                 0:"aPolUSDC",
#             },
#             "block_height":{
#                 0:36839447,
#             },
#             "block_day":{
#                 0: datetime(2022,12,15,0,0,0, tzinfo=timezone.utc),
#             },
#             "minted_to_treasury_amount":{
#                 0:191.977829,
#             },
#             "minted_amount":{
#                 0:193.399934
#             }
#         }
#     )

#     expected.market = expected.market.astype(pd.StringDtype()) # type: ignore
#     expected.reserve = expected.reserve.astype(pd.StringDtype()) # type: ignore
#     expected.symbol = expected.symbol.astype(pd.StringDtype()) # type: ignore
#     expected.atoken = expected.atoken.astype(pd.StringDtype()) # type: ignore
#     expected.atoken_symbol = expected.atoken_symbol.astype(pd.StringDtype()) # type: ignore
#     expected.block_height = expected.block_height.astype('int')
#     expected.minted_to_treasury_amount = expected.minted_to_treasury_amount.astype('float')
#     expected.minted_amount = expected.minted_amount.astype('float')

#     result = v3_minted_to_treasury_table(context, v3_minted_to_treasury_table_sample)
#     ic(expected)
#     ic(result)
#     assert_frame_equal(result, expected, check_exact=True)  # type: ignore


# def test_treasury_accrued_incentives_table():
#     """
#     Tests the treasury_accrued_incentives asset on both aave_v3 and aave_v2 (including null returns)

#     """

#     avax_v3_key = MultiPartitionKey(
#         {
#             "date": '2022-12-15',
#             "market": 'avax_v3'
#         }
#     )  # type: ignore

#     eth_arc_key = MultiPartitionKey(
#         {
#             "date": '2023-01-29',
#             "market": 'aave_arc'
#         }
#     )  # type: ignore

#     context_avax_v3 = build_op_context(partition_key=avax_v3_key)
#     context_eth_arc = build_op_context(partition_key=eth_arc_key)

#     treasury_accrued_incentives_by_day_v3 = pd.DataFrame(
#         [
#             {
#                 'network': 'avalanche',
#                 'market': 'avax_v3',
#                 'collector_contract': '0x5ba7fd868c40c16f7aDfAe6CF87121E13FC2F7a0'.lower(),
#                 'block_height': 23644293,
#                 'block_day': datetime(2022,12,15,0,0,0),
#                 'rewards_token_address': '0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7'.lower(),
#                 'rewards_token_symbol': 'WAVAX',
#                 'accrued_rewards': 584.4829744983532,
#             }
#         ]
#     )

#     rewards_v3_expected = treasury_accrued_incentives_by_day_v3

#     rewards_v3_expected.network = rewards_v3_expected.network.astype(pd.StringDtype()) # type: ignore
#     rewards_v3_expected.market = rewards_v3_expected.market.astype(pd.StringDtype()) # type: ignore
#     rewards_v3_expected.collector_contract = rewards_v3_expected.collector_contract.astype(pd.StringDtype()) # type: ignore
#     rewards_v3_expected.block_height = rewards_v3_expected.block_height.astype('int')
#     rewards_v3_expected.block_day = pd.to_datetime(rewards_v3_expected.block_day, utc=True)
#     rewards_v3_expected.collector_contract = rewards_v3_expected.collector_contract.astype(pd.StringDtype()) # type: ignore
#     rewards_v3_expected.rewards_token_address = rewards_v3_expected.rewards_token_address.astype(pd.StringDtype()) # type: ignore
#     rewards_v3_expected.accrued_rewards = rewards_v3_expected.accrued_rewards.astype('float')

#     treasury_accrued_incentives_by_day_arc = pd.DataFrame()
#     rewards_arc_expected = pd.DataFrame()

#     rewards_v3_result = treasury_accrued_incentives_table(context_avax_v3, treasury_accrued_incentives_by_day_v3)
#     rewards_arc_result = treasury_accrued_incentives_table(context_eth_arc, treasury_accrued_incentives_by_day_arc)

#     assert_frame_equal(rewards_v3_expected, rewards_v3_result, check_exact=True)  # type: ignore
#     assert_frame_equal(rewards_arc_expected, rewards_arc_result, check_exact=True)  # type: ignore


# def test_user_lm_rewards_claimed_table():
#     """
#     Tests the user_lm_rewards_claimed asset on both aave_v3 and aave_v2 (including null returns)

#     """

#     pkey_eth = MultiPartitionKey(
#         {
#             "date": '2022-11-26',
#             "market": 'ethereum_v2'
#         }
#     )  # type: ignore

#     pkey_arb = MultiPartitionKey(
#         {
#             "date": '2022-11-26',
#             "market": 'arbitrum_v3'
#         }
#     )  # type: ignore

#     context_eth = build_op_context(partition_key=pkey_eth)
#     context_arb = build_op_context(partition_key=pkey_arb)

#     expected_eth = pd.DataFrame(
#         [
#             {
#                 'block_day': datetime(2022,11,26,0,0,0, tzinfo=timezone.utc),
#                 'chain': 'ethereum',
#                 'market': 'ethereum_v2',
#                 'reward_vault': 'ecosystem_reserve',
#                 'token_address': '0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9'.lower(),
#                 'balancer_claims': 1178.178995987,
#                 'incentives_claims': 0,
#                 'stkaave_claims': 103.964332841,
#             },
#             {
#                 'block_day': datetime(2022,11,26,0,0,0, tzinfo=timezone.utc),
#                 'chain': 'ethereum',
#                 'market': 'ethereum_v2',
#                 'reward_vault': 'incentives_controller',
#                 'token_address': '0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9'.lower(),
#                 'balancer_claims': 0,
#                 'incentives_claims': 83.24038401,
#                 'stkaave_claims': 0,
#             }
#         ]
#     )
#     ic(expected_eth)
#     # the function should handle markets that are not aave_v2 on mainnet gracefully
#     expected_non_eth = pd.DataFrame()

#     expected_eth.block_day = pd.to_datetime(expected_eth.block_day, utc=True)
#     expected_eth.chain = expected_eth.chain.astype(pd.StringDtype()) # type: ignore
#     expected_eth.reward_vault = expected_eth.reward_vault.astype(pd.StringDtype()) # type: ignore
#     expected_eth.market = expected_eth.market.astype(pd.StringDtype()) # type: ignore
#     expected_eth.token_address = expected_eth.token_address.astype(pd.StringDtype()) # type: ignore
#     expected_eth.balancer_claims = expected_eth.balancer_claims.astype('float')
#     expected_eth.incentives_claims = expected_eth.incentives_claims.astype('float')
#     expected_eth.stkaave_claims = expected_eth.stkaave_claims.astype('float')

#     eth_result = user_lm_rewards_table(context_eth, expected_eth)
#     non_eth_result = user_lm_rewards_table(context_arb, pd.DataFrame())

#     assert_frame_equal(expected_eth, eth_result, check_exact=True)  # type: ignore
#     assert_frame_equal(expected_non_eth, non_eth_result, check_exact=True)  # type: ignore

if __name__ == "__main__":
    # test_blocks_by_day()
    test_atoken_measures_by_day()

