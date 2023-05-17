"""Tests for assets & ops in the protocol module"""
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
from aave_data.assets.protocol.protocol_data_lake import *

from aave_data.resources.financials_config import *  # pylint: disable=wildcard-import, unused-wildcard-import

from aave_data.resources.helpers import (
    standardise_types
)

def test_protocol_data_by_day():
    """
    Tests the protocol_data_by_day asset against a reference response

    """

    pkey = MultiPartitionKey(
        {
            "date": '2022-11-26',
            "market": 'polygon_v3'
        }
    )  # type: ignore

    context = build_op_context(partition_key=pkey)

    market_tokens_by_day_sample_output = pd.DataFrame(
        [
            {
                "reserve": "0x2791bca1f2de4661ed88a30c99a7a9449aa84174",
                "name": "USD Coin (PoS)",
                "symbol": "USDC",
                "decimals": 6,
                "atoken": "0x625e7708f30ca75bfd92586e17077590c60eb4cd",
                "atoken_symbol": "aPolUSDC",
                "pool": "0x794a61358d6845594f94dc1db02a252b5b4814ad",
                "market": "polygon_v3",
                "atoken_decimals": 6,
                "block_height": 36068925,
                "block_day": datetime(2022, 11, 26, 0, 0, 0),
            }
        ]
    )
    market_tokens_by_day_sample_output = standardise_types(market_tokens_by_day_sample_output)

    expected = pd.DataFrame(
        [
            {
                "block_day": datetime(2022, 11, 26, 0, 0, 0),
                "block_height": 36068925,
                "market": "polygon_v3",
                "reserve": "0x2791bca1f2de4661ed88a30c99a7a9449aa84174",
                "symbol": "aPolUSDC",
                "decimals": 6,
                "ltv": 0.825,
                "liquidation_threshold": 0.85,
                "liquidation_bonus": 1.04,
                "reserve_factor": 0.1,
                "usage_as_collateral_enabled": True,
                "borrowing_enabled": True,
                "stable_borrow_rate_enabled": True,
                "is_active": True,
                "is_frozen": False,
                "unbacked_atokens": 0.0,
                "scaled_accrued_to_treasury": 43.39621,
                "atoken_supply": 32987105.149763,
                "stable_debt": 292202.63886,
                "variable_debt": 9883077.51599,
                "liquidity_rate": 0.004113700668945843,
                "variable_borrow_rate": 0.013709420849309104,
                "stable_borrow_rate": 0.05171367760616364,
                "average_stable_rate": 0.052311889505631086,
                "liquidity_index": 1.0098515920906572,
                "variable_borrow_index": 1.0162322913936128,
                "last_update_timestamp": datetime(2022, 11, 25, 23, 59, 18),
                "available_liquidity": 22811824.994913,
                "reserve_emode_category": 1,
                "borrow_cap": 30680000,
                "supply_cap": 2000000000,
                "is_paused": False,
                "siloed_borrowing": False,
                "liquidation_protocol_fee": 0.1,
                "unbacked_mint_cap": 0,
                "debt_ceiling": 0,
                "debt_ceiling_decimals": 2,
            }
        ]
    )
    expected = standardise_types(expected)

    result = protocol_data_by_day(context, market_tokens_by_day_sample_output)

    assert_frame_equal(result, expected, check_exact=True)

def test_raw_incentives_by_day():
    """
    Tests the raw_incentives_by_day asset against a reference response

    """

    pkey = MultiPartitionKey(
        {
            "date": '2023-03-15',
            "market": 'polygon_v3'
        }
    )  # type: ignore

    context = build_op_context(partition_key=pkey)

    block_numbers_by_day_sample = pd.DataFrame(
        [
            {
                'block_day': datetime(2023,3,15,0,0,0),
                'block_time': datetime(2022,3,15,0,0,0),
                'block_height': 40353814,
                'end_block': 40391766,
                'chain': 'polygon',
                'market': 'polygon_v3',
            }
        ]
    )
    block_numbers_by_day_sample = standardise_types(block_numbers_by_day_sample)

    expected = pd.DataFrame(
        [
            {
                "block_day": datetime(2023,3,15,0,0,0),
                "block_height": 40353814,
                "market": "polygon_v3",
                "underlying_asset": "0xfa68fb4628dff1028cfec22b4162fccd0d45efb6",
                "token_address": "0x80ca0d8c38d2e2bcbab66aa1648bd1c7160500fe",
                "incentive_controller_address": "0x929ec64c34a17401f460460d4b9390518e5b473e",
                "reward_token_symbol": 'SD',
                "reward_token_address": '0x1d734a02ef1e1f5886e66b0673b71af5b53ffa94',
                "reward_oracle_address": '0x30e9671a8092429a358a4e31d41381aa0d10b0a0',
                "emission_per_second": 3910108024691358.0,
                "incentives_last_update_timestamp": 1678835458,
                "token_incentives_index": 1421224559199312.0,
                "emission_end_timestamp": 1691310675,
                "reward_price_feed": 1135753.0,
                "reward_token_decimals": 18,
                "precision": 18,
                "price_feed_decimals": 6,
                "token_type": 'atoken',
            }
        ]
    )
    expected = standardise_types(expected)

    result = raw_incentives_by_day(context, block_numbers_by_day_sample)

    assert_frame_equal(result, expected, check_exact=True)


def test_emode_config_by_day():

    pkey = MultiPartitionKey(
            {
                "date": '2023-03-15',
                "market": 'polygon_v3'
            }
        )  # type: ignore

    context = build_op_context(partition_key=pkey)

    
    protocol_data_by_day_sample = pd.DataFrame(
            [
                {
                    "block_day": datetime(2023, 3, 15, 0, 0, 0),
                    "block_height": 40353814,
                    "market": "polygon_v3",
                    "reserve": "0x2791bca1f2de4661ed88a30c99a7a9449aa84174",
                    "symbol": "aPolUSDC",
                    "reserve_emode_category": 1,
                },
                {
                    "block_day": datetime(2023, 3, 15, 0, 0, 0),
                    "block_height": 40353814,
                    "market": "polygon_v3",
                    "reserve": "0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270",
                    "symbol": "aPolWMATIC",
                    "reserve_emode_category": 2,
                },
            ]
        )

    protocol_data_by_day_sample = standardise_types(protocol_data_by_day_sample)

    expected = pd.DataFrame(
            [
                {
                    "block_day": datetime(2023, 3, 15, 0, 0, 0),
                    "block_height": 40353814,
                    "market": "polygon_v3",
                    "reserve_emode_category": 1,
                    "emode_category_name": "Stablecoins",
                    "emode_ltv": 0.97,
                    "emode_liquidation_threshold": 0.975,
                    "emode_liquidation_bonus": 1.01,
                    "emode_price_address": "0x0000000000000000000000000000000000000000",
                },
                {
                    "block_day": datetime(2023, 3, 15, 0, 0, 0),
                    "block_height": 40353814,
                    "market": "polygon_v3",
                    "reserve_emode_category": 2,
                    "emode_category_name": "MATIC correlated",
                    "emode_ltv": 0.925,
                    "emode_liquidation_threshold": 0.95,
                    "emode_liquidation_bonus": 1.01,
                    "emode_price_address": "0x0000000000000000000000000000000000000000",
                },
            ]
        )
    
    expected = standardise_types(expected)
    
    result = emode_config_by_day(context, protocol_data_by_day_sample)

    assert_frame_equal(result, expected, check_exact=True)

def test_matic_lsd_token_supply_by_day():

    pkey = '2023-03-15'

    context = build_op_context(partition_key=pkey)

    blocks_by_day_sample = pd.DataFrame(
            [
                {
                    "block_day": datetime(2023, 3, 15, 0, 0, 0),
                    "block_time": datetime(2023, 3, 15, 0, 0, 0),
                    "block_height": 40353814,
                    "end_block": 40391766,
                    "chain": "polygon",
                },
                {
                    "block_day": datetime(2023, 3, 15, 0, 0, 0),
                    "block_time": datetime(2023, 3, 15, 0, 0, 0),
                    "block_height": 16829610,
                    "end_block": 16836710,
                    "chain": "ethereum",
                },
            ]
        )

    blocks_by_day_sample = standardise_types(blocks_by_day_sample)

    expected = pd.DataFrame(
            [
                {
                    "block_day": datetime(2023, 3, 15, 0, 0, 0),
                    "block_height": 40353814,
                    "chain": "polygon",
                    "address": "0x3a58a54c066fdc0f2d55fc9c89f0415c92ebf3c4",
                    "symbol": "stMATIC",
                    "decimals": 18,
                    "total_supply": 45113198.567459114,
                },
                {
                    "block_day": datetime(2023, 3, 15, 0, 0, 0),
                    "block_height": 40353814,
                    "chain": "polygon",
                    "address": "0xfa68fb4628dff1028cfec22b4162fccd0d45efb6",
                    "symbol": "MaticX",
                    "decimals": 18,
                    "total_supply": 21576294.82481394,
                },
                {
                    "block_day": datetime(2023, 3, 15, 0, 0, 0),
                    "block_height": 16829610,
                    "chain": "ethereum",
                    "address": "0x9ee91f9f426fa633d227f7a9b000e28b9dfd8599",
                    "symbol": "stMATIC",
                    "decimals": 18,
                    "total_supply": 80971954.03109746,
                },
                {
                    "block_day": datetime(2023, 3, 15, 0, 0, 0),
                    "block_height": 16829610,
                    "chain": "ethereum",
                    "address": "0xf03a7eb46d01d9ecaa104558c732cf82f6b6b645",
                    "symbol": "MaticX",
                    "decimals": 18,
                    "total_supply": 45987071.46845635,
                },
            ]
        )
    
    expected = standardise_types(expected)
    
    result = matic_lsd_token_supply_by_day(context, blocks_by_day_sample)

    assert_frame_equal(result, expected, check_exact=True)

def test_beacon_chain_staking_returns_by_day():
    """
    Tests the beacon_chain_staking_returns_by_day asset against a reference response

    """

    pkey =  '2023-05-13'

    context = build_op_context(partition_key=pkey)

    expected = pd.DataFrame(
        [
            {
                'partition_date': datetime(2023,5,13,0,0,0),
                'beaconchain_day': 892,
                'day_start': datetime(2023,5,12,12,0,23),
                'day_end': datetime(2023,5,13,12,0,23),
                'apr': 0.0562568139599815,
                'cl_apr': 0.0348226211039776,
                'el_apr': 0.0214341928560039,
            }
        ]
    )
    expected = standardise_types(expected)

    result = beacon_chain_staking_returns_by_day(context)

    assert_frame_equal(result.head(1), expected, check_exact=True)

def test_compound_v2_by_day():
    """
    Tests the compound_v2_by_hour asset against a reference response

    """
    
    pkey = '2023-05-15'

    context = build_op_context(partition_key=pkey)

    # result = protocol_state_by_hour_op(context)

    blocks_by_day_output = pd.DataFrame(
        [
            {
                'block_day': datetime(2023,5,15,0,0,0),
                'block_height': 17261505,
                'end_block': 17268587,
                'chain': 'ethereum',
            },
            {
                'block_day': datetime(2023,5,14,0,0,0),
                'block_height': 17254453,
                'end_block': 17261504,
                'chain': 'ethereum',
            }
        ]
    )
    blocks_by_day_output = standardise_types(blocks_by_day_output)

    expected = pd.DataFrame(
        [
            {
                'block_day': datetime(2023,5,15,0,0,0),
                'block_height': 17261505,
                'chain': 'ethereum',
                'compound_version': 'compound_v2',
                'symbol': 'cDAI',
                "address": "0x5d3a536e4d6dbd6114cc1ead35777bab948e3643",
                "underlying_symbol": "DAI",
                "underlying_address": "0x6b175474e89094c44da98b954eedeac495271d0f",
                'supply_apy': 0.016814667471989786,
                'borrow_apy': 0.035634551812653514,
                'deposits': 286659454.689363,
                'borrows': 160602734.047785,
            }
        ]
    )
    expected = standardise_types(expected)


    result = compound_v2_by_day(context, blocks_by_day_output)

    assert_frame_equal(result.head(1), expected, check_exact=True)

def test_compound_v3_by_day():
    """
    Tests the compound_v3_by_hour asset against a reference response

    """
    
    pkey = '2023-05-15'

    context = build_op_context(partition_key=pkey)

    # result = protocol_state_by_hour_op(context)

    blocks_by_day_output = pd.DataFrame(
        [
            {
                'block_day': datetime(2023,5,15,0,0,0),
                'block_height': 17261505,
                'end_block': 17268587,
                'chain': 'ethereum',
            },
            {
                'block_day': datetime(2023,5,14,0,0,0),
                'block_height': 17254453,
                'end_block': 17261504,
                'chain': 'ethereum',
            }
        ]
    )
    blocks_by_day_output = standardise_types(blocks_by_day_output)

    expected = pd.DataFrame(
        [
            {
                'block_day': datetime(2023,5,15,0,0,0),
                'block_height': 17261505,
                'chain': 'ethereum',
                'compound_version': 'compound_v3',
                'symbol': 'cUSDC',
                "address": "0xc3d688b66703497daa19211eedff47f25384cdc3",
                "underlying_symbol": "USDC",
                "underlying_address": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
                'supply_apy': 0.023241,
                'borrow_apy': 0.040029,
                'deposits': 266580252.771759,
                'borrows': 190632878.286075,
            }
        ]
    )
    expected = standardise_types(expected)

    result = compound_v3_by_day(context, blocks_by_day_output)
    ic(result)
    assert_frame_equal(result.head(1), expected, check_exact=True)

if __name__ == "__main__":
    # test_protocol_data_by_day()
    # test_raw_incentives_by_day()
    # test_incentives_by_day()
    # test_emode_config_by_day()
    # test_beacon_chain_staking_returns_by_day()
    test_compound_v3_by_day()