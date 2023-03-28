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
from aave_data.assets.protocol.protocol_data_lake import (
                                                        protocol_data_by_day,
                                                        raw_incentives_by_day
                                                    )

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

if __name__ == "__main__":
    # test_protocol_data_by_day()
    test_raw_incentives_by_day()