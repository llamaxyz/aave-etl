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
from aave_data.assets.protocol.protocol_data_warehouse import (
                                                        incentives_by_day
                                                    )

from aave_data.resources.financials_config import *  # pylint: disable=wildcard-import, unused-wildcard-import

from aave_data.resources.helpers import (
    standardise_types
)

def test_incentives_by_day():
    """
    Tests the incentives_by_day asset against a reference response

    """
    # todo move this to test_protocol_data_warehouse
    
    pkey = MultiPartitionKey(
        {
            "date": '2023-03-27',
            "market": 'polygon_v3'
        }
    )  # type: ignore

    context = build_op_context(partition_key=pkey)

    raw_incentives_by_day_sample = pd.DataFrame(
        [
            {
                "block_day": datetime(2023,3,27,0,0,0),
                "block_height": 40805643,
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
            },
            {   # a 2nd asset with rewards
                "block_day": datetime(2023,3,27,0,0,0),
                "block_height": 40805643,
                "market": "polygon_v3",
                "underlying_asset": "0x3a58a54c066fdc0f2d55fc9c89f0415c92ebf3c4",
                "token_address": "0xea1132120ddcdda2f119e99fa7a27a0d036f7ac9",
                "incentive_controller_address": "0x929ec64c34a17401f460460d4b9390518e5b473e",
                "reward_token_symbol": 'LDO',
                "reward_token_address": '0xc3c7d422809852031b44ab29eec9f1eff2a58756',
                "reward_oracle_address": '0x30e9671a8092429a358a4e31d41381aa0d10b0a0',
                "emission_per_second": 3100198412698414.0,
                "incentives_last_update_timestamp": 1679874173,
                "token_incentives_index": 52127909615153.0,
                "emission_end_timestamp": 1684584000,
                "reward_price_feed": 3.19e+18,
                "reward_token_decimals": 18,
                "precision": 18,
                "price_feed_decimals": 18,
                "token_type": 'atoken',
            },
            {   # an asset with expired rewards
                "block_day": datetime(2023,3,27,0,0,0),
                "block_height": 40805643,
                "market": "polygon_v3",
                "underlying_asset": "0xfa68fb4628dff1028cfec22b4162fccd0d45efb6",
                "token_address": "0x80ca0d8c38d2e2bcbab66aa1648bd1c7160500fe",
                "incentive_controller_address": "0x929ec64c34a17401f460460d4b9390518e5b473e",
                "reward_token_symbol": 'SD_expired',
                "reward_token_address": '0x1d734a02ef1e1f5886e66b0673b71af5b53ffa94',
                "reward_oracle_address": '0x30e9671a8092429a358a4e31d41381aa0d10b0a0',
                "emission_per_second": 3510108024691358.0,
                "incentives_last_update_timestamp": 1678835458,
                "token_incentives_index": 1421224559199312.0,
                "emission_end_timestamp": 1679662800,
                "reward_price_feed": 1135753.0,
                "reward_token_decimals": 18,
                "precision": 18,
                "price_feed_decimals": 6,
                "token_type": 'atoken',
            }
        ]
    )
    raw_incentives_by_day_sample = standardise_types(raw_incentives_by_day_sample)

    protocol_data_by_day_sample = pd.DataFrame(
        [
            {
                "block_day": datetime(2023, 3, 27, 0, 0, 0),
                "block_height": 40805643,
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
            },
            {
                "block_day": datetime(2023, 3, 27, 0, 0, 0),
                "block_height": 40805643,
                "market": 'polygon_v3',
                "reserve": '0xfa68fb4628dff1028cfec22b4162fccd0d45efb6',
                "symbol": 'aPolMATICX',
                "decimals": 18,
                "ltv": 0.58,
                "liquidation_threshold": 0.67,
                "liquidation_bonus": 1.1,
                "reserve_factor": 0.2,
                "usage_as_collateral_enabled": True,
                "borrowing_enabled": True,
                "stable_borrow_rate_enabled": False,
                "is_active": True,
                "is_frozen": False,
                "unbacked_atokens": 0.0,
                "scaled_accrued_to_treasury": 0.91905075631900646,
                "atoken_supply": 9067563.730126325,
                "stable_debt": 0.0,
                "variable_debt": 455591.72832620103,
                "liquidity_rate": 0.00028000539449643313,
                "variable_borrow_rate": 0.0069661359763045945,
                "stable_borrow_rate": 0.060558266997038077,
                "average_stable_rate": 0.0,
                "liquidity_index": 1.0000171133605387,
                "variable_borrow_index": 1.0002732976059296,
                "last_update_timestamp": datetime(2023, 3, 27, 0, 0, 0),
                "available_liquidity": 8611972.0018001236,
                "reserve_emode_category": 2,
                "borrow_cap": 5200000,
                "supply_cap": 17200000,
                "is_paused": False,
                "siloed_borrowing": False,
                "liquidation_protocol_fee": 0.1,
                "unbacked_mint_cap": 0,
                "debt_ceiling": 0,
                "debt_ceiling_decimals": 2
            },
            {
                "block_day": datetime(2023, 3, 27, 0, 0, 0),
                "block_height": 40805643,
                "market": 'polygon_v3',
                "reserve": '0x3a58a54c066fdc0f2d55fc9c89f0415c92ebf3c4',
                "symbol": 'aPolSTMATIC',
                "decimals": 18,
                "ltv": 0.5,
                "liquidation_threshold": 0.65,
                "liquidation_bonus": 1.1,
                "reserve_factor": 0.2,
                "usage_as_collateral_enabled": True,
                "borrowing_enabled": False,
                "stable_borrow_rate_enabled": False,
                "is_active": True,
                "is_frozen": False,
                "unbacked_atokens": 0.0,
                "scaled_accrued_to_treasury": 0.000362979302076407,
                "atoken_supply": 11292113.98316204,
                "stable_debt": 0.0,
                "variable_debt": 0.0,
                "liquidity_rate": 0.0,
                "variable_borrow_rate": 0.0,
                "stable_borrow_rate": 0.09,
                "average_stable_rate": 0.0,
                "liquidity_index": 1.0000561590710746,
                "variable_borrow_index": 1.0,
                "last_update_timestamp": datetime(2023, 3, 27, 0, 0, 0),
                "available_liquidity": 11292113.98316204,
                "reserve_emode_category": 2,
                "borrow_cap": 0,
                "supply_cap": 15000000,
                "is_paused": False,
                "siloed_borrowing": False,
                "liquidation_protocol_fee": 0.2,
                "unbacked_mint_cap": 0,
                "debt_ceiling": 0,
                "debt_ceiling_decimals": 2
            },
        ]
    )
    protocol_data_by_day_sample = standardise_types(protocol_data_by_day_sample)

    aave_oracle_prices_by_day_sample = pd.DataFrame(
        {
            "reserve": {
                0: "0xfa68fb4628dff1028cfec22b4162fccd0d45efb6",
                1: "0x3a58a54c066fdc0f2d55fc9c89f0415c92ebf3c4",
            },
            "symbol": {0: "aPolMATICX", 1: "aPolSTMATIC"},
            "market": {0: "polygon_v3", 1: "polygon_v3"},
            "block_height": {0: 40805643, 1: 40805643},
            "block_day": {
                0: datetime(2023, 3, 27, 0, 0, 0),
                1: datetime(2023, 3, 27, 0, 0, 0),
            },
            "usd_price": {0: 1.16865609, 1: 1.1765653},
        }
    )

    aave_oracle_prices_by_day_sample = standardise_types(aave_oracle_prices_by_day_sample)

    expected = pd.DataFrame(
        [
            {
                "block_day": datetime(2023,3,27,0,0,0),
                "block_height": 40805643,
                "market": "polygon_v3",
                "reserve": "0x3a58a54c066fdc0f2d55fc9c89f0415c92ebf3c4",
                "symbol": "aPolSTMATIC",
                "reward_token_symbol": 'LDO',
                "reward_token_address": '0xc3c7d422809852031b44ab29eec9f1eff2a58756',
                "supply_rewards_apr": 0.023474453506071684,
                "variable_borrow_rewards_apr": 0.0,
                "stable_borrow_rewards_apr": 0.0,
            },
            {
                "block_day": datetime(2023,3,27,0,0,0),
                "block_height": 40805643,
                "market": "polygon_v3",
                "reserve": "0xfa68fb4628dff1028cfec22b4162fccd0d45efb6",
                "symbol": "aPolMATICX",
                "reward_token_symbol": 'SD',
                "reward_token_address": '0x1d734a02ef1e1f5886e66b0673b71af5b53ffa94',
                "supply_rewards_apr": 0.01321605727791571,
                "variable_borrow_rewards_apr": 0.0,
                "stable_borrow_rewards_apr": 0.0,
            }
        ]
    )

    expected = standardise_types(expected)

    result = incentives_by_day(context, raw_incentives_by_day_sample, protocol_data_by_day_sample, aave_oracle_prices_by_day_sample)

    assert_frame_equal(result, expected, check_exact=True)


if __name__ == "__main__":
    # test_protocol_data_by_day()
    # test_raw_incentives_by_day()
    test_incentives_by_day()