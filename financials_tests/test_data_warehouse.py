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
    non_atoken_measures_by_day,
    user_rewards_by_day,
    treasury_incentives_by_day
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
            "transfers_transfer_type":{
                0:"IN", # in_external
                1:"IN", # in_internal
                2:"OUT", # out_external
                3:"OUT", # out_internal
            },
            "transfers_from_address":{
                0:"0x0000000000000000000000000000000000000000",
                1:"0x7734280A4337F37Fbf4651073Db7c28C80B339e9",
                2:"0xe8599f3cc5d38a9ad6f3684cd5cea72f10dbc383",
                3:"0xe8599f3cc5d38a9ad6f3684cd5cea72f10dbc383"
            },
            "transfers_to_address":{
                0:"0xe8599f3cc5d38a9ad6f3684cd5cea72f10dbc383",
                1:"0xe8599f3cc5d38a9ad6f3684cd5cea72f10dbc383",
                2:"0xe8599f3cc5d38a9ad6f3684cd5cea72fexternal",
                3:"0x7734280A4337F37Fbf4651073Db7c28C80B339e9"
            },
            "transfers_contract_address":{
                0:"0x078f358208685046a11c85e8ad32895ded33a249",
                1:"0x078f358208685046a11c85e8ad32895ded33a249",
                2:"0x078f358208685046a11c85e8ad32895ded33a249",
                3:"0x078f358208685046a11c85e8ad32895ded33a249"
            },
            "transfers_contract_name":{
                0:"Aave Polygon WBTC",
                1:"Aave Polygon WBTC",
                2:"Aave Polygon WBTC",
                3:"Aave Polygon WBTC"
            },
            "transfers_contract_decimals":{
                0:8,
                1:8,
                2:8,
                3:8
            },
            "transfers_contract_symbol":{
                0:"aPolWBTC",
                1:"aPolWBTC",
                2:"aPolWBTC",
                3:"aPolWBTC"
            },
            "block_day":{
                0:datetime(2023,1,19,0,0,0),
                1:datetime(2023,1,19,0,0,0),
                2:datetime(2023,1,19,0,0,0),
                3:datetime(2023,1,19,0,0,0),
            },
            "amount_transferred":{
                0:0.1,
                1:0.2,
                2:0.3,
                3:0.4
            },
            "start_block":{
                0:38249632,
                1:38249632,
                2:38249632,
                3:38249632
            },
            "end_block":{
                0:38288978,
                1:38288978,
                2:38288978,
                3:38288978
            },
            "market":{
                0:"polygon_v3",
                1:"polygon_v3",
                2:"polygon_v3",
                3:"polygon_v3"
            },
            "collector":{
                0:"0xe8599f3cc5d38a9ad6f3684cd5cea72f10dbc383",
                1:"0xe8599f3cc5d38a9ad6f3684cd5cea72f10dbc383",
                2:"0xe8599f3cc5d38a9ad6f3684cd5cea72f10dbc383",
                3:"0xe8599f3cc5d38a9ad6f3684cd5cea72f10dbc383"
            }
        }
    )
    
    collector_atoken_transfers_by_day_sample_output = standardise_types(collector_atoken_transfers_by_day_sample_output)
    collector_atoken_transfers_by_day_sample_output_eth = pd.DataFrame()

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
            },
            {
                'chain': 'polygon',
                'description': 'V2 collector',
                'contract_address': '0x7734280a4337f37fbf4651073db7c28c80b339e9',
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
                'tokens_in_external': 0.1,
                'tokens_in_internal': 0.2,
                'tokens_out_external': 0.3,
                'tokens_out_internal': 0.4,
                'minted_to_treasury_amount': 0.00109192,
                'minted_amount': 0.0010951,
                'chain': 'polygon'
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
                'tokens_in_external': float(0),
                'tokens_in_internal': float(0),
                'tokens_out_external': float(0),
                'tokens_out_internal': float(0),
                'minted_to_treasury_amount': float(0),
                'minted_amount': float(0),
                'chain': 'ethereum'
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
    result_eth = atoken_measures_by_day(
                    context_eth,
                    collector_atoken_balances_by_day_sample_output_eth,
                    collector_atoken_transfers_by_day_sample_output_eth,
                    pd.DataFrame(),
                    pd.DataFrame(),
                    internal_external_addresses_sample
                    )
    
    result_nul = atoken_measures_by_day(
                    context_pol,
                    pd.DataFrame(),
                    pd.DataFrame(),
                    pd.DataFrame(),
                    pd.DataFrame(),
                    internal_external_addresses_sample
                    )


    assert_frame_equal(result_pol, expected_pol, check_exact=True)
    assert_frame_equal(result_eth, expected_eth, check_exact=True)
    assert_frame_equal(result_nul, pd.DataFrame(), check_exact=True)


def test_non_atoken_measures_by_day():
    """
    Tests the non_atoken measures asset
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

    non_atoken_balances_by_day_sample_output_eth = pd.DataFrame(
        [
            {
                'contract_address': "0x464c71f6c2f760dda6093dcb91c24c39e5d6e18c",
                'chain': 'ethereum',
                'market': 'ethereum_v2',
                'token': '0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9',
                'decimals': 18,
                'symbol': 'AAVE',
                'block_height': 16515917,
                'block_day': datetime(2023,1,30,0,0,0),
                'balance': 1234.909
            }
        ]
    )
    non_atoken_balances_by_day_sample_output_eth = standardise_types(non_atoken_balances_by_day_sample_output_eth)
 

    non_atoken_transfers_by_day_sample_output_eth = pd.DataFrame(
        {
            "transfers_transfer_type":{
                0:"IN", # in_external
                1:"IN", # in_internal
                2:"OUT", # out_external
                3:"OUT", # out_internal
            },
            "transfers_from_address":{
                0:"0x0000000000000000000000000000000000000000",
                1:"0xd784927ff2f95ba542bfc824c8a8a98f3495f6b5",
                2:"0x464c71f6c2f760dda6093dcb91c24c39e5d6e18c",
                3:"0x464c71f6c2f760dda6093dcb91c24c39e5d6e18c"
            },
            "transfers_to_address":{
                0:"0x464c71f6c2f760dda6093dcb91c24c39e5d6e18c",
                1:"0x464c71f6c2f760dda6093dcb91c24c39e5d6e18c",
                2:"0xe8599f3cc5d38a9ad6f3684cd5cea72fexternal",
                3:"0xd784927ff2f95ba542bfc824c8a8a98f3495f6b5"
            },
            "transfers_contract_address":{
                0:"0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9",
                1:"0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9",
                2:"0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9",
                3:"0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9"
            },
            "transfers_contract_name":{
                0:"Aave Token",
                1:"Aave Token",
                2:"Aave Token",
                3:"Aave Token"
            },
            "transfers_contract_decimals":{
                0:18,
                1:18,
                2:18,
                3:18
            },
            "transfers_contract_symbol":{
                0:"AAVE",
                1:"AAVE",
                2:"AAVE",
                3:"AAVE"
            },
            "block_day":{
                0:datetime(2023,1,30,0,0,0),
                1:datetime(2023,1,30,0,0,0),
                2:datetime(2023,1,30,0,0,0),
                3:datetime(2023,1,30,0,0,0),
            },
            "amount_transferred":{
                0:0.1,
                1:0.2,
                2:0.3,
                3:0.4
            },
            "start_block":{
                0:16515917,
                1:16515917,
                2:16515917,
                3:16515917
            },
            "end_block":{
                0:16523084,
                1:16523084,
                2:16523084,
                3:16523084
            },
            "market":{
                0:"ethereum_v2",
                1:"ethereum_v2",
                2:"ethereum_v2",
                3:"ethereum_v2"
            },
            "collector":{
                0:"0x464c71f6c2f760dda6093dcb91c24c39e5d6e18c",
                1:"0x464c71f6c2f760dda6093dcb91c24c39e5d6e18c",
                2:"0x464c71f6c2f760dda6093dcb91c24c39e5d6e18c",
                3:"0x464c71f6c2f760dda6093dcb91c24c39e5d6e18c"
            }
        }
    )
    non_atoken_transfers_by_day_sample_output_eth = standardise_types(non_atoken_transfers_by_day_sample_output_eth)

    internal_external_addresses_sample = pd.DataFrame(
        [
            {
                'chain': 'ethereum',
                'description': 'V2 collector',
                'contract_address': '0x464c71f6c2f760dda6093dcb91c24c39e5d6e18c',
                'internal_external': 'aave_internal',
            },
            {
                'chain': 'ethereum',
                'description': 'incentives controller',
                'contract_address': '0xd784927ff2f95ba542bfc824c8a8a98f3495f6b5',
                'internal_external': 'aave_internal',
            }
        ]
    )

    expected_eth = pd.DataFrame(
        [
            {
                'contract_address': "0x464c71f6c2f760dda6093dcb91c24c39e5d6e18c",
                'chain': 'ethereum',
                'market': 'ethereum_v2',
                'token': '0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9',
                'decimals': 18,
                'symbol': 'AAVE',
                'block_height': 16515917,
                'block_day': datetime(2023,1,30,0,0,0),
                'balance': 1234.909,
                'tokens_in_external': 0.1,
                'tokens_in_internal': 0.2,
                'tokens_out_external': 0.3,
                'tokens_out_internal': 0.4,
            }
        ]
    )
    expected_eth = standardise_types(expected_eth)

    expected_pol = pd.DataFrame()

    result_eth = non_atoken_measures_by_day(
                    context_eth,
                    non_atoken_balances_by_day_sample_output_eth,
                    non_atoken_transfers_by_day_sample_output_eth,
                    internal_external_addresses_sample
                    )
    result_pol = non_atoken_measures_by_day(
                    context_pol,
                    pd.DataFrame(),
                    pd.DataFrame(),
                    internal_external_addresses_sample
                    )

    assert_frame_equal(result_eth, expected_eth, check_exact=True)
    assert_frame_equal(result_pol, expected_pol, check_exact=True)
    
def test_user_rewards_by_day():
    """
    Tests the user_rewards_by_day function
    """
    pkey_eth = MultiPartitionKey(
        {
            "date": '2023-01-30',
            "market": 'ethereum_v2'
        }
    )  # type: ignore

    pkey_pol = MultiPartitionKey(
        {
            "date": '2023-01-30',
            "market": 'polygon_v3'
        }
    )  # type: ignore

    context_eth = build_op_context(partition_key=pkey_eth)
    context_pol = build_op_context(partition_key=pkey_pol)
    
    user_lm_reward_claimed_eth_sample = pd.DataFrame(
        [
            {   
                "block_day": datetime(2023,1,30,0,0,0),
                "chain": "ethereum",
                "market": "ethereum_v2",
                "reward_vault": "ecosystem_reserve",
                "vault_address": "0x25f2226b597e8f9514b3f68f00f494cf4f286491",
                "token_address": "0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9",
                "sm_stkAAVE_claims": 1033.82,
                "sm_stkABPT_claims": 449.626,
                "lm_aave_v2_claims": float(0)
            },
            {   
                "block_day": datetime(2023,1,30,0,0,0),
                "chain": "ethereum",
                "market": "ethereum_v2",
                "reward_vault": "incentives_controller_v2",
                "vault_address": "0xd784927ff2f95ba542bfc824c8a8a98f3495f6b5",
                "token_address": "0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9",
                "sm_stkAAVE_claims": float(0),
                "sm_stkABPT_claims": float(0),
                "lm_aave_v2_claims": 59.4723,
            },
        ]
    )
    user_lm_reward_claimed_eth_sample = standardise_types(user_lm_reward_claimed_eth_sample)

    expected_eth = pd.DataFrame(
        [
            {   
                "block_day": datetime(2023,1,30,0,0,0),
                "chain": "ethereum",
                "market": "ethereum_v2",
                "reward_vault": "ecosystem_reserve",
                "vault_address": "0x25f2226b597e8f9514b3f68f00f494cf4f286491",
                "token_address": "0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9",
                "sm_stkAAVE_claims": 1033.82,
                "sm_stkABPT_claims": 449.626,
                "lm_aave_v2_claims": float(0),
                "sm_stkAAVE_owed": float(0),
                "sm_stkABPT_owed": float(0),
                "lm_aave_v2_owed": float(0),
            },
            {   
                "block_day": datetime(2023,1,30,0,0,0),
                "chain": "ethereum",
                "market": "ethereum_v2",
                "reward_vault": "incentives_controller_v2",
                "vault_address": "0xd784927ff2f95ba542bfc824c8a8a98f3495f6b5",
                "token_address": "0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9",
                "sm_stkAAVE_claims": float(0),
                "sm_stkABPT_claims": float(0),
                "lm_aave_v2_claims": 59.4723,
                "sm_stkAAVE_owed": float(0),
                "sm_stkABPT_owed": float(0),
                "lm_aave_v2_owed": float(0),
            },
        ]
    )
    expected_eth = standardise_types(expected_eth)

    result_eth = user_rewards_by_day(context_eth, user_lm_reward_claimed_eth_sample)
    result_pol = user_rewards_by_day(context_pol, pd.DataFrame())

    assert_frame_equal(result_eth, expected_eth, check_exact=True)
    assert_frame_equal(result_pol, pd.DataFrame(), check_exact=True)


def test_treasury_incentives_by_day():
    """
    Tests the table with treasury incentives data
    """

    pkey_eth = MultiPartitionKey(
        {
            "date": '2023-01-30',
            "market": 'ethereum_v2'
        }
    )  # type: ignore

    pkey_pol = MultiPartitionKey(
        {
            "date": '2023-01-30',
            "market": 'polygon_v3'
        }
    )  # type: ignore

    context_eth = build_op_context(partition_key=pkey_eth)
    context_pol = build_op_context(partition_key=pkey_pol)
    
    treasury_accrued_incentives_eth_sample = pd.DataFrame(
        [
            {   
                "chain": "ethereum",
                "market": "ethereum_v2",
                "collector_contract": "0x464c71f6c2f760dda6093dcb91c24c39e5d6e18c",
                "block_day": datetime(2023,1,30,0,0,0),
                "rewards_token_address": "0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9",
                "rewards_token_symbol": "stkAAVE",
                "accrued_rewards": 561.626,
            },
        ]
    )
    treasury_accrued_incentives_eth_sample = standardise_types(treasury_accrued_incentives_eth_sample)

    expected_eth = pd.DataFrame(
        [
            {   
                "chain": "ethereum",
                "market": "ethereum_v2",
                "collector_contract": "0x464c71f6c2f760dda6093dcb91c24c39e5d6e18c",
                "block_day": datetime(2023,1,30,0,0,0),
                "rewards_token_address": "0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9",
                "rewards_token_symbol": "stkAAVE",
                "accrued_rewards": 561.626,
                "held_rewards": float(0),
            },
        ]
    )
    expected_eth = standardise_types(expected_eth)

    result_eth = treasury_incentives_by_day(context_eth, treasury_accrued_incentives_eth_sample)
    result_pol = treasury_incentives_by_day(context_pol, pd.DataFrame())
    ic(expected_eth)
    ic(result_eth)

    assert_frame_equal(result_eth, expected_eth, check_exact=True)
    assert_frame_equal(result_pol, pd.DataFrame(), check_exact=True)



if __name__ == "__main__":
    # test_blocks_by_day()
    # test_non_atoken_measures_by_day()
    # test_user_rewards_by_day()
    # test_treasury_incentives_by_day()
    test_atoken_measures_by_day()
