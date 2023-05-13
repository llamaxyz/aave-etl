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
from aave_data.assets.protocol.protocol_hourly_data_lake import *

from aave_data.resources.financials_config import *  # pylint: disable=wildcard-import, unused-wildcard-import

from aave_data.resources.helpers import (
    standardise_types
)

def test_compound_v2_by_hour():
    """
    Tests the compound_v2_by_hour asset against a reference response

    """
    
    pkey = MultiPartitionKey(
        {
            "time": '2023-05-13-06:00',
            "market": 'ethereum_v3'
        }
    )  # type: ignore

    context = build_op_context(partition_key=pkey)

    # result = protocol_state_by_hour_op(context)

    block_numbers_by_hour_sample_output = pd.DataFrame(
        [
            {
                'block_hour': datetime(2023,5,13,6,0,0),
                'block_hour': datetime(2023,5,13,6,0,0),
                'block_height': 17249150,
                'chain': 'ethereum',
                'market': 'ethereum_v3',
            }
        ]
    )

    expected = pd.DataFrame(
        [
            {
                'block_hour': datetime(2023,5,13,6,0,0),
                'block_height': 17249150,
                'chain': 'ethereum',
                'compound_version': 'compound_v2',
                'symbol': 'cDAI',
                "address": "0x5d3a536e4d6dbd6114cc1ead35777bab948e3643",
                "underlying_symbol": "DAI",
                "underlying_address": "0x6b175474e89094c44da98b954eedeac495271d0f",
                'supply_apy': 0.016803,
                'borrow_apy': 0.035622,
                'deposits': 286711010.985461,
                'borrows': 160577088.247921,
            }
        ]
    )
    expected = standardise_types(expected)

    result = compound_v2_by_hour(context, block_numbers_by_hour_sample_output)

    assert_frame_equal(result.head(1), expected, check_exact=True)

if __name__ == "__main__":

    test_compound_v2_by_hour()