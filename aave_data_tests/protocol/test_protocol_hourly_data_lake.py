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

def protocol_state_by_hour():
    """
    Tests the block_numbers_by_hour asset against a reference response

    """
    
    pkey = MultiPartitionKey(
        {
            "time": '2023-03-27-04:00',
            "market": 'polygon_v3'
        }
    )  # type: ignore

    context = build_op_context(partition_key=pkey)

    # result = protocol_state_by_hour_op(context)