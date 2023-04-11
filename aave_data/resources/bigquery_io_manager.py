import os
import json
import pandas as pd
import numpy as np
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Mapping, Optional, Sequence, Tuple, Union
from time import sleep

from pandas import DataFrame as PandasDataFrame
# from pandas import read_sql
# from pyspark.sql import DataFrame as SparkDataFrame
# from snowflake.connector.pandas_tools import pd_writer
# from snowflake.sqlalchemy import URL  # pylint: disable=no-name-in-module,import-error
# from sqlalchemy import create_engine

import pandas_gbq 
from google.oauth2 import service_account
from google.cloud.bigquery import Client, LoadJobConfig
from icecream import ic
from google.api_core.retry import Retry

from aave_data.resources.helpers import standardise_types

from dagster import (
    IOManager,
    InputContext,
    MetadataValue,
    OutputContext,
    TableColumn,
    TableSchema,
    io_manager,
    TimeWindowPartitionsDefinition,
    MultiPartitionsDefinition
)

# Borrowed heavily from: 
#  https://github.com/dagster-io/dagster/blob/master/examples/project_fully_featured/project_fully_featured/resources/snowflake_io_manager.py


BIGQUERY_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"

INITIAL_RETRY = 0.01 #seconds
MAX_RETRIES = 10


@io_manager(config_schema={
    "project": str,
    "dataset": str,
    "service_account_creds": str,
    "service_account_file": str,
    "use_service_account_file": bool,
    "append_only": bool,})
def bigquery_io_manager(init_context):
    return BigQueryIOManager(
        config = {
            "project": init_context.resource_config["project"],
            "dataset": init_context.resource_config["dataset"],
            "service_account_creds": init_context.resource_config["service_account_creds"],
            "service_account_file": init_context.resource_config["service_account_file"],
            "use_service_account_file": init_context.resource_config["use_service_account_file"],
            "append_only": init_context.resource_config["append_only"],
        }
    )


class BigQueryIOManager(IOManager):
    """
    This IOManager can handle outputs that are Pandas DataFrames.
    Partition aware 
    The IOManager assumes that the dataset exists in BigQuery
    """

    def __init__(self, config):
        self._config = config
        # initialise the pandas_gbq context
        if config['use_service_account_file']:
            bq_creds = service_account.Credentials.from_service_account_file(config['service_account_file'])
        else:
            bq_creds = service_account.Credentials.from_service_account_info(json.loads(config["service_account_creds"]))
        pandas_gbq.context.credentials = bq_creds
        pandas_gbq.context.project = config["project"]
        pandas_gbq.context.dialect = 'standard'
        # initialise the google client for write operations
        self.client = Client(credentials=bq_creds, project=config["project"])


    def handle_output(self, context: OutputContext, obj: PandasDataFrame):
        # dbt handling
        if isinstance(obj, type(None)):
            return
            
        # ic(context.asset_key)
        dataset = self._config["dataset"]
        table = context.asset_key.path[-1]  # type: ignore
        # ic(table)

        # set the partition mode and associated vars based on the partition type
        if context.has_asset_partitions:
            if isinstance(context.asset_partitions_def, TimeWindowPartitionsDefinition):
                partition_type = "time_window"
                partition_key = context.asset_partition_key
                time_window = context.asset_partitions_time_window
            elif isinstance(context.asset_partitions_def, MultiPartitionsDefinition): 
                partition_type = "multi"
                partition_key = context.asset_partition_key
                time_window = None
            else:
                raise ValueError("Unsupported partition type")
        else:
            partition_type = None
            partition_key = None
            time_window = None

        if not obj.empty:
            # check for append_only config - if set, don't delete existing data prior to write
            if not self._config['append_only']:
                # enforced idempotency using delete-write pattern.  Used for partitioned assets, non-partitioned assets use overwrite.
                cleanup_query = self._get_cleanup_statement(table, dataset, partition_type, time_window, partition_key)
                # ic(cleanup_query)

                i = 0
                err_404 = False
                delay_time = INITIAL_RETRY
                while True:
                    try:
                        pd.read_gbq(cleanup_query, dialect='standard', use_bqstorage_api=True)
                        break
                    except Exception as e:
                        if i > MAX_RETRIES:
                            raise e
                        elif "Reason: 404" in str(e):
                            # table missing, delete will fail with 404.  Don't retry
                            err_404 = True
                            break
                        elif "Reason: 400 Could not serialize access" in str(e):
                            # concurrent access issue, move through to retry logic
                            pass
                        else:
                            # raise any other GenericGBQException rather than retry
                            raise e

                        if err_404:
                            break
                        else:    
                            context.log.warning(f"Retry {i} cleanup read_gbq after {delay_time} seconds")
                            sleep(delay_time)
                            i += 1
                            delay_time *= 2

            # ic(obj)
            # add a load timestamp to the table
            obj["_dagster_load_timestamp"] = datetime.utcnow()
            
            # add partition metadata to the table
            obj["_dagster_partition_type"] = partition_type
            obj["_dagster_partition_key"] = partition_key
            obj["_dagster_partition_time"] = time_window[0] if time_window else None
            obj._dagster_partition_type = obj._dagster_partition_type.astype(pd.StringDtype())
            obj._dagster_partition_key = obj._dagster_partition_key.astype(pd.StringDtype())

            if isinstance(obj, PandasDataFrame):
                metadata = self._handle_pandas_output(context, obj, dataset, table)
            else:
                raise Exception(
                    "BigQueryIOManager only supports pandas DataFrames"
                )
        else:
            metadata = {"rows": 0}

        context.add_output_metadata(
            dict(
                query=self._get_select_statement(
                    table,
                    dataset,
                    partition_type,
                    time_window,
                    partition_key
                ),
                # query=self._get_select_statement(
                #     table,
                #     dataset,
                #     None,
                #     time_window,
                # ),
                **metadata,
            )
        )


    def _handle_pandas_output(
        self, context, obj: PandasDataFrame, dataset: str, table: str
    ) -> Mapping[str, Any]:
        obj = standardise_types(obj)
        # obj.info()
        # ic(obj[['symbol','usd_price']])
        # use the google-cloud-bigquery library to write the dataframe to bigquery
        # pandas-gbq hits rate limits when writing many small table updates
        
        i = 0
        delay_time = INITIAL_RETRY
        while True:
            try:
                bqjob = self.client.load_table_from_dataframe(obj, f'{dataset}.{table}', job_config=LoadJobConfig(write_disposition='WRITE_APPEND'))
                bqjob.result()
                break
            except Exception as e:
                if i > MAX_RETRIES:
                    raise e
                context.log.warning(f"Retry {i} load_table_from_dataframe after {delay_time} seconds")
                sleep(delay_time)
                i += 1
                delay_time *= 2


        # obj.to_gbq(destination_table = f'{dataset}.{table}', if_exists = 'append', progress_bar = False, )

        return {
            "rows": obj.shape[0],
            "dataframe_columns": MetadataValue.table_schema(
                TableSchema(
                    columns=[
                        TableColumn(name=name, type=str(dtype))
                        for name, dtype in obj.dtypes.items() 
                    ]
                )
            ),
        }


    def _get_cleanup_statement(
        self, 
        table: str,
        dataset: str,
        partition_type: Optional[str] = None,
        time_window: Optional[Tuple[datetime, datetime]] = None,
        partition_key: Optional[str] = None,
    ) -> str:
        """
        Returns a SQL statement that deletes data in the given table to make way for the output data
        being written.
        """
        # ic(time_window)
        if partition_type == "time_window":
            return f"DELETE FROM {dataset}.{table} {self._time_window_where_clause(time_window)}"
        elif partition_type == "multi":
            return f"DELETE FROM {dataset}.{table} WHERE _dagster_partition_key = '{partition_key}'"
        else:
            return f"DELETE FROM {dataset}.{table} WHERE 1=1"

    def load_input(self, context: InputContext) -> PandasDataFrame:
        # asset_key = context.asset_key
        dataset = self._config["dataset"]
        table = context.asset_key.path[-1]  # type: ignore

        # context.has_partition_key / context.partition_key are the run partition key which is the ouput
        # context.has_asset_partitions / context.asset_partitions_def are the asset partition key which is the input

        # set the partition mode and associated vars based on the partition type
        if context.has_asset_partitions and context.has_partition_key:
            if isinstance(context.asset_partitions_def, TimeWindowPartitionsDefinition):
                partition_type = "time_window"
                partition_key = context.asset_partition_key
                time_window = context.asset_partitions_time_window
            elif isinstance(context.asset_partitions_def, MultiPartitionsDefinition): 
                partition_type = "multi"
                partition_key = context.asset_partition_key
                time_window = None
            else:
                raise ValueError("Unsupported partition type")
        else:
            partition_type = None
            partition_key = None
            time_window = None

        sql=self._get_select_statement(
                    table,
                    dataset,
                    partition_type,
                    time_window,
                    partition_key
        )
        # ic(sql)
        # context.log.info(f"query: {sql}")
        try:
            result = pd.read_gbq(sql, dialect='standard', use_bqstorage_api=True)
        except pandas_gbq.exceptions.GenericGBQException as err:
            # skip error if the table does not exist
            if "Reason: 404 Not found: Table" in str(err):
                result = pd.DataFrame()
            else:
                raise pandas_gbq.exceptions.GenericGBQException(err)

        return result


    def _get_select_statement(
        self,
        table: str,
        dataset: str,
        # columns: Optional[Sequence[str]],
        partition_type: Optional[str] = None,
        time_window: Optional[Tuple[datetime, datetime]] = None,
        partition_key: Optional[str] = None,
    ):
        # col_str = ", ".join(columns) if columns else "*"
        excluded_cols = "_dagster_partition_type, _dagster_partition_key, _dagster_partition_time, _dagster_load_timestamp"
        if partition_type == "time_window":
            return f"SELECT * EXCEPT ({excluded_cols}) FROM {dataset}.{table} {self._time_window_where_clause(time_window)}"
        elif partition_type == "multi":
            return f"SELECT * EXCEPT ({excluded_cols}) FROM {dataset}.{table} WHERE _dagster_partition_key = '{partition_key}'"
        else:
            return f"SELECT * EXCEPT ({excluded_cols}) FROM {dataset}.{table} WHERE 1=1"

        if time_window:
            return (
                # f"""SELECT {col_str} FROM {self._config["database"]}.{dataset}.{table}\n"""
                f"""SELECT {col_str} FROM {dataset}.{table}\n"""
                + self._time_window_where_clause(time_window)
            )
        else:
            return f"""SELECT {col_str} FROM {dataset}.{table}"""

    def _time_window_where_clause(self, time_window: Tuple[datetime, datetime]) -> str:
        start_dt, end_dt = time_window
        # return f"""WHERE _dagster_partition_time BETWEEN '{start_dt.strftime(BIGQUERY_DATETIME_FORMAT)}' AND '{end_dt.strftime(BIGQUERY_DATETIME_FORMAT)}'"""
        return f"""WHERE _dagster_partition_time >= '{start_dt.strftime(BIGQUERY_DATETIME_FORMAT)}' AND _dagster_partition_time < '{end_dt.strftime(BIGQUERY_DATETIME_FORMAT)}'"""   

if __name__ == '__main__':
    # initialise_pandas_gbq()
    pass