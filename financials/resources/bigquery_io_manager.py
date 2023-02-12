import os
import json
import pandas as pd
import numpy as np
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Mapping, Optional, Sequence, Tuple, Union

from pandas import DataFrame as PandasDataFrame
# from pandas import read_sql
# from pyspark.sql import DataFrame as SparkDataFrame
# from snowflake.connector.pandas_tools import pd_writer
# from snowflake.sqlalchemy import URL  # pylint: disable=no-name-in-module,import-error
# from sqlalchemy import create_engine

import pandas_gbq 
from google.oauth2 import service_account
from icecream import ic

from financials.resources.helpers import standardise_types

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


@io_manager(config_schema={
    "project": str,
    "dataset": str,
    "service_account_creds": str,
    "service_account_file": str,
    "use_service_account_file": bool,})
def bigquery_io_manager(init_context):
    return BigQueryIOManager(
        config = {
            "project": init_context.resource_config["project"],
            "dataset": init_context.resource_config["dataset"],
            "service_account_creds": init_context.resource_config["service_account_creds"],
            "service_account_file": init_context.resource_config["service_account_file"],
            "use_service_account_file": init_context.resource_config["use_service_account_file"],
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

    def handle_output(self, context: OutputContext, obj: PandasDataFrame):
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
            # enforced idempotency using delete-write pattern.  Used for partitioned assets, non-partitioned assets use overwrite.
            cleanup_query = self._get_cleanup_statement(table, dataset, partition_type, time_window, partition_key)
            # ic(cleanup_query)
            try:
                pd.read_gbq(cleanup_query, dialect='standard')
            except pandas_gbq.exceptions.GenericGBQException as err:
                # skip error if the table does not exist
                if not "Reason: 404 Not found: Table" in str(err):
                    raise pandas_gbq.exceptions.GenericGBQException(err)


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
                metadata = self._handle_pandas_output(obj, dataset, table)
            # TODO fix this later for dbt
            # elif obj is None:  # dbt
            #     config = dict(SHARED_SNOWFLAKE_CONF)
            #     config["schema"] = schema
            #     with connect_snowflake(config=config) as con:
            #         df = read_sql(f"SELECT * FROM {context.name} LIMIT 5", con=con)
            #         num_rows = con.execute(f"SELECT COUNT(*) FROM {context.name}").fetchone()

            #     metadata = {"data_sample": MetadataValue.md(df.to_markdown()), "rows": num_rows}
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
        self, obj: PandasDataFrame, dataset: str, table: str
    ) -> Mapping[str, Any]:
        obj = standardise_types(obj)
        # obj.info()
        # ic(obj[['symbol','usd_price']])
        obj.to_gbq(destination_table = f'{dataset}.{table}', if_exists = 'append', progress_bar = False)

        return {
            "rows": obj.shape[0],
            "dataframe_columns": MetadataValue.table_schema(
                TableSchema(
                    columns=[
                        TableColumn(name=name, type=str(dtype))
                        for name, dtype in obj.dtypes.iteritems()
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

        sql=self._get_select_statement(
                    table,
                    dataset,
                    partition_type,
                    time_window,
                    partition_key
        )
        # ic(sql)
        
        try:
            result = pd.read_gbq(sql)
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
        return f"""WHERE _dagster_partition_time BETWEEN '{start_dt.strftime(BIGQUERY_DATETIME_FORMAT)}' AND '{end_dt.strftime(BIGQUERY_DATETIME_FORMAT)}'"""

if __name__ == '__main__':
    initialise_pandas_gbq()