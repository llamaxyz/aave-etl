# from .repository import aave
import json
import os
import sys
from icecream import ic

from dagster import (
    load_assets_from_modules,
    Definitions,
    define_asset_job,
    AssetSelection,
    schedule,
    build_asset_reconciliation_sensor,
    fs_io_manager, 
    ExperimentalWarning,
    MultiPartitionsDefinition,
    DailyPartitionsDefinition,
    ScheduleDefinition
)
from aave_data.assets.financials import data_lake, data_warehouse
from aave_data.assets.financials.data_lake import market_day_multipartition
from aave_data.resources.bigquery_io_manager import bigquery_io_manager
from aave_data.resources.financials_config import FINANCIAL_PARTITION_START_DATE

from dagster_gcp.gcs.io_manager import gcs_pickle_io_manager
from dagster_gcp.gcs.resources import gcs_resource

from dagster._utils import file_relative_path
from dagster_dbt import dbt_cli_resource, load_assets_from_dbt_project

from google.oauth2 import service_account
from google.cloud import storage



if not sys.warnoptions:
    import warnings
    # warnings.simplefilter("ignore")
    warnings.filterwarnings("ignore", category=ExperimentalWarning)



########################
# dbt config
########################

DBT_PROJECT_DIR = file_relative_path(__file__, "../aave_dbt")
DBT_PROFILES_DIR = file_relative_path(__file__, "../aave_dbt/config")

########################
# logic for dev/prod environments
########################
# check for environment variable DAGSTER_DEPLOYMENT is set to a valid value
if 'DAGSTER_DEPLOYMENT' in os.environ and os.environ['DAGSTER_DEPLOYMENT'] in ['local_filesystem', 'local_cloud', 'prod']:
    dagster_deployment = os.environ['DAGSTER_DEPLOYMENT']
else:
    errmsg = "Environment variable DAGSTER_DEPLOYMENT must be set to either 'local_filesystem', 'local_cloud', or 'prod'"
    raise EnvironmentError(errmsg)

# grab the appropriate service account credentials for the environment
if dagster_deployment == 'local_cloud':
    # creds_env_var = os.environ['AAVE_ETL_DEV_BIGQUERY_SERVICE_ACCOUNT_CREDENTIALS']
    creds_env_var = "not_configured"
    creds_file = '.devcontainer/llama_aave_dev_service_account.json'
elif dagster_deployment == 'prod':
    creds_env_var = os.environ['AAVE_ETL_PROD_BIGQUERY_SERVICE_ACCOUNT_CREDENTIALS']
    creds_file = "not_configured"
else:
    creds_env_var = "local_filesystem_mode"

# configure the resource definitions for each environment option
resource_defs = {
    "local_filesystem": {
        "data_lake_io_manager": fs_io_manager,
        "data_warehouse_io_manager": fs_io_manager,
    },
    "local_cloud": {
        "data_lake_io_manager": bigquery_io_manager.configured(
            {
                "project": "aave-dev",
                "dataset": "financials_data_lake",
                "service_account_creds": creds_env_var,
                "service_account_file" : creds_file,
                "use_service_account_file": True,
            },
        ),
        "data_warehouse_io_manager": bigquery_io_manager.configured(
            {
                "project": "aave-dev",
                "dataset": "warehouse",
                "service_account_creds": creds_env_var,
                "service_account_file" : creds_file,
                "use_service_account_file": True,
            },
        ),
        "datamart_io_manager": bigquery_io_manager.configured(
            {
                "project": "aave-dev",
                "dataset": "datamart",
                "service_account_creds": creds_env_var,
                "service_account_file" : creds_file,
                "use_service_account_file": True,
            },
        ),
        "dbt": dbt_cli_resource.configured(
            {
                "project_dir": DBT_PROJECT_DIR,
                "profiles_dir": DBT_PROFILES_DIR,
                "target": "dev"
            }
        )
    },
    "prod": {
        "data_lake_io_manager": bigquery_io_manager.configured(
            {
                "project": "aave-prod",
                "dataset": "financials_data_lake",
                "service_account_creds": creds_env_var,
                "service_account_file" : creds_file,
                "use_service_account_file": False,
            },
        ),
        "data_warehouse_io_manager": bigquery_io_manager.configured(
            {
                "project": "aave-prod",
                "dataset": "warehouse",
                "service_account_creds": creds_env_var,
                "service_account_file" : creds_file,
                "use_service_account_file": False,
            },
        ),
        "datamart_io_manager": bigquery_io_manager.configured(
            {
                "project": "aave-prod",
                "dataset": "datamart",
                "service_account_creds": creds_env_var,
                "service_account_file" : creds_file,
                "use_service_account_file": False,
            },
        ),
        "dbt": dbt_cli_resource.configured(
            {
                "project_dir": DBT_PROJECT_DIR,
                "profiles_dir": DBT_PROFILES_DIR,
                "target": "prod"
            }
        )
    },
}

############################################
# Assets
############################################


financials_data_lake_assets = load_assets_from_modules(
    modules=[data_lake],
    key_prefix="financials_data_lake",
    group_name="financials_data_lake"
)

warehouse_assets = load_assets_from_modules(
    modules=[data_warehouse],
    key_prefix="warehouse",
    group_name="warehouse"
)


dbt_assets = load_assets_from_dbt_project(
    DBT_PROJECT_DIR,
    io_manager_key="datamart_io_manager",
    # partitions_def=market_day_multipartition
)

############################################
# Jobs
############################################
data_lake_chunk_1 = [
    'financials_data_lake/market_tokens_by_day',
    'financials_data_lake/treasury_accrued_incentives_by_day',
    'financials_data_lake/non_atoken_balances_by_day',
    'financials_data_lake/non_atoken_transfers_by_day',
    'financials_data_lake/user_lm_rewards_claimed',
    ]
data_lake_chunk_2 = [
    'financials_data_lake/aave_oracle_prices_by_day',
    'financials_data_lake/collector_atoken_balances_by_day',
    'financials_data_lake/collector_atoken_transfers_by_day',
    'financials_data_lake/v3_accrued_fees_by_day',
    'financials_data_lake/v3_minted_to_treasury_by_day',

    ]
data_lake_chunk_3 = [
    'financials_data_lake/tx_classification',
    'financials_data_lake/display_names',
    'financials_data_lake/internal_external_addresses',
    'financials_data_lake/balance_group_lists',
    'financials_data_lake/streams_metadata',
    ]
# data_lake_chunk_4 = [
#     'financials_data_lake/tx_classification',
#     'financials_data_lake/display_names',
#     'financials_data_lake/internal_external_addresses'
#     'financials_data_lake/balance_group_lists'
# ]
# unpartitioned_data_lake_assets = [
#     'financials_data_lake/tx_classification',
#     'financials_data_lake/display_names',
#     'financials_data_lake/internal_external_addresses'
# ]

# financials_data_lake_update_job = define_asset_job(
#     name='financials_data_lake_update_job',
#     selection=AssetSelection.groups('financials_data_lake') - AssetSelection.keys(*unpartitioned_data_lake_assets),
#     partitions_def=market_day_multipartition
# )

financials_data_lake_partitioned_job = define_asset_job(
    name='financials_data_lake_partitioned',
    selection=AssetSelection.groups('financials_data_lake') - AssetSelection.keys(*data_lake_chunk_3),
    partitions_def=market_day_multipartition
)

financials_data_lake_unpartitioned_job = define_asset_job(
    name='financials_data_lake_unpartitioned',
    selection=AssetSelection.keys(*data_lake_chunk_3),
    partitions_def=market_day_multipartition
)

warehouse_datamart_job = define_asset_job(
    name='warehouse_datamart',
    selection=AssetSelection.groups('warehouse', 'datamart'),
)

# Updates block_numbers_by_day and thus triggers all downstream jobs via the reconciliation sensor
financials_root_job = define_asset_job(
    name='financials_root_job',
    selection=AssetSelection.keys('financials_data_lake/block_numbers_by_day'),
    partitions_def=market_day_multipartition
)

financials_chunk1_job = define_asset_job(
    name='financials_chunk1_job',
    selection=AssetSelection.keys(*data_lake_chunk_1),
    partitions_def=market_day_multipartition
)

financials_chunk2_job = define_asset_job(
    name='financials_chunk2_job',
    selection=AssetSelection.keys(*data_lake_chunk_2),
    partitions_def=market_day_multipartition
)

financials_chunk3_job = define_asset_job(
    name='financials_chunk3_job',
    selection=AssetSelection.keys(*data_lake_chunk_3),
    partitions_def=market_day_multipartition
)

############################################
# Schedules
############################################

daily_partition = DailyPartitionsDefinition(start_date=FINANCIAL_PARTITION_START_DATE)

def get_multipartition_keys_with_dimension_value(
    partition_def: MultiPartitionsDefinition, 
    dimension_values: dict,
):
    """ Helper function for schedule
        Returns a list of partition keys that match the given dimension values
    """
    matching_keys = []
    for partition_key in partition_def.get_partition_keys():

        date, market = partition_key.split("|")
        if all(
            [
                date == value
                for dimension, value in dimension_values.items()
            ]
        ):
            matching_keys.append(partition_key)
    return matching_keys


@schedule(
    cron_schedule="0 2 * * *",
    job=financials_root_job,
    execution_timezone='UTC',
    name="financials_root_schedule",
)
def financials_root_schedule(context):
    time_partitions = daily_partition.get_partition_keys(context.scheduled_execution_time)

    # Run for the latest time partition. Prior partitions will have been handled by prior ticks.
    curr_date = time_partitions[-1]
    context.log.info(f"Constructing run schedule for current date: {curr_date}")
    for multipartition_key in get_multipartition_keys_with_dimension_value(
        market_day_multipartition, {"date": curr_date}
    ):
        context.log.info(f"Generating run request for partition {multipartition_key}")
        yield financials_root_job.run_request_for_partition(
            partition_key=multipartition_key,
            run_key=multipartition_key,
        )

@schedule(
    cron_schedule="0 2 * * *",
    job=financials_data_lake_partitioned_job,
    execution_timezone='UTC',
    name="financials_data_lake_partitioned_schedule",
)
def financials_data_lake_partitioned_schedule(context):
    time_partitions = daily_partition.get_partition_keys(context.scheduled_execution_time)

    # Run for the latest time partition. Prior partitions will have been handled by prior ticks.
    curr_date = time_partitions[-1]
    context.log.info(f"Constructing run schedule for current date: {curr_date}")
    for multipartition_key in get_multipartition_keys_with_dimension_value(
        market_day_multipartition, {"date": curr_date}
    ):
        context.log.info(f"Generating run request for partition {multipartition_key}")
        yield financials_data_lake_partitioned_job.run_request_for_partition(
            partition_key=multipartition_key,
            run_key=multipartition_key,
        )

financials_data_lake_unpartitioned_schedule = ScheduleDefinition(
    job = financials_data_lake_unpartitioned_job,
    cron_schedule="0 2 * * *",
    execution_timezone='UTC',
    name="financials_data_lake_unpartitioned_schedule"
    )

warehouse_datamart_schedule = ScheduleDefinition(
    job = warehouse_datamart_job,
    cron_schedule="30 2 * * *",
    execution_timezone='UTC',
    name="warehouse_datamart_schedule"
    )

####################################################
# Sensor Code - not working pending sensor performance improvements
############################################
#
# # break assets up into chunks to avoid sensor timeout
# data_lake_chunk_1 = [
#     'financials_data_lake/market_tokens_by_day',
#     # 'financials_data_lake/treasury_accrued_incentives_by_day',
#     # 'financials_data_lake/non_atoken_balances_by_day',
#     # 'financials_data_lake/non_atoken_transfers_by_day',
#     # 'financials_data_lake/user_lm_rewards_claimed',
#     ]
# data_lake_chunk_2 = [
#     'financials_data_lake/aave_oracle_prices_by_day',
#     'financials_data_lake/collector_atoken_balances_by_day',
#     'financials_data_lake/collector_atoken_transfers_by_day',
#     'financials_data_lake/v3_accrued_fees_by_day',
#     'financials_data_lake/v3_minted_to_treasury_by_day',
#     'financials_data_lake/tx_classification',
#     'financials_data_lake/display_names',
#     'financials_data_lake/internal_external_addresses'
#     ]
# data_lake_chunk_3 = [
#     'financials_data_lake/aave_oracle_prices_by_day',
#     'financials_data_lake/collector_atoken_balances_by_day',
#     'financials_data_lake/collector_atoken_transfers_by_day',
#     'financials_data_lake/v3_accrued_fees_by_day',
#     'financials_data_lake/v3_minted_to_treasury_by_day',
#     ]
# data_lake_chunk_4 = [
#     'financials_data_lake/tx_classification',
#     'financials_data_lake/display_names',
#     'financials_data_lake/internal_external_addresses'
# ]

financials_data_lake_sensor = build_asset_reconciliation_sensor(
    name="financials_data_lake_sensor",
    asset_selection=AssetSelection.groups('financials_data_lake') - AssetSelection.keys('financials_data_lake/block_numbers_by_day'),# - AssetSelection.assets(*dbt_assets),
    minimum_interval_seconds=60*3
)

financials_warehouse_sensor = build_asset_reconciliation_sensor(
    name="financials_warehouse_sensor",
    asset_selection=AssetSelection.groups('warehouse'),
    minimum_interval_seconds=60*3
)

dbt_sensor = build_asset_reconciliation_sensor(
    name="dbt_sensor",
    asset_selection=AssetSelection.assets(*dbt_assets),
    minimum_interval_seconds=60*3
)
# # both_list = minimal_list.append(exclude_list)
minimal_sensor = build_asset_reconciliation_sensor(
    name="minimal_sensor",
    asset_selection=AssetSelection.keys('financials_data_lake/market_tokens_by_day'),
    # asset_selection=AssetSelection.groups('financials_data_lake') - AssetSelection.keys(*exclude_list) - AssetSelection.keys('financials_data_lake/block_numbers_by_day'),
    # asset_selection=AssetSelection.groups('financials_data_lake'),
    minimum_interval_seconds=60*3
)
# financials_sensor = build_asset_reconciliation_sensor(
#     name="financials_sensor",
#     # asset_selection=AssetSelection.keys(*data_lake_chunk_1),
#     asset_selection=AssetSelection.all() - AssetSelection.keys('financials_data_lake/block_numbers_by_day'),
#     minimum_interval_seconds=60*3
# )
#####################################################################


defs = Definitions(
    assets=[*financials_data_lake_assets, *warehouse_assets, *dbt_assets],
    jobs=[
          financials_root_job,
          financials_chunk1_job,
          financials_chunk2_job,
          financials_chunk3_job,
          financials_data_lake_unpartitioned_job,
          financials_data_lake_partitioned_job,
          warehouse_datamart_job
          ],
    schedules = [
        financials_root_schedule,
        warehouse_datamart_schedule,
        financials_data_lake_unpartitioned_schedule,
        financials_data_lake_partitioned_schedule
        ],
    sensors=[financials_data_lake_sensor, financials_warehouse_sensor, dbt_sensor, minimal_sensor],
    resources=resource_defs[dagster_deployment],
)




# from dagster import  build_sensor_context, DagsterInstance
# if __name__ == "__main__":

#     # you may need to change this line to get your prod dagster instance
#     with DagsterInstance.get() as instance:
#         sensor = build_asset_reconciliation_sensor(AssetSelection.groups('financials_data_lake') - AssetSelection.keys('financials_data_lake/block_numbers_by_day'))
#         cursor = sensor.evaluate_tick(
#             build_sensor_context(
#                 instance=instance,
#                 repository_def=defs,
#             )
#         )
    # context = build_schedule_context()
    # pass
    # schedules = schedule_def(context)
    # print(schedule_def)