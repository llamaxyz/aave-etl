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
    ScheduleDefinition,
    build_schedule_from_partitioned_job
)
from aave_data.assets.financials import data_lake, data_warehouse
from aave_data.assets.protocol import protocol_data_lake, protocol_data_warehouse, protocol_hourly_data_lake
from aave_data.assets.financials.data_lake import market_day_multipartition
from aave_data.resources.bigquery_io_manager import bigquery_io_manager
from aave_data.resources.financials_config import FINANCIAL_PARTITION_START_DATE
from aave_data.assets.protocol.protocol_data_lake import daily_partitions_def, chain_day_multipartition
from aave_data.assets.protocol.protocol_hourly_data_lake import market_hour_multipartition, HOURLY_PARTITION_START_DATE



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
                "append_only": False,
            },
        ),
        "protocol_data_lake_io_manager": bigquery_io_manager.configured(
            {
                "project": "aave-dev",
                "dataset": "protocol_data_lake",
                "service_account_creds": creds_env_var,
                "service_account_file" : creds_file,
                "use_service_account_file": True,
                "append_only": False,
            },
        ),
        "protocol_data_lake_append_io_manager": bigquery_io_manager.configured(
            {
                "project": "aave-dev",
                "dataset": "protocol_data_lake",
                "service_account_creds": creds_env_var,
                "service_account_file" : creds_file,
                "use_service_account_file": True,
                "append_only": True,
            },
        ),
        "data_warehouse_io_manager": bigquery_io_manager.configured(
            {
                "project": "aave-dev",
                "dataset": "warehouse",
                "service_account_creds": creds_env_var,
                "service_account_file" : creds_file,
                "use_service_account_file": True,
                "append_only": False,
            },
        ),
        "datamart_io_manager": bigquery_io_manager.configured(
            {
                "project": "aave-dev",
                "dataset": "datamart",
                "service_account_creds": creds_env_var,
                "service_account_file" : creds_file,
                "use_service_account_file": True,
                "append_only": False,
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
                "append_only": False,
            },
        ),
        "protocol_data_lake_io_manager": bigquery_io_manager.configured(
            {
                "project": "aave-prod",
                "dataset": "protocol_data_lake",
                "service_account_creds": creds_env_var,
                "service_account_file" : creds_file,
                "use_service_account_file": False,
                "append_only": False,
            },
        ),
        "protocol_data_lake_append_io_manager": bigquery_io_manager.configured(
            {
                "project": "aave-prod",
                "dataset": "protocol_data_lake",
                "service_account_creds": creds_env_var,
                "service_account_file" : creds_file,
                "use_service_account_file": False,
                "append_only": True,
            },
        ),
        "data_warehouse_io_manager": bigquery_io_manager.configured(
            {
                "project": "aave-prod",
                "dataset": "warehouse",
                "service_account_creds": creds_env_var,
                "service_account_file" : creds_file,
                "use_service_account_file": False,
                "append_only": False,
            },
        ),
        "datamart_io_manager": bigquery_io_manager.configured(
            {
                "project": "aave-prod",
                "dataset": "datamart",
                "service_account_creds": creds_env_var,
                "service_account_file" : creds_file,
                "use_service_account_file": False,
                "append_only": False,
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

protocol_data_lake_assets = load_assets_from_modules(
    modules=[protocol_data_lake],
    key_prefix="protocol_data_lake",
    group_name="protocol_data_lake"
)

protocol_hourly_data_lake_assets = load_assets_from_modules(
    modules=[protocol_hourly_data_lake],
    key_prefix="protocol_hourly_data_lake",
    group_name="protocol_hourly_data_lake"
)

warehouse_assets = load_assets_from_modules(
    modules=[data_warehouse, protocol_data_warehouse],
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

data_lake_unpartitioned_assets = [
    'financials_data_lake/tx_classification',
    'financials_data_lake/display_names',
    'financials_data_lake/internal_external_addresses',
    'financials_data_lake/balance_group_lists',
    'financials_data_lake/streams_metadata',
    'financials_data_lake/streaming_payments_state',
    'protocol_data_lake/coingecko_data_by_day',
    ]

daily_partitioned_assets = [
    'protocol_data_lake/matic_lsd_token_supply_by_day',
    'protocol_data_lake/safety_module_rpc',
    ]

daily_midday_partitioned_assets = [
    'protocol_data_lake/beacon_chain_staking_returns_by_day',
]

liquidity_depth_assets = [
    'protocol_data_lake/liquidity_depth_raw',
    'warehouse/liquidity_depth',
]

chain_day_partitioned_assets = [
    'protocol_data_lake/balancer_bpt_data_by_day',
]

datamart_hourly_assets = [
    'market_config_by_hour',
    'market_state_by_hour',
    'market_config_by_time',
    'market_state_by_time',
]



data_lake_partitioned_job = define_asset_job(
    name='data_lake_partitioned',
    selection= (
            AssetSelection.groups('financials_data_lake', 'protocol_data_lake')
            - AssetSelection.keys(*data_lake_unpartitioned_assets)
            - AssetSelection.keys(*daily_partitioned_assets)
            - AssetSelection.keys(*daily_midday_partitioned_assets)
            - AssetSelection.keys(*liquidity_depth_assets)
            - AssetSelection.keys(*chain_day_partitioned_assets)
    ),
    partitions_def=market_day_multipartition
)


data_lake_unpartitioned_job = define_asset_job(
    name='data_lake_unpartitioned',
    selection=AssetSelection.keys(*data_lake_unpartitioned_assets),
    # partitions_def=market_day_multipartition
)

warehouse_datamart_job = define_asset_job(
    name='warehouse_datamart',
    selection=AssetSelection.groups('warehouse', 'datamart') - AssetSelection.keys(*liquidity_depth_assets),
)

daily_partitioned_job = define_asset_job(
    name='daily_partitioned',
    selection=AssetSelection.keys(*daily_partitioned_assets),
    partitions_def=daily_partitions_def
)

daily_midday_partitioned_job = define_asset_job(
    name='daily_midday_partitioned',
    selection=AssetSelection.keys(*daily_midday_partitioned_assets),
    partitions_def=daily_partitions_def
)

chain_day_partitioned_job = define_asset_job(
    name='chain_day_partitioned',
    selection=AssetSelection.keys(*chain_day_partitioned_assets),
    partitions_def=chain_day_multipartition
)

# Updates block_numbers_by_day and thus triggers all downstream jobs via the reconciliation sensor
financials_root_job = define_asset_job(
    name='financials_root_job',
    selection=AssetSelection.keys('financials_data_lake/block_numbers_by_day'),
    partitions_def=market_day_multipartition
)

# these assets use a different schedule and aren't partitioned
liquidity_depth_job = define_asset_job(
    name='liquidity_depth_job',
    selection=AssetSelection.keys(*liquidity_depth_assets, 'liquidity_depth_lsd'),
)

data_lake_hourly_partitioned_job = define_asset_job(
    name='data_lake_hourly_partitioned',
    selection= (
            AssetSelection.groups('protocol_hourly_data_lake')
    ),
    partitions_def=market_hour_multipartition
)


datamart_hourly_job = define_asset_job(
    name='datamart_hourly',
    selection= (
            AssetSelection.keys(*datamart_hourly_assets)
    ),
)

############################################
# Schedules
############################################

data_lake_partitioned_schedule = build_schedule_from_partitioned_job(
    job=data_lake_partitioned_job,
    minute_of_hour=0,
    hour_of_day=1,
    name="data_lake_partitioned_schedule",
)

data_lake_unpartitioned_schedule = ScheduleDefinition(
    job = data_lake_unpartitioned_job,
    cron_schedule="0 1 * * *",
    execution_timezone='UTC',
    name="data_lake_unpartitioned_schedule"
    )

warehouse_datamart_schedule = ScheduleDefinition(
    job = warehouse_datamart_job,
    cron_schedule=["15 1 * * *", "30 1 * * *" ],
    execution_timezone='UTC',
    name="warehouse_datamart_schedule"
    )

daily_partitioned_schedule = build_schedule_from_partitioned_job(
    job=daily_partitioned_job,
    minute_of_hour=25,
    hour_of_day=1,
    name="daily_partitioned_schedule",
)

daily_midday_partitioned_schedule = build_schedule_from_partitioned_job(
    job=daily_midday_partitioned_job,
    minute_of_hour=20,
    hour_of_day=12,
    name="daily_midday_partitioned_schedule",
)

liquidity_depth_schedule = ScheduleDefinition(
    job = liquidity_depth_job,
    cron_schedule="0 */2 * * *",
    execution_timezone='UTC',
    name="liquidity_depth_schedule"
)

chain_day_partitioned_schedule = build_schedule_from_partitioned_job(
    job=chain_day_partitioned_job,
    hour_of_day=1,
    minute_of_hour=25,
    name="chain_day_partitioned_schedule",
)

data_lake_hourly_partitioned_schedule = build_schedule_from_partitioned_job(
    job=data_lake_hourly_partitioned_job,
    minute_of_hour=5,
    name="data_lake_hourly_partitioned_schedule",
)

datamart_hourly_schedule = ScheduleDefinition(
    job=datamart_hourly_job,
    cron_schedule="10 * * * *",
    execution_timezone='UTC',
    name="datamart_hourly_schedule",
)

############################################
# Definitions
############################################

defs = Definitions(
    assets=[
        *financials_data_lake_assets, 
        *protocol_data_lake_assets, 
        *protocol_hourly_data_lake_assets, 
        *warehouse_assets, 
        *dbt_assets,
        ],
    jobs=[
          data_lake_unpartitioned_job,
          data_lake_partitioned_job,
          warehouse_datamart_job,
          daily_partitioned_job,
          daily_midday_partitioned_job,
          chain_day_partitioned_job,
          data_lake_hourly_partitioned_job,
          datamart_hourly_job
          ],
    schedules = [
        warehouse_datamart_schedule,
        data_lake_unpartitioned_schedule,
        data_lake_partitioned_schedule,
        daily_partitioned_schedule,
        daily_midday_partitioned_schedule,
        liquidity_depth_schedule,
        chain_day_partitioned_schedule,
        data_lake_hourly_partitioned_schedule,
        datamart_hourly_schedule,
        ],
    resources=resource_defs[dagster_deployment],
)



