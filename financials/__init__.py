# from .repository import aave
import json
import os
from dagster import (
    load_assets_from_modules,
    Definitions,
    define_asset_job,
    AssetSelection,
    ScheduleDefinition,
    PartitionScheduleDefinition,
    build_schedule_from_partitioned_job,
    build_asset_reconciliation_sensor,
    fs_io_manager,
    ResourceDefinition
)
from financials.assets import data_lake, data_warehouse
from financials.assets.data_lake import market_day_multipartition
from financials.resources.bigquery_io_manager import bigquery_io_manager

from dagster_gcp.gcs.io_manager import gcs_pickle_io_manager
from dagster_gcp.gcs.resources import gcs_resource

from dagster._utils import file_relative_path
from dagster_dbt import dbt_cli_resource, load_assets_from_dbt_project

from google.oauth2 import service_account
from google.cloud import storage


financial_assets = load_assets_from_modules(
    # modules=[financials]
    modules=[data_lake, data_warehouse]
    # modules=[data_lake_minimal]
)

# financials_update_job = define_asset_job(
#     name='financials_update_job',
#     selection=AssetSelection.keys('block_numbers_by_day')
# )

financials_update_job = define_asset_job(
    name='financials_update_job',
    selection=AssetSelection.keys('block_numbers_by_day'),
    partitions_def=market_day_multipartition
)

financials_update_sensor = build_asset_reconciliation_sensor(
    name="financials_update_sensor",
    asset_selection=AssetSelection.all() - AssetSelection.keys('block_numbers_by_day'),
    minimum_interval_seconds=60*3
)

########################
# dbt config
########################

DBT_PROJECT_DIR = file_relative_path(__file__, "../dbt_financials")
DBT_PROFILES_DIR = file_relative_path(__file__, "../dbt_financials/config")

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
        "dbt": dbt_cli_resource.configured({ "project_dir": DBT_PROJECT_DIR, "profiles_dir": DBT_PROFILES_DIR, "target": "dev"})
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
        "dbt": dbt_cli_resource.configured({ "project_dir": DBT_PROJECT_DIR, "profiles_dir": DBT_PROFILES_DIR, "target": "prod"})
    },
}



dbt_assets = load_assets_from_dbt_project(
    DBT_PROJECT_DIR,
    io_manager_key="datamart_io_manager"
)

####################
# config for data lake in gcs storage buckets, not used
####################

# if dagster_deployment == 'local_filesystem':
#     # dev on local machine, files stored on local filesystem
#     resource_defs = {
#         "data_lake_io_manager": fs_io_manager,
#     }
# elif dagster_deployment == 'local_cloud':
#     # dev on local machine, files stored in GCS dev bucket and BQ dev tables
#     service_account_creds = os.environ['AAVE_ETL_DEV_BIGQUERY_SERVICE_ACCOUNT_CREDENTIALS']
#     gcs_credentials = service_account.Credentials.from_service_account_info(json.loads(service_account_creds))
#     storage_client = storage.Client(credentials=gcs_credentials)
#     resource_defs = {
#         "data_lake_io_manager": gcs_pickle_io_manager.configured(
#             {
#                 "gcs_bucket": "llama_aave_dev_datalake",
#                 "gcs_prefix": "financials"
#             }
#         ),
#         "data_lake_io_manager": gcs_pickle_io_manager.configured(
#             {
#                 "gcs_bucket": "llama_aave_dev_datalake",
#                 "gcs_prefix": "financials"
#             }
#         ),
#         "gcs": ResourceDefinition.hardcoded_resource(storage_client)
#     }
# elif dagster_deployment == 'prod':
#     # running on dagster cloud in prod GCS bucket and BQ env
#     service_account_creds = os.environ['AAVE_ETL_PROD_BIGQUERY_SERVICE_ACCOUNT_CREDENTIALS']
#     gcs_credentials = service_account.Credentials.from_service_account_info(json.loads(service_account_creds))
#     storage_client = storage.Client(credentials=gcs_credentials)
#     resource_defs = {
#         "data_lake_io_manager": gcs_pickle_io_manager.configured(
#             {
#                 "gcs_bucket": "llama_aave_prod_datalake",
#                 "gcs_prefix": "financials"
#             }
#         ),
#         "data_lake_io_manager": gcs_pickle_io_manager.configured(
#             {
#                 "gcs_bucket": "llama_aave_dev_datalake",
#                 "gcs_prefix": "financials"
#             }
#         ),
#         "gcs": ResourceDefinition.hardcoded_resource(storage_client)
#     }
# else:
#     errmsg = "Environment variable DAGSTER_DEPLOYMENT must be set to either 'local_filesystem', 'local_cloud', or 'prod'"
#     raise EnvironmentError(errmsg)


    
# financials_update_job_schedule = ScheduleDefinition(
#     name="financials_update_job_schedule", job=financials_update_job, cron_schedule="6 * * * *"
# )

# todo need to figure out how to define a partitoned schedule
# financials_update_job_schedule = build_schedule_from_partitioned_job(financials_update_job)


defs = Definitions(
    assets=[*financial_assets, *dbt_assets],
    # schedules=[financials_update_job_schedule]
    jobs=[financials_update_job],
    sensors=[financials_update_sensor],
    resources=resource_defs[dagster_deployment],
)