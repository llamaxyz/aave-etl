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

from dagster_gcp.gcs.io_manager import gcs_pickle_io_manager
from dagster_gcp.gcs.resources import gcs_resource

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
    asset_selection=AssetSelection.all(),

)


if 'DAGSTER_DEPLOYMENT' in os.environ:
    dagster_deployment = os.environ['DAGSTER_DEPLOYMENT']
else:
    errmsg = "Environment variable DAGSTER_DEPLOYMENT must be set to either 'local_filesystem', 'local_cloud', or 'prod'"
    raise EnvironmentError(errmsg)

if dagster_deployment == 'local_filesystem':
    # dev on local machine, files stored on local filesystem
    resource_defs = {
        "data_lake_io_manager": fs_io_manager,
    }
elif dagster_deployment == 'local_cloud':
    # dev on local machine, files stored in GCS dev bucket and BQ dev tables
    gcs_credentials = service_account.Credentials.from_service_account_info(json.loads(os.environ['AAVE_ETL_DEV_BIGQUERY_SERVICE_ACCOUNT_CREDENTIALS']))
    storage_client = storage.Client(credentials=gcs_credentials)
    resource_defs = {
        "data_lake_io_manager": gcs_pickle_io_manager.configured(
            {
                "gcs_bucket": "llama_aave_dev_datalake",
                "gcs_prefix": "financials"
            }
        ),
        "gcs": ResourceDefinition.hardcoded_resource(storage_client)
    }
elif dagster_deployment == 'prod':
    # running on dagster cloud in prod GCS bucket and BQ env
    gcs_credentials = service_account.Credentials.from_service_account_info(json.loads(os.environ['AAVE_ETL_PROD_BIGQUERY_SERVICE_ACCOUNT_CREDENTIALS']))
    storage_client = storage.Client(credentials=gcs_credentials)
    resource_defs = {
        "data_lake_io_manager": gcs_pickle_io_manager.configured(
            {
                "gcs_bucket": "llama_aave_prod_datalake",
                "gcs_prefix": "financials"
            }
        ),
        "gcs": ResourceDefinition.hardcoded_resource(storage_client)
    }
else:
    errmsg = "Environment variable DAGSTER_DEPLOYMENT must be set to either 'local_filesystem', 'local_cloud', or 'prod'"
    raise EnvironmentError(errmsg)


    
# financials_update_job_schedule = ScheduleDefinition(
#     name="financials_update_job_schedule", job=financials_update_job, cron_schedule="6 * * * *"
# )

# todo need to figure out how to define a partitoned schedule
# financials_update_job_schedule = build_schedule_from_partitioned_job(financials_update_job)


defs = Definitions(
    assets=financial_assets,
    # schedules=[financials_update_job_schedule]
    jobs=[financials_update_job],
    sensors=[financials_update_sensor],
    resources=resource_defs,
)