from dagster import Definitions, load_assets_from_modules

from .assets import bronze_layer, silver_layer, gold_layer
from .resources import mysql, minio, postgres
from .jobs import bronze_layer_job, silver_layer_job, gold_layer_job

all_assets = load_assets_from_modules([bronze_layer, silver_layer, gold_layer])
all_jobs = [bronze_layer_job, silver_layer_job, gold_layer_job]

defs = Definitions(
    assets=all_assets,
    resources={
        "mysql_io_manager": mysql,
        "minio_io_manager": minio,
        "psql_io_manager": postgres
    },
    jobs= all_jobs
)