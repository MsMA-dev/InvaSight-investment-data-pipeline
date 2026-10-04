"""Runtime settings, all read from the environment (see .env.example).

PIPELINE_MODE switches the storage and warehouse together:
  local  - landing zone on the local filesystem, DuckDB warehouse (default;
           runs anywhere with no cloud accounts)
  cloud  - Azure Blob Storage landing zone, Snowflake warehouse (the original
           deployment)
"""
import os

PIPELINE_MODE = os.getenv("PIPELINE_MODE", "local").lower()
if PIPELINE_MODE not in ("local", "cloud"):
    raise ValueError(f"PIPELINE_MODE must be 'local' or 'cloud', got {PIPELINE_MODE!r}")

# sample: replay the fake API responses in data/sample (no keys needed)
# api:    call the real market data APIs (keys from the environment)
MARKET_DATA_SOURCE = os.getenv("MARKET_DATA_SOURCE", "sample").lower()

DATA_DIR = os.getenv("DATA_DIR", "/opt/airflow/data")
SAMPLE_DIR = os.path.join(DATA_DIR, "sample")
LANDING_DIR = os.path.join(DATA_DIR, "landing")
DUCKDB_PATH = os.getenv("DUCKDB_PATH", os.path.join(DATA_DIR, "warehouse", "invasight.duckdb"))

LEDGER_ROWS = int(os.getenv("LEDGER_ROWS", "1000"))

# Azure Blob (cloud mode)
WASB_CONN_ID = os.getenv("WASB_CONN_ID", "wasb_default")
BLOB_CONTAINER = os.getenv("BLOB_CONTAINER", "landing")

# Snowflake (cloud mode)
SNOWFLAKE_CONN_ID = os.getenv("SNOWFLAKE_CONN_ID", "snowflake_default")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "INVASIGHT")
SNOWFLAKE_STAGE = os.getenv("SNOWFLAKE_STAGE", f"@{SNOWFLAKE_DATABASE}.RAW.LANDING_STAGE")

# One slot: DuckDB allows a single writer, and on Snowflake it stops two dbt
# builds racing on the same tables. Every load and dbt task takes this pool.
WAREHOUSE_POOL = "warehouse_pool"

# dbt lives in its own virtualenv inside the image (see airflow/Dockerfile),
# the project is mounted from the repo's dbt/ folder.
DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/opt/dbt/invasight")
DBT_BIN = os.getenv("DBT_BIN", "/home/airflow/dbt-venv/bin/dbt")
DBT_TARGET = "snowflake" if PIPELINE_MODE == "cloud" else "local"


def dbt_build_cmd(*select_args):
    select = " ".join(select_args)
    return (
        f"cd {DBT_PROJECT_DIR} && {DBT_BIN} deps --quiet"
        f" && {DBT_BIN} build --target {DBT_TARGET} --select {select}"
    )
