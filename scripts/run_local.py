"""Run the whole pipeline once without Airflow: the same task functions the
DAGs call, in DAG order, then dbt build. Used by CI and for a quick check.

    pip install -r requirements-dev.txt
    python scripts/run_local.py

Writes to data/landing/ and data/warehouse/invasight.duckdb.
"""
import os
import subprocess
import sys
from datetime import datetime, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(ROOT, "data")
DUCKDB_PATH = os.path.join(DATA_DIR, "warehouse", "invasight.duckdb")

os.environ.setdefault("PIPELINE_MODE", "local")
os.environ.setdefault("MARKET_DATA_SOURCE", "sample")
os.environ["DATA_DIR"] = DATA_DIR
os.environ["DUCKDB_PATH"] = DUCKDB_PATH
sys.path.insert(0, os.path.join(ROOT, "airflow", "dags"))

from pipelines import ledger, market_data  # noqa: E402
from pipelines.warehouse import RAW_EQUITY, RAW_FX, RAW_LEDGER, RAW_METALS, load_files_duckdb  # noqa: E402


def step(name):
    print(f"\n=== {name}")


def main():
    now = datetime.now(timezone.utc)
    ds = now.strftime("%Y-%m-%d")

    step("daily_market_data_dag")
    market_data.check_api_availability()
    load_files_duckdb(RAW_FX, market_data.fetch_exchange_rates(ds=ds))
    load_files_duckdb(RAW_METALS, market_data.fetch_metal_prices(ds=ds))
    load_files_duckdb(RAW_EQUITY, market_data.fetch_daily_equity_prices(ds=ds))

    step("internal_ledger_dag")
    load_files_duckdb(RAW_LEDGER, ledger.generate_internal_ledger(data_interval_start=now))

    step("dbt build")
    dbt_dir = os.path.join(ROOT, "dbt", "invasight")
    env = {**os.environ, "DBT_PROFILES_DIR": dbt_dir, "DBT_TARGET": "local"}
    dbt = os.getenv("DBT_BIN", "dbt")
    subprocess.run([dbt, "deps", "--quiet"], cwd=dbt_dir, env=env, check=True)
    subprocess.run([dbt, "build"], cwd=dbt_dir, env=env, check=True)


if __name__ == "__main__":
    main()
