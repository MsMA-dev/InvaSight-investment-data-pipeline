"""Market data: daily, check APIs -> fetch FX / metals / equities in parallel
-> land -> load RAW -> one dbt build for everything downstream."""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from pipelines.market_data import (
    check_api_availability,
    fetch_daily_equity_prices,
    fetch_exchange_rates,
    fetch_metal_prices,
)
from pipelines.tasks import dbt_build_task, load_task
from pipelines.warehouse import RAW_EQUITY, RAW_FX, RAW_METALS


with DAG(
    "daily_market_data_dag",
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    start_date=datetime(2026, 9, 1),
    schedule="0 23 * * *",  # after US market close
    catchup=False,
    max_active_runs=1,
    tags=["market-data", "ingestion"],
) as dag:

    check_api_availability_task = PythonOperator(
        task_id="check_api_availability",
        python_callable=check_api_availability,
    )

    fetch_exchange_rates_task = PythonOperator(
        task_id="fetch_exchange_rates",
        python_callable=fetch_exchange_rates,
    )
    fetch_metal_prices_task = PythonOperator(
        task_id="fetch_metal_prices",
        python_callable=fetch_metal_prices,
    )
    fetch_daily_equity_prices_task = PythonOperator(
        task_id="fetch_daily_equity_prices",
        python_callable=fetch_daily_equity_prices,
    )

    load_exchange_rates = load_task(RAW_FX, "fetch_exchange_rates")
    load_metal_prices = load_task(RAW_METALS, "fetch_metal_prices")
    load_equity_prices = load_task(RAW_EQUITY, "fetch_daily_equity_prices")

    dbt_build = dbt_build_task(
        "source:raw.fx_rates_raw+",
        "source:raw.metal_prices_raw+",
        "source:raw.equity_prices_raw+",
    )

    check_api_availability_task >> [
        fetch_exchange_rates_task,
        fetch_metal_prices_task,
        fetch_daily_equity_prices_task,
    ]

    fetch_exchange_rates_task >> load_exchange_rates
    fetch_metal_prices_task >> load_metal_prices
    fetch_daily_equity_prices_task >> load_equity_prices

    [load_exchange_rates, load_metal_prices, load_equity_prices] >> dbt_build
