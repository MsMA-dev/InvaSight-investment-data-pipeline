"""Internal ledger: every 45 minutes, generate a batch -> land -> load RAW -> dbt build."""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from pipelines.ledger import generate_internal_ledger
from pipelines.tasks import dbt_build_task, load_task
from pipelines.warehouse import RAW_LEDGER


with DAG(
    "internal_ledger_dag",
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    start_date=datetime(2026, 9, 1),
    schedule=timedelta(minutes=45),
    catchup=False,
    max_active_runs=1,
    tags=["ledger", "ingestion"],
) as dag:

    generate_internal_ledger_task = PythonOperator(
        task_id="generate_internal_ledger",
        python_callable=generate_internal_ledger,
    )

    load_ledger = load_task(RAW_LEDGER, "generate_internal_ledger")

    dbt_build = dbt_build_task("source:raw.ledger_transactions_raw+")

    generate_internal_ledger_task >> load_ledger >> dbt_build
