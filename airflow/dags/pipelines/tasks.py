"""Operator factories shared by both DAGs, so each DAG file reads as just its flow."""
from datetime import timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

from pipelines.config import PIPELINE_MODE, SNOWFLAKE_CONN_ID, WAREHOUSE_POOL, dbt_build_cmd
from pipelines.warehouse import RAW_LEDGER, copy_json_sql, copy_ledger_sql, load_from_upstream


def load_task(table, upstream_task_id):
    """Load the files landed by `upstream_task_id` into RAW.<table>."""
    task_id = f"load_{table.removesuffix('_raw')}"

    if PIPELINE_MODE == "cloud":
        from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

        sql = copy_ledger_sql(upstream_task_id) if table == RAW_LEDGER else copy_json_sql(table, upstream_task_id)
        return SQLExecuteQueryOperator(
            task_id=task_id,
            conn_id=SNOWFLAKE_CONN_ID,
            sql=sql,
            show_return_value_in_logs=True,
            pool=WAREHOUSE_POOL,
        )

    return PythonOperator(
        task_id=task_id,
        python_callable=load_from_upstream,
        op_kwargs={"table": table, "upstream_task_id": upstream_task_id},
        pool=WAREHOUSE_POOL,
    )


def dbt_build_task(*select_args):
    """dbt build (run + test) for only the models downstream of what just landed."""
    return BashOperator(
        task_id="dbt_build",
        bash_command=dbt_build_cmd(*select_args),
        execution_timeout=timedelta(hours=1),
        pool=WAREHOUSE_POOL,
    )
