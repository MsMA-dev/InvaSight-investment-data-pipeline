"""CI gate: every DAG in airflow/dags must import cleanly inside the built image.

Run inside the image with the dags folder mounted at /opt/airflow/dags.
"""
import sys

from airflow.dag_processing.dagbag import DagBag

EXPECTED_DAGS = {"internal_ledger_dag", "daily_market_data_dag"}

bag = DagBag(dag_folder="/opt/airflow/dags", include_examples=False)

if bag.import_errors:
    for path, err in bag.import_errors.items():
        print(f"IMPORT ERROR in {path}:\n{err}")
    sys.exit(1)

missing = EXPECTED_DAGS - set(bag.dag_ids)
if missing:
    print(f"Missing DAGs: {sorted(missing)}")
    sys.exit(1)

# bag.dags holds the parsed DAGs; get_dag() would query the metadata DB, which CI doesn't have
for dag_id, dag in sorted(bag.dags.items()):
    for task in dag.tasks:
        if task.task_id.startswith(("load_", "dbt_")) and task.pool != "warehouse_pool":
            print(f"{dag_id}.{task.task_id} must run in warehouse_pool")
            sys.exit(1)
    print(f"OK  {dag_id}: {len(dag.tasks)} tasks")
