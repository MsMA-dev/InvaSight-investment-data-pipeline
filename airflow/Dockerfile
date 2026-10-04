FROM apache/airflow:3.3.2

USER airflow

# Pin airflow itself so installing providers can't pull in a different version.
# The Azure/Snowflake providers are only used in PIPELINE_MODE=cloud.
RUN pip install --no-cache-dir \
    apache-airflow==3.3.2 \
    apache-airflow-providers-microsoft-azure \
    apache-airflow-providers-snowflake \
    boto3 \
    duckdb==1.4.4

# dbt gets its own virtualenv so its dependencies can't clash with Airflow's.
# Same duckdb version as above: both processes open the same database file.
RUN python -m venv /home/airflow/dbt-venv && \
    /home/airflow/dbt-venv/bin/pip install --no-cache-dir \
    dbt-core==1.11.* \
    dbt-duckdb==1.11.0 \
    dbt-snowflake==1.11.* \
    duckdb==1.4.4
