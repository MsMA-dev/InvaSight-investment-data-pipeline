"""Load landed files into the warehouse RAW schema.

Both backends keep the same contract:
  * RAW stores data as delivered: JSON payloads whole, ledger CSV columns as text
  * a file is loaded at most once, so retries and re-runs never duplicate rows
    (Snowflake's COPY load history; a _load_history table in DuckDB)
  * a bad file fails the task, so dbt never builds on partial data
"""
import json
import os

from pipelines.config import DUCKDB_PATH, SNOWFLAKE_DATABASE, SNOWFLAKE_STAGE
from pipelines.landing import local_path

RAW_LEDGER = "ledger_transactions_raw"
RAW_FX = "fx_rates_raw"
RAW_METALS = "metal_prices_raw"
RAW_EQUITY = "equity_prices_raw"
JSON_TABLES = (RAW_FX, RAW_METALS, RAW_EQUITY)

# Same order as ledger.LEDGER_COLUMNS minus the trailing "total", which is
# derivable and not kept in RAW
LEDGER_RAW_COLUMNS = [
    "transaction_id", "client_id", "client_name", "portfolio_id", "ticker",
    "transaction_type", "quantity", "price", "currency", "fee_amount",
    "transaction_date", "transaction_ts",
]


# ---------------------------------------------------------------- DuckDB (local)

DUCKDB_DDL = [
    "create schema if not exists raw",
    *[
        f"""create table if not exists raw.{t} (
            raw json,
            file_name varchar,
            loaded_at timestamp default current_timestamp
        )"""
        for t in JSON_TABLES
    ],
    f"""create table if not exists raw.{RAW_LEDGER} (
        {", ".join(f"{c} varchar" for c in LEDGER_RAW_COLUMNS)},
        batch_id varchar,
        file_name varchar,
        _loaded_at timestamp default current_timestamp
    )""",
    """create table if not exists raw._load_history (
        table_name varchar,
        file_name varchar,
        row_count bigint,
        loaded_at timestamp default current_timestamp,
        primary key (table_name, file_name)
    )""",
]


def _connect():
    import duckdb  # only needed in local mode

    os.makedirs(os.path.dirname(DUCKDB_PATH), exist_ok=True)
    con = duckdb.connect(DUCKDB_PATH)
    for stmt in DUCKDB_DDL:
        con.execute(stmt)
    return con


def _already_loaded(con, table, file_name):
    return con.execute(
        "select 1 from raw._load_history where table_name = ? and file_name = ?",
        [table, file_name],
    ).fetchone() is not None


def load_files_duckdb(table, file_names):
    """Load landed files (relative paths) into raw.<table>, skipping any already loaded."""
    if isinstance(file_names, str):
        file_names = [file_names]

    con = _connect()
    loaded = 0
    try:
        for file_name in file_names:
            if _already_loaded(con, table, file_name):
                print(f"Skip {file_name}: already loaded into raw.{table}")
                continue

            path = local_path(file_name)
            con.execute("begin")
            if table == RAW_LEDGER:
                batch_id = os.path.splitext(os.path.basename(file_name))[0]
                cols = ", ".join(LEDGER_RAW_COLUMNS)
                con.execute(
                    f"""insert into raw.{table} ({cols}, batch_id, file_name)
                        select {cols}, ?, ?
                        from read_csv(?, header = true, all_varchar = true)""",
                    [batch_id, file_name, path],
                )
                rows = con.execute(
                    f"select count(*) from raw.{table} where file_name = ?", [file_name]
                ).fetchone()[0]
            else:
                with open(path, encoding="utf-8") as f:
                    payload = json.load(f)  # fails the task on malformed JSON
                con.execute(
                    f"insert into raw.{table} (raw, file_name) values (?, ?)",
                    [json.dumps(payload), file_name],
                )
                rows = 1
            con.execute(
                "insert into raw._load_history (table_name, file_name, row_count) values (?, ?, ?)",
                [table, file_name, rows],
            )
            con.execute("commit")
            loaded += 1
            print(f"Loaded {file_name} into raw.{table} ({rows} rows)")
    finally:
        con.close()
    print(f"{loaded} new file(s) loaded into raw.{table}")


def load_from_upstream(table, upstream_task_id, ti=None, **_):
    """PythonOperator callable: load whatever the upstream task landed."""
    load_files_duckdb(table, ti.xcom_pull(task_ids=upstream_task_id))


# ------------------------------------------------------------- Snowflake (cloud)

def _uploaded_files(upload_task_id):
    # The upstream task returns the list of paths it landed; Airflow renders
    # this into FILES = ('a', 'b') at run time
    return f"""{{{{ ti.xcom_pull(task_ids='{upload_task_id}') | join("', '") }}}}"""


# No ON_ERROR = CONTINUE: a bad file must fail the task. Retries are safe
# because COPY's load history skips files it already loaded.

def copy_json_sql(table, upload_task_id):
    return f"""
COPY INTO {SNOWFLAKE_DATABASE}.RAW.{table.upper()} (RAW, FILE_NAME)
FROM (SELECT $1, METADATA$FILENAME FROM {SNOWFLAKE_STAGE})
FILES = ('{_uploaded_files(upload_task_id)}')
FILE_FORMAT = (TYPE = JSON);
"""


def copy_ledger_sql(upload_task_id):
    # The CSV's trailing TOTAL column has no RAW counterpart and is dropped.
    # BATCH_ID is the file stem (the run timestamp).
    positions = ", ".join(f"${i}" for i in range(1, len(LEDGER_RAW_COLUMNS) + 1))
    return f"""
COPY INTO {SNOWFLAKE_DATABASE}.RAW.{RAW_LEDGER.upper()} (
    {", ".join(c.upper() for c in LEDGER_RAW_COLUMNS)},
    BATCH_ID, FILE_NAME
)
FROM (
    SELECT {positions},
           SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', -1), '.', 1),
           METADATA$FILENAME
    FROM {SNOWFLAKE_STAGE}
)
FILES = ('{_uploaded_files(upload_task_id)}')
FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');
"""
