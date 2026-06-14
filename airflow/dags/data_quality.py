"""
data_quality — daily data quality checks.

Schedule: daily at 02:00 UTC.
Checks:
  1. freshness_check  — all symbols updated within the last 2 hours
  2. dbt_test         — all dbt tests pass (not_null, accepted_values)
  3. row_count_check  — no anomalous drop in candle count per symbol
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule

DEFAULT_ARGS = {
    "owner":            "workbench",
    "retries":          1,
    "retry_delay":      timedelta(minutes=3),
    "email_on_failure": False,
}

import os as _os

CH_ENV = {
    "PATH":                 "/home/airflow/.local/bin:/usr/local/bin:/usr/local/sbin:/usr/sbin:/usr/bin:/sbin:/bin",
    "CLICKHOUSE_HOST":      _os.environ.get("CLICKHOUSE_HOST",      "clickhouse"),
    "CLICKHOUSE_HTTP_PORT": _os.environ.get("CLICKHOUSE_HTTP_PORT", "8123"),
    "CLICKHOUSE_DB":        _os.environ.get("CLICKHOUSE_DB",        "crypto"),
    "CLICKHOUSE_USER":      _os.environ.get("CLICKHOUSE_USER",      "crypto_user"),
    "CLICKHOUSE_PASSWORD":  _os.environ.get("CLICKHOUSE_PASSWORD",  ""),
}

FRESHNESS_SQL = """
SELECT symbol,
       toInt32(now() - max(open_time)) AS seconds_stale
FROM crypto.silver_klines
WHERE interval = '1m'
GROUP BY symbol
HAVING seconds_stale > 7200   -- stale if no update in the last 2 hours
"""

ROW_COUNT_SQL = """
SELECT symbol,
       count() AS candles_24h
FROM crypto.silver_klines
WHERE interval = '1m'
  AND open_time >= now() - INTERVAL 24 HOUR
GROUP BY symbol
HAVING candles_24h < 1000     -- fewer than 1000 candles per day is suspicious
"""


def _check_query(sql: str, description: str, **context) -> None:
    """Run a ClickHouse query; raise if it returns any rows (anomalies found)."""
    import clickhouse_connect, os

    client = clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "clickhouse"),
        port=int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123")),
        database=os.getenv("CLICKHOUSE_DB", "crypto"),
        username=os.getenv("CLICKHOUSE_USER", "crypto_user"),
        password=os.getenv("CLICKHOUSE_PASSWORD", ""),
    )
    rows = client.query(sql).result_rows
    if rows:
        raise ValueError(f"{description} — anomalies found: {rows}")
    print(f"{description}: OK (0 anomalies)")


with DAG(
    dag_id="data_quality",
    description="Daily data quality checks: freshness, dbt tests, row counts",
    schedule="0 2 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["quality", "dbt", "daily"],
) as dag:

    freshness_check = PythonOperator(
        task_id="freshness_check",
        python_callable=_check_query,
        op_kwargs={
            "sql":         FRESHNESS_SQL,
            "description": "Freshness check",
        },
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            "export DBT_PROFILES_DIR=$(mktemp -d /tmp/dbt_run.XXXXXX) && "
            "cp -r /app/dbt/. \"$DBT_PROFILES_DIR/\" && "
            "rm -rf \"$DBT_PROFILES_DIR/target\" \"$DBT_PROFILES_DIR/logs\" && "
            "cd \"$DBT_PROFILES_DIR\" && dbt test --quiet"
        ),
        env=CH_ENV,
    )

    row_count_check = PythonOperator(
        task_id="row_count_check",
        python_callable=_check_query,
        op_kwargs={
            "sql":         ROW_COUNT_SQL,
            "description": "Row count check",
        },
    )

    # Sequential: freshness → model correctness → data volume.
    # dbt_test only runs when data is fresh; row_count_check only after models pass.
    freshness_check >> dbt_test >> row_count_check
