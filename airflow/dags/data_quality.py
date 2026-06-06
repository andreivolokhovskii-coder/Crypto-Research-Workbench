"""
data_quality — ежедневная проверка качества данных.

Расписание: раз в день в 02:00 UTC.
Проверки:
  1. freshness_check  — все символы обновлялись в последние 2 часа
  2. dbt_test         — проходят все dbt тесты (not_null, accepted_values)
  3. row_count_check  — нет аномальных провалов в количестве свечей
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

CH_ENV = {
    "PATH":                 "/home/airflow/.local/bin:/usr/local/bin:/usr/local/sbin:/usr/sbin:/usr/bin:/sbin:/bin",
    "CLICKHOUSE_HOST":      "clickhouse",
    "CLICKHOUSE_HTTP_PORT": "8123",
    "CLICKHOUSE_DB":        "crypto",
    "CLICKHOUSE_USER":      "crypto_user",
    "CLICKHOUSE_PASSWORD":  "{{ var.value.get('CLICKHOUSE_PASSWORD', '') }}",
}

FRESHNESS_SQL = """
SELECT symbol,
       toInt32(now() - max(open_time)) AS seconds_stale
FROM crypto.silver_klines
WHERE interval = '1m'
GROUP BY symbol
HAVING seconds_stale > 7200   -- более 2 часов без обновления
"""

ROW_COUNT_SQL = """
SELECT symbol,
       count() AS candles_24h
FROM crypto.silver_klines
WHERE interval = '1m'
  AND open_time >= now() - INTERVAL 24 HOUR
GROUP BY symbol
HAVING candles_24h < 1000     -- менее 1000 свечей за сутки = подозрительно мало
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

    [freshness_check, dbt_test, row_count_check]
