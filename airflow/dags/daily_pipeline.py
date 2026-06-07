"""
daily_pipeline — ежедневный пайплайн обновления данных.

Расписание: каждые 6 часов.
Шаги:
  1. incremental_klines   — докачивает свечи за последние 8 часов (с перекрытием)
  2. metadata_refresh     — обновляет coin metadata из CoinGecko
  3. dbt_build            — пересобирает золотой слой (fact_candles, mart_volatility, dim_coin)
"""

from __future__ import annotations

from datetime import datetime, timedelta

import os

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

DEFAULT_ARGS = {
    "owner":            "workbench",
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

COMMON_ENV = {
    "PATH":                 "/home/airflow/.local/bin:/usr/local/bin:/usr/local/sbin:/usr/sbin:/usr/bin:/sbin:/bin",
    "CLICKHOUSE_HOST":      os.environ.get("CLICKHOUSE_HOST",      "clickhouse"),
    "CLICKHOUSE_HTTP_PORT": os.environ.get("CLICKHOUSE_HTTP_PORT", "8123"),
    "CLICKHOUSE_DB":        os.environ.get("CLICKHOUSE_DB",        "crypto"),
    "CLICKHOUSE_USER":      os.environ.get("CLICKHOUSE_USER",      "crypto_user"),
    "CLICKHOUSE_PASSWORD":  os.environ.get("CLICKHOUSE_PASSWORD",  ""),
    "S3_ENDPOINT":          os.environ.get("S3_ENDPOINT",          "http://minio:9000"),
    "AWS_ACCESS_KEY_ID":    os.environ.get("AWS_ACCESS_KEY_ID",    "minioadmin"),
    "AWS_SECRET_ACCESS_KEY":os.environ.get("AWS_SECRET_ACCESS_KEY",""),
    "MINIO_BUCKET_BRONZE":  os.environ.get("MINIO_BUCKET_BRONZE",  "bronze"),
    "MINIO_BUCKET_SILVER":  os.environ.get("MINIO_BUCKET_SILVER",  "silver"),
    "LOG_LEVEL":            os.environ.get("LOG_LEVEL",            "INFO"),
    "PYTHONUNBUFFERED":     "1",
}

with DAG(
    dag_id="daily_pipeline",
    description="Incremental klines + metadata refresh + dbt build",
    schedule="0 */6 * * *",          # каждые 6 часов
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["ingestion", "dbt", "daily"],
    max_active_runs=1,
) as dag:

    # ─── 1. Инкрементальный бэкфилл klines (последние 8 часов с перекрытием) ───
    incremental_klines = BashOperator(
        task_id="incremental_klines",
        bash_command=(
            "python /app/ingestion/historical/klines_backfill.py "
            "--interval 1m --days 0 "
            # days=0 → скрипт возьмёт последние 2ч; передаём явно через env
        ),
        env={
            **COMMON_ENV,
            "DEFAULT_BACKFILL_DAYS": "1",   # перекрытие 1 день чтобы не пропустить
        },
    )

    # ─── 2. Обновление coin metadata ────────────────────────────────────────────
    metadata_refresh = BashOperator(
        task_id="metadata_refresh",
        bash_command="python /app/ingestion/metadata/coingecko_dims.py --top 100",
        env=COMMON_ENV,
    )

    # ─── 3. dbt build ───────────────────────────────────────────────────────────
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            # /app is mounted read-only — copy the dbt project to a unique
            # writable scratch dir (per task-run, to avoid races with other
            # dbt tasks) so `dbt deps`/logs/target can write there
            "export DBT_PROFILES_DIR=$(mktemp -d /tmp/dbt_run.XXXXXX) && "
            "cp -r /app/dbt/. \"$DBT_PROFILES_DIR/\" && "
            # Drop stale compile cache — its paths point at the source dir
            # and confuse dbt's partial-parse dependency inference
            "rm -rf \"$DBT_PROFILES_DIR/target\" \"$DBT_PROFILES_DIR/logs\" && "
            "cd \"$DBT_PROFILES_DIR\" && "
            # dbt_packages is already vendored in the project — re-running
            # `dbt deps` re-downloads it and produces a version drift that
            # breaks dbt's static ref() dependency inference
            "dbt build --quiet"
        ),
        env={
            "PATH":                 "/home/airflow/.local/bin:/usr/local/bin:/usr/local/sbin:/usr/sbin:/usr/bin:/sbin:/bin",
            "CLICKHOUSE_HOST":      os.environ.get("CLICKHOUSE_HOST",      "clickhouse"),
            "CLICKHOUSE_HTTP_PORT": os.environ.get("CLICKHOUSE_HTTP_PORT", "8123"),
            "CLICKHOUSE_DB":        os.environ.get("CLICKHOUSE_DB",        "crypto"),
            "CLICKHOUSE_USER":      os.environ.get("CLICKHOUSE_USER",      "crypto_user"),
            "CLICKHOUSE_PASSWORD":  os.environ.get("CLICKHOUSE_PASSWORD",  ""),
        },
    )

    # ─── DAG flow ────────────────────────────────────────────────────────────────
    # metadata и klines параллельно → dbt
    [incremental_klines, metadata_refresh] >> dbt_build
