"""
historical_backfill — одноразовый/ручной бэкфилл исторических klines.

Запускается вручную через Airflow UI (не по расписанию).
Параметры передаются через dag_run.conf:
  exchange  (default: binance)
  symbols   (default: из .env)
  interval  (default: 1m)
  days      (default: 30)
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="historical_backfill",
    description="Manual historical OHLCV backfill via klines_backfill.py",
    schedule=None,           # только ручной запуск
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ingestion", "historical"],
    params={
        "exchange": "binance",
        "symbols":  "BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT,XRPUSDT",
        "interval": "1m",
        "days":     30,
    },
) as dag:

    run_backfill = BashOperator(
        task_id="run_klines_backfill",
        # Pass params via env vars — never interpolate user-supplied values
        # directly into a bash_command string (command injection risk).
        bash_command=(
            "python /app/ingestion/historical/klines_backfill.py "
            "--exchange \"$BF_EXCHANGE\" "
            "--symbols  \"$BF_SYMBOLS\" "
            "--interval \"$BF_INTERVAL\" "
            "--days     \"$BF_DAYS\""
        ),
        env={
            "PATH":                "/home/airflow/.local/bin:/usr/local/bin:/usr/local/sbin:/usr/sbin:/usr/bin:/sbin:/bin",
            # Backfill params — sourced from DAG params, not interpolated into the shell string
            "BF_EXCHANGE":         "{{ params.exchange }}",
            "BF_SYMBOLS":          "{{ params.symbols }}",
            "BF_INTERVAL":         "{{ params.interval }}",
            "BF_DAYS":             "{{ params.days | string }}",
            # ClickHouse / MinIO creds
            "CLICKHOUSE_HOST":     "clickhouse",
            "CLICKHOUSE_HTTP_PORT":"8123",
            "CLICKHOUSE_DB":       "{{ var.value.get('CLICKHOUSE_DB',       'crypto') }}",
            "CLICKHOUSE_USER":     "{{ var.value.get('CLICKHOUSE_USER',     'crypto_user') }}",
            "CLICKHOUSE_PASSWORD": "{{ var.value.get('CLICKHOUSE_PASSWORD', '') }}",
            "S3_ENDPOINT":         "http://minio:9000",
            "AWS_ACCESS_KEY_ID":   "{{ var.value.get('MINIO_ROOT_USER',     'minioadmin') }}",
            "AWS_SECRET_ACCESS_KEY":"{{ var.value.get('MINIO_ROOT_PASSWORD','minioadmin') }}",
            "MINIO_BUCKET_BRONZE": "bronze",
            "MINIO_BUCKET_SILVER": "silver",
            "LOG_LEVEL":           "INFO",
            "PYTHONUNBUFFERED":    "1",
        },
    )
