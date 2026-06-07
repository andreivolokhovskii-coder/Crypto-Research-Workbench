"""
spark_batch — submits Spark batch jobs to the local standalone cluster.

Schedule: every 6 hours (offset by 30 min so it runs after daily_pipeline).
Steps:
  1. volatility_batch — computes rolling vol + market regime (silver → gold)

Setup required (one-time):
  Airflow UI → Admin → Connections → Add:
    Conn Id:   spark_default
    Conn Type: Spark
    Host:      spark://spark-master
    Port:      7077
"""
from __future__ import annotations

from datetime import datetime, timedelta

import os

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

DEFAULT_ARGS = {
    "owner":            "workbench",
    "retries":          1,
    "retry_delay":      timedelta(minutes=10),
    "email_on_failure": False,
}

with DAG(
    dag_id="spark_batch",
    description="Spark batch: realized volatility + market regime classification",
    schedule="30 */6 * * *",         # 00:30, 06:30, 12:30, 18:30 UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["spark", "batch", "gold"],
    max_active_runs=1,
) as dag:

    volatility_batch = SparkSubmitOperator(
        task_id="volatility_batch",
        conn_id="spark_default",
        application="/app/spark_jobs/volatility_batch.py",
        # ClickHouse JDBC driver downloaded from Maven on first run
        packages="com.clickhouse:clickhouse-jdbc:0.6.5",
        conf={
            "spark.executor.memory": "1g",
            "spark.driver.memory":   "512m",
            "spark.sql.session.timeZone": "UTC",
        },
        env_vars={
            "CLICKHOUSE_HOST":      os.environ.get("CLICKHOUSE_HOST",      "clickhouse"),
            "CLICKHOUSE_HTTP_PORT": os.environ.get("CLICKHOUSE_HTTP_PORT", "8123"),
            "CLICKHOUSE_DB":        os.environ.get("CLICKHOUSE_DB",        "crypto"),
            "CLICKHOUSE_USER":      os.environ.get("CLICKHOUSE_USER",      "crypto_user"),
            "CLICKHOUSE_PASSWORD":  os.environ.get("CLICKHOUSE_PASSWORD",  ""),
            "SPARK_LOOKBACK_DAYS":  "90",
        },
        name="volatility_batch",
        verbose=False,
    )
