#!/usr/bin/env python3
"""
volatility_batch.py — Spark batch job: realized volatility + market regime.

Reads silver_klines from ClickHouse, rolls up 1m candles to daily OHLCV,
computes rolling realized volatility and ATR, then classifies each trading day
per symbol as one of: trending_up / trending_down / volatile / ranging.

Writes results to ClickHouse mart_market_regime (ReplacingMergeTree).

Why Spark and not dbt?
  dbt (ClickHouse SQL) is great for deterministic per-partition transforms.
  This job benefits from Spark when the dataset grows large: it partitions
  the read across workers, computes window functions in parallel, and can be
  extended to cross-symbol correlation matrices that ClickHouse handles poorly.

Submit from the host (after `docker compose up -d`):

    docker exec workbench-spark-master /opt/spark/bin/spark-submit \\
        --master spark://spark-master:7077 \\
        --packages com.clickhouse:clickhouse-jdbc:0.6.5 \\
        --conf spark.executor.memory=1g \\
        --conf spark.driver.memory=512m \\
        /app/spark_jobs/volatility_batch.py

Or via Makefile:

    make spark-volatility
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# Config — read from environment (set in .env / docker-compose environment)
# ---------------------------------------------------------------------------

CH_HOST    = os.getenv("CLICKHOUSE_HOST",         "clickhouse")
CH_PORT    = os.getenv("CLICKHOUSE_HTTP_PORT",     "8123")
CH_DB      = os.getenv("CLICKHOUSE_DB",            "crypto")
CH_USER    = os.getenv("CLICKHOUSE_USER",          "crypto_user")
CH_PASS    = os.getenv("CLICKHOUSE_PASSWORD",      "")

JDBC_URL   = f"jdbc:ch://{CH_HOST}:{CH_PORT}/{CH_DB}"
JDBC_DRIVER = "com.clickhouse.jdbc.ClickHouseDriver"

LOOKBACK_DAYS = int(os.getenv("SPARK_LOOKBACK_DAYS", "90"))

# Regime classification thresholds
VOL_HIGH_FACTOR = 1.5   # vol_7d > factor × avg(vol_30d) → "volatile"
TREND_THRESHOLD = 0.02  # |close − SMA20| / SMA20 > threshold → "trending_*"


# ---------------------------------------------------------------------------
# SparkSession
# ---------------------------------------------------------------------------

def build_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("volatility_batch")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# I/O
# ---------------------------------------------------------------------------

def _jdbc_opts(spark):
    return (
        spark.read.format("jdbc")
        .option("url",      JDBC_URL)
        .option("driver",   JDBC_DRIVER)
        .option("user",     CH_USER)
        .option("password", CH_PASS)
        .option("fetchsize", "50000")
    )


def read_silver_klines(spark: SparkSession):
    cutoff = (
        datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)
    ).strftime("%Y-%m-%d %H:%M:%S")
    query = (
        f"(SELECT exchange, symbol, open_time, open, high, low, close, volume "
        f"FROM {CH_DB}.silver_klines FINAL "
        f"WHERE interval = '1m' AND open_time >= '{cutoff}') AS klines"
    )
    return _jdbc_opts(spark).option("dbtable", query).load()


def write_regime(df, spark: SparkSession) -> None:
    (
        df.write.format("jdbc")
        .option("url",       JDBC_URL)
        .option("dbtable",   f"{CH_DB}.mart_market_regime")
        .option("driver",    JDBC_DRIVER)
        .option("user",      CH_USER)
        .option("password",  CH_PASS)
        .option("batchsize", "10000")
        .mode("append")
        .save()
    )


# ---------------------------------------------------------------------------
# Transformations
# ---------------------------------------------------------------------------

def aggregate_daily(klines):
    """Roll 1m candles up to daily OHLCV per (exchange, symbol)."""
    return (
        klines
        .withColumn("trade_date", F.to_date("open_time"))
        .groupBy("exchange", "symbol", "trade_date")
        .agg(
            # Use min/max of the ordered-by-open_time first/last values
            # argMin / argMax are approximated here via first on a sorted frame
            F.min("open_time").alias("_min_ts"),
            F.max("open_time").alias("_max_ts"),
            F.max("high").alias("day_high"),
            F.min("low").alias("day_low"),
            F.sum("volume").alias("day_volume"),
        )
        # Re-join to get open at min(open_time) and close at max(open_time)
        .join(
            klines.select(
                "exchange", "symbol",
                F.col("open_time").alias("_min_ts"),
                F.col("open").alias("day_open"),
            ),
            on=["exchange", "symbol", "_min_ts"],
            how="left",
        )
        .join(
            klines.select(
                "exchange", "symbol",
                F.col("open_time").alias("_max_ts"),
                F.col("close").alias("day_close"),
            ),
            on=["exchange", "symbol", "_max_ts"],
            how="left",
        )
        .drop("_min_ts", "_max_ts")
    )


def compute_metrics(daily):
    """Add log_return, true range, rolling vol, SMA, ATR, and regime label."""
    sym_w    = Window.partitionBy("exchange", "symbol").orderBy("trade_date")
    rol_7_w  = sym_w.rowsBetween(-6, 0)
    rol_14_w = sym_w.rowsBetween(-13, 0)
    rol_20_w = sym_w.rowsBetween(-19, 0)
    rol_30_w = sym_w.rowsBetween(-29, 0)

    df = (
        daily
        .withColumn("prev_close", F.lag("day_close", 1).over(sym_w))
        .withColumn(
            "log_return",
            F.when(
                F.col("prev_close") > 0,
                F.log(F.col("day_close") / F.col("prev_close")),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "true_range",
            F.greatest(
                F.col("day_high") - F.col("day_low"),
                F.abs(F.col("day_high") - F.col("prev_close")),
                F.abs(F.col("day_low")  - F.col("prev_close")),
            ),
        )
    )

    df = (
        df
        .withColumn("vol_7d",      F.stddev("log_return").over(rol_7_w)  * F.sqrt(F.lit(365.0)))
        .withColumn("vol_30d",     F.stddev("log_return").over(rol_30_w) * F.sqrt(F.lit(365.0)))
        .withColumn("vol_30d_avg", F.avg("vol_30d").over(rol_30_w))
        .withColumn("sma_20",      F.avg("day_close").over(rol_20_w))
        .withColumn("atr_14",      F.avg("true_range").over(rol_14_w))
    )

    df = df.withColumn(
        "regime",
        F.when(
            F.col("vol_7d") > VOL_HIGH_FACTOR * F.col("vol_30d_avg"),
            "volatile",
        ).when(
            (F.col("day_close") - F.col("sma_20")) / F.col("sma_20") > TREND_THRESHOLD,
            "trending_up",
        ).when(
            (F.col("sma_20") - F.col("day_close")) / F.col("sma_20") > TREND_THRESHOLD,
            "trending_down",
        ).otherwise("ranging"),
    )

    return (
        df
        .filter(F.col("prev_close").isNotNull())   # drop first row per symbol
        .select(
            "exchange",
            "symbol",
            F.to_timestamp("trade_date").alias("trade_date"),
            F.round("day_open",   8).alias("day_open"),
            F.round("day_high",   8).alias("day_high"),
            F.round("day_low",    8).alias("day_low"),
            F.round("day_close",  8).alias("day_close"),
            F.round("day_volume", 4).alias("day_volume"),
            F.round("log_return", 8).alias("log_return"),
            F.round(F.coalesce("vol_7d",      F.lit(0.0)), 6).alias("realized_vol_7d"),
            F.round(F.coalesce("vol_30d",     F.lit(0.0)), 6).alias("realized_vol_30d"),
            F.round(F.coalesce("atr_14",      F.lit(0.0)), 8).alias("atr_14"),
            "regime",
            F.current_timestamp().alias("computed_at"),
        )
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    spark = build_session()
    spark.sparkContext.setLogLevel("WARN")

    print(f"[volatility_batch] Reading silver_klines — last {LOOKBACK_DAYS} days …")
    klines = read_silver_klines(spark)
    kline_count = klines.count()
    print(f"[volatility_batch]   {kline_count:,} raw 1m candles")

    daily  = aggregate_daily(klines)
    result = compute_metrics(daily)

    regime_count = result.count()
    print(f"[volatility_batch] Writing {regime_count:,} rows → mart_market_regime …")
    write_regime(result, spark)
    print("[volatility_batch] Done.")

    spark.stop()


if __name__ == "__main__":
    main()
