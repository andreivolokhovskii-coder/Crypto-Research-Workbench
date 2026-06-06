#!/usr/bin/env python3
"""
coingecko_dims.py — Coin metadata refresh from CoinGecko API (free tier).

Fetches top-N coins by market cap, stores:
  - raw JSON snapshot in MinIO bronze + ClickHouse bronze_coin_metadata
  - normalized rows  in MinIO silver + ClickHouse silver_coin_metadata

Usage inside app container:
    python ingestion/metadata/coingecko_dims.py
    python ingestion/metadata/coingecko_dims.py --top 250
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone, date
from io import BytesIO

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
import boto3
from botocore.client import Config
import clickhouse_connect
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
    stream=sys.stdout,
)
log = logging.getLogger("coingecko_dims")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

COINGECKO_BASE = "https://api.coingecko.com/api/v3"
DEFAULT_TOP     = 100          # top coins by market cap
PAGE_SIZE       = 250          # max per CoinGecko page
REQUEST_DELAY   = 1.5          # seconds between requests (free tier rate limit)

S3_ENDPOINT   = os.getenv("S3_ENDPOINT",          "http://minio:9000")
S3_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID",    "minioadmin")
S3_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY","minioadmin")
BUCKET_BRONZE = os.getenv("MINIO_BUCKET_BRONZE",  "bronze")
BUCKET_SILVER = os.getenv("MINIO_BUCKET_SILVER",  "silver")

CH_HOST       = os.getenv("CLICKHOUSE_HOST",        "clickhouse")
CH_PORT       = int(os.getenv("CLICKHOUSE_HTTP_PORT","8123"))
CH_DB         = os.getenv("CLICKHOUSE_DB",          "crypto")
CH_USER       = os.getenv("CLICKHOUSE_USER",        "crypto_user")
CH_PASSWORD   = os.getenv("CLICKHOUSE_PASSWORD",    "")


# ---------------------------------------------------------------------------
# CoinGecko API
# ---------------------------------------------------------------------------

def fetch_markets(top: int) -> list[dict]:
    """Fetch top-N coins from /coins/markets (paginated)."""
    coins: list[dict] = []
    page = 1
    while len(coins) < top:
        per_page = min(PAGE_SIZE, top - len(coins))
        url = (
            f"{COINGECKO_BASE}/coins/markets"
            f"?vs_currency=usd&order=market_cap_desc"
            f"&per_page={per_page}&page={page}"
            f"&sparkline=false&locale=en"
        )
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        batch = resp.json()
        if not batch:
            break
        coins.extend(batch)
        log.info("  fetched page %d → %d coins total", page, len(coins))
        page += 1
        if len(batch) < per_page:
            break
        time.sleep(REQUEST_DELAY)

    return coins[:top]


# ---------------------------------------------------------------------------
# DataFrames
# ---------------------------------------------------------------------------

def build_bronze_df(coins: list[dict], snapshot_date: date) -> pd.DataFrame:
    rows = []
    for c in coins:
        rows.append({
            "source":          "coingecko",
            "coin_id":         c["id"],
            "symbol":          c["symbol"].upper(),
            "name":            c["name"],
            "payload":         json.dumps(c, ensure_ascii=False),
            "_partition_date": snapshot_date,
        })
    df = pd.DataFrame(rows)
    df["_partition_date"] = pd.to_datetime(df["_partition_date"])
    return df


def build_silver_df(coins: list[dict], snapshot_date: date) -> pd.DataFrame:
    rows = []
    for c in coins:
        rows.append({
            "source":          "coingecko",
            "coin_id":         c["id"],
            "symbol":          c["symbol"].upper(),
            "name":            c["name"],
            "market_cap_rank": c.get("market_cap_rank"),
            "category":        None,
            "coingecko_id":    c["id"],
            "snapshot_date":   snapshot_date,
        })
    df = pd.DataFrame(rows)
    # Nullable int — keep as float64 (ClickHouse Nullable(Int32) accepts None)
    df["market_cap_rank"] = pd.array(df["market_cap_rank"], dtype="Int32")
    df["snapshot_date"]   = pd.to_datetime(df["snapshot_date"])
    return df


# ---------------------------------------------------------------------------
# MinIO / S3
# ---------------------------------------------------------------------------

def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def upload_parquet(s3, bucket: str, key: str, df: pd.DataFrame) -> None:
    buf = BytesIO()
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False), buf, compression="snappy")
    buf.seek(0)
    s3.put_object(Bucket=bucket, Key=key, Body=buf.read())
    log.debug("  s3://%s/%s  (%d rows)", bucket, key, len(df))


# ---------------------------------------------------------------------------
# ClickHouse
# ---------------------------------------------------------------------------

def ch_client():
    return clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT,
        database=CH_DB, username=CH_USER, password=CH_PASSWORD,
    )


BRONZE_COLS = ["source", "coin_id", "symbol", "name", "payload", "_partition_date"]
SILVER_COLS = ["source", "coin_id", "symbol", "name",
               "market_cap_rank", "category", "coingecko_id", "snapshot_date"]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    p = argparse.ArgumentParser(description="CoinGecko coin metadata refresh")
    p.add_argument("--top", type=int, default=DEFAULT_TOP,
                   help="number of top coins to fetch (default: %(default)s)")
    args = p.parse_args()

    today = datetime.now(timezone.utc).date()
    date_str = today.strftime("%Y-%m-%d")

    log.info("Fetching top %d coins from CoinGecko ...", args.top)
    coins = fetch_markets(args.top)
    log.info("Fetched %d coins", len(coins))

    bronze_df = build_bronze_df(coins, today)
    silver_df = build_silver_df(coins, today)

    s3 = s3_client()
    ch = ch_client()

    # Bronze
    bronze_key = f"metadata/coins/source=coingecko/date={date_str}/data.parquet"
    upload_parquet(s3, BUCKET_BRONZE, bronze_key, bronze_df)
    ch.insert_df("bronze_coin_metadata", bronze_df[BRONZE_COLS], column_names=BRONZE_COLS)
    log.info("Bronze: %d rows → MinIO + ClickHouse", len(bronze_df))

    # Silver
    silver_key = f"metadata/coins/source=coingecko/date={date_str}/data.parquet"
    upload_parquet(s3, BUCKET_SILVER, silver_key, silver_df)
    ch.insert_df("silver_coin_metadata", silver_df[SILVER_COLS], column_names=SILVER_COLS)
    log.info("Silver: %d rows → MinIO + ClickHouse", len(silver_df))

    log.info("Metadata refresh complete.")


if __name__ == "__main__":
    main()
