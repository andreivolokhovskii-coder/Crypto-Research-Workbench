#!/usr/bin/env python3
"""
klines_backfill.py — Historical OHLCV klines backfill.

Flow:
  1. Fetch candles from exchange via ccxt (paginated)
  2. Write bronze Parquet to MinIO  (raw ms-timestamps)
  3. Write silver Parquet to MinIO  (UTC DateTime, normalized)
  4. Insert both into ClickHouse bronze_klines + silver_klines

Usage inside app container:
    python ingestion/historical/klines_backfill.py
    python ingestion/historical/klines_backfill.py --days 7 --interval 1h
    python ingestion/historical/klines_backfill.py --symbols BTCUSDT,ETHUSDT --days 90 --exchange binance
"""

import argparse
import logging
import os
import sys
from datetime import datetime, timezone, timedelta
from io import BytesIO

import ccxt
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
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
log = logging.getLogger("klines_backfill")

# ---------------------------------------------------------------------------
# Config (from environment / .env)
# ---------------------------------------------------------------------------

EXCHANGE_ID   = os.getenv("DEFAULT_EXCHANGE",       "binance")
SYMBOLS_ENV   = os.getenv("DEFAULT_SYMBOLS",        "BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT,XRPUSDT")
INTERVAL      = os.getenv("DEFAULT_KLINE_INTERVAL", "1m")
BACKFILL_DAYS = int(os.getenv("DEFAULT_BACKFILL_DAYS", "30"))

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

FETCH_LIMIT   = 1000  # candles per ccxt request

INTERVAL_MS: dict[str, int] = {
    "1m":  60_000,      "3m":  180_000,    "5m":  300_000,
    "15m": 900_000,     "30m": 1_800_000,  "1h":  3_600_000,
    "2h":  7_200_000,   "4h":  14_400_000, "6h":  21_600_000,
    "8h":  28_800_000,  "12h": 43_200_000, "1d":  86_400_000,
}

# Common quote currencies for symbol normalization (longest first to avoid
# prefix collisions, e.g. "USDT" before "USD").
QUOTE_CURRENCIES = ["USDT", "BUSD", "USDC", "TUSD", "BTC", "ETH", "BNB", "USD"]


# ---------------------------------------------------------------------------
# Exchange helpers
# ---------------------------------------------------------------------------

def build_exchange(exchange_id: str) -> ccxt.Exchange:
    cls = getattr(ccxt, exchange_id, None)
    if cls is None:
        raise ValueError(f"Unknown ccxt exchange: {exchange_id}")
    exchange = cls({"enableRateLimit": True})
    exchange.load_markets()
    return exchange


def normalize_symbol(exchange: ccxt.Exchange, raw: str) -> str:
    """Convert Binance-native 'BTCUSDT' to ccxt unified 'BTC/USDT'."""
    if raw in exchange.markets:
        return raw
    for quote in QUOTE_CURRENCIES:
        if raw.endswith(quote):
            candidate = raw[: -len(quote)] + "/" + quote
            if candidate in exchange.markets:
                return candidate
    for symbol, mkt in exchange.markets.items():
        if mkt.get("id", "").upper() == raw.upper():
            return symbol
    log.warning("Symbol %s not found in %s markets; using as-is", raw, exchange.id)
    return raw


def fetch_klines(
    exchange: ccxt.Exchange,
    symbol: str,
    interval: str,
    since_ms: int,
    until_ms: int,
) -> list[list]:
    """Paginate ccxt.fetch_ohlcv to collect all candles in [since_ms, until_ms)."""
    iv_ms = INTERVAL_MS.get(interval, 60_000)
    result: list[list] = []
    cursor = since_ms

    while cursor < until_ms:
        batch = exchange.fetch_ohlcv(symbol, interval, since=cursor, limit=FETCH_LIMIT)
        if not batch:
            break
        # Keep only candles strictly before until_ms
        batch = [c for c in batch if c[0] < until_ms]
        result.extend(batch)
        if len(batch) < FETCH_LIMIT:
            break
        cursor = batch[-1][0] + iv_ms

    # Deduplicate and sort by open_time
    seen: set[int] = set()
    unique = []
    for c in result:
        if c[0] not in seen:
            seen.add(c[0])
            unique.append(c)
    return sorted(unique, key=lambda x: x[0])


# ---------------------------------------------------------------------------
# DataFrame construction
# ---------------------------------------------------------------------------

def build_bronze_df(
    rows: list[list],
    exchange: str,
    symbol: str,
    interval: str,
) -> pd.DataFrame:
    iv_ms = INTERVAL_MS.get(interval, 60_000)
    df = pd.DataFrame(rows, columns=["open_time", "open", "high", "low", "close", "volume"])
    df["exchange"]           = exchange
    df["symbol"]             = symbol
    df["interval"]           = interval
    df["close_time"]         = (df["open_time"] + iv_ms - 1).astype("int64")
    df["quote_asset_volume"] = 0.0
    df["trade_count"]        = 0
    df["open_time"]          = df["open_time"].astype("int64")
    df["trade_count"]        = df["trade_count"].astype("int32")
    # _partition_date as Python date (one per row, used for partitioning)
    df["_partition_date"] = (
        pd.to_datetime(df["open_time"], unit="ms", utc=True)
        .dt.tz_localize(None)
        .dt.normalize()
    )
    return df


def build_silver_df(bronze: pd.DataFrame) -> pd.DataFrame:
    df = bronze.copy()
    df["open_time"]  = pd.to_datetime(df["open_time"],  unit="ms", utc=True).dt.tz_localize(None)
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True).dt.tz_localize(None)
    df = df.rename(columns={"quote_asset_volume": "quote_volume"})
    return df[[
        "exchange", "symbol", "interval",
        "open_time", "close_time",
        "open", "high", "low", "close", "volume", "quote_volume", "trade_count",
        "_partition_date",
    ]]


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


def s3_key(exchange: str, symbol: str, interval: str, date_str: str) -> str:
    safe = symbol.replace("/", "-")
    return f"klines/{exchange}/{safe}/{interval}/date={date_str}/data.parquet"


def upload_df_as_parquet(s3, bucket: str, key: str, df: pd.DataFrame) -> None:
    buf = BytesIO()
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, buf, compression="snappy")
    buf.seek(0)
    s3.put_object(Bucket=bucket, Key=key, Body=buf.read())
    log.debug("    s3://%s/%s  (%d rows)", bucket, key, len(df))


# ---------------------------------------------------------------------------
# ClickHouse
# ---------------------------------------------------------------------------

def ch_client():
    return clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT,
        database=CH_DB, username=CH_USER, password=CH_PASSWORD,
    )


BRONZE_COLS = [
    "exchange", "symbol", "interval",
    "open_time", "open", "high", "low", "close",
    "volume", "close_time", "quote_asset_volume", "trade_count",
    "_partition_date",
]

SILVER_COLS = [
    "exchange", "symbol", "interval",
    "open_time", "close_time",
    "open", "high", "low", "close", "volume", "quote_volume", "trade_count",
    "_partition_date",
]


def ch_insert(ch, table: str, df: pd.DataFrame, cols: list[str]) -> None:
    ch.insert_df(table, df[cols], column_names=cols)


# ---------------------------------------------------------------------------
# Per-symbol pipeline
# ---------------------------------------------------------------------------

def process_symbol(
    exchange_obj: ccxt.Exchange,
    exchange_id: str,
    raw_symbol: str,
    interval: str,
    since_ms: int,
    until_ms: int,
    s3,
    ch,
) -> None:
    symbol = normalize_symbol(exchange_obj, raw_symbol)
    log.info("[%s] %s  interval=%s", exchange_id, symbol, interval)

    rows = fetch_klines(exchange_obj, symbol, interval, since_ms, until_ms)
    if not rows:
        log.warning("[%s] %s — no data returned, skipping", exchange_id, symbol)
        return
    log.info("  fetched %d candles", len(rows))

    bronze = build_bronze_df(rows, exchange_id, symbol, interval)
    silver = build_silver_df(bronze)

    # Partition uploads by date
    for date_ts, grp in bronze.groupby("_partition_date"):
        date_str = pd.Timestamp(date_ts).strftime("%Y-%m-%d")
        key = s3_key(exchange_id, symbol, interval, date_str)

        # Bronze parquet — add _source_file reference
        grp = grp.copy()
        grp["_source_file"] = f"s3://{BUCKET_BRONZE}/{key}"
        upload_df_as_parquet(s3, BUCKET_BRONZE, key, grp)

        # Silver parquet
        silver_grp = silver[silver["_partition_date"] == date_ts]
        upload_df_as_parquet(s3, BUCKET_SILVER, key, silver_grp)

    log.info("  uploaded parquet partitions to MinIO (bronze + silver)")

    # ClickHouse — add _source_file column (not in silver)
    bronze["_source_file"] = ""  # bulk marker; real path is in MinIO
    ch_insert(ch, "bronze_klines", bronze, BRONZE_COLS)
    log.info("  inserted %d rows → bronze_klines", len(bronze))

    ch_insert(ch, "silver_klines", silver, SILVER_COLS)
    log.info("  inserted %d rows → silver_klines", len(silver))


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Historical OHLCV klines backfill")
    p.add_argument("--exchange", default=EXCHANGE_ID,
                   help="ccxt exchange id (default: %(default)s)")
    p.add_argument("--symbols",  default=SYMBOLS_ENV,
                   help="comma-separated symbols (default: %(default)s)")
    p.add_argument("--interval", default=INTERVAL,
                   help="candle interval (default: %(default)s)")
    p.add_argument("--days",     type=int, default=BACKFILL_DAYS,
                   help="how many days back to fetch (default: %(default)s)")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]

    now_utc  = datetime.now(timezone.utc)
    until_ms = int(now_utc.timestamp() * 1000)
    since_ms = int((now_utc - timedelta(days=args.days)).timestamp() * 1000)

    log.info(
        "Backfill start  exchange=%s  symbols=%s  interval=%s  days=%d",
        args.exchange, symbols, args.interval, args.days,
    )
    log.info(
        "Date range: %s → %s",
        datetime.fromtimestamp(since_ms / 1000, timezone.utc).strftime("%Y-%m-%d"),
        datetime.fromtimestamp(until_ms / 1000, timezone.utc).strftime("%Y-%m-%d"),
    )

    exchange_obj = build_exchange(args.exchange)
    s3 = s3_client()
    ch = ch_client()

    errors: list[str] = []
    for raw_symbol in symbols:
        try:
            process_symbol(
                exchange_obj, args.exchange, raw_symbol,
                args.interval, since_ms, until_ms, s3, ch,
            )
        except Exception as exc:
            log.error("FAILED %s: %s", raw_symbol, exc, exc_info=True)
            errors.append(raw_symbol)

    if errors:
        log.error("Backfill finished with errors for: %s", errors)
        sys.exit(1)
    log.info("Backfill complete — all symbols OK.")


if __name__ == "__main__":
    main()
