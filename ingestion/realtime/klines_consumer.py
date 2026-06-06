#!/usr/bin/env python3
"""
klines_consumer.py — Kafka klines.raw → ClickHouse consumer.

Reads JSON messages from the `klines.raw` Kafka topic and:
  - rt_latest_kline  — upserts every tick (closed + in-progress)
  - bronze_klines    — inserts only CLOSED candles
  - silver_klines    — inserts only CLOSED candles (ReplacingMergeTree dedupes)
  - rt_signals       — emits volume_spike / large_candle signals on close

Run inside the app container:
    python ingestion/realtime/klines_consumer.py
"""
from __future__ import annotations

import json
import logging
import os
import sys
import time
from collections import defaultdict, deque
from datetime import datetime, timezone

import clickhouse_connect
from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
    stream=sys.stdout,
)
log = logging.getLogger("klines_consumer")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_KLINES     = os.getenv("KAFKA_TOPIC_KLINES_RAW",  "klines.raw")
GROUP_ID         = "klines-consumer-v1"

CH_HOST      = os.getenv("CLICKHOUSE_HOST",         "clickhouse")
CH_PORT      = int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123"))
CH_DB        = os.getenv("CLICKHOUSE_DB",            "crypto")
CH_USER      = os.getenv("CLICKHOUSE_USER",          "crypto_user")
CH_PASSWORD  = os.getenv("CLICKHOUSE_PASSWORD",      "")

BATCH_SIZE   = 50     # flush to ClickHouse every N closed candles
FLUSH_EVERY  = 10.0   # or every N seconds, whichever comes first

# Signal thresholds
VOL_SPIKE_ZSCORE  = 2.5   # volume z-score for spike detection
LARGE_CANDLE_ATR  = 3.0   # candle_range > N × ATR
ROLLING_WINDOW    = 60    # candles for rolling stats


# ---------------------------------------------------------------------------
# Rolling stats per symbol (in-memory, for signal detection)
# ---------------------------------------------------------------------------

class RollingStats:
    """Keeps last N closed-candle volumes and ranges for z-score / ATR."""
    def __init__(self, window: int = ROLLING_WINDOW):
        self._w = window
        self.volumes: dict[str, deque] = defaultdict(lambda: deque(maxlen=self._w))
        self.ranges:  dict[str, deque] = defaultdict(lambda: deque(maxlen=self._w))

    def add(self, key: str, volume: float, candle_range: float) -> None:
        self.volumes[key].append(volume)
        self.ranges[key].append(candle_range)

    def vol_zscore(self, key: str, volume: float) -> float | None:
        vols = list(self.volumes[key])
        if len(vols) < 10:
            return None
        mean = sum(vols) / len(vols)
        std  = (sum((v - mean) ** 2 for v in vols) / len(vols)) ** 0.5
        return (volume - mean) / std if std > 0 else 0.0

    def atr(self, key: str) -> float | None:
        r = list(self.ranges[key])
        return sum(r) / len(r) if len(r) >= 5 else None


# ---------------------------------------------------------------------------
# ClickHouse helpers
# ---------------------------------------------------------------------------

def ch_client() -> clickhouse_connect.driver.Client:
    return clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT,
        database=CH_DB, username=CH_USER, password=CH_PASSWORD,
    )


def upsert_latest(ch, records: list[dict]) -> None:
    if not records:
        return
    rows = [[
        r["exchange"], r["symbol"], r["interval"],
        datetime.fromtimestamp(r["open_time"]  / 1000, tz=timezone.utc).replace(tzinfo=None),
        datetime.fromtimestamp(r["close_time"] / 1000, tz=timezone.utc).replace(tzinfo=None),
        r["open"], r["high"], r["low"], r["close"],
        r["volume"], r["quote_volume"], r["trade_count"],
        int(r["is_closed"]),
    ] for r in records]
    ch.insert("rt_latest_kline", rows, column_names=[
        "exchange","symbol","interval","open_time","close_time",
        "open","high","low","close","volume","quote_volume","trade_count","is_closed",
    ])


def insert_bronze(ch, records: list[dict]) -> None:
    if not records:
        return
    iv_ms = 60_000
    rows = [[
        r["exchange"], r["symbol"], r["interval"],
        r["open_time"],
        r["open"], r["high"], r["low"], r["close"], r["volume"],
        r["close_time"], r["quote_volume"], r["trade_count"],
        "", datetime.fromtimestamp(r["open_time"] / 1000, tz=timezone.utc).date(),
    ] for r in records]
    ch.insert("bronze_klines", rows, column_names=[
        "exchange","symbol","interval","open_time",
        "open","high","low","close","volume",
        "close_time","quote_asset_volume","trade_count",
        "_source_file","_partition_date",
    ])


def insert_silver(ch, records: list[dict]) -> None:
    if not records:
        return
    rows = [[
        r["exchange"], r["symbol"], r["interval"],
        datetime.fromtimestamp(r["open_time"]  / 1000, tz=timezone.utc).replace(tzinfo=None),
        datetime.fromtimestamp(r["close_time"] / 1000, tz=timezone.utc).replace(tzinfo=None),
        r["open"], r["high"], r["low"], r["close"],
        r["volume"], r["quote_volume"], r["trade_count"],
        datetime.fromtimestamp(r["open_time"] / 1000, tz=timezone.utc).date(),
    ] for r in records]
    ch.insert("silver_klines", rows, column_names=[
        "exchange","symbol","interval","open_time","close_time",
        "open","high","low","close","volume","quote_volume","trade_count",
        "_partition_date",
    ])


def insert_signals(ch, signals: list[dict]) -> None:
    if not signals:
        return
    rows = [[
        s["exchange"], s["symbol"], s["signal_type"],
        s["value"], s["threshold"], s["description"],
        s["open_time"],
    ] for s in signals]
    ch.insert("rt_signals", rows, column_names=[
        "exchange","symbol","signal_type",
        "value","threshold","description","open_time",
    ])


# ---------------------------------------------------------------------------
# Signal detection
# ---------------------------------------------------------------------------

def detect_signals(record: dict, stats: RollingStats) -> list[dict]:
    key   = f"{record['exchange']}:{record['symbol']}"
    sigs  = []
    ts    = datetime.fromtimestamp(record["open_time"] / 1000, tz=timezone.utc).replace(tzinfo=None)

    # Volume spike
    zscore = stats.vol_zscore(key, record["volume"])
    if zscore is not None and zscore > VOL_SPIKE_ZSCORE:
        sigs.append({
            "exchange":    record["exchange"],
            "symbol":      record["symbol"],
            "signal_type": "volume_spike",
            "value":       round(zscore, 3),
            "threshold":   VOL_SPIKE_ZSCORE,
            "description": f"Volume z-score={zscore:.2f} vol={record['volume']:.0f}",
            "open_time":   ts,
        })

    # Large candle
    candle_range = record["high"] - record["low"]
    atr = stats.atr(key)
    if atr and atr > 0:
        ratio = candle_range / atr
        if ratio > LARGE_CANDLE_ATR:
            sigs.append({
                "exchange":    record["exchange"],
                "symbol":      record["symbol"],
                "signal_type": "large_candle",
                "value":       round(ratio, 3),
                "threshold":   LARGE_CANDLE_ATR,
                "description": f"Range={candle_range:.4f} ATR={atr:.4f} ratio={ratio:.2f}x",
                "open_time":   ts,
            })

    return sigs


# ---------------------------------------------------------------------------
# Main consumer loop
# ---------------------------------------------------------------------------

def main() -> None:
    log.info("klines_consumer starting  kafka=%s  topic=%s  group=%s",
             KAFKA_BOOTSTRAP, TOPIC_KLINES, GROUP_ID)

    consumer = Consumer({
        "bootstrap.servers":  KAFKA_BOOTSTRAP,
        "group.id":           GROUP_ID,
        "auto.offset.reset":  "latest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([TOPIC_KLINES])

    ch     = ch_client()
    stats  = RollingStats()
    latest_buf: list[dict] = []   # all ticks for rt_latest_kline
    closed_buf: list[dict] = []   # closed candles for bronze + silver
    signal_buf: list[dict] = []
    last_flush  = time.monotonic()
    total_msgs  = 0
    total_closed = 0

    log.info("Listening for messages …")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                pass
            elif msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    log.error("Kafka error: %s", msg.error())
            else:
                try:
                    record = json.loads(msg.value().decode())
                except (json.JSONDecodeError, UnicodeDecodeError):
                    continue

                latest_buf.append(record)
                total_msgs += 1

                if record.get("is_closed"):
                    closed_buf.append(record)
                    total_closed += 1

                    key = f"{record['exchange']}:{record['symbol']}"
                    candle_range = record["high"] - record["low"]
                    signal_buf.extend(detect_signals(record, stats))
                    stats.add(key, record["volume"], candle_range)

            # Flush batches
            elapsed = time.monotonic() - last_flush
            should_flush = len(closed_buf) >= BATCH_SIZE or elapsed >= FLUSH_EVERY

            if should_flush and (latest_buf or closed_buf):
                try:
                    upsert_latest(ch, latest_buf)
                    insert_bronze(ch,  closed_buf)
                    insert_silver(ch,  closed_buf)
                    if signal_buf:
                        insert_signals(ch, signal_buf)
                        for s in signal_buf:
                            log.info("SIGNAL [%s] %s  value=%.3f  %s",
                                     s["signal_type"], s["symbol"],
                                     s["value"], s["description"])

                    log.info("Flushed  ticks=%d  closed=%d  signals=%d  "
                             "(total ticks=%d closed=%d)",
                             len(latest_buf), len(closed_buf), len(signal_buf),
                             total_msgs, total_closed)
                except Exception as e:
                    log.error("Flush failed: %s", e)

                latest_buf.clear()
                closed_buf.clear()
                signal_buf.clear()
                last_flush = time.monotonic()

    except KeyboardInterrupt:
        log.info("Shutting down …")
    finally:
        consumer.close()
        log.info("Consumer closed.")


if __name__ == "__main__":
    main()
