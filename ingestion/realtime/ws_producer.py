#!/usr/bin/env python3
"""
ws_producer.py — Binance WebSocket → Kafka klines producer.

Connects to Binance combined stream for configured symbols/intervals,
publishes every kline update (closed and in-progress) to the Kafka
topic `klines.raw` as JSON.

Run inside the app container:
    python ingestion/realtime/ws_producer.py
    python ingestion/realtime/ws_producer.py --symbols BTCUSDT,ETHUSDT --interval 1m
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
from datetime import datetime, timezone

import websockets
from confluent_kafka import Producer
from dotenv import load_dotenv
from pydantic import BaseModel, ValidationError

load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
    stream=sys.stdout,
)
log = logging.getLogger("ws_producer")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_KLINES     = os.getenv("KAFKA_TOPIC_KLINES_RAW",  "klines.raw")
EXCHANGE_ID      = os.getenv("DEFAULT_EXCHANGE",         "binance")
SYMBOLS_ENV      = os.getenv("DEFAULT_SYMBOLS",          "BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT,XRPUSDT")
INTERVAL         = os.getenv("DEFAULT_KLINE_INTERVAL",   "1m")

TOPIC_DLQ        = os.getenv("KAFKA_TOPIC_KLINES_DLQ",  "klines.dlq")

BINANCE_WS_BASE  = "wss://stream.binance.com:9443/stream"
RECONNECT_DELAY  = 5   # seconds between reconnect attempts


# ---------------------------------------------------------------------------
# Kafka producer
# ---------------------------------------------------------------------------

def make_producer() -> Producer:
    return Producer({
        "bootstrap.servers":  KAFKA_BOOTSTRAP,
        "acks":               "1",
        "linger.ms":          50,
        "batch.num.messages": 100,
        "compression.type":   "lz4",
    })


def delivery_report(err, msg):
    if err:
        log.warning("Delivery failed for %s: %s", msg.key(), err)


def _send_to_dlq(producer: Producer, raw: bytes, reason: str, error: str) -> None:
    """Forward an unprocessable WebSocket message to the dead-letter topic."""
    import base64
    from datetime import datetime, timezone
    payload = json.dumps({
        "reason":   reason,
        "error":    error,
        "raw_b64":  base64.b64encode(raw).decode(),
        "topic":    TOPIC_KLINES,
        "ts_utc":   datetime.now(timezone.utc).isoformat(),
    }).encode()
    try:
        producer.produce(TOPIC_DLQ, value=payload)
        producer.poll(0)
    except Exception as exc:
        log.error("Failed to write to DLQ (message lost permanently): %s", exc)


# ---------------------------------------------------------------------------
# Data contract — Pydantic models for Binance WebSocket kline schema
# ---------------------------------------------------------------------------

class _KlinePayload(BaseModel):
    s: str    # symbol (e.g. "BTCUSDT")
    i: str    # interval
    t: int    # open_time ms
    T: int    # close_time ms
    o: float  # open
    h: float  # high
    l: float  # low   # noqa: E741
    c: float  # close
    v: float  # volume
    q: float  # quote_volume
    n: int    # trade_count
    x: bool   # is_closed
    model_config = {"extra": "ignore"}


class _KlineEvent(BaseModel):
    e: str           # event type — expected "kline"
    E: int           # event time ms
    k: _KlinePayload
    model_config = {"extra": "ignore"}


# ---------------------------------------------------------------------------
# Message parsing
# ---------------------------------------------------------------------------

INTERVAL_MS = {
    "1m": 60_000, "3m": 180_000, "5m": 300_000, "15m": 900_000,
    "30m": 1_800_000, "1h": 3_600_000, "4h": 14_400_000, "1d": 86_400_000,
}

# Quote currencies in priority order (longest suffix first to avoid partial match).
_QUOTE_CURRENCIES = ["USDT", "BUSD", "USDC", "BTC", "ETH", "BNB"]


def _normalize_symbol(raw: str) -> str:
    """Convert Binance symbol (e.g. 'BTCUSDT') to 'BTC/USDT' format."""
    s = raw.upper()
    for quote in _QUOTE_CURRENCIES:
        if s.endswith(quote) and len(s) > len(quote):
            return f"{s[:-len(quote)]}/{quote}"
    return s  # unknown quote currency — return as-is


def parse_kline(msg: dict) -> dict | None:
    """Parse Binance kline stream message into a unified record.

    Returns None for non-kline events (normal, silent skip).
    Raises ValidationError if the event is a kline but schema is wrong.
    """
    data = msg.get("data", {})
    if data.get("e") != "kline":
        return None
    event = _KlineEvent.model_validate(data)
    k = event.k
    return {
        "exchange":     EXCHANGE_ID,
        "symbol":       _normalize_symbol(k.s),
        "interval":     k.i,
        "open_time":    k.t,
        "close_time":   k.T,
        "open":         k.o,
        "high":         k.h,
        "low":          k.l,
        "close":        k.c,
        "volume":       k.v,
        "quote_volume": k.q,
        "trade_count":  k.n,
        "is_closed":    k.x,
        "ts":           event.E,
    }


# ---------------------------------------------------------------------------
# WebSocket loop
# ---------------------------------------------------------------------------

async def stream(producer: Producer, dlq_producer: Producer, symbols: list[str], interval: str) -> None:
    streams = "/".join(f"{s.lower()}@kline_{interval}" for s in symbols)
    url = f"{BINANCE_WS_BASE}?streams={streams}"
    log.info("Connecting to: %s", url)

    msg_count = 0
    async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
        log.info("Connected — streaming %d symbols @ %s", len(symbols), interval)
        async for raw in ws:
            raw_bytes = raw.encode() if isinstance(raw, str) else raw
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError as exc:
                log.warning("JSON decode error — routing to DLQ: %s", exc)
                _send_to_dlq(dlq_producer, raw_bytes, "json_decode_error", str(exc))
                continue

            try:
                record = parse_kline(msg)
            except ValidationError as exc:
                log.warning("Schema validation failed — routing to DLQ: %s", exc)
                _send_to_dlq(dlq_producer, raw_bytes, "schema_validation_error", str(exc))
                continue

            if record is None:
                continue

            key = f"{record['exchange']}:{record['symbol']}:{record['interval']}"
            producer.produce(
                topic=TOPIC_KLINES,
                key=key.encode(),
                value=json.dumps(record).encode(),
                callback=delivery_report,
            )
            producer.poll(0)  # non-blocking flush of callbacks

            msg_count += 1
            if msg_count % 100 == 0:
                log.info("Published %d messages (last: %s close=%.4f closed=%s)",
                         msg_count, record["symbol"], record["close"], record["is_closed"])


async def run_with_reconnect(producer: Producer, dlq_producer: Producer, symbols: list[str], interval: str) -> None:
    """Reconnect loop — keeps the producer alive despite transient WS errors."""
    while True:
        try:
            await stream(producer, dlq_producer, symbols, interval)
        except (websockets.ConnectionClosed, OSError, asyncio.TimeoutError) as e:
            log.warning("WebSocket disconnected: %s — reconnecting in %ds", e, RECONNECT_DELAY)
        except Exception as e:
            log.error("Unexpected error: %s — reconnecting in %ds", e, RECONNECT_DELAY)
        await asyncio.sleep(RECONNECT_DELAY)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Binance WebSocket → Kafka klines producer")
    p.add_argument("--symbols",  default=SYMBOLS_ENV,
                   help="comma-separated Binance symbols (default: %(default)s)")
    p.add_argument("--interval", default=INTERVAL,
                   help="kline interval (default: %(default)s)")
    return p.parse_args()


def main() -> None:
    args  = parse_args()
    syms  = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    log.info("ws_producer starting  symbols=%s  interval=%s  kafka=%s  topic=%s",
             syms, args.interval, KAFKA_BOOTSTRAP, TOPIC_KLINES)

    producer     = make_producer()
    dlq_producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP, "acks": "1"})
    try:
        asyncio.run(run_with_reconnect(producer, dlq_producer, syms, args.interval))
    except KeyboardInterrupt:
        log.info("Shutting down …")
    finally:
        producer.flush(timeout=10)
        dlq_producer.flush(timeout=5)
        log.info("Kafka producers flushed. Bye.")


if __name__ == "__main__":
    main()
