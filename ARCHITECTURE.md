# Crypto Research Workbench — Architecture Guide

> A reference Data Engineering project: from raw exchange data to an analytics dashboard in a single command.

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Technology Stack](#2-technology-stack)
3. [Medallion Architecture](#3-medallion-architecture)
4. [System Components](#4-system-components)
5. [Data Flows](#5-data-flows)
6. [One-Command Deploy](#6-one-command-deploy)
7. [Orchestration with Airflow](#7-orchestration-with-airflow)
8. [Transformations: dbt and Spark](#8-transformations-dbt-and-spark)
9. [Analytics: Superset](#9-analytics-superset)
10. [Key Architectural Decisions](#10-key-architectural-decisions)

---

## 1. Project Overview

### What We Build

A full-stack platform for collecting, storing, and analyzing cryptocurrency market data. The project covers the entire data lifecycle:

```
Binance Exchange
    ↓
Data Ingestion (REST + WebSocket)
    ↓
Raw Data Storage (MinIO + ClickHouse)
    ↓
Streaming (Kafka)
    ↓
Transformations (dbt + Spark)
    ↓
Analytics (Superset)
    ↓
Orchestration (Airflow)
```

### What the System Does

- Downloads 30 days of history for 5 symbols (BTC/ETH/SOL/BNB/XRP) on deploy
- Streams live data from the exchange via WebSocket
- Detects anomalies in real time: volume spikes and abnormally large candles
- Computes rolling volatility metrics via Apache Spark
- Classifies market regimes (trending / volatile / ranging)
- Renders everything on an interactive dashboard — fully automatic, no manual clicks

---

## 2. Technology Stack

### Tools Overview

| Layer | Tool | Version | Purpose |
|-------|------|---------|---------|
| Analytical DB | ClickHouse | 23.8 LTS | Primary columnar store |
| Object Storage | MinIO | latest | S3-compatible storage for Parquet files |
| Operational DB | PostgreSQL | 15 | Airflow metadata |
| Message Queue | Apache Kafka | 7.6 (KRaft) | Event streaming without ZooKeeper |
| Batch Processing | Apache Spark | 3.5.3 | Heavy computations, horizontal scaling |
| Transformations | dbt | 1.7.1 | SQL transformations with tests and lineage |
| Orchestration | Apache Airflow | 2.8 | Scheduling and pipeline monitoring |
| Analytics | Apache Superset | 3.0.3 | BI dashboards |
| Notebooks | JupyterLab | latest | Exploratory analysis |
| Containerization | Docker Compose | v2 | Full stack with one command |

### Why These Tools

**ClickHouse over PostgreSQL for analytics**

PostgreSQL is a row-oriented OLTP database. ClickHouse is a columnar OLAP database.
On a query like `SELECT AVG(close) FROM silver_klines GROUP BY symbol, toDate(open_time)`,
ClickHouse reads only the `close`, `symbol`, and `open_time` columns — everything else stays on disk.
At 200k rows the difference is negligible; at 200M rows it is critical.

**MinIO over local disk**

MinIO implements the S3 API. Code that writes to MinIO works unchanged against AWS S3 or GCS —
just swap the endpoint in `.env`. This makes the project production-portable by design.

**Kafka over direct writes**

Kafka provides: buffering under peak load, multiple independent consumers, guaranteed delivery,
and the ability to replay any topic from any offset. If ClickHouse goes down, messages wait in
the topic and are processed after recovery.

**dbt over raw SQL**

dbt is not just SQL — it adds versioning, automated tests, documentation, and a dependency graph.
`dbt build` runs transformations in the correct order and verifies data quality automatically.

**Spark alongside dbt**

This is not duplication — it is separation of concerns. dbt handles SQL transformations inside
ClickHouse: ETL, aggregations, enrichment. Spark handles computations that scale horizontally:
rolling window metrics across 90 days of per-minute data for all symbols. Add more workers —
compute time stays constant.

---

## 3. Medallion Architecture

### Concept

The Medallion Architecture is the industry standard for organizing data in a lakehouse.
Data passes through three levels of refinement:

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   BRONZE    │───▶│   SILVER    │───▶│    GOLD     │
│  Raw Data   │    │  Cleaned    │    │  Analytics  │
│  Append-only│    │  Normalized │    │  Aggregated │
└─────────────┘    └─────────────┘    └─────────────┘
```

### Bronze — Immutability

Data exactly as it arrived from the exchange. Millisecond timestamps, raw fields. **Never delete. Never modify.**

```sql
CREATE TABLE bronze_klines (
    ingested_at     DateTime DEFAULT now(),
    exchange        LowCardinality(String),
    symbol          LowCardinality(String),
    interval        LowCardinality(String),
    open_time       Int64,           -- Unix milliseconds as returned by the API
    open            Float64,
    high            Float64,
    low             Float64,
    close           Float64,
    volume          Float64,
    _source_file    String           -- path to the Parquet file in MinIO
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(_partition_date)
ORDER BY (exchange, symbol, interval, open_time);
```

Why keep raw data? If a transformation had a bug, replay from bronze. Bronze is the source of truth
from which the entire pipeline can be reconstructed.

### Silver — Idempotency via ReplacingMergeTree

Basic normalization: `Int64` milliseconds → `DateTime` UTC, correct types.
The critical choice is the table engine:

```sql
CREATE TABLE silver_klines (
    exchange    LowCardinality(String),
    symbol      LowCardinality(String),
    interval    LowCardinality(String),
    open_time   DateTime,            -- normalized UTC DateTime
    close_time  DateTime,
    open        Float64,
    ...
) ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (exchange, symbol, interval, open_time);
```

`ReplacingMergeTree(ingested_at)` deduplicates on the primary key, keeping the row with
the highest `ingested_at`. The pipeline can run twice without producing duplicates.
`SELECT ... FINAL` forces an in-flight merge when querying.

### Gold — dbt Builds Analytics

```
silver_klines
    └── stg_klines (view — thin wrapper with FINAL)
            ├── fact_candles    (enriched: price_change_pct, is_bullish)
            └── mart_volatility (rolling vol 7d/30d, ATR-14)
```

dbt builds the dependency graph automatically from `{{ ref('stg_klines') }}`
and runs models in the correct order. Tests are declared in `schema.yml`:

```yaml
- name: is_bullish
  tests:
    - accepted_values:
        values: [0, 1]   # dbt generates the SQL assertion automatically
```

### Real-Time Layer

A separate layer for very low-latency data:

```sql
rt_latest_kline  -- current price per symbol, updated on every tick
rt_signals       -- trade signals with a 7-day TTL (MergeTree + TTL clause)
```

---

## 4. System Components

### Ingestion: Historical Data

**File:** `ingestion/historical/klines_backfill.py`

Uses `ccxt` — a universal client for 100+ exchanges.
The same code works with Binance, Bybit, OKX, and Coinbase.

```python
# Paginated fetch with deduplication
exchange = ccxt.binance()
seen = set()
while start < end:
    candles = exchange.fetch_ohlcv(symbol, timeframe, since=start, limit=1000)
    fresh = [c for c in candles if c[0] not in seen]
    seen.update(c[0] for c in fresh)
    start = candles[-1][0] + 1
```

Output goes to two destinations in parallel:
- **MinIO** — Parquet with Snappy compression, date-partitioned. Long-term storage, replay source.
- **ClickHouse** — bronze + silver for immediate queries.

### Ingestion: Streaming

**WebSocket Producer** (`ingestion/realtime/ws_producer.py`)

```
Binance WS → [Pydantic validation] → Kafka topic: klines.raw
```

Pydantic validates every message at the boundary. Invalid messages go to a Dead Letter Queue
(`klines.dlq`) rather than being silently dropped. Reconnect uses exponential backoff: 1s, 2s, 4s … up to 60s.

**Kafka Consumer** (`ingestion/realtime/klines_consumer.py`)

Key pattern: **at-least-once delivery with manual offset commit**.

```
klines.raw → [batch: 50 closed candles OR 10 seconds, whichever comes first]
    ├── rt_latest_kline  — every tick, including open candles
    ├── bronze_klines    — closed candles only
    ├── silver_klines    — closed candles only
    └── rt_signals       — emitted when a detector fires
         ↓
    commit offset — ONLY after a successful ClickHouse flush
```

Real-time anomaly detection (rolling window of 60 candles):
- **volume_spike**: volume z-score > 2.5σ
- **large_candle**: candle range > 3× ATR-14

### Ingestion: Metadata (CoinGecko)

**File:** `ingestion/metadata/coingecko_dims.py`

Fetches the top-100 coins with rank, categories, and market cap to populate `dim_coin`.
Runs daily via Airflow with rate limiting: 1.5 s between requests on the free tier.

---

## 5. Data Flows

### Historical Pipeline (Batch)

```
make deploy
    │
    ├── klines_backfill.py
    │   5 symbols × 30 days × 1440 candles/day ≈ 216k rows
    │   ├──▶ MinIO: s3://bronze/klines/binance/BTCUSDT/1m/date=2024-05-01/data.parquet
    │   └──▶ ClickHouse: bronze_klines + silver_klines
    │
    ├── coingecko_dims.py
    │   └──▶ ClickHouse: bronze_coin_metadata + silver_coin_metadata
    │
    ├── dbt build
    │   ├──▶ stg_klines      (view over silver_klines FINAL)
    │   ├──▶ fact_candles    (enriched candles with derived metrics)
    │   ├──▶ dim_coin        (latest snapshot per coin_id)
    │   └──▶ mart_volatility (rolling vol 7d/30d + ATR-14)
    │
    └── spark-submit volatility_batch.py
        └──▶ mart_market_regime (daily OHLCV + vol metrics + regime label)
```

### Real-Time Pipeline (Streaming)

```
Binance WebSocket (5 symbols, combined stream)
    │
    ▼
ws_producer.py
    │  {"symbol": "BTCUSDT", "close": 67123.5, "volume": 12.4, "is_closed": true}
    ▼
Kafka topic: klines.raw  (7-day retention)
    │
    ▼
klines_consumer.py
    ├── rt_latest_kline  ──▶ Superset "Live Prices"
    ├── silver_klines    ──▶ source for the next dbt run
    └── rt_signals       ──▶ Superset "Trading Signals"
```

### Daily Pipeline (Airflow)

```
Every 6 h  (00:00, 06:00, 12:00, 18:00 UTC):
    [incremental_klines, metadata_refresh] ──▶ dbt_build

Every 6 h, +30 min offset (00:30, 06:30, 12:30, 18:30 UTC):
    spark_volatility_batch ──▶ mart_market_regime

Daily at 02:00 UTC:
    freshness_check ──▶ dbt_test ──▶ row_count_check
```

---

## 6. One-Command Deploy

### setup.sh — Secret Generation

```bash
# Cryptographically strong passwords — no default "admin/admin"
gen_pass()   { openssl rand -hex 24; }    # 48 hex characters
gen_fernet() { python3 -c "import base64, os;
    print(base64.urlsafe_b64encode(os.urandom(32)).decode())"; }

# Use | as the sed delimiter — base64 output contains forward slashes
sed -i "s|change_me_clickhouse_password|${CLICKHOUSE_PASSWORD}|g" .env
```

Every deploy gets unique passwords. `.env` is in `.gitignore` and never reaches git.
Only `.env.example` with placeholder values is committed.

### Makefile Deploy Sequence

```makefile
deploy:
    @bash setup.sh
    DOCKER_BUILDKIT=0 $(COMPOSE) up --build -d
    # DOCKER_BUILDKIT=0 works around an IPv6 DNS resolution bug on some Linux hosts

    @until docker inspect workbench-clickhouse \
        --format='{{.State.Health.Status}}' | grep -q healthy; do sleep 2; done
    # wait for ClickHouse (healthcheck: HTTP GET /ping)

    @until docker inspect workbench-minio-init \
        --format='{{.State.Status}}' | grep -qE 'exited'; do sleep 2; done
    # wait for MinIO bucket creation (one-shot container)

    $(COMPOSE) run --rm app python ingestion/historical/klines_backfill.py
    $(COMPOSE) run --rm app python ingestion/metadata/coingecko_dims.py || true
    $(COMPOSE) run --rm dbt dbt deps && dbt build
    $(COMPOSE) exec spark-master spark-submit volatility_batch.py || true
```

### Service Start Order (depends_on)

```yaml
airflow-webserver:
  depends_on:
    postgres:
      condition: service_healthy                  # waits for pg_isready
    airflow-init:
      condition: service_completed_successfully   # waits for exit 0
```

`service_completed_successfully` is used for one-shot init containers.
`service_healthy` is used for long-running services with a healthcheck.

---

## 7. Orchestration with Airflow

### Architecture

**LocalExecutor** — all tasks run as separate processes on the same machine.
For production with higher throughput: CeleryExecutor (multiple workers) or KubernetesExecutor (one pod per task).

### DAG: daily_pipeline — Fan-In Pattern

```python
# Credentials read from the container environment at DAG parse time
COMMON_ENV = {
    "CLICKHOUSE_PASSWORD": os.environ.get("CLICKHOUSE_PASSWORD", ""),
}

# Two independent tasks run in parallel, dbt runs only after both succeed
[incremental_klines, metadata_refresh] >> dbt_build
```

### DAG: spark_batch — SparkSubmitOperator

```python
volatility_batch = SparkSubmitOperator(
    conn_id="spark_default",   # connection created automatically by airflow-init
    application="/app/spark_jobs/volatility_batch.py",
    packages="com.clickhouse:clickhouse-jdbc:0.6.5",
    conf={"spark.executor.memory": "1g"},
)
```

The +30-minute offset guarantees that the dbt pipeline has already completed.

### DAG: data_quality — Sequential Checks

```python
freshness_check >> dbt_test >> row_count_check
```

If data is stale (>2 h without updates), dbt tests are skipped — no point testing a broken layer.

### Resources Created Automatically by airflow-init

```bash
# Spark connection — without this, spark_batch fails immediately
airflow connections add spark_default \
    --conn-type spark --conn-host "spark://spark-master" --conn-port 7077

# Variables — belt-and-suspenders for any DAG using var.value.get()
airflow variables set CLICKHOUSE_PASSWORD "${CLICKHOUSE_PASSWORD}"
airflow variables set MINIO_ROOT_PASSWORD  "${MINIO_ROOT_PASSWORD}"
```

---

## 8. Transformations: dbt and Spark

### dbt: SQL with Tests and Lineage

dbt does not execute computations — it generates and runs SQL inside ClickHouse.

```
dbt/
├── models/
│   ├── staging/
│   │   ├── stg_klines.sql     -- view: thin wrapper over silver_klines FINAL
│   │   └── sources.yml        -- source declaration
│   └── marts/
│       ├── fact_candles.sql   -- enriched candles (price_change_pct, is_bullish)
│       ├── mart_volatility.sql
│       └── schema.yml         -- not_null, accepted_values tests
├── macros/
└── profiles.yml               -- credentials via env_var()
```

### mart_volatility: Rolling Window in ClickHouse SQL

```sql
-- Annualized realized volatility — quant finance standard
round(
    stddevSamp(log_return) over (
        partition by exchange, symbol
        order by trade_date
        rows between 6 preceding and current row   -- 7-day window
    ) * sqrt(365), 6                               -- annualize
) as realized_vol_7d
```

`log(close / prev_close)` is the log return. It approximates a normal distribution
better than the simple percentage change `(close - prev) / prev`.

### Spark: Market Regime Classification

```python
window_7d = Window.partitionBy("exchange", "symbol") \
                  .orderBy("trade_date") \
                  .rowsBetween(-6, 0)

df = df.withColumn(
    "vol_7d",
    F.stddev_samp("log_return").over(window_7d) * F.sqrt(F.lit(365))
)

regime = F.when(F.col("vol_7d") > F.lit(1.5) * F.col("vol_30d"), "volatile") \
          .when((F.col("close") - F.col("sma_20")) / F.col("sma_20") > 0.02, "trending_up") \
          .when((F.col("sma_20") - F.col("close")) / F.col("sma_20") > 0.02, "trending_down") \
          .otherwise("ranging")
```

### dbt vs Spark — Separation of Concerns

| | dbt | Spark |
|---|---|---|
| Computation type | SQL transformations | Distributed computation |
| Scaling | Vertical (ClickHouse) | Horizontal (add workers) |
| Best for | ETL, aggregations, joins | Heavy window functions at scale |
| Testing | Built-in schema tests | PySpark unit tests |
| Used for | fact_candles, mart_volatility | mart_market_regime |

---

## 9. Analytics: Superset

### Auto-Configuration on Container Start

Superset is fully configured via a Python script in `docker/superset/entrypoint.sh`.
No manual clicks in the UI after deploy:

```python
app = create_app()
with app.app_context():
    # 1. Always recreate the DB connection (password may have changed on redeploy)
    database = Database(
        database_name="ClickHouse",
        sqlalchemy_uri=f"clickhouse+http://{user}:{pw}@{host}:{port}/{db}"
    )

    # 2. Create a pre-aggregated view to work around the double-grain bug
    _ch_exec("""
        CREATE OR REPLACE VIEW crypto.v_daily_klines AS
        SELECT exchange, symbol,
               toDate(open_time)            AS trade_date,
               argMin(open,  open_time)     AS day_open,
               max(high)                    AS day_high,
               min(low)                     AS day_low,
               argMax(close, open_time)     AS day_close,
               sum(volume)                  AS day_volume
        FROM crypto.silver_klines
        WHERE interval = '1m'
        GROUP BY exchange, symbol, trade_date
    """)

    # 3. Register datasets and sync column metadata
    # 4. Create dashboard (version-gated — only recreated when DASHBOARD_V changes)
```

### The Double-Grain Bug Fix

ClickHouse strict mode requires every non-aggregated SELECT column to appear in GROUP BY.
`clickhouse-sqlalchemy` applied the time grain function twice, producing:

```sql
-- What ClickHouse received (NOT_AN_AGGREGATE error):
GROUP BY toStartOfDay(toDateTime(toStartOfDay(toDateTime(open_time))))

-- Fix: pre-aggregated view + time_grain_sqla=None in chart params
-- No grain function is applied; data is already at the target granularity.
SELECT trade_date, symbol, AVG(day_close)
FROM v_daily_klines
GROUP BY trade_date, symbol   -- valid ClickHouse GROUP BY
```

### Dashboard Layout

| Row | Chart 1 | Chart 2 | Data Source |
|-----|---------|---------|-------------|
| 1 | Price History (line, all symbols) | Volume History (bar) | v_daily_klines |
| 2 | Realized Volatility 7d | Market Regime (table) | mart_volatility, mart_market_regime |
| 3 | Live Prices (table) | Trading Signals (table) | rt_latest_kline, rt_signals |

---

## 10. Key Architectural Decisions

### Idempotency — Every Step Is Replayable

- `ReplacingMergeTree` deduplicates repeated inserts in ClickHouse
- `dbt build` recreates tables via atomic `EXCHANGE TABLES` (no downtime)
- `CREATE OR REPLACE VIEW` — safe view recreation
- Superset dashboard is version-gated via `json_metadata.init_version`

### Secret Management

```
.env.example  → committed to git (placeholder values only)
.env          → in .gitignore, never committed
setup.sh      → generates unique passwords on every deploy

dbt profiles.yml:  credentials via {{ env_var('CLICKHOUSE_PASSWORD') }}
Airflow DAGs:      credentials via os.environ.get('CLICKHOUSE_PASSWORD')
Docker Compose:    injects from .env via environment: ${CLICKHOUSE_PASSWORD}
```

### Graceful Degradation

Non-critical steps do not block the deploy:

```makefile
$(COMPOSE) run --rm app python coingecko_dims.py \
    || echo "[warn] CoinGecko unavailable — dim_coin will be empty"

$(COMPOSE) exec spark-master spark-submit volatility_batch.py \
    || echo "[warn] Spark failed — run 'make spark-volatility' manually"
```

### Lambda Architecture — Batch and Streaming in One Layer

```
Batch (historical):   ccxt REST API → MinIO Parquet → silver_klines
Streaming (live):     Binance WS → Kafka → klines_consumer → silver_klines

Convergence point: silver_klines
    ↓
dbt and Spark read from one source regardless of how the data arrived
```

### Infrastructure as Code

The entire stack is declared, not scripted:

```
docker-compose.yml  — all services, networks, volumes, healthchecks
Makefile            — all operations (deploy, reset, backfill, logs)
setup.sh            — secret generation
clickhouse/ddl/     — database schema (applied on first start)
dbt/                — transformations with tests
airflow/dags/       — scheduling and orchestration
```

A new developer clones the repo and runs `make deploy` — everything comes up.

---

## Project File Structure

```
Crypto-Research-Workbench/
│
├── docker-compose.yml               # main service orchestrator
├── Makefile                         # entry points for all operations
├── setup.sh                         # secret generation + .env creation
├── .env.example                     # environment variable template
│
├── docker/
│   ├── app/Dockerfile               # Python ingestion / processing
│   ├── dbt/Dockerfile               # dbt-clickhouse
│   └── superset/
│       ├── Dockerfile               # Superset + clickhouse-sqlalchemy
│       └── entrypoint.sh            # auto-setup: DB, datasets, dashboard
│
├── ingestion/
│   ├── historical/klines_backfill.py    # REST API → MinIO + ClickHouse
│   ├── realtime/ws_producer.py          # WebSocket → Kafka
│   ├── realtime/klines_consumer.py      # Kafka → ClickHouse + signals
│   └── metadata/coingecko_dims.py       # CoinGecko → dim_coin
│
├── dbt/
│   ├── models/
│   │   ├── staging/stg_klines.sql
│   │   └── marts/
│   │       ├── fact_candles.sql
│   │       ├── mart_volatility.sql
│   │       └── schema.yml
│   └── profiles.yml
│
├── spark_jobs/
│   └── volatility_batch.py          # rolling vol + market regime
│
├── airflow/dags/
│   ├── daily_pipeline.py            # every 6 h: backfill + metadata + dbt
│   ├── spark_batch.py               # every 6 h (+30 min): Spark job
│   ├── data_quality.py              # daily: freshness + tests + row counts
│   └── historical_backfill.py       # manual trigger with params
│
└── clickhouse/ddl/
    ├── 01_init.sql                  # bronze + silver + gold schemas
    └── 02_streaming.sql             # rt_latest_kline + rt_signals
```

---

## Summary

This project is not just a set of connected tools. It demonstrates **principles**:

1. **Medallion Architecture** — bronze → silver → gold, each layer cleaner than the last
2. **Idempotency** — every step can be re-run without side effects
3. **Observability** — Airflow DAGs, dbt tests, data quality checks
4. **Infrastructure as Code** — the entire stack lives in git; `make deploy` and you're done
5. **Separation of Concerns** — ingestion / transformation / serving / orchestration

This is how data platforms look at real companies. The scale may differ; the principles are the same.
