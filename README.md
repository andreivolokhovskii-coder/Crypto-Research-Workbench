# Crypto Research Workbench

A self-hosted, open-source platform for collecting, normalising, and exploring crypto market data.
One command brings up the full stack; data starts flowing within seconds.

---

## What it does

| Capability | Details |
|---|---|
| **Historical ingestion** | Pulls OHLCV klines from any ccxt exchange, stores bronze → silver → gold via medallion architecture |
| **Live streaming** | Binance WebSocket → Kafka → ClickHouse in real time, signals emitted on each closed candle |
| **Analytical layer** | dbt models: `fact_candles`, `mart_volatility` (rolling 7d/30d vol, ATR), `dim_coin` |
| **Research queries** | 10 saved SQL scans: unusual volume, movers, breakout candidates, regime summary, data health |
| **Notebooks** | 3 Jupyter notebooks: daily market scan, asset deep-dive, cross-asset comparison |
| **Orchestration** | Airflow DAGs: daily pipeline, historical backfill, data quality checks |
| **Dashboards** | Superset with ClickHouse datasets pre-configured |
| **Signals** | `volume_spike`, `large_candle` — auto-detected on every closed candle |

---

## Quick start

```bash
git clone https://github.com/your-username/crypto-research-workbench
cd crypto-research-workbench

cp .env.example .env      # adjust passwords if needed
docker compose up -d      # pull + start all services (~3 min first run)

# Load 30 days of historical data
docker compose run --rm app python ingestion/historical/klines_backfill.py

# Build analytical models
docker compose run --rm dbt sh -c "dbt deps && dbt build"

# Start live streaming
docker compose up -d ws-producer stream-consumer
```

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         DATA SOURCES                                │
│   Binance REST API (ccxt)          Binance WebSocket                │
│   CoinGecko REST API               (5 symbols, 1m klines)          │
└──────────────┬────────────────────────────────┬────────────────────┘
               │ historical                     │ real-time
               ▼                                ▼
┌──────────────────────────┐       ┌────────────────────────┐
│   klines_backfill.py     │       │    ws_producer.py      │
│   coingecko_dims.py      │       │   (WebSocket → Kafka)  │
└──────────────┬───────────┘       └────────────┬───────────┘
               │                                │
               ▼                                ▼
┌──────────────────────────────────────────────────────────────────┐
│                    STORAGE LAYER                                  │
│                                                                   │
│  MinIO (S3-compatible)            Kafka (KRaft, 5 partitions)    │
│  ├── bronze/klines/               topic: klines.raw              │
│  ├── silver/klines/               topic: trades.raw              │
│  └── bronze/metadata/                                            │
└──────────────┬──────────────────────────────┬────────────────────┘
               │                              │
               ▼                              ▼
┌─────────────────────────┐    ┌──────────────────────────────┐
│  ClickHouse (medallion) │    │   klines_consumer.py         │
│                         │    │   (Kafka → ClickHouse)       │
│  BRONZE                 │◄───┤                              │
│  ├── bronze_klines      │    │   Writes:                    │
│  └── bronze_coin_meta   │    │   ├── rt_latest_kline        │
│                         │    │   ├── bronze_klines          │
│  SILVER                 │    │   ├── silver_klines          │
│  ├── silver_klines      │    │   └── rt_signals             │
│  └── silver_coin_meta   │    └──────────────────────────────┘
│                         │
│  GOLD (dbt managed)     │
│  ├── fact_candles       │
│  ├── mart_volatility    │
│  ├── dim_coin           │
│  └── stg_klines (view)  │
│                         │
│  REALTIME               │
│  ├── rt_latest_kline    │
│  └── rt_signals         │
└──────────────┬──────────┘
               │
               ▼
┌─────────────────────────────────────────────────────────────────┐
│                    SERVING LAYER                                 │
│                                                                  │
│  Superset (dashboards)    JupyterLab (notebooks)                │
│  localhost:8088           localhost:8888                        │
│                                                                  │
│  SQL Lab                  Airflow (orchestration)               │
│  (10 research queries)    localhost:8085                        │
└─────────────────────────────────────────────────────────────────┘
```

---

## Service URLs

| Service | URL | Credentials |
|---|---|---|
| Airflow | http://localhost:8085 | admin / admin |
| Superset | http://localhost:8088 | admin / admin |
| JupyterLab | http://localhost:8888 | no auth |
| MinIO Console | http://localhost:9002 | minioadmin / see .env |
| Spark Master UI | http://localhost:8081 | — |
| ClickHouse HTTP | http://localhost:8123 | crypto_user / see .env |

---

## Stack

| Component | Technology |
|---|---|
| Message broker | Kafka 3.6 (KRaft, no ZooKeeper) |
| Analytical DB | ClickHouse 23.8 |
| Object storage | MinIO (S3-compatible) |
| Batch processing | Apache Spark 3.5 |
| Orchestration | Apache Airflow 2.8 |
| Transformation | dbt-core 1.7 + dbt-clickhouse |
| Dashboards | Apache Superset 3.0 |
| Notebooks | JupyterLab 4 |
| Ingestion | Python 3.11 + ccxt + confluent-kafka + websockets |
| Metadata storage | PostgreSQL 15 (Airflow backend) |

---

## Data model

### Medallion layers

```
bronze_klines       raw ms-timestamps, append-only
silver_klines       UTC DateTime, ReplacingMergeTree (idempotent loads)
fact_candles        + price_change_pct, candle_range, is_bullish   (dbt)
mart_volatility     7d/30d annualised vol, 14-day ATR              (dbt)
dim_coin            market_cap_rank, coingecko_id                  (dbt)
rt_latest_kline     latest tick per symbol, updated in real time
rt_signals          volume_spike / large_candle, TTL 7 days
```

### Default universe

`BTCUSDT · ETHUSDT · SOLUSDT · BNBUSDT · XRPUSDT` — change via `DEFAULT_SYMBOLS` in `.env`.

---

## Research queries

Pre-built SQL files in `queries/`:

| File | What it finds |
|---|---|
| `01_unusual_volume.sql` | Volume z-score > 2 in the last hour |
| `02_realized_volatility_scan.sql` | 7d/30d vol ranking + expansion ratio |
| `03_top_movers.sql` | Biggest % moves in last 24h |
| `04_price_volume_divergence.sql` | Price/volume divergence patterns |
| `05_intraday_range_anomalies.sql` | Candles with range z-score > 3 |
| `06_market_regime_summary.sql` | Regime classification per symbol |
| `07_long_wick_scan.sql` | Wicks > 2× body size |
| `08_active_symbols_scan.sql` | 1h / 4h / 24h activity snapshot |
| `09_breakout_candidates.sql` | Near 30d high/low with rising volume |
| `10_data_health.sql` | Freshness and completeness check |

Paste any of these into Superset SQL Lab → http://localhost:8088/superset/sqllab/

---

## Airflow DAGs

| DAG | Schedule | Description |
|---|---|---|
| `daily_pipeline` | every 6h | incremental klines + metadata + dbt build |
| `data_quality` | daily 02:00 UTC | freshness + dbt test + row count checks |
| `historical_backfill` | manual | full backfill, parameterised via UI |

---

## Notebooks

`notebooks/` — open in JupyterLab at http://localhost:8888:

- `01_daily_market_scan.ipynb` — movers, vol snapshot, regime map
- `02_asset_deep_dive.ipynb` — candlestick chart, rolling vol, return distribution, volume profile
- `03_cross_asset_comparison.ipynb` — rebased performance, correlation matrix, risk/return scatter

---

## Makefile targets

```bash
make up               # docker compose up -d
make down             # docker compose down
make backfill         # run historical klines backfill
make metadata-refresh # refresh CoinGecko coin metadata
make dbt-build        # run dbt build inside container
make dbt-test         # run dbt tests
make clickhouse-client# open ClickHouse shell
make pytest           # run Python test suite
make lint             # ruff lint
```

---

## Configuration

All settings are in `.env` (copy from `.env.example`).
Key variables:

```ini
DEFAULT_SYMBOLS=BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT,XRPUSDT
DEFAULT_KLINE_INTERVAL=1m
DEFAULT_BACKFILL_DAYS=30
DEFAULT_EXCHANGE=binance
```

---

## Project layout

```
├── ingestion/
│   ├── historical/klines_backfill.py   historical OHLCV pipeline
│   ├── metadata/coingecko_dims.py      coin metadata from CoinGecko
│   └── realtime/
│       ├── ws_producer.py              Binance WS → Kafka
│       └── klines_consumer.py          Kafka → ClickHouse + signals
├── dbt/
│   ├── models/staging/                 stg_klines (view)
│   └── models/marts/                   fact_candles, mart_volatility, dim_coin
├── airflow/dags/                       daily_pipeline, data_quality, historical_backfill
├── clickhouse/ddl/                     schema DDL (bronze/silver/gold/rt)
├── spark_jobs/                         batch Spark jobs (extensible)
├── notebooks/                          3 research notebooks
├── queries/                            10 saved SQL queries
├── docker/                             Dockerfiles + entrypoints
└── docker-compose.yml                  full stack definition
```

---

## Adding a new exchange

1. Check ccxt supports it: `python -c "import ccxt; print('okx' in dir(ccxt))"`
2. Set `DEFAULT_EXCHANGE=okx` in `.env`
3. Adjust `DEFAULT_SYMBOLS` to the exchange's symbol format
4. Re-run `make backfill`

---

## License

MIT — see [LICENSE](LICENSE).
