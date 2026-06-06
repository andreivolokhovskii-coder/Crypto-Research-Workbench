# Resume / Portfolio Bullets

Concise bullets for CV, LinkedIn, or interview talking points.

---

## One-liner

> Built a self-hosted crypto research platform (ClickHouse + Kafka + Spark + Airflow + dbt + Superset) processing 200 k+ candles historically and streaming live market data from Binance WebSocket in real time.

---

## Data Engineering bullets

- Designed and implemented a **medallion data architecture** (bronze → silver → gold) for crypto OHLCV data using **ClickHouse** as the analytical serving layer, achieving sub-second query latency on 200 k+ row fact tables.

- Built a **real-time streaming pipeline**: Binance WebSocket → **Kafka** (KRaft, 5 partitions) → Python consumer → ClickHouse, with automated signal detection (volume z-score, ATR-based anomalies) on every closed candle.

- Developed a **dbt transformation layer** with 4 models, 21 automated tests (not_null, accepted_values, uniqueness), and documented lineage — `fact_candles`, `mart_volatility` (7d/30d annualised vol, 14-day ATR), `dim_coin`.

- Engineered an **idempotent historical ingestion pipeline** (ccxt + pandas + pyarrow) with paginated API fetching, Parquet storage in MinIO and parallel ClickHouse bulk inserts — safe to re-run without duplicates (ReplacingMergeTree).

- Orchestrated the full data lifecycle with **Apache Airflow 2.8**: daily incremental pipeline, metadata refresh, dbt build, and data quality DAG with freshness + row-count checks.

- Containerised the entire platform with **Docker Compose** (13 services): ClickHouse, Kafka, Spark, MinIO, Airflow, Superset, Jupyter — reproducible one-command startup on any machine.

---

## Platform / Infrastructure bullets

- Replaced Bitnami images (retired from Docker Hub) with official Apache Spark 3.5 and Confluent Platform Kafka 7.6 images; resolved Windows CRLF entrypoint issues and Superset CSP headers blocking React rendering.

- Configured **Kafka KRaft mode** (no ZooKeeper) with Confluent Platform, partition-keyed by exchange:symbol for ordered per-symbol processing.

- Integrated **MinIO** as a local S3-compatible bronze/silver data lake with separate buckets per medallion layer and Parquet+Snappy compression.

---

## Analytics / Research bullets

- Wrote 10 reusable **SQL research queries** (unusual volume scan, breakout candidates, regime classification, long-wick detector, price/volume divergence) that run directly in Superset SQL Lab against live ClickHouse data.

- Created 3 **Jupyter notebooks** (daily market scan, asset deep-dive, cross-asset comparison) using Plotly for interactive charts — connected directly to ClickHouse via clickhouse-connect.

- Implemented **rolling realized volatility** (annualised 7d/30d stddev of log returns) and **Average True Range** using ClickHouse window functions (`lagInFrame`, `stddevPop OVER`).

---

## Project scale

| Metric | Value |
|---|---|
| Historical candles loaded | 216,000+ (30 days × 5 symbols × 1m) |
| Live tick rate | ~5 msg/s (5 symbols) |
| dbt tests | 21 / 21 passing |
| Docker services | 13 |
| Lines of Python | ~1,500 |
| Airflow DAGs | 3 |
| SQL research queries | 10 |
| Jupyter notebooks | 3 |
