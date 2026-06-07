-- =============================================================================
-- Crypto Research Workbench — ClickHouse DDL Initialization
-- Runs automatically on first container start via /docker-entrypoint-initdb.d/
--
-- Layer layout:
--   bronze_*  — raw landed data, append-only, partitioned by month
--   silver_*  — cleaned and normalized, ReplacingMergeTree for idempotent loads
--   fact_*    — gold facts (populated by dbt or Spark jobs)
--   dim_*     — gold dimensions
-- =============================================================================

CREATE DATABASE IF NOT EXISTS crypto;

-- =============================================================================
-- BRONZE LAYER — raw landed data
-- =============================================================================

-- Raw OHLCV / klines from exchange REST APIs
CREATE TABLE IF NOT EXISTS crypto.bronze_klines
(
    ingested_at        DateTime     DEFAULT now(),
    exchange           LowCardinality(String),
    symbol             LowCardinality(String),
    interval           LowCardinality(String),
    open_time          Int64,
    open               Float64,
    high               Float64,
    low                Float64,
    close              Float64,
    volume             Float64,
    close_time         Int64,
    quote_asset_volume Float64,
    trade_count        Int32,
    _source_file       String       DEFAULT '',
    _partition_date    Date         DEFAULT today()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(_partition_date)
ORDER BY (exchange, symbol, interval, open_time)
SETTINGS index_granularity = 8192;

-- Raw coin metadata snapshots
CREATE TABLE IF NOT EXISTS crypto.bronze_coin_metadata
(
    ingested_at     DateTime     DEFAULT now(),
    source          LowCardinality(String),
    coin_id         String,
    symbol          LowCardinality(String),
    name            String,
    payload         String,
    _partition_date Date         DEFAULT today()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(_partition_date)
ORDER BY (source, coin_id, ingested_at)
SETTINGS index_granularity = 8192;

-- Raw trade events from WebSocket streams
CREATE TABLE IF NOT EXISTS crypto.bronze_trades
(
    ingested_at     DateTime     DEFAULT now(),
    exchange        LowCardinality(String),
    symbol          LowCardinality(String),
    trade_id        String,
    price           Float64,
    quantity        Float64,
    trade_time      Int64,
    is_buyer_maker  UInt8,
    _partition_date Date         DEFAULT today()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(_partition_date)
ORDER BY (exchange, symbol, trade_time, trade_id)
SETTINGS index_granularity = 8192;

-- =============================================================================
-- SILVER LAYER — cleaned and normalized data
-- ReplacingMergeTree enables idempotent re-loads
-- =============================================================================

-- Normalized candles with UTC DateTime columns
CREATE TABLE IF NOT EXISTS crypto.silver_klines
(
    exchange           LowCardinality(String),
    symbol             LowCardinality(String),
    interval           LowCardinality(String),
    open_time          DateTime,
    close_time         DateTime,
    open               Float64,
    high               Float64,
    low                Float64,
    close              Float64,
    volume             Float64,
    quote_volume       Float64,
    trade_count        Int32,
    ingested_at        DateTime     DEFAULT now(),
    _partition_date    Date         DEFAULT toDate(open_time)
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(_partition_date)
ORDER BY (exchange, symbol, interval, open_time)
SETTINGS index_granularity = 8192;

-- Normalized coin reference data
CREATE TABLE IF NOT EXISTS crypto.silver_coin_metadata
(
    source              LowCardinality(String),
    coin_id             String,
    symbol              LowCardinality(String),
    name                String,
    market_cap_rank     Nullable(Int32),
    category            Nullable(String),
    coingecko_id        Nullable(String),
    snapshot_date       Date,
    ingested_at         DateTime     DEFAULT now()
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (source, coin_id, snapshot_date)
SETTINGS index_granularity = 8192;

-- =============================================================================
-- GOLD LAYER — analytics-ready tables
-- These are the primary targets for dbt models and Spark gold jobs.
-- DDL stubs defined here; dbt manages the actual transformation logic.
-- =============================================================================

-- Fact: enriched candles with derived metrics
CREATE TABLE IF NOT EXISTS crypto.fact_candles
(
    exchange           LowCardinality(String),
    symbol             LowCardinality(String),
    interval           LowCardinality(String),
    open_time          DateTime,
    close_time         DateTime,
    open               Float64,
    high               Float64,
    low                Float64,
    close              Float64,
    volume             Float64,
    quote_volume       Float64,
    trade_count        Int32,
    price_change       Float64,
    price_change_pct   Float64,
    candle_range       Float64,
    is_bullish         UInt8,
    _partition_date    Date         DEFAULT toDate(open_time),
    ingested_at        DateTime     DEFAULT now()
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(_partition_date)
ORDER BY (exchange, symbol, interval, open_time)
SETTINGS index_granularity = 8192;

-- Dimension: coins
CREATE TABLE IF NOT EXISTS crypto.dim_coin
(
    coin_id         String,
    symbol          LowCardinality(String),
    name            String,
    market_cap_rank Nullable(Int32),
    category        Nullable(String),
    updated_at      DateTime     DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (coin_id)
SETTINGS index_granularity = 8192;

-- Dimension: exchanges
CREATE TABLE IF NOT EXISTS crypto.dim_exchange
(
    exchange_id     LowCardinality(String),
    exchange_name   String,
    country         Nullable(String),
    updated_at      DateTime     DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (exchange_id)
SETTINGS index_granularity = 8192;

-- Mart: rolling volatility metrics per symbol (populated by dbt)
CREATE TABLE IF NOT EXISTS crypto.mart_volatility
(
    exchange        LowCardinality(String),
    symbol          LowCardinality(String),
    interval        LowCardinality(String),
    window_start    DateTime,
    window_end      DateTime,
    realized_vol_7d Float64,
    realized_vol_30d Float64,
    avg_true_range  Float64,
    updated_at      DateTime     DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (exchange, symbol, interval, window_start)
SETTINGS index_granularity = 8192;

-- Mart: daily market regime per symbol (populated by Spark volatility_batch.py)
-- Regime values: trending_up | trending_down | volatile | ranging
CREATE TABLE IF NOT EXISTS crypto.mart_market_regime
(
    exchange         LowCardinality(String),
    symbol           LowCardinality(String),
    trade_date       DateTime,
    day_open         Float64,
    day_high         Float64,
    day_low          Float64,
    day_close        Float64,
    day_volume       Float64,
    log_return       Float64,
    realized_vol_7d  Float64,
    realized_vol_30d Float64,
    atr_14           Float64,
    regime           LowCardinality(String),
    computed_at      DateTime     DEFAULT now()
)
ENGINE = ReplacingMergeTree(computed_at)
PARTITION BY toYYYYMM(trade_date)
ORDER BY (exchange, symbol, trade_date)
SETTINGS index_granularity = 8192;

-- Mart: volume profile (populated by dbt)
CREATE TABLE IF NOT EXISTS crypto.mart_volume_profile
(
    exchange        LowCardinality(String),
    symbol          LowCardinality(String),
    interval        LowCardinality(String),
    period_start    DateTime,
    total_volume    Float64,
    avg_volume      Float64,
    volume_zscore   Float64,
    is_volume_spike UInt8,
    updated_at      DateTime     DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (exchange, symbol, interval, period_start)
SETTINGS index_granularity = 8192;
