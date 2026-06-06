-- =============================================================================
-- Crypto Research Workbench — Streaming layer DDL
-- Tables for real-time klines and derived signals.
-- =============================================================================

-- Latest candle per symbol (updated on every tick, even non-closed candles)
CREATE TABLE IF NOT EXISTS crypto.rt_latest_kline
(
    exchange        LowCardinality(String),
    symbol          LowCardinality(String),
    interval        LowCardinality(String),
    open_time       DateTime,
    close_time      DateTime,
    open            Float64,
    high            Float64,
    low             Float64,
    close           Float64,
    volume          Float64,
    quote_volume    Float64,
    trade_count     Int32,
    is_closed       UInt8,
    updated_at      DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (exchange, symbol, interval, open_time)
SETTINGS index_granularity = 8192;

-- Real-time signals feed
CREATE TABLE IF NOT EXISTS crypto.rt_signals
(
    detected_at     DateTime DEFAULT now(),
    exchange        LowCardinality(String),
    symbol          LowCardinality(String),
    signal_type     LowCardinality(String),
    -- context values depend on signal_type
    value           Float64,
    threshold       Float64,
    description     String,
    open_time       DateTime
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(detected_at)
ORDER BY (detected_at, exchange, symbol, signal_type)
TTL detected_at + INTERVAL 7 DAY     -- auto-expire old signals
SETTINGS index_granularity = 8192;
