{{
    config(materialized='view')
}}

-- Thin pass-through over silver_klines.
-- Gives downstream marts a stable dbt ref name instead of a raw table name.
select
    exchange,
    symbol,
    interval,
    open_time,
    close_time,
    open,
    high,
    low,
    close,
    volume,
    quote_volume,
    trade_count
-- FINAL forces ClickHouse to collapse unmerged duplicate versions from
-- ReplacingMergeTree before returning rows; without it, downstream marts
-- can receive multiple versions of the same candle until a background merge runs.
from {{ source('crypto', 'silver_klines') }} FINAL
