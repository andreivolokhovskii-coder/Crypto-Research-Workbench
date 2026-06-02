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
from {{ source('crypto', 'silver_klines') }}
