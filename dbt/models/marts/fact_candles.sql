{{
    config(
        materialized='table',
        engine='ReplacingMergeTree(ingested_at)',
        order_by='(exchange, symbol, interval, open_time)',
        partition_by='toYYYYMM(open_time)',
        settings={'index_granularity': 8192}
    )
}}

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
    trade_count,

    -- Derived price metrics
    close - open                                        as price_change,
    round((close - open) / nullIf(open, 0) * 100, 4)   as price_change_pct,
    high - low                                          as candle_range,
    if(close >= open, 1, 0)                             as is_bullish,

    toDate(open_time)                                   as _partition_date,
    now()                                               as ingested_at

from {{ ref('stg_klines') }}
