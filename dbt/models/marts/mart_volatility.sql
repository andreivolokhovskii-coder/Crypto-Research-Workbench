{{
    config(
        materialized='table',
        engine='ReplacingMergeTree(updated_at)',
        order_by='(exchange, symbol, interval, window_start)',
        partition_by='toYYYYMM(window_start)',
        settings={'index_granularity': 8192}
    )
}}

-- Daily OHLCV aggregated from 1-minute candles
with daily as (
    select
        exchange,
        symbol,
        toDate(open_time)           as trade_date,
        argMin(open,  open_time)    as day_open,
        max(high)                   as day_high,
        min(low)                    as day_low,
        argMax(close, open_time)    as day_close,
        sum(volume)                 as day_volume
    from {{ ref('stg_klines') }}
    where interval = '1m'
    group by exchange, symbol, trade_date
),

-- Previous close for log-return and ATR computation
with_prev as (
    select
        *,
        lagInFrame(day_close, 1, day_close) over (
            partition by exchange, symbol
            order by trade_date
            rows between unbounded preceding and current row
        ) as prev_close
    from daily
),

-- Log return + True Range per day
with_metrics as (
    select
        *,
        if(prev_close > 0, log(day_close / prev_close), 0) as log_return,
        greatest(
            day_high - day_low,
            abs(day_high - prev_close),
            abs(day_low  - prev_close)
        )                                                   as true_range
    from with_prev
)

select
    exchange,
    symbol,
    '1d'                                        as interval,
    toDateTime(trade_date)                      as window_start,
    toDateTime(trade_date + toIntervalDay(1))   as window_end,

    -- Annualized realized volatility over a 7-day rolling window
    round(
        stddevSamp(log_return) over (
            partition by exchange, symbol
            order by trade_date
            rows between 6 preceding and current row
        ) * sqrt(365), 6
    ) as realized_vol_7d,

    -- Annualized realized volatility over a 30-day rolling window
    round(
        stddevSamp(log_return) over (
            partition by exchange, symbol
            order by trade_date
            rows between 29 preceding and current row
        ) * sqrt(365), 6
    ) as realized_vol_30d,

    -- 14-day Average True Range
    round(
        avg(true_range) over (
            partition by exchange, symbol
            order by trade_date
            rows between 13 preceding and current row
        ), 6
    ) as avg_true_range,

    now() as updated_at

from with_metrics
