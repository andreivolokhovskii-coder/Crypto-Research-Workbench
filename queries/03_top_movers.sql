-- Top movers scan (last 24h)
-- Symbols ranked by absolute price change over the last 24 hours.

WITH daily_range AS (
    SELECT
        exchange,
        symbol,
        argMin(open,  open_time)                        AS open_24h,
        argMax(close, open_time)                        AS close_24h,
        max(high)                                       AS high_24h,
        min(low)                                        AS low_24h,
        sum(volume)                                     AS total_vol_24h,
        sum(quote_volume)                               AS total_quote_24h
    FROM crypto.fact_candles
    WHERE interval = '1m'
      AND open_time >= now() - INTERVAL 24 HOUR
    GROUP BY exchange, symbol
)
SELECT
    exchange,
    symbol,
    round(open_24h,    4)   AS open_24h,
    round(close_24h,   4)   AS close_24h,
    round(high_24h,    4)   AS high_24h,
    round(low_24h,     4)   AS low_24h,
    round((close_24h - open_24h) / nullIf(open_24h, 0) * 100, 2) AS pct_change_24h,
    round(total_vol_24h,   0) AS volume_24h,
    round(total_quote_24h, 0) AS quote_volume_24h
FROM daily_range
ORDER BY abs(pct_change_24h) DESC;
