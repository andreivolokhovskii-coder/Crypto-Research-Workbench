-- Breakout candidate scan
-- Symbols where the current price is near a recent N-day high or low,
-- combined with rising volume — a classic breakout setup.
-- "Near" = within 0.5% of the N-day high/low.

WITH daily AS (
    SELECT
        exchange,
        symbol,
        toDate(open_time)        AS trade_date,
        max(high)                AS day_high,
        min(low)                 AS day_low,
        argMax(close, open_time) AS day_close,
        sum(volume)              AS day_vol
    FROM crypto.fact_candles
    WHERE interval  = '1m'
      AND open_time >= now() - INTERVAL 30 DAY
    GROUP BY exchange, symbol, trade_date
),
range_30d AS (
    SELECT
        exchange, symbol,
        max(day_high)  AS high_30d,
        min(day_low)   AS low_30d,
        avg(day_vol)   AS avg_daily_vol
    FROM daily
    GROUP BY exchange, symbol
),
latest AS (
    SELECT exchange, symbol, day_close AS last_close, day_vol AS last_vol
    FROM daily
    WHERE (exchange, symbol, trade_date) IN (
        SELECT exchange, symbol, max(trade_date) FROM daily GROUP BY exchange, symbol
    )
)
SELECT
    l.exchange,
    l.symbol,
    round(l.last_close,  4)  AS last_close,
    round(r.high_30d,    4)  AS high_30d,
    round(r.low_30d,     4)  AS low_30d,
    round((r.high_30d - l.last_close) / nullIf(r.high_30d, 0) * 100, 2) AS pct_from_high,
    round((l.last_close - r.low_30d)  / nullIf(r.low_30d,  0) * 100, 2) AS pct_from_low,
    round(l.last_vol / nullIf(r.avg_daily_vol, 0), 2)                    AS vol_vs_avg,
    CASE
        WHEN (r.high_30d - l.last_close) / nullIf(r.high_30d, 0) < 0.005
             AND l.last_vol > r.avg_daily_vol * 1.2 THEN 'near_high_high_vol'
        WHEN (r.high_30d - l.last_close) / nullIf(r.high_30d, 0) < 0.005 THEN 'near_high'
        WHEN (l.last_close - r.low_30d)  / nullIf(r.low_30d,  0) < 0.005 THEN 'near_low'
        ELSE 'mid_range'
    END AS setup
FROM latest l
JOIN range_30d r USING (exchange, symbol)
ORDER BY setup, pct_from_high;
