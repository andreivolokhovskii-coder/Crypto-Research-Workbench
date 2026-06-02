-- Unusual volume scan
-- Symbols where recent hourly volume is significantly above their 30-day average.
-- Z-score > 2 = volume spike worth investigating.

WITH hourly AS (
    SELECT
        exchange,
        symbol,
        toStartOfHour(open_time)        AS hour,
        sum(volume)                     AS vol,
        sum(quote_volume)               AS quote_vol
    FROM crypto.fact_candles
    WHERE interval = '1m'
      AND open_time >= now() - INTERVAL 30 DAY
    GROUP BY exchange, symbol, hour
),
stats AS (
    SELECT
        exchange,
        symbol,
        avg(vol)    AS avg_vol,
        stddevPop(vol) AS std_vol
    FROM hourly
    GROUP BY exchange, symbol
),
latest AS (
    SELECT exchange, symbol, vol AS last_hour_vol, quote_vol AS last_hour_quote_vol
    FROM hourly
    WHERE (exchange, symbol, hour) IN (
        SELECT exchange, symbol, max(hour) FROM hourly GROUP BY exchange, symbol
    )
)
SELECT
    l.exchange,
    l.symbol,
    round(l.last_hour_vol, 2)                           AS last_hour_vol,
    round(l.last_hour_quote_vol, 0)                     AS last_hour_quote_vol,
    round(s.avg_vol, 2)                                 AS avg_hourly_vol,
    round((l.last_hour_vol - s.avg_vol) / nullIf(s.std_vol, 0), 2) AS volume_zscore
FROM latest l
JOIN stats s USING (exchange, symbol)
ORDER BY volume_zscore DESC;
