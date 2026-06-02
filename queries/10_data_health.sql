-- Data health / freshness check
-- Quick diagnostic: are all symbols current, are there gaps, how complete is the data?

SELECT
    exchange,
    symbol,
    interval,
    count()                             AS total_candles,
    min(open_time)                      AS earliest,
    max(open_time)                      AS latest,
    -- Seconds since last candle (freshness)
    toInt32(now() - max(open_time))     AS seconds_since_last,
    -- Expected candles for 1m over 30 days = 43200
    CASE interval
        WHEN '1m' THEN round(count() / 43200.0 * 100, 1)
        WHEN '1h' THEN round(count() / 720.0   * 100, 1)
        WHEN '1d' THEN round(count() / 30.0    * 100, 1)
        ELSE NULL
    END AS completeness_pct,
    if(toInt32(now() - max(open_time)) > 300, 'STALE', 'OK') AS freshness_status
FROM crypto.silver_klines
GROUP BY exchange, symbol, interval
ORDER BY exchange, symbol, interval;
