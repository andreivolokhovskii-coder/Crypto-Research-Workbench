-- Intraday range anomalies
-- Candles where the high-low range is unusually large relative to recent history.
-- Useful for spotting flash crashes, spikes, and manipulation attempts.

WITH stats AS (
    SELECT
        exchange,
        symbol,
        avg(candle_range)       AS avg_range,
        stddevPop(candle_range) AS std_range
    FROM crypto.fact_candles
    WHERE interval = '1m'
      AND open_time >= now() - INTERVAL 7 DAY
    GROUP BY exchange, symbol
)
SELECT
    f.exchange,
    f.symbol,
    f.open_time,
    round(f.open,         4) AS open,
    round(f.high,         4) AS high,
    round(f.low,          4) AS low,
    round(f.close,        4) AS close,
    round(f.candle_range, 4) AS range,
    round((f.candle_range - s.avg_range) / nullIf(s.std_range, 0), 2) AS range_zscore,
    f.is_bullish
FROM crypto.fact_candles f
JOIN stats s USING (exchange, symbol)
WHERE f.interval = '1m'
  AND f.open_time >= now() - INTERVAL 24 HOUR
  AND (f.candle_range - s.avg_range) / nullIf(s.std_range, 0) > 3.0
ORDER BY range_zscore DESC
LIMIT 50;
