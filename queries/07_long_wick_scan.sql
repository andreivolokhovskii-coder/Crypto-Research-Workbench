-- Long wick scan (last 24h)
-- Candles with unusually long upper or lower wicks relative to body size.
-- Long lower wick = buyers absorbed selling (bullish signal context).
-- Long upper wick = sellers absorbed buying (bearish signal context).
-- Wick ratio > 2x body = notable.

SELECT
    exchange,
    symbol,
    open_time,
    round(open,  4) AS open,
    round(high,  4) AS high,
    round(low,   4) AS low,
    round(close, 4) AS close,
    -- Body size (abs)
    round(abs(close - open), 4)                             AS body,
    -- Upper wick
    round(high - greatest(open, close), 4)                  AS upper_wick,
    -- Lower wick
    round(least(open, close) - low, 4)                      AS lower_wick,
    -- Wick-to-body ratios
    round((high - greatest(open, close)) / nullIf(abs(close - open), 0), 2) AS upper_wick_ratio,
    round((least(open, close) - low)     / nullIf(abs(close - open), 0), 2) AS lower_wick_ratio,
    if(close >= open, 'bullish', 'bearish')                 AS candle_dir
FROM crypto.fact_candles
WHERE interval  = '1m'
  AND open_time >= now() - INTERVAL 24 HOUR
  AND abs(close - open) > 0   -- exclude doji-like candles
  AND (
        (high - greatest(open, close)) / nullIf(abs(close - open), 0) > 2
     OR (least(open, close) - low)     / nullIf(abs(close - open), 0) > 2
  )
ORDER BY greatest(upper_wick_ratio, lower_wick_ratio) DESC
LIMIT 50;
