-- Active symbols scan
-- Summary of activity over the last 1h, 4h, 24h for quick context.

SELECT
    exchange,
    symbol,
    -- 1-hour window
    round(sumIf(volume,      open_time >= now() - INTERVAL 1 HOUR), 0)  AS vol_1h,
    round(sumIf(quote_volume,open_time >= now() - INTERVAL 1 HOUR), 0)  AS qvol_1h,
    round(
        (argMaxIf(close, open_time, open_time >= now() - INTERVAL 1 HOUR)
       - argMinIf(open,  open_time, open_time >= now() - INTERVAL 1 HOUR))
       / nullIf(argMinIf(open, open_time, open_time >= now() - INTERVAL 1 HOUR), 0) * 100,
        2
    ) AS move_pct_1h,
    -- 4-hour window
    round(sumIf(volume,      open_time >= now() - INTERVAL 4 HOUR), 0)  AS vol_4h,
    round(
        (argMaxIf(close, open_time, open_time >= now() - INTERVAL 4 HOUR)
       - argMinIf(open,  open_time, open_time >= now() - INTERVAL 4 HOUR))
       / nullIf(argMinIf(open, open_time, open_time >= now() - INTERVAL 4 HOUR), 0) * 100,
        2
    ) AS move_pct_4h,
    -- 24-hour window
    round(sumIf(volume, open_time >= now() - INTERVAL 24 HOUR), 0)      AS vol_24h,
    round(
        (argMaxIf(close, open_time, open_time >= now() - INTERVAL 24 HOUR)
       - argMinIf(open,  open_time, open_time >= now() - INTERVAL 24 HOUR))
       / nullIf(argMinIf(open, open_time, open_time >= now() - INTERVAL 24 HOUR), 0) * 100,
        2
    ) AS move_pct_24h
FROM crypto.fact_candles
WHERE interval  = '1m'
  AND open_time >= now() - INTERVAL 24 HOUR
GROUP BY exchange, symbol
ORDER BY qvol_24h DESC;
