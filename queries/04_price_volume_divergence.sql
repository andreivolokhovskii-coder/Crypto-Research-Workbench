-- Price-volume divergence scan
-- Detects sessions where price moved significantly but volume was below average,
-- or volume spiked while price barely moved. Both can signal anomalous behaviour.

WITH hourly AS (
    SELECT
        exchange,
        symbol,
        toStartOfHour(open_time)                        AS hour,
        argMin(open,  open_time)                        AS h_open,
        argMax(close, open_time)                        AS h_close,
        sum(volume)                                     AS vol
    FROM crypto.fact_candles
    WHERE interval = '1m'
      AND open_time >= now() - INTERVAL 7 DAY
    GROUP BY exchange, symbol, hour
),
stats AS (
    SELECT exchange, symbol,
           avg(vol)         AS avg_vol,
           stddevPop(vol)   AS std_vol,
           avg(abs((h_close - h_open) / nullIf(h_open, 0) * 100)) AS avg_abs_move_pct
    FROM hourly
    GROUP BY exchange, symbol
)
SELECT
    h.exchange,
    h.symbol,
    h.hour,
    round((h.h_close - h.h_open) / nullIf(h.h_open, 0) * 100, 3)  AS price_move_pct,
    round(h.vol, 0)                                                 AS volume,
    round((h.vol - s.avg_vol) / nullIf(s.std_vol, 0), 2)           AS vol_zscore,
    round(s.avg_abs_move_pct, 3)                                    AS avg_move_pct,
    CASE
        WHEN abs((h.h_close - h.h_open) / nullIf(h.h_open, 0) * 100) < s.avg_abs_move_pct * 0.3
             AND (h.vol - s.avg_vol) / nullIf(s.std_vol, 0) > 1.5
            THEN 'high_vol_low_move'
        WHEN abs((h.h_close - h.h_open) / nullIf(h.h_open, 0) * 100) > s.avg_abs_move_pct * 2
             AND (h.vol - s.avg_vol) / nullIf(s.std_vol, 0) < -0.5
            THEN 'high_move_low_vol'
        ELSE 'normal'
    END AS divergence_type
FROM hourly h
JOIN stats s USING (exchange, symbol)
WHERE divergence_type != 'normal'
ORDER BY h.hour DESC, abs(vol_zscore) DESC;
