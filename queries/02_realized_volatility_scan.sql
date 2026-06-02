-- Realized volatility scan
-- 7-day and 30-day annualized realized volatility for all tracked symbols,
-- with rank and ratio (vol_7d / vol_30d) to spot accelerating/decelerating moves.

SELECT
    exchange,
    symbol,
    round(realized_vol_7d  * 100, 2)    AS vol_7d_pct,
    round(realized_vol_30d * 100, 2)    AS vol_30d_pct,
    round(realized_vol_7d / nullIf(realized_vol_30d, 0), 2) AS vol_ratio,  -- >1 = vol expanding
    round(avg_true_range, 4)            AS atr_14d,
    window_start                        AS as_of_date
FROM crypto.mart_volatility
WHERE (exchange, symbol, window_start) IN (
    SELECT exchange, symbol, max(window_start)
    FROM crypto.mart_volatility
    GROUP BY exchange, symbol
)
ORDER BY vol_7d_pct DESC;
