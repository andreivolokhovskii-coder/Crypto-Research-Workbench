-- Market regime summary (last 30 days)
-- Classifies each symbol into a regime based on realized volatility
-- and directional trend, useful as context for any research workflow.

WITH daily_close AS (
    SELECT
        exchange,
        symbol,
        toDate(open_time)       AS trade_date,
        argMax(close, open_time) AS day_close
    FROM crypto.fact_candles
    WHERE interval = '1m'
      AND open_time >= now() - INTERVAL 30 DAY
    GROUP BY exchange, symbol, trade_date
),
trend AS (
    SELECT
        exchange,
        symbol,
        -- Linear trend: positive = uptrend
        (argMax(day_close, trade_date) - argMin(day_close, trade_date))
            / nullIf(argMin(day_close, trade_date), 0) * 100  AS trend_pct_30d,
        count()                                                 AS trading_days
    FROM daily_close
    GROUP BY exchange, symbol
)
SELECT
    v.exchange,
    v.symbol,
    round(v.realized_vol_30d * 100, 1)  AS vol_30d_pct,
    round(t.trend_pct_30d,          1)  AS trend_30d_pct,
    v.avg_true_range                    AS atr,
    CASE
        WHEN v.realized_vol_30d > 0.6  AND t.trend_pct_30d > 10   THEN 'high_vol_uptrend'
        WHEN v.realized_vol_30d > 0.6  AND t.trend_pct_30d < -10  THEN 'high_vol_downtrend'
        WHEN v.realized_vol_30d > 0.6                             THEN 'high_vol_sideways'
        WHEN v.realized_vol_30d < 0.2  AND t.trend_pct_30d > 5    THEN 'low_vol_uptrend'
        WHEN v.realized_vol_30d < 0.2  AND t.trend_pct_30d < -5   THEN 'low_vol_downtrend'
        WHEN v.realized_vol_30d < 0.2                             THEN 'low_vol_sideways'
        WHEN t.trend_pct_30d > 10                                 THEN 'mid_vol_uptrend'
        WHEN t.trend_pct_30d < -10                                THEN 'mid_vol_downtrend'
        ELSE                                                           'mid_vol_sideways'
    END AS regime
FROM crypto.mart_volatility v
JOIN trend t USING (exchange, symbol)
WHERE (v.exchange, v.symbol, v.window_start) IN (
    SELECT exchange, symbol, max(window_start)
    FROM crypto.mart_volatility GROUP BY exchange, symbol
)
ORDER BY vol_30d_pct DESC;
