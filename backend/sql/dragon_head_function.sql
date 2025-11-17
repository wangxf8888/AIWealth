-- backend/sql/dragon_head_function.sql
CREATE OR REPLACE FUNCTION get_dragon_head_stocks(target_date DATE, max_count INT DEFAULT 5)
RETURNS TABLE(
    ts_code VARCHAR(20),
    name VARCHAR(100),
    current_price NUMERIC(10,2),
    high_20d NUMERIC(10,2),
    pullback_pct NUMERIC(8,2),
    volume_ratio NUMERIC(8,4),
    expected_profit NUMERIC(6,2)
) AS $$
BEGIN
    RETURN QUERY
    WITH recent_data AS (
        SELECT 
            d.ts_code,
            s.name,
            d.close AS current_price,
            d.volume,
            d.turnover_rate,
            -- 获取过去20个交易日最高价
            MAX(d2.high) OVER (PARTITION BY d.ts_code ORDER BY d2.trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS high_20d,
            -- 计算回调幅度
            ROUND((MAX(d2.high) OVER (PARTITION BY d.ts_code ORDER BY d2.trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) - d.close) / 
                  NULLIF(MAX(d2.high) OVER (PARTITION BY d.ts_code ORDER BY d2.trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW), 0) * 100, 2) AS pullback_pct,
            -- 近5日平均成交量
            AVG(d3.volume) OVER (PARTITION BY d.ts_code ORDER BY d3.trade_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS avg_vol_5d
        FROM daily_kline d
        JOIN stock_basic s ON d.ts_code = s.ts_code
        LEFT JOIN daily_kline d2 ON d.ts_code = d2.ts_code AND d2.trade_date <= d.trade_date AND d2.trade_date >= d.trade_date - INTERVAL '20 days'
        LEFT JOIN daily_kline d3 ON d.ts_code = d3.ts_code AND d3.trade_date <= d.trade_date AND d3.trade_date >= d.trade_date - INTERVAL '5 days'
        WHERE d.trade_date = target_date
          AND s.is_st = false
    ),
    strong_up AS (
        SELECT *,
               -- 判断是否满足“短期涨幅50%+至少3个涨停”
               (SELECT COUNT(*) 
                FROM daily_kline dk 
                WHERE dk.ts_code = rd.ts_code 
                  AND dk.trade_date BETWEEN rd.target_date - INTERVAL '30 days' AND rd.target_date
                  AND dk.close_rate >= 9.8) AS limit_up_count,
               (SELECT MAX(close) / MIN(close) - 1
                FROM daily_kline dk 
                WHERE dk.ts_code = rd.ts_code 
                  AND dk.trade_date BETWEEN rd.target_date - INTERVAL '30 days' AND rd.target_date) AS total_return_30d
        FROM recent_data rd
        CROSS JOIN (SELECT target_date) t
    )
    SELECT 
        ts_code,
        name,
        current_price,
        high_20d,
        pullback_pct,
        ROUND(COALESCE(volume / NULLIF(avg_vol_5d, 0), 1.0), 2) AS volume_ratio,
        5.0 AS expected_profit  -- 固定5%目标
    FROM strong_up
    WHERE limit_up_count >= 3
      AND total_return_30d >= 0.5  -- 涨幅>=50%
      AND pullback_pct BETWEEN 12.0 AND 30.0  -- 回调12%-30%
      AND volume_ratio < 0.7  -- 缩量（成交量<5日均量70%）
      AND current_price > 0
    ORDER BY pullback_pct DESC, volume_ratio ASC  -- 优先选回调深+缩量明显的
    LIMIT max_count;
END;
$$ LANGUAGE plpgsql;

