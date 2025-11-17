#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
龙抬头策略参数优化器
- 扫描回调幅度（pullback_pct）和预期收益（expected_profit）的组合
- 输出收益对比表，找出最优参数
"""

import os
import psycopg2
import pandas as pd
import logging
from dotenv import load_dotenv
from strategy_validator import DragonHeadValidator

load_dotenv()

logging.basicConfig(level=logging.WARNING)  # 减少日志干扰
logger = logging.getLogger(__name__)

# 🔧 可调参数范围
PULLBACK_RANGES = [
    (10, 25),
    (12, 28),
    (15, 30),
    (10, 30),
    (12, 35),
    (15, 35)
]

EXPECTED_PROFIT_VALUES = [3.0, 4.0, 5.0, 6.0, 7.0, 8.0]

# 回测区间
START_DATE = '2024-01-01'
END_DATE = '2024-12-31'

def create_temp_function(conn, pullback_min, pullback_max, expected_profit):
    """动态创建带参数的选股函数"""
    with conn.cursor() as cur:
        cur.execute(f"""
        CREATE OR REPLACE FUNCTION get_dragon_head_optimize(target_date DATE, max_count INT DEFAULT 5)
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
                    d.close_rate,
                    MAX(d2.high) AS high_20d,
                    ROUND((MAX(d2.high) - d.close) / NULLIF(MAX(d2.high), 0) * 100, 2) AS pullback_pct,
                    AVG(d3.volume) AS avg_vol_5d
                FROM daily_kline d
                JOIN stock_basic s ON d.ts_code = s.ts_code
                JOIN daily_kline d2 ON d.ts_code = d2.ts_code 
                    AND d2.trade_date BETWEEN target_date - INTERVAL '20 days' AND target_date
                JOIN daily_kline d3 ON d.ts_code = d3.ts_code 
                    AND d3.trade_date BETWEEN target_date - INTERVAL '5 days' AND target_date
                WHERE d.trade_date = target_date
                  AND s.is_st = false
                  AND d.close_rate IS NOT NULL
                  AND d.close_rate > -3.0
                  AND d.close_rate < 3.0
                GROUP BY d.ts_code, s.name, d.close, d.volume, d.close_rate
            ),
            strong_up AS (
                SELECT 
                    rd.*,
                    (SELECT COUNT(*) 
                     FROM daily_kline dk 
                     WHERE dk.ts_code = rd.ts_code 
                       AND dk.trade_date BETWEEN target_date - INTERVAL '30 days' AND target_date
                       AND dk.close_rate >= 9.8) AS limit_up_count,
                    (SELECT 
                        CASE 
                            WHEN MIN(close) > 0 THEN MAX(close) / MIN(close) - 1 
                            ELSE 0 
                        END
                     FROM daily_kline dk 
                     WHERE dk.ts_code = rd.ts_code 
                       AND dk.trade_date BETWEEN target_date - INTERVAL '30 days' AND target_date) AS total_return_30d
                FROM recent_data rd
            )
            SELECT 
                rd.ts_code,
                rd.name,
                rd.current_price,
                rd.high_20d,
                rd.pullback_pct,
                ROUND(COALESCE(rd.volume / NULLIF(rd.avg_vol_5d, 0), 1.0), 2) AS volume_ratio,
                {expected_profit}::NUMERIC AS expected_profit
            FROM strong_up rd
            WHERE rd.limit_up_count >= 3
              AND rd.total_return_30d >= 0.5
              AND rd.pullback_pct BETWEEN {pullback_min} AND {pullback_max}
              AND ROUND(COALESCE(rd.volume / NULLIF(rd.avg_vol_5d, 0), 1.0), 2) < 0.7
              AND rd.current_price > 0
            ORDER BY rd.pullback_pct DESC, ROUND(COALESCE(rd.volume / NULLIF(rd.avg_vol_5d, 0), 1.0), 2) ASC
            LIMIT max_count;
        END;
        $$ LANGUAGE plpgsql;
        """)
        conn.commit()

def run_optimization():
    # 连接数据库
    conn = psycopg2.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        database=os.getenv('DB_NAME', 'aiwealth'),
        user=os.getenv('DB_USER', 'aiwealth'),
        password=os.getenv('DB_PASSWORD')
    )
    
    results = []
    
    total_combinations = len(PULLBACK_RANGES) * len(EXPECTED_PROFIT_VALUES)
    current = 0
    
    for (pb_min, pb_max) in PULLBACK_RANGES:
        for exp_profit in EXPECTED_PROFIT_VALUES:
            current += 1
            print(f"[{current}/{total_combinations}] 测试回调 {pb_min}-{pb_max}% | 预期收益 {exp_profit}%")
            
            try:
                # 创建临时函数
                create_temp_function(conn, pb_min, pb_max, exp_profit)
                
                # 运行回测
                validator = DragonHeadValidator()
                validator.conn = conn  # 复用连接
                
                # 临时替换函数名
                original_query = "SELECT * FROM get_dragon_head_stocks(%s, %s)"
                validator.get_dragon_head_query = "SELECT * FROM get_dragon_head_optimize(%s, %s)"
                
                # Monkey patch the validate_strategy method to use new query
                def patched_validate_strategy(self, start_date, end_date, max_candidates=5):
                    trading_dates = self.get_trading_dates(start_date, end_date)
                    all_signals = []
                    current_position = None
                    
                    for signal_date in trading_dates:
                        if current_position is not None:
                            sell_date = current_position.get('sell_date')
                            if sell_date and sell_date <= signal_date:
                                current_position = None
                            else:
                                continue
                        
                        with self.conn.cursor() as cur:
                            cur.execute(self.get_dragon_head_query, (signal_date, max_candidates))
                            candidates = cur.fetchall()
                            
                            if not candidates:
                                continue
                            
                            best_candidate = candidates[0]
                            ts_code, name, current_price, high_20d, pullback_pct, volume_ratio, expected_profit = best_candidate
                            
                            signal_result = self.validate_single_signal(
                                signal_date, ts_code, name, current_price, expected_profit
                            )
                            
                            if signal_result:
                                all_signals.append(signal_result)
                                current_position = signal_result
                    
                    if all_signals:
                        df = pd.DataFrame(all_signals)
                        return {
                            'total_signals': len(df),
                            'success_rate': (df['is_success'].sum() / len(df)) * 100,
                            'total_profit': df['profit_rate'].sum(),
                            'avg_profit': df['profit_rate'].mean()
                        }
                    return {'total_signals': 0, 'success_rate': 0, 'total_profit': 0, 'avg_profit': 0}
                
                # 绑定补丁方法
                validator.validate_strategy = patched_validate_strategy.__get__(validator, DragonHeadValidator)
                validator.get_dragon_head_query = "SELECT * FROM get_dragon_head_optimize(%s, %s)"
                
                # 执行回测
                result = validator.validate_strategy(START_DATE, END_DATE, max_candidates=5)
                
                results.append({
                    'pullback_min': pb_min,
                    'pullback_max': pb_max,
                    'expected_profit': exp_profit,
                    'total_signals': result['total_signals'],
                    'success_rate': round(result['success_rate'], 2),
                    'total_profit': round(result['total_profit'], 2),
                    'avg_profit': round(result['avg_profit'], 2)
                })
                
            except Exception as e:
                print(f"  ❌ 失败: {e}")
                results.append({
                    'pullback_min': pb_min,
                    'pullback_max': pb_max,
                    'expected_profit': exp_profit,
                    'total_signals': 0,
                    'success_rate': 0,
                    'total_profit': 0,
                    'avg_profit': 0
                })
    
    conn.close()
    
    # 转为 DataFrame 并排序
    df_results = pd.DataFrame(results)
    df_results = df_results.sort_values('total_profit', ascending=False)
    
    # 保存到 CSV
    df_results.to_csv('strategy_optimization_results.csv', index=False)
    print("\n" + "="*100)
    print("✅ 优化完成！结果已保存到 strategy_optimization_results.csv")
    print("="*100)
    
    # 打印 Top 10
    print("\n🏆 Top 10 参数组合（按总收益排序）:")
    print(df_results.head(10).to_string(index=False, float_format="%.2f"))
    
    # 找出最高收益
    best = df_results.iloc[0]
    print(f"\n🎯 最佳参数:")
    print(f"   回调幅度: {best['pullback_min']:.0f}% - {best['pullback_max']:.0f}%")
    print(f"   预期收益: {best['expected_profit']:.1f}%")
    print(f"   总收益率: {best['total_profit']:.2f}%")
    print(f"   交易次数: {best['total_signals']}")
    print(f"   成功率: {best['success_rate']:.2f}%")

if __name__ == "__main__":
    run_optimization()

