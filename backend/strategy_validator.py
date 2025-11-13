#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
龙抬头策略验证器（简化版）
- 取消做T功能
- 添加每日累计成功率和收益
- 修复decimal.Decimal类型错误
"""

import os
import psycopg2
from datetime import datetime
import pandas as pd
import logging
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'database': os.getenv('DB_NAME', 'aiwealth'),
    'user': os.getenv('DB_USER', 'aiwealth'),
    'password': os.getenv('DB_PASSWORD')
}

class DragonHeadValidator:
    def __init__(self):
        self.conn = None
        
    def connect_db(self):
        self.conn = psycopg2.connect(**DB_CONFIG)
        
    def close_db(self):
        if self.conn:
            self.conn.close()
            
    def get_trading_dates(self, start_date, end_date):
        """获取交易日期列表"""
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT trade_date 
                FROM daily_kline 
                WHERE trade_date BETWEEN %s AND %s
                ORDER BY trade_date
            """, (start_date, end_date))
            return [row[0] for row in cur.fetchall()]
            
    def get_next_trading_dates(self, start_date, days=3):
        """获取从start_date开始的接下来N个交易日"""
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT trade_date 
                FROM daily_kline 
                WHERE trade_date > %s
                ORDER BY trade_date
                LIMIT %s
            """, (start_date, days))
            return [row[0] for row in cur.fetchall()]
            
    def validate_single_signal(self, signal_date, ts_code, name, current_price, expected_profit):
        """验证单个买入信号的表现"""
        target_price = current_price * (1 + expected_profit / 100)
        next_dates = self.get_next_trading_dates(signal_date, 4)
        if len(next_dates) < 1:
            return None
            
        buy_date = next_dates[0]
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT open FROM daily_kline 
                WHERE ts_code = %s AND trade_date = %s
            """, (ts_code, buy_date))
            buy_result = cur.fetchone()
            if not buy_result or buy_result[0] is None:
                return None
                
            buy_price = buy_result[0]
            sell_price = None
            sell_date = None
            is_success = False
            holding_days = 3
            
            for i, check_date in enumerate(next_dates[1:4]):
                cur.execute("""
                    SELECT high, close FROM daily_kline 
                    WHERE ts_code = %s AND trade_date = %s
                """, (ts_code, check_date))
                price_result = cur.fetchone()
                if price_result and price_result[0] is not None:
                    day_high = price_result[0]
                    if day_high >= target_price:
                        sell_price = target_price
                        sell_date = check_date
                        is_success = True
                        holding_days = i + 1
                        break
            
            if sell_price is None:
                if len(next_dates) >= 4:
                    third_day = next_dates[3]
                    cur.execute("""
                        SELECT close FROM daily_kline 
                        WHERE ts_code = %s AND trade_date = %s
                    """, (ts_code, third_day))
                    final_result = cur.fetchone()
                    if final_result and final_result[0] is not None:
                        sell_price = final_result[0]
                        sell_date = third_day
                        is_success = False
                        holding_days = 3
                    else:
                        return None
                else:
                    return None
            
            profit_rate = (sell_price - buy_price) / buy_price * 100 if buy_price > 0 else 0
                
            return {
                'signal_date': signal_date,
                'ts_code': ts_code,
                'name': name,
                'buy_price': buy_price,
                'target_price': target_price,
                'sell_price': sell_price,
                'sell_date': sell_date,
                'is_success': is_success,
                'holding_days': holding_days,
                'profit_rate': round(profit_rate, 2)
            }
    
    def get_stock_details(self, ts_code, trade_date):
        """获取股票详细信息用于日志"""
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT close, high_rate, volume, turnover_rate 
                FROM daily_kline 
                WHERE ts_code = %s AND trade_date = %s
            """, (ts_code, trade_date))
            result = cur.fetchone()
            if result:
                return {
                    'close': result[0],
                    'high_rate': result[1],
                    'volume': result[2],
                    'turnover_rate': result[3]
                }
        return None
    
    def validate_strategy(self, start_date, end_date, max_candidates=5):
        """
        验证策略表现（单股持仓模式，无做T）
        """
        logger.info(f"验证龙抬头策略表现（单股持仓模式）: {start_date} 到 {end_date}")
        
        trading_dates = self.get_trading_dates(start_date, end_date)
        all_signals = []
        current_position = None
        operation_log = []
        
        # 用于累计统计
        cumulative_signals = 0
        cumulative_success = 0
        cumulative_profit = 0.0
        
        for signal_date in trading_dates:
            logger.info(f"处理信号日期: {signal_date}")
            
            # 检查当前持仓状态
            if current_position is not None:
                sell_date = current_position.get('sell_date')
                if sell_date and sell_date <= signal_date:
                    # 持仓已结束，更新累计统计
                    cumulative_signals += 1
                    if current_position['is_success']:
                        cumulative_success += 1
                    cumulative_profit += float(current_position['profit_rate'])
                    
                    operation_log.append(f"\n{signal_date}: 📊 持仓 {current_position['name']} 已结束")
                    operation_log.append(f"   结果: {'成功' if current_position['is_success'] else '失败'} | 收益: {current_position['profit_rate']:.2f}%")
                    operation_log.append(f"   累计统计: 交易{cumulative_signals}次 | 成功率{cumulative_success/cumulative_signals*100:.1f}% | 总收益{cumulative_profit:.2f}%")
                    current_position = None
                else:
                    operation_log.append(f"\n{signal_date}: 📈 继续持有 {current_position['name']}({current_position['ts_code']})，跳过新信号")
                    continue
            
            # 获取龙抬头候选股
            with self.conn.cursor() as cur:
                cur.execute("""
                    SELECT * FROM get_dragon_head_stocks(%s, %s)
                """, (signal_date, max_candidates))
                candidates = cur.fetchall()
                
                if not candidates:
                    operation_log.append(f"\n{signal_date}: ❌ 无符合条件的龙抬头信号，空仓等待")
                    continue
                
                # 打印备选股列表
                operation_log.append(f"\n{signal_date}: 🎯 龙抬头备选股池（共{len(candidates)}只）")
                operation_log.append("-" * 80)
                
                for i, cand in enumerate(candidates):
                    ts_code, name, current_price, high_20d, pullback_pct, volume_ratio, expected_profit = cand
                    
                    stock_details = self.get_stock_details(ts_code, signal_date)
                    if stock_details:
                        volume_str = f"{stock_details['volume']:,}" if stock_details['volume'] else "N/A"
                        turnover_str = f"{stock_details['turnover_rate']:.2f}%" if stock_details['turnover_rate'] else "N/A"
                        high_rate_str = f"{stock_details['high_rate']:.2f}%" if stock_details['high_rate'] else "N/A"
                    else:
                        volume_str = "N/A"
                        turnover_str = "N/A"
                        high_rate_str = "N/A"
                    
                    rank_suffix = "🥇" if i == 0 else "🥈" if i == 1 else "🥉" if i == 2 else ""
                    operation_log.append(
                        f"{i+1:2d}. {name}({ts_code}) {rank_suffix}\n"
                        f"    当前价: {current_price:8.2f} | 回调幅度: {pullback_pct:6.2f}% | 预期收益: {expected_profit:6.2f}%\n"
                        f"    量能比: {volume_ratio:6.2f}x | 当日最高涨幅: {high_rate_str:>8} | 成交量: {volume_str:>12} | 换手率: {turnover_str:>6}"
                    )
                
                operation_log.append("-" * 80)
                
                # 选择最佳股票（第一个）
                best_candidate = candidates[0]
                ts_code, name, current_price, high_20d, pullback_pct, volume_ratio, expected_profit = best_candidate
                
                # 简化选择理由
                operation_log.append(f"✅ 最终选择: {name}({ts_code}) - 排名第1的优质龙抬头信号")
                
                # 验证信号
                signal_result = self.validate_single_signal(
                    signal_date, ts_code, name, current_price, expected_profit
                )
                
                if signal_result:
                    all_signals.append(signal_result)
                    current_position = signal_result
                    
                    # 记录交易
                    next_dates = self.get_next_trading_dates(signal_date, 1)
                    actual_buy_date = next_dates[0] if next_dates else signal_date
                    
                    operation_log.append(f"💰 {actual_buy_date}: 买入 {name}({ts_code}) @ {signal_result['buy_price']:.2f}")
                    operation_log.append(f"   目标卖出价: {signal_result['target_price']:.2f} | 预期收益率: {expected_profit:.2f}%")
                    
                    # 注意：此时还不知道结果，结果会在卖出日更新
                else:
                    operation_log.append(f"❌ {signal_date}: 候选股数据不完整，无法执行交易")
        
        # 处理最后一个持仓（如果有的话）
        if current_position is not None:
            cumulative_signals += 1
            if current_position['is_success']:
                cumulative_success += 1
            cumulative_profit += float(current_position['profit_rate'])
            
            operation_log.append(f"\n最终持仓处理: {current_position['name']} 已结束")
            operation_log.append(f"   结果: {'成功' if current_position['is_success'] else '失败'} | 收益: {current_position['profit_rate']:.2f}%")
            operation_log.append(f"   累计统计: 交易{cumulative_signals}次 | 成功率{cumulative_success/cumulative_signals*100:.1f}% | 总收益{cumulative_profit:.2f}%")
        
        # 输出详细操作日志
        logger.info("\n" + "=" * 100)
        logger.info("📈 龙抬头策略详细操作日志")
        logger.info("=" * 100)
        for log_entry in operation_log:
            logger.info(log_entry)
        logger.info("=" * 100)
        
        # 计算最终策略表现
        if all_signals:
            df = pd.DataFrame(all_signals)
            total_signals = int(len(df))
            success_signals = int(df['is_success'].sum())
            success_rate = float((success_signals / total_signals * 100) if total_signals > 0 else 0.0)
            total_profit = float(df['profit_rate'].sum())
            avg_profit = float(df['profit_rate'].mean())
            
            with self.conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO strategy_summary 
                    (start_date, end_date, total_signals, success_signals, success_rate,
                     total_profit_rate, avg_profit_per_signal)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    start_date, end_date, total_signals, success_signals, 
                    round(success_rate, 2), round(total_profit, 2), round(avg_profit, 2)
                ))
                self.conn.commit()
                
            logger.info(f"\n📊 最终策略表现:")
            logger.info(f"   总交易次数: {total_signals}")
            logger.info(f"   成功率: {success_rate:.2f}%")
            logger.info(f"   总收益率: {total_profit:.2f}%")
            logger.info(f"   平均每笔收益: {avg_profit:.2f}%")
            
            # 分析策略偏差
            self.analyze_strategy_deviation(all_signals)
        
        return all_signals

    def analyze_strategy_deviation(self, signals):
        """分析策略偏差，找出表现异常的交易"""
        logger.info("\n" + "=" * 60)
        logger.info("🔍 策略偏差分析")
        logger.info("=" * 60)
        
        if not signals:
            logger.info("无交易信号，无法进行偏差分析")
            return
            
        df = pd.DataFrame(signals)
        avg_profit = df['profit_rate'].mean()
        std_profit = df['profit_rate'].std()
        
        # 找出异常亏损的交易（低于平均值2个标准差）
        extreme_losses = df[df['profit_rate'] <= (avg_profit - 2 * std_profit)]
        extreme_wins = df[df['profit_rate'] >= (avg_profit + 2 * std_profit)]
        
        if not extreme_losses.empty:
            logger.info(f"📉 异常亏损交易（需重点关注）:")
            for _, signal in extreme_losses.iterrows():
                logger.info(f"   {signal['signal_date']} {signal['name']}({signal['ts_code']}): {signal['profit_rate']:.2f}%")
        
        if not extreme_wins.empty:
            logger.info(f"📈 超额收益交易（策略优势体现）:")
            for _, signal in extreme_wins.iterrows():
                logger.info(f"   {signal['signal_date']} {signal['name']}({signal['ts_code']}): {signal['profit_rate']:.2f}%")
        
        # 按月份分析表现
        df['signal_month'] = pd.to_datetime(df['signal_date']).dt.to_period('M')
        monthly_stats = df.groupby('signal_month').agg({
            'profit_rate': ['mean', 'sum', 'count'],
            'is_success': 'mean'
        }).round(2)
        
        logger.info(f"\n📅 月度表现分析:")
        logger.info(f"   月份        平均收益   总收益   交易次数   成功率")
        logger.info(f"   {'-'*50}")
        for month, stats in monthly_stats.iterrows():
            avg_ret = stats[('profit_rate', 'mean')]
            total_ret = stats[('profit_rate', 'sum')]
            count = stats[('profit_rate', 'count')]
            success_rate = stats[('is_success', 'mean')] * 100
            logger.info(f"   {month}    {avg_ret:8.2f}%  {total_ret:8.2f}%     {count:3d}      {success_rate:6.1f}%")

if __name__ == "__main__":
    validator = DragonHeadValidator()
    validator.connect_db()
    
    try:
        # 验证2025年龙抬头策略表现
        signals = validator.validate_strategy('2025-01-01', '2025-10-01', max_candidates=5)
    finally:
        validator.close_db()

