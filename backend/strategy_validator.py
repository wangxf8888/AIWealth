#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
龙抬头策略验证器（完整版，适配新规则 + 你的表结构）
- 表名: strategy_performance
- 表名: strategy_summary
- 字段: 均含 created_at
- 新规则:
    1. 10天内 ≥5 次涨停
    2. 从高点回调 15%~30%
    3. 连续2天企稳（-3%~+3%）
    4. 第一天缩量，第二天放量
    5. 信号日 = 企稳第二天
    6. 买入日 = 第三天，且开盘跌幅 -3% ~ -1%
    7. 止盈 +5%，止损 -5%
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
            
    def get_next_trading_date(self, date):
        """获取下一个交易日"""
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT MIN(trade_date) 
                FROM daily_kline 
                WHERE trade_date > %s
            """, (date,))
            result = cur.fetchone()
            return result[0] if result and result[0] else None
            
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
                    'close': float(result[0]) if result[0] is not None else None,  # ✅ 转 float
                    'high_rate': float(result[1]) if result[1] is not None else None,
                    'volume': result[2],
                    'turnover_rate': float(result[3]) if result[3] is not None else None
                }
        return None
    
    def validate_strategy(self, start_date, end_date, max_candidates=5):
        """
        验证策略表现（单股持仓模式）
        """
        logger.info(f"验证新龙抬头策略表现: {start_date} 到 {end_date}")
        
        trading_dates = self.get_trading_dates(start_date, end_date)
        all_signals = []
        current_position = None
        operation_log = []
        
        # 清空本次回测的信号记录
        with self.conn.cursor() as cur:
            cur.execute("DELETE FROM strategy_performance WHERE signal_date BETWEEN %s AND %s", (start_date, end_date))
            self.conn.commit()
        
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
                    # 持仓已结束
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
            
            # 获取龙抬头候选股（新规则）
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
                    ts_code, name, current_price, high_10d, pullback_pct, vol_ratio1, vol_ratio2, expected_profit = cand
                    
                    # ✅ 转为 float 避免 Decimal 错误
                    current_price = float(current_price)
                    high_10d = float(high_10d)
                    pullback_pct = float(pullback_pct)
                    vol_ratio1 = float(vol_ratio1)
                    vol_ratio2 = float(vol_ratio2)
                    expected_profit = float(expected_profit)
                    
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
                        f"    量比(第1天): {vol_ratio1:6.2f}x | 量比(第2天): {vol_ratio2:6.2f}x | 当日最高涨幅: {high_rate_str:>8} | 成交量: {volume_str:>12}"
                    )
                
                operation_log.append("-" * 80)
                
                # 选择最佳股票（第一个）
                best_candidate = candidates[0]
                ts_code, name, current_price, high_10d, pullback_pct, vol_ratio1, vol_ratio2, expected_profit = best_candidate
                
                # ✅ 转为 float
                current_price = float(current_price)
                name = str(name)
                ts_code = str(ts_code)
                
                operation_log.append(f"✅ 最终选择: {name}({ts_code}) - 排名第1的优质龙抬头信号")
                
                # 获取买入日（下一个交易日）
                buy_date = self.get_next_trading_date(signal_date)
                if not buy_date:
                    operation_log.append(f"❌ 无法确定买入日，跳过")
                    continue
                
                # 检查买入条件：开盘跌幅 -3% ~ -1%
                with self.conn.cursor() as cur2:
                    cur2.execute("""
                        SELECT open FROM daily_kline 
                        WHERE ts_code = %s AND trade_date = %s
                    """, (ts_code, buy_date))
                    buy_result = cur2.fetchone()
                    if not buy_result or buy_result[0] is None:
                        operation_log.append(f"❌ 买入日数据缺失，跳过")
                        continue
                    
                    open_price = float(buy_result[0])  # ✅ 转 float
                    open_rate = (open_price - current_price) / current_price * 100
                    
                    if not (-3.0 <= open_rate <= -1.0):
                        operation_log.append(f"❌ 开盘跌幅 {open_rate:.2f}%，不满足 -3% ~ -1% 买入条件，跳过")
                        continue
                
                # 设置止盈止损（现在 open_price 是 float，可安全运算）
                target_price = open_price * 1.05          # ✅ 不再报错
                stop_loss_price = open_price * 0.95        # ✅ 不再报错
                
                # 模拟持有（最多5天）
                sell_price = None
                sell_date = None
                is_success = False
                holding_days = 0
                
                current_check_date = buy_date
                for day in range(1, 6):  # 第1天到第5天
                    current_check_date = self.get_next_trading_date(current_check_date)
                    if not current_check_date:
                        break
                    
                    with self.conn.cursor() as cur3:
                        cur3.execute("""
                            SELECT high, low, close FROM daily_kline 
                            WHERE ts_code = %s AND trade_date = %s
                        """, (ts_code, current_check_date))
                        price_result = cur3.fetchone()
                        if not price_result:
                            continue
                        
                        day_high = float(price_result[0])
                        day_low = float(price_result[1])
                        day_close = float(price_result[2])
                        holding_days = day
                        
                        # 先检查是否止损
                        if day_low <= stop_loss_price:
                            sell_price = stop_loss_price
                            sell_date = current_check_date
                            is_success = False
                            break
                        # 再检查是否止盈
                        elif day_high >= target_price:
                            sell_price = target_price
                            sell_date = current_check_date
                            is_success = True
                            break
                        # 如果是最后一天，收盘卖出
                        elif day == 5:
                            sell_price = day_close
                            sell_date = current_check_date
                            is_success = (day_close > open_price)
                            break
                
                if sell_price is None:
                    operation_log.append(f"❌ 无法确定卖出价格，跳过")
                    continue
                
                profit_rate = (sell_price - open_price) / open_price * 100 if open_price > 0 else 0
                signal_result = {
                    'signal_date': signal_date,
                    'ts_code': ts_code,
                    'name': name,
                    'buy_price': open_price,
                    'target_price': target_price,
                    'sell_price': sell_price,
                    'sell_date': sell_date,
                    'is_success': is_success,
                    'holding_days': holding_days,
                    'profit_rate': round(profit_rate, 2)
                }
                
                all_signals.append(signal_result)
                current_position = signal_result
                
                # 写入数据库
                with self.conn.cursor() as cur4:
                    cur4.execute("""
                        INSERT INTO strategy_performance 
                        (signal_date, ts_code, name, buy_price, target_price, 
                         sell_price, sell_date, is_success, holding_days, profit_rate, created_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    """, (
                        signal_result['signal_date'],
                        signal_result['ts_code'],
                        signal_result['name'],
                        signal_result['buy_price'],
                        signal_result['target_price'],
                        signal_result['sell_price'],
                        signal_result['sell_date'],
                        signal_result['is_success'],
                        signal_result['holding_days'],
                        signal_result['profit_rate']
                    ))
                    self.conn.commit()
                
                operation_log.append(f"💰 {buy_date}: 买入 {name}({ts_code}) @ {open_price:.2f} (跌幅 {open_rate:.2f}%)")
                operation_log.append(f"   止盈价: {target_price:.2f} | 止损价: {stop_loss_price:.2f}")
        
        # 处理最后一个持仓
        if current_position is not None:
            cumulative_signals += 1
            if current_position['is_success']:
                cumulative_success += 1
            cumulative_profit += float(current_position['profit_rate'])
            
            operation_log.append(f"\n最终持仓处理: {current_position['name']} 已结束")
            operation_log.append(f"   结果: {'成功' if current_position['is_success'] else '失败'} | 收益: {current_position['profit_rate']:.2f}%")
            operation_log.append(f"   累计统计: 交易{cumulative_signals}次 | 成功率{cumulative_success/cumulative_signals*100:.1f}% | 总收益{cumulative_profit:.2f}%")
        
        # 输出日志
        logger.info("\n" + "=" * 100)
        logger.info("📈 龙抬头策略详细操作日志")
        logger.info("=" * 100)
        for log_entry in operation_log:
            logger.info(log_entry)
        logger.info("=" * 100)
        
        # 计算最终表现
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
                     total_profit_rate, avg_profit_per_signal, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
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
            
            self.analyze_strategy_deviation(all_signals)
        
        return all_signals

    def analyze_strategy_deviation(self, signals):
        """分析策略偏差"""
        logger.info("\n" + "=" * 60)
        logger.info("🔍 策略偏差分析")
        logger.info("=" * 60)
        
        if not signals:
            logger.info("无交易信号，无法进行偏差分析")
            return
            
        df = pd.DataFrame(signals)
        avg_profit = df['profit_rate'].mean()
        std_profit = df['profit_rate'].std()
        
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
        
        # 月度分析
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
        # ⚠️ 请务必使用你有数据的日期范围！例如 2024 年
        signals = validator.validate_strategy('2023-01-01', '2024-12-31', max_candidates=5)
    finally:
        validator.close_db()

