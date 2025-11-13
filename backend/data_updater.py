#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AIWealth 数据更新脚本
- 只包含非ST、未退市的活跃A股
- 自动计算涨跌幅（保留2位小数）
- 多线程并发获取数据
"""

import os
import baostock as bs
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
import pandas as pd
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
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

if not DB_CONFIG['password']:
    logger.error("❌ 数据库密码未设置！请检查 .env 文件")
    raise SystemExit("缺少数据库密码配置")

MAJOR_INDEXES = [
    ("sh.000001", "上证指数"),
    ("sz.399001", "深证成指"),
    ("sh.000300", "沪深300"),
    ("sh.000905", "中证500"),
    ("sh.000852", "中证1000"),
]

CONCEPTS = {
    "白酒": ["sh.600519", "sz.000858", "sh.600809", "sz.002304", "sh.603589"],
    "新能源": ["sz.300750", "sh.601012", "sz.002466", "sh.600438", "sz.300274"],
    "医药": ["sh.600276", "sz.000538", "sz.300760", "sh.603259", "sh.600519"]
}

class DataUpdater:
    def __init__(self):
        self.conn = None
        
    def connect_db(self):
        self.conn = psycopg2.connect(**DB_CONFIG)
        logger.info("数据库连接成功")
        
    def close_db(self):
        if self.conn:
            self.conn.close()
            logger.info("数据库连接已关闭")
    
    def is_st_stock(self, name):
        if not name:
            return False
        name = name.strip()
        return (name.startswith('退市') or 
                name.startswith('*ST') or 
                (name.startswith('ST') and not name.startswith('STO')))
            
    def update_stock_basic(self):
        logger.info("开始更新股票基本信息...")
        bs.login()
        try:
            rs = bs.query_stock_basic()
            stock_list = []
            while (rs.error_code == '0') & rs.next():
                stock_list.append(rs.get_row_data())
                
            df = pd.DataFrame(stock_list, columns=rs.fields)
            df = df[(df['type'] == '1') & (df['status'] == '1')]
            
            if df.empty:
                logger.warning("未获取到活跃股票数据")
                return
                
            records = []
            for _, row in df.iterrows():
                stock_name = row['code_name']
                if self.is_st_stock(stock_name):
                    continue
                    
                records.append((
                    row['code'],
                    row['code'][3:],
                    stock_name,
                    False
                ))
            
            with self.conn.cursor() as cur:
                execute_values(
                    cur,
                    """
                    INSERT INTO stock_basic 
                    (ts_code, symbol, name, is_st)
                    VALUES %s
                    ON CONFLICT (ts_code) 
                    DO UPDATE SET 
                        name = EXCLUDED.name,
                        is_st = EXCLUDED.is_st
                    """,
                    records
                )
                self.conn.commit()
                logger.info(f"活跃非ST股票信息更新完成，共 {len(records)} 条记录")
        finally:
            bs.logout()
            
    def fetch_stock_kline(self, ts_code, start_date, end_date):
        kline_data = []
        try:
            lg = bs.login()
            if lg.error_code != '0':
                return kline_data
                
            rs = bs.query_history_k_data_plus(
                ts_code,
                "date,open,high,low,close,volume,amount,turn",
                start_date=start_date,
                end_date=end_date,
                frequency="d",
                adjustflag="1"
            )
            
            while (rs.error_code == '0') & rs.next():
                row = rs.get_row_data()
                kline_data.append((
                    ts_code,
                    row[0],
                    float(row[1]) if row[1] else None,
                    float(row[2]) if row[2] else None,
                    float(row[3]) if row[3] else None,
                    float(row[4]) if row[4] else None,
                    int(row[5]) if row[5] else None,
                    float(row[6]) if row[6] else None,
                    float(row[7]) if row[7] else None
                ))
            bs.logout()
            return kline_data
        except Exception as e:
            logger.error(f"获取 {ts_code} 数据失败: {e}")
            try:
                bs.logout()
            except:
                pass
            return kline_data
    
    def _bulk_insert_kline(self, kline_data):
        if not kline_data:
            return
        with self.conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO daily_kline 
                (ts_code, trade_date, open, high, low, close, volume, amount, turnover_rate)
                VALUES %s
                ON CONFLICT (ts_code, trade_date) 
                DO UPDATE SET
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume,
                    amount = EXCLUDED.amount,
                    turnover_rate = EXCLUDED.turnover_rate
                """,
                kline_data,
                page_size=10000
            )
            self.conn.commit()
    
    def update_price_rates(self):
        logger.info("开始计算价格涨跌幅...")
        with self.conn.cursor() as cur:
            cur.execute("""
                WITH price_changes AS (
                    SELECT 
                        id,
                        ts_code,
                        trade_date,
                        open,
                        high, 
                        low,
                        close,
                        LAG(close, 1) OVER (PARTITION BY ts_code ORDER BY trade_date) as prev_close
                    FROM daily_kline
                )
                UPDATE daily_kline 
                SET 
                    open_rate = ROUND(
                        CASE 
                            WHEN pc.prev_close > 0 THEN (pc.open - pc.prev_close) / pc.prev_close * 100 
                            ELSE NULL 
                        END, 2
                    ),
                    high_rate = ROUND(
                        CASE 
                            WHEN pc.prev_close > 0 THEN (pc.high - pc.prev_close) / pc.prev_close * 100 
                            ELSE NULL 
                        END, 2
                    ),
                    low_rate = ROUND(
                        CASE 
                            WHEN pc.prev_close > 0 THEN (pc.low - pc.prev_close) / pc.prev_close * 100 
                            ELSE NULL 
                        END, 2
                    ),
                    close_rate = ROUND(
                        CASE 
                            WHEN pc.prev_close > 0 THEN (pc.close - pc.prev_close) / pc.prev_close * 100 
                            ELSE NULL 
                        END, 2
                    )
                FROM price_changes pc
                WHERE daily_kline.id = pc.id
                  AND pc.prev_close IS NOT NULL;
            """)
            self.conn.commit()
            logger.info("✅ 价格涨跌幅计算完成")
    
    def update_daily_kline(self, start_date="2023-01-01"):
        logger.info(f"开始更新日线数据，起始日期: {start_date}...")
        with self.conn.cursor() as cur:
            cur.execute("SELECT ts_code FROM stock_basic WHERE is_st = false")
            stocks = [row[0] for row in cur.fetchall()]
        logger.info(f"共找到 {len(stocks)} 只非ST活跃股票")
        end_date = datetime.now().strftime("%Y-%m-%d")
        
        all_kline_data = []
        total_records = 0
        batch_insert_size = 100000
        
        with ThreadPoolExecutor(max_workers=12) as executor:
            future_to_stock = {
                executor.submit(self.fetch_stock_kline, ts_code, start_date, end_date): ts_code 
                for ts_code in stocks
            }
            completed = 0
            for future in as_completed(future_to_stock):
                kline_data = future.result()
                all_kline_data.extend(kline_data)
                completed += 1
                if completed % 100 == 0:
                    logger.info(f"进度: {completed}/{len(stocks)} 只股票完成")
                if len(all_kline_data) >= batch_insert_size:
                    self._bulk_insert_kline(all_kline_data)
                    total_records += len(all_kline_data)
                    logger.info(f"已插入 {total_records} 条日线数据")
                    all_kline_data = []
        
        if all_kline_data:
            self._bulk_insert_kline(all_kline_data)
            total_records += len(all_kline_data)
        self.update_price_rates()
        logger.info(f"✅ 日线数据更新完成，共 {total_records} 条记录")
        
    def update_index_kline(self, start_date="2023-01-01"):
        logger.info("开始更新指数数据...")
        end_date = datetime.now().strftime("%Y-%m-%d")
        bs.login()
        try:
            all_index_data = []
            for index_code, index_name in MAJOR_INDEXES:
                try:
                    rs = bs.query_history_k_data_plus(
                        index_code,
                        "date,open,high,low,close,volume,amount",
                        start_date=start_date,
                        end_date=end_date,
                        frequency="d",
                        adjustflag="3"
                    )
                    index_data = []
                    while (rs.error_code == '0') & rs.next():
                        row = rs.get_row_data()
                        index_data.append((
                            index_code,
                            index_name,
                            row[0],
                            float(row[1]) if row[1] else None,
                            float(row[2]) if row[2] else None,
                            float(row[3]) if row[3] else None,
                            float(row[4]) if row[4] else None,
                            int(row[5]) if row[5] else None,
                            float(row[6]) if row[6] else None
                        ))
                    all_index_data.extend(index_data)
                except Exception as e:
                    logger.error(f"获取 {index_name} 数据失败: {e}")
                    continue
            if all_index_data:
                with self.conn.cursor() as cur:
                    execute_values(
                        cur,
                        """
                        INSERT INTO index_kline 
                        (index_code, index_name, trade_date, open, high, low, close, volume, amount)
                        VALUES %s
                        ON CONFLICT (index_code, trade_date) 
                        DO UPDATE SET
                            open = EXCLUDED.open,
                            high = EXCLUDED.high,
                            low = EXCLUDED.low,
                            close = EXCLUDED.close,
                            volume = EXCLUDED.volume,
                            amount = EXCLUDED.amount
                        """,
                        all_index_data
                    )
                    self.conn.commit()
                    logger.info(f"指数数据更新完成，共 {len(all_index_data)} 条记录")
        finally:
            bs.logout()
            
    def update_concept_stocks(self):
        logger.info("开始更新板块成分股...")
        concept_records = []
        for concept_name, stock_codes in CONCEPTS.items():
            for ts_code in stock_codes:
                concept_records.append((concept_name, ts_code))
        with self.conn.cursor() as cur:
            cur.execute("DELETE FROM concept_stocks")
            if concept_records:
                execute_values(
                    cur,
                    "INSERT INTO concept_stocks (concept_name, ts_code) VALUES %s",
                    concept_records
                )
                self.conn.commit()
                logger.info(f"板块成分股更新完成，共 {len(concept_records)} 条记录")
            
    def run_full_update(self):
        try:
            self.connect_db()
            self.update_stock_basic()
            self.update_daily_kline()
            self.update_index_kline()
            self.update_concept_stocks()
            logger.info("✅ 全量数据更新完成！")
        except Exception as e:
            logger.error(f"数据更新失败: {e}")
            raise
        finally:
            self.close_db()

if __name__ == "__main__":
    updater = DataUpdater()
    updater.run_full_update()

