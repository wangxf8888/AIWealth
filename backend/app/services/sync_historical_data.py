# /home/AIWealth/backend/app/sync_historical_data.py
import baostock as bs
import sqlite3
import pandas as pd
import time
from datetime import datetime, timedelta
from pathlib import Path
import sys
from pathlib import Path

# 将父目录 (app) 加入系统路径，以便能导入 db.py
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from db import get_conn, DB_PATH

# 配置目标年份范围
START_YEAR = 2020
END_YEAR = 2024  # 2024 年部分数据可能已在现有逻辑中，这里主要补全完整年份

def login_bs():
    lg = bs.login()
    if lg.error_code != '0':
        print(f"❌ Baostock 登录失败：{lg.error_msg}")
        return None
    print("✅ Baostock 登录成功")
    return lg

def get_existing_dates(conn):
    """获取数据库中已存在的日期集合，用于去重"""
    c = conn.cursor()
    c.execute("SELECT DISTINCT date FROM stock_daily_k")
    return set(row[0] for row in c.fetchall())

def fetch_year_data(code, code_name, year, existing_dates):
    """拉取指定股票指定年份的数据"""
    start_date = f"{year}-01-01"
    end_date = f"{year}-12-31"

    # 1. 获取日 K
    rs_daily = bs.query_history_k_data_plus(
        code, "date,open,high,low,close,preclose,volume,amount,turn,pctChg",
        start_date=start_date, end_date=end_date,
        frequency="d", adjustflag="3"
    )

    if rs_daily.error_code != '0':
        return 0, f"Daily Error: {rs_daily.error_msg}"

    daily_list = []
    while rs_daily.next():
        daily_list.append(rs_daily.get_row_data())

    if not daily_list:
        return 0, "No Data"

    df_daily = pd.DataFrame(daily_list, columns=['date','open','high','low','close','preclose','volume','amount','turn','pctChg'])

    # 2. 获取小时 K (用于计算分时指标，虽然旧数据可能不全，但尽量尝试)
    # 注意：Baostock 对历史小时线支持有限，如果报错则跳过小时线，只存日线
    hourly_map = {}
    try:
        rs_hourly = bs.query_history_k_data_plus(
            code, "date,time,open,high,low,close",
            start_date=start_date, end_date=end_date,
            frequency="60", adjustflag="3"
        )
        if rs_hourly.error_code == '0':
            h_list = []
            while rs_hourly.next():
                h_list.append(rs_hourly.get_row_data())
            if h_list:
                df_h = pd.DataFrame(h_list, columns=['date','time','open','high','low','close'])
                grouped = df_h.groupby('date')
                for date_str, group in grouped:
                    hourly_map[date_str] = group.sort_values('time').head(4).to_dict('records')
    except Exception as e:
        pass # 小时线获取失败不影响日线入库

    conn = get_conn()
    c = conn.cursor()
    inserted_count = 0

    for _, row in df_daily.iterrows():
        date_str = row['date']

        # 去重检查
        if date_str in existing_dates:
            continue

        # 数据清洗
        def safe_float(val, default=0.0):
            if val is None or val == '': return default
            try: return float(val)
            except: return default

        preclose = safe_float(row['preclose'])
        if preclose == 0: preclose = safe_float(row['close'])
        if preclose == 0: continue

        close_val = safe_float(row['close'])
        open_val = safe_float(row['open'])
        high_val = safe_float(row['high'])
        low_val = safe_float(row['low'])

        # 计算涨跌幅百分比
        o_r = round((open_val - preclose) / preclose * 100, 2)
        c_r = round((close_val - preclose) / preclose * 100, 2)
        h_r = round((high_val - preclose) / preclose * 100, 2)
        l_r = round((low_val - preclose) / preclose * 100, 2)

        # 处理小时线数据
        h_rows = hourly_map.get(date_str, [])
        hour_rates = {}
        for i, h_row in enumerate(h_rows):
            if i >= 4: break
            h_idx = i + 1
            ho = safe_float(h_row.get('open'))
            hc = safe_float(h_row.get('close'))
            hh = safe_float(h_row.get('high'))
            hl = safe_float(h_row.get('low'))

            if preclose != 0:
                hour_rates[f'hour{h_idx}_open_rate'] = round((ho - preclose) / preclose * 100, 2)
                hour_rates[f'hour{h_idx}_close_rate'] = round((hc - preclose) / preclose * 100, 2)
                hour_rates[f'hour{h_idx}_high_rate'] = round((hh - preclose) / preclose * 100, 2)
                hour_rates[f'hour{h_idx}_low_rate'] = round((hl - preclose) / preclose * 100, 2)

        sql = """
            INSERT INTO stock_daily_k
            (date, code, code_name, open, high, low, close, preclose, volume, amount, turn, pctChg,
             open_rate, close_rate, high_rate, low_rate,
             hour1_open_rate, hour1_close_rate, hour1_high_rate, hour1_low_rate,
             hour2_open_rate, hour2_close_rate, hour2_high_rate, hour2_low_rate,
             hour3_open_rate, hour3_close_rate, hour3_high_rate, hour3_low_rate,
             hour4_open_rate, hour4_close_rate, hour4_high_rate, hour4_low_rate)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        vals = (
            date_str, code, code_name,
            open_val, high_val, low_val, close_val,
            preclose, safe_float(row['volume']), safe_float(row['amount']), safe_float(row['turn']), safe_float(row['pctChg']),
            o_r, c_r, h_r, l_r,
            hour_rates.get('hour1_open_rate'), hour_rates.get('hour1_close_rate'), hour_rates.get('hour1_high_rate'), hour_rates.get('hour1_low_rate'),
            hour_rates.get('hour2_open_rate'), hour_rates.get('hour2_close_rate'), hour_rates.get('hour2_high_rate'), hour_rates.get('hour2_low_rate'),
            hour_rates.get('hour3_open_rate'), hour_rates.get('hour3_close_rate'), hour_rates.get('hour3_high_rate'), hour_rates.get('hour3_low_rate'),
            hour_rates.get('hour4_open_rate'), hour_rates.get('hour4_close_rate'), hour_rates.get('hour4_high_rate'), hour_rates.get('hour4_low_rate')
        )

        try:
            c.execute(sql, vals)
            inserted_count += 1
            existing_dates.add(date_str) # 内存中去重
        except Exception as e:
            # 忽略主键冲突等错误
            pass

    conn.commit()
    conn.close()
    return inserted_count, "Success"

def run_sync():
    print(f"🚀 开始补充历史数据：{START_YEAR} - {END_YEAR}")
    lg = login_bs()
    if not lg: return

    conn = get_conn()
    c = conn.cursor()

    # 获取所有股票列表
    c.execute("SELECT code, code_name FROM stock_basic")
    stocks = c.fetchall()
    conn.close()

    if not stocks:
        print("❌ 未找到股票基础信息，请先运行 sync_data.py 更新基础表")
        bs.logout()
        return

    total_stocks = len(stocks)
    print(f"📊 待处理股票数量：{total_stocks}")

    # 获取已存在的日期集合 (内存占用稍大，但查询快)
    # 如果内存不足，可改为每次查询数据库，但速度慢
    print("⏳ 正在加载已有日期索引...")
    conn_temp = get_conn()
    existing_dates = get_existing_dates(conn_temp)
    conn_temp.close()
    print(f"✅ 已存在 {len(existing_dates)} 个交易日的记录")

    for idx, (code, code_name) in enumerate(stocks):
        if idx % 5 == 0:
            print(f"\n--- 进度：{idx}/{total_stocks} ({code}) ---")

        total_inserted = 0
        for year in range(START_YEAR, END_YEAR + 1):
            # 简单判断：如果该年第一天已有数据，假设该年数据完整 (可根据需要优化)
            # 这里为了严谨，每年都尝试插入，依靠 UNIQUE 约束去重

            count, msg = fetch_year_data(code, code_name, year, existing_dates)
            total_inserted += count

            # 防封 IP 休眠
            time.sleep(0.05)

        if total_inserted > 0:
            print(f"[{idx+1}/{total_stocks}] ✅ {code}: 新增 {total_inserted} 条记录")
        else:
            if idx % 200 == 0:
                print(f"[{idx+1}/{total_stocks}] ⏭️ {code}: 无新数据")

    bs.logout()
    print("\n🎉 历史数据补充完成！")
    print("💡 下一步：请重新运行 build_core_pool_history.py 以生成新的回测核心池")

if __name__ == "__main__":
    run_sync()
