import baostock as bs
import sqlite3
import pandas as pd
import time
from datetime import datetime, timedelta
import sys
import os
from pathlib import Path

# 【智能导入修复】
# 尝试相对导入 (适用于 python -m services.sync_data)
try:
    from ..db import get_conn, DB_PATH
except ImportError:
    # 如果失败 (适用于直接运行 python sync_data.py 或 crontab)
    # 手动将 app 目录加入路径
    current_dir = Path(__file__).resolve().parent
    app_dir = current_dir.parent
    if str(app_dir) not in sys.path:
        sys.path.insert(0, str(app_dir))

    from db import get_conn, DB_PATH

# 全局标记，防止重复登录
_current_session = None

def get_bs_session():
    """获取或创建一个 Baostock 会话"""
    global _current_session
    if _current_session is None or _current_session.error_code != '0':
        # 尝试关闭旧连接
        if _current_session is not None:
            try:
                bs.logout()
            except:
                pass
        _current_session = bs.login()
        if _current_session.error_code != '0':
            print(f"❌ Login failed: {_current_session.error_msg}")
            return None
    return _current_session

def close_bs_session():
    """关闭全局会话"""
    global _current_session
    if _current_session is not None:
        try:
            bs.logout()
        except:
            pass
    _current_session = None

def update_basics():
    """
    【重构版】分别更新股票和指数到不同的基础表
    - stock_basic: 存储 sec_type=1 的股票
    - index_basic: 存储 sec_type=2 的指数
    """
    print(">>> Starting update_basics (Split Tables)...")

    lg = bs.login()
    if lg.error_code != '0':
        print(f"❌ Login failed: {lg.error_msg}")
        return

    conn = get_conn()
    c = conn.cursor()
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    try:
        # 确定基准日期
        now_str = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        print(f"Step 1: Fetching all security codes for date: {now_str} ...")

        rs_list = bs.query_all_stock(day=now_str)

        # 降级策略：如果今天不行，试昨天
        if rs_list.error_code != '0':
            yesterday = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")
            print(f"⚠️ Fallback to date: {yesterday} ...")
            rs_list = bs.query_all_stock(day=yesterday)

        if rs_list.error_code != '0':
            print(f"❌ Critical: Could not fetch code list from Baostock.")
            bs.logout()
            return

        code_candidates = []
        while rs_list.error_code == '0' and rs_list.next():
            row = rs_list.get_row_data()
            if row and len(row) > 0:
                code_candidates.append(row[0])

        if not code_candidates:
            print(f"⚠️ Warning: No candidates found. Is it a trading day?")
            bs.logout()
            return

        print(f"✅ Found {len(code_candidates)} candidates. Processing details...")

        stock_count = 0
        index_count = 0
        skip_count = 0

        for i, code_raw in enumerate(code_candidates):
            try:
                rs_detail = bs.query_stock_basic(code=code_raw)
                if rs_detail.error_code != '0' or not rs_detail.next():
                    skip_count += 1
                    continue

                row = rs_detail.get_row_data()
                # fields: code, code_name, ipoDate, outDate, type, status
                if len(row) < 6:
                    skip_count += 1
                    continue

                d_code = row[0]       # 格式：sh.600000
                d_name = row[1]
                d_type = row[4]       # '1'=股票，'2'=指数
                d_status = row[5]     # '1'=上市

                # 过滤：只处理有效状态
                if d_status != '1':
                    skip_count += 1
                    continue

                # 过滤：只处理股票和指数
                if d_type not in ['1', '2']:
                    skip_count += 1
                    continue

                # 校验格式 (必须是 sh.xxxxxx 或 sz.xxxxxx)
                if '.' not in d_code or len(d_code) != 9:
                    skip_count += 1
                    continue

                is_st = 1 if 'ST' in d_name else 0

                if d_type == '1':
                    # 写入 stock_basic
                    c.execute("""
                        INSERT OR REPLACE INTO stock_basic
                        (code, code_name, tradeStatus, is_st, ipo_date, out_date, update_time)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (d_code, d_name, d_status, is_st, row[2], row[3], now_str))
                    stock_count += 1

                elif d_type == '2':
                    # 写入 index_basic
                    # 注意：指数的 ipoDate/outDate 可能对应 base_date/base_point，这里做简单映射
                    base_point = 0.0
                    try:
                        if row[3]: base_point = float(row[3])
                    except:
                        pass

                    c.execute("""
                        INSERT OR REPLACE INTO index_basic
                        (code, code_name, tradeStatus, base_date, base_point, update_time)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (d_code, d_name, d_status, row[2], base_point, now_str))
                    index_count += 1

                if (i + 1) % 5 == 0:
                    conn.commit()
                    print(f"   Progress: {i+1}/{len(code_candidates)}, Stocks: {stock_count}, Indices: {index_count}")

            except Exception as e:
                skip_count += 1
                # print(f"Error processing {code_raw}: {e}")
                pass

            # 防限流休眠
            if (i + 1) % 50 == 0:
                time.sleep(0.1)

        conn.commit()
        conn.close()

        print(f"\n✅ === Update Basics Finished ===")
        print(f"   Stocks Inserted: {stock_count}")
        print(f"   Indices Inserted: {index_count}")
        print(f"   Skipped: {skip_count}")

    except Exception as e:
        print(f"❌ Critical Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        bs.logout()
        print("<<< Logout success.")

def fetch_and_convert_hourly_rates(code, date_str, preclose):
    """
    获取小时 K 线 (复用全局会话，避免频繁登录登出)
    """
    if preclose == 0:
        return {}

    lg = get_bs_session()
    if not lg:
        return {}

    try:
        # 增加重试逻辑
        max_retries = 2
        for attempt in range(max_retries):
            try:
                rs = bs.query_history_k_data_plus(
                    code, "date,time,open,high,low,close",
                    start_date=date_str, end_date=date_str,
                    frequency="60", adjustflag="3"
                )

                if rs.error_code == '0':
                    data_list = []
                    while rs.next():
                        data_list.append(rs.get_row_data())

                    if data_list:
                        df = pd.DataFrame(data_list, columns=['date','time','open','high','low','close'])
                        df = df.sort_values('time').head(4)

                        result = {}
                        for i, (_, row) in enumerate(df.iterrows()):
                            if i >= 4: break
                            h = i + 1
                            try:
                                o = float(row['open'])
                                c_val = float(row['close'])
                                h_val = float(row['high'])
                                l_val = float(row['low'])

                                result[f'hour{h}_open_rate'] = round((o - preclose) / preclose * 100, 2)
                                result[f'hour{h}_close_rate'] = round((c_val - preclose) / preclose * 100, 2)
                                result[f'hour{h}_high_rate'] = round((h_val - preclose) / preclose * 100, 2)
                                result[f'hour{h}_low_rate'] = round((l_val - preclose) / preclose * 100, 2)
                            except:
                                continue
                        return result
                    else:
                        return {} # 无数据
                else:
                    # 如果是限流或网络错误，等待后重试
                    if 'limit' in rs.error_msg.lower() or 'network' in rs.error_msg.lower():
                        time.sleep(2)
                        continue
                    else:
                        return {}
            except Exception as e:
                # 捕获 Bad file descriptor 等底层错误
                if "Bad file descriptor" in str(e):
                    # 尝试重置会话
                    close_bs_session()
                    get_bs_session()
                    time.sleep(1)
                    continue
                if attempt == max_retries - 1:
                    return {}
                time.sleep(1)
        return {}
    except Exception as e:
        return {}

def update_daily_k(days_to_update=3):
    """
    【修复版】智能增量更新 K 线
    逻辑：检查【今天】是否有数据。如果没有，则拉取最近 days_to_update 天的数据。
    这样可以防止因起始日存在而跳过最新数据的情况。
    """
    print(f">>> Starting Smart Incremental K-Line Update (Last {days_to_update} Days)...")

    lg = bs.login()
    if lg.error_code != '0':
        print(f"❌ Login failed: {lg.error_msg}")
        return

    conn = get_conn()
    c = conn.cursor()

    today = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=days_to_update)).strftime("%Y-%m-%d")

    print(f"📅 Check Range: {start_date} to {today}")
    print(f"🎯 Target: Ensure data for {today} exists.")

    total_new_records = 0
    processed_stocks = 0
    skip_count = 0
    update_count = 0

    c.execute("SELECT code, code_name FROM stock_basic")
    stocks = c.fetchall()
    total_stocks = len(stocks)
    print(f"📊 Total stocks to check: {total_stocks}")

    for idx, (code, code_name) in enumerate(stocks):
        processed_stocks += 1

        # 【核心修复 1】检查【今天】是否有数据，而不是检查 start_date
        # 如果今天已经有数据了，说明这只股票今天已经更新过，可以跳过
        c.execute("SELECT date FROM stock_daily_k WHERE code=? AND date=? LIMIT 1", (code, today))
        has_today_data = c.fetchone()

        if has_today_data:
            skip_count += 1
            if processed_stocks % 5 == 0:
                print(f"[{processed_stocks}/{total_stocks}] ⏭️ {code}: Already updated for today.")
            continue

        # 【核心修复 2】如果今天没数据，我们需要拉取最近几天的数据来补全
        # 可能是因为今天刚收盘数据还没插，或者是前几天停牌今天才复牌
        try:
            # 1. 获取日 K (从 start_date 到 today)
            rs_daily = bs.query_history_k_data_plus(
                code, "date,open,high,low,close,preclose,volume,amount,turn,pctChg",
                start_date=start_date, end_date=today,
                frequency="d", adjustflag="3"
            )

            if rs_daily.error_code != '0':
                # 如果是非交易日或无数据，Baostock 可能报错或返回空，这不算错误
                if "no data" not in rs_daily.error_msg.lower():
                    pass # 静默失败
                continue

            daily_list = []
            while rs_daily.next():
                daily_list.append(rs_daily.get_row_data())

            if not daily_list:
                # 确实没有新数据 (比如停牌)
                if processed_stocks % 5 == 0:
                    print(f"[{processed_stocks}/{total_stocks}] ⚪ {code}: No new data from BS (maybe suspended).")
                continue

            # 2. 批量获取小时 K
            rs_hourly = bs.query_history_k_data_plus(
                code, "date,time,open,high,low,close",
                start_date=start_date, end_date=today,
                frequency="60", adjustflag="3"
            )

            hourly_map = {}
            if rs_hourly.error_code == '0':
                temp_h_list = []
                while rs_hourly.next():
                    temp_h_list.append(rs_hourly.get_row_data())
                if temp_h_list:
                    df_h = pd.DataFrame(temp_h_list, columns=['date','time','open','high','low','close'])
                    grouped = df_h.groupby('date')
                    for date_str, group in grouped:
                        hourly_map[date_str] = group.sort_values('time').head(4).to_dict('records')

            # 3. 处理并入库
            df_d = pd.DataFrame(daily_list, columns=['date','open','high','low','close','preclose','volume','amount','turn','pctChg'])
            inserted_count = 0

            for _, row in df_d.iterrows():
                date_str = row['date']

                # 二次查重：防止重复插入中间日期
                c.execute("SELECT id FROM stock_daily_k WHERE code=? AND date=?", (code, date_str))
                if c.fetchone():
                    continue

                def safe_float(val, default=0.0):
                    if val is None or val == '': return default
                    try: return float(val)
                    except: return default

                preclose = safe_float(row['preclose'])
                if preclose == 0: preclose = safe_float(row['close'])
                if preclose == 0: continue

                open_val = safe_float(row['open'])
                close_val = safe_float(row['close'])
                high_val = safe_float(row['high'])
                low_val = safe_float(row['low'])

                # 计算涨跌幅
                o_r = round((open_val - preclose) / preclose * 100, 2)
                c_r = round((close_val - preclose) / preclose * 100, 2)
                h_r = round((high_val - preclose) / preclose * 100, 2)
                l_r = round((low_val - preclose) / preclose * 100, 2)

                # 获取小时数据
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
                c.execute(sql, vals)
                inserted_count += 1
                total_new_records += 1

            if inserted_count > 0:
                update_count += 1
                # 打印前 10 个更新的股票，避免刷屏
                if update_count <= 10:
                    print(f"[{processed_stocks}/{total_stocks}] ✅ {code} ({code_name}): Added {inserted_count} days (incl. {today})")
                elif update_count == 11:
                    print(f"... (more updates omitted for brevity)")

            # 每 100 只提交一次
            if processed_stocks % 100 == 0:
                conn.commit()

            time.sleep(0.05) # 防限流

        except Exception as e:
            if processed_stocks % 50 == 0:
                print(f"[{processed_stocks}/{total_stocks}] ❌ {code}: {str(e)[:50]}")

    conn.commit()
    conn.close()
    bs.logout()

    print(f"\n🎉 UPDATE FINISHED!")
    print(f"   Total Stocks Checked: {total_stocks}")
    print(f"   Stocks Skipped (Already Updated): {skip_count}")
    print(f"   Stocks Updated (New Data): {update_count}")
    print(f"   Total New Records Inserted: {total_new_records}")


def check_focus_strategy():
    """
    简易版龙抬头筛选逻辑 (仅针对股票)
    """
    conn = get_conn()
    c = conn.cursor()
    today = datetime.now().strftime("%Y-%m-%d")

    # 从 stock_daily_k 查询
    c.execute("""
        SELECT d.code, d.turn, d.pctChg, s.code_name
        FROM stock_daily_k d
        JOIN stock_basic s ON d.code = s.code
        WHERE d.date = ? AND d.pctChg > -2 AND d.pctChg < 2 AND d.turn > 5 AND s.is_st = 0
    """, (today,))

    candidates = c.fetchall()

    # 注意：原代码中的 focus_stock 表已被移除，这里仅打印或可改为插入 daily_candidates
    print(f"Found {len(candidates)} potential focus stocks based on simple strategy.")

    # 如果需要存入 daily_candidates，可以在此处添加逻辑
    # 此处仅为演示，不做入库操作，避免依赖不存在的表

    conn.close()
    print("Focus strategy check completed.")


# ==========================================
# 【关键修复】添加主入口，确保脚本运行时自动执行
# ==========================================
if __name__ == "__main__":
    import traceback

    print("="*50)
    print("🤖 AIWealth Auto Data Sync Started")
    print("="*50)

    try:
        # 1. 更新基础信息
        update_basics()

        # 2. 更新 K 线 (默认最近 3 天，如需全量可改为 730)
        # 建议日常运行保持 3-5 天即可，首次初始化可手动改大
        update_daily_k(days_to_update=3)

        print("\n✅ All tasks completed successfully.")
    except Exception as e:
        print(f"\n❌ Script failed: {e}")
        traceback.print_exc()
        sys.exit(1)