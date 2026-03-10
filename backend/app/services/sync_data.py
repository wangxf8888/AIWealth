import baostock as bs
import sqlite3
import pandas as pd
import time
from datetime import datetime, timedelta
from ..db import get_conn, DB_PATH

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
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        print(f"Step 1: Fetching all security codes for date: {yesterday} ...")

        rs_list = bs.query_all_stock(day=yesterday)

        # 降级策略：如果昨天不行，试前天
        if rs_list.error_code != '0':
            day_before = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")
            print(f"⚠️ Fallback to date: {day_before} ...")
            rs_list = bs.query_all_stock(day=day_before)

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

                if (i + 1) % 500 == 0:
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

def update_daily_k():
    """
    【终极稳健版】全量历史数据同步 (2 年) + 自动重试 + 数据清洗
    """
    print(">>> Starting ROBUST Full History Update (2 Years)...")

    lg = bs.login()
    if lg.error_code != '0':
        print(f"❌ Login failed: {lg.error_msg}")
        return

    conn = get_conn()
    c = conn.cursor()

    today = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=730)).strftime("%Y-%m-%d")
    print(f"📅 Date Range: {start_date} to {today}")
    print("⚡ Strategy: Batch Fetch + Auto-Retry + Data Cleaning")

    total_new_records = 0
    processed_stocks = 0
    final_failures = 0

    # ==========================================
    # 第一部分：更新股票
    # ==========================================
    print("\n--- Processing Stocks ---")
    c.execute("SELECT code, code_name FROM stock_basic")
    stocks = c.fetchall()
    total_stocks = len(stocks)

    for idx, (code, code_name) in enumerate(stocks):
        processed_stocks += 1
        max_retries = 2
        attempt = 0
        success = False

        while attempt < max_retries and not success:
            try:
                attempt += 1
                if attempt > 1:
                    print(f"   🔄 Retrying {code} (Attempt {attempt}/{max_retries})...")
                    time.sleep(2) # 重试前休眠
                    # 重新登录以防会话过期
                    if lg.error_code != '0':
                        lg = bs.login()

                # 【快速跳过检查】
                c.execute("SELECT date FROM stock_daily_k WHERE code=? AND date=? LIMIT 1", (code, start_date))
                if c.fetchone():
                    if processed_stocks % 50 == 0:
                        print(f"[{processed_stocks}/{total_stocks}] ✅ {code} ({code_name}): Skipped")
                    success = True # 视为成功，跳出重试循环
                    continue

                # 1. 获取日 K
                rs_daily = bs.query_history_k_data_plus(
                    code, "date,open,high,low,close,preclose,volume,amount,turn,pctChg",
                    start_date=start_date, end_date=today,
                    frequency="d", adjustflag="3"
                )

                if rs_daily.error_code != '0':
                    raise Exception(f"Daily Query Failed: {rs_daily.error_msg}")

                daily_list = []
                while rs_daily.next():
                    daily_list.append(rs_daily.get_row_data())

                if not daily_list:
                    # 无数据不算错误，直接跳过
                    success = True
                    break

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

                    # 查重
                    c.execute("SELECT id FROM stock_daily_k WHERE code=? AND date=?", (code, date_str))
                    if c.fetchone():
                        continue

                    # 【关键修复】安全转换浮点数，处理空字符串
                    def safe_float(val, default=0.0):
                        if val is None or val == '':
                            return default
                        try:
                            return float(val)
                        except ValueError:
                            return default

                    preclose = safe_float(row['preclose'])
                    if preclose == 0:
                        # 如果 preclose 为 0，尝试用 close 代替
                        preclose = safe_float(row['close'])
                        if preclose == 0:
                            continue # 无法计算，跳过该行

                    close_val = safe_float(row['close'])
                    open_val = safe_float(row['open'])
                    high_val = safe_float(row['high'])
                    low_val = safe_float(row['low'])
                    vol_val = safe_float(row['volume'])
                    amt_val = safe_float(row['amount'])
                    turn_val = safe_float(row['turn'])
                    pct_val = safe_float(row['pctChg'])

                    # 计算 Rates
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
                        preclose, vol_val, amt_val, turn_val, pct_val,
                        o_r, c_r, h_r, l_r,
                        hour_rates.get('hour1_open_rate'), hour_rates.get('hour1_close_rate'), hour_rates.get('hour1_high_rate'), hour_rates.get('hour1_low_rate'),
                        hour_rates.get('hour2_open_rate'), hour_rates.get('hour2_close_rate'), hour_rates.get('hour2_high_rate'), hour_rates.get('hour2_low_rate'),
                        hour_rates.get('hour3_open_rate'), hour_rates.get('hour3_close_rate'), hour_rates.get('hour3_high_rate'), hour_rates.get('hour3_low_rate'),
                        hour_rates.get('hour4_open_rate'), hour_rates.get('hour4_close_rate'), hour_rates.get('hour4_high_rate'), hour_rates.get('hour4_low_rate')
                    )

                    c.execute(sql, vals)
                    inserted_count += 1
                    total_new_records += 1

                # 成功处理完一只股票
                status = "✅" if inserted_count > 0 else "⚪"
                if attempt > 1:
                    print(f"[{processed_stocks}/{total_stocks}] {status} {code} ({code_name}): +{inserted_count} (Recovered)")
                elif processed_stocks % 10 == 0 or inserted_count > 0:
                    print(f"[{processed_stocks}/{total_stocks}] {status} {code} ({code_name}): +{inserted_count}")

                success = True

            except Exception as e:
                error_msg = str(e)
                if attempt == max_retries:
                    final_failures += 1
                    print(f"[{processed_stocks}/{total_stocks}] ❌ {code}: FAILED after retries. Error: {error_msg}")
                else:
                    print(f"[{processed_stocks}/{total_stocks}] ⚠️ {code}: Error ({error_msg}). Retrying...")

        # 每 100 只提交一次
        if processed_stocks % 100 == 0:
            conn.commit()
            print(f"   >>> Checkpoint saved at {processed_stocks}.")

        # 正常休眠防限流
        time.sleep(0.1)

    conn.commit()

    # ==========================================
    # 第二部分：指数 (简化版，不含复杂重试逻辑，因数量少)
    # ==========================================
    print("\n--- Processing Indices ---")
    c.execute("SELECT code, code_name FROM index_basic")
    indices = c.fetchall()

    for code, code_name in indices:
        try:
            c.execute("SELECT date FROM index_daily_k WHERE code=? AND date=? LIMIT 1", (code, start_date))
            if c.fetchone():
                continue

            rs_d = bs.query_history_k_data_plus(code, "date,open,high,low,close,preclose,volume,amount,turn,pctChg",
                                                start_date=start_date, end_date=today, frequency="d", adjustflag="3")
            if rs_d.error_code != '0': continue

            d_list = []
            while rs_d.next(): d_list.append(rs_d.get_row_data())
            if not d_list: continue

            df_d = pd.DataFrame(d_list, columns=['date','open','high','low','close','preclose','volume','amount','turn','pctChg'])
            inserted = 0

            for _, row in df_d.iterrows():
                date_str = row['date']
                c.execute("SELECT id FROM index_daily_k WHERE code=? AND date=?", (code, date_str))
                if c.fetchone(): continue

                # 安全转换
                def safe_float(v, d=0.0):
                    return float(v) if v and v != '' else d

                preclose = safe_float(row['preclose'])
                if preclose == 0: preclose = safe_float(row['close'])
                if preclose == 0: continue

                vals = (
                    date_str, code, code_name,
                    safe_float(row['open']), safe_float(row['high']), safe_float(row['low']), safe_float(row['close']),
                    preclose, safe_float(row['volume']), safe_float(row['amount']), safe_float(row['turn']), safe_float(row['pctChg']),
                    round((safe_float(row['open']) - preclose)/preclose*100, 2),
                    round((safe_float(row['close']) - preclose)/preclose*100, 2),
                    round((safe_float(row['high']) - preclose)/preclose*100, 2),
                    round((safe_float(row['low']) - preclose)/preclose*100, 2)
                )

                sql = """INSERT INTO index_daily_k (...columns...) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)"""
                # 注意：上面 SQL 省略了列名以节省空间，实际请使用完整 SQL，参考股票部分，只是小时部分填 NULL
                # 为严谨，这里复用完整的 SQL 字符串（略长，建议直接复制股票部分的 SQL 模板，把小时值换成 NULL）
                # 此处为了代码简洁，假设你已知道如何构造，或者直接使用下面的简写逻辑：

                full_sql = """
                    INSERT INTO index_daily_k
                    (date, code, code_name, open, high, low, close, preclose, volume, amount, turn, pctChg,
                     open_rate, close_rate, high_rate, low_rate,
                     hour1_open_rate, hour1_close_rate, hour1_high_rate, hour1_low_rate,
                     hour2_open_rate, hour2_close_rate, hour2_high_rate, hour2_low_rate,
                     hour3_open_rate, hour3_close_rate, hour3_high_rate, hour3_low_rate,
                     hour4_open_rate, hour4_close_rate, hour4_high_rate, hour4_low_rate)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                            NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
                """
                c.execute(full_sql, vals)
                inserted += 1
                total_new_records += 1

            print(f"✅ {code} ({code_name}): +{inserted}")
            time.sleep(0.1)
        except Exception as e:
            print(f"❌ Index {code}: {e}")

    conn.commit()
    conn.close()
    bs.logout()

    print(f"\n🎉 ROBUST UPDATE FINISHED!")
    print(f"Total Records: {total_new_records}")
    print(f"Final Failures: {final_failures}")

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
