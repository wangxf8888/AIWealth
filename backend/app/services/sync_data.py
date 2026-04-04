# backend/app/services/sync_data.py
import baostock as bs
import sqlite3
import pandas as pd
import time
from datetime import datetime, timedelta
import sys
from pathlib import Path

# 【智能导入修复】
try:
    from ..db import get_conn, DB_PATH
except ImportError:
    current_dir = Path(__file__).resolve().parent
    app_dir = current_dir.parent
    if str(app_dir) not in sys.path:
        sys.path.insert(0, str(app_dir))
    from db import get_conn, DB_PATH

_current_session = None

def get_bs_session():
    global _current_session
    if _current_session is None or _current_session.error_code != '0':
        if _current_session is not None:
            try: bs.logout()
            except: pass
        _current_session = bs.login()
        if _current_session.error_code != '0':
            print(f"❌ Login failed: {_current_session.error_msg}")
            return None
    return _current_session

def close_bs_session():
    global _current_session
    if _current_session is not None:
        try: bs.logout()
        except: pass
    _current_session = None

def update_basics():
    print(">>> Starting Smart Incremental Basics Update...")
    lg = bs.login()
    if lg.error_code != '0':
        print(f"❌ Login failed: {lg.error_msg}")
        return

    conn = get_conn()
    c = conn.cursor()
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    today_str = datetime.now().strftime("%Y-%m-%d")

    try:
        # 1. 获取今日全市场代码列表（极快，1~2秒）
        print(f"Step 1: Fetching today's security list...")
        rs_list = bs.query_all_stock(day=today_str)
        if rs_list.error_code != '0':
            today_str = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
            rs_list = bs.query_all_stock(day=today_str)
        if rs_list.error_code != '0':
            print("❌ Failed to fetch stock list from Baostock.")
            bs.logout(); return

        today_codes = set()
        while rs_list.error_code == '0' and rs_list.next():
            row = rs_list.get_row_data()
            if row and len(row) > 0: today_codes.add(row[0])

        # 2. 获取数据库中已存在的代码
        c.execute("SELECT code FROM stock_basic")
        existing_codes = set(row[0] for row in c.fetchall())

        # 3. 增量计算：只处理新增股票
        new_codes = today_codes - existing_codes
        print(f"📊 今日市场: {len(today_codes)} 只 | 库中已有: {len(existing_codes)} 只 | 🆕 新增待处理: {len(new_codes)} 只")

        if not new_codes:
            print("✅ 无新增股票，跳过详细信息拉取。基本信息更新完成。")
            conn.close(); bs.logout(); return

        stock_count = 0
        index_count = 0
        skip_count = 0

        # 4. 仅对新增股票调用耗时的 query_stock_basic
        print(f"🔄 正在获取 {len(new_codes)} 只新股的详细信息...")
        for i, code_raw in enumerate(new_codes):
            try:
                rs_detail = bs.query_stock_basic(code=code_raw)
                if rs_detail.error_code != '0' or not rs_detail.next():
                    skip_count += 1; continue
                row = rs_detail.get_row_data()
                if len(row) < 6: skip_count += 1; continue

                d_code, d_name, d_type, d_status = row[0], row[1], row[4], row[5]
                if d_status != '1' or d_type not in ['1', '2']: skip_count += 1; continue
                if '.' not in d_code or len(d_code) != 9: skip_count += 1; continue

                is_st = 1 if 'ST' in d_name else 0
                if d_type == '1':
                    c.execute("INSERT OR REPLACE INTO stock_basic (code, code_name, tradeStatus, is_st, ipo_date, out_date, update_time) VALUES (?, ?, ?, ?, ?, ?, ?)",
                              (d_code, d_name, d_status, is_st, row[2], row[3], now_str))
                    stock_count += 1
                elif d_type == '2':
                    base_point = float(row[3]) if row[3] else 0.0
                    c.execute("INSERT OR REPLACE INTO index_basic (code, code_name, tradeStatus, base_date, base_point, update_time) VALUES (?, ?, ?, ?, ?, ?)",
                              (d_code, d_name, d_status, row[2], base_point, now_str))
                    index_count += 1

                if (i + 1) % 5 == 0: conn.commit()
                time.sleep(0.1) # 防封
            except Exception:
                skip_count += 1

        conn.commit()
        print(f"\n✅ === Incremental Update Finished ===")
        print(f"   新增股票: {stock_count} | 新增指数: {index_count} | 跳过: {skip_count}")

    except Exception as e:
        print(f"❌ Critical Error: {e}")
        import traceback; traceback.print_exc()
    finally:
        conn.close(); bs.logout()
        print("<<< Logout success.")

def update_daily_k(days_to_update=3):
    print(f">>> Starting Optimized K-Line Update (Last {days_to_update} Days)...")
    lg = bs.login()
    if lg.error_code != '0':
        print(f"❌ Login failed: {lg.error_msg}")
        return

    conn = get_conn()
    c = conn.cursor()
    today = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=days_to_update)).strftime("%Y-%m-%d")

    # 1. 批量获取已有今日数据的股票，避免逐条查询 DB
    c.execute("SELECT DISTINCT code FROM stock_daily_k WHERE date=?", (today,))
    updated_codes = set(row[0] for row in c.fetchall())
    print(f"📊 已有今日数据: {len(updated_codes)} 只，直接跳过")

    c.execute("SELECT code, code_name FROM stock_basic")
    all_stocks = c.fetchall()
    stocks_to_update = [(code, name) for code, name in all_stocks if code not in updated_codes]
    total = len(stocks_to_update)
    print(f"🎯 待更新: {total} 只股票 | 时间范围: {start_date} ~ {today}")

    batch_data = []
    inserted_count = 0
    api_calls = 0

    for idx, (code, code_name) in enumerate(stocks_to_update):
        try:
            # 调用日线
            rs_daily = bs.query_history_k_data_plus(
                code, "date,open,high,low,close,preclose,volume,amount,turn,pctChg",
                start_date=start_date, end_date=today, frequency="d", adjustflag="3"
            )
            api_calls += 1
            if rs_daily.error_code != '0': continue

            daily_list = []
            while rs_daily.next(): daily_list.append(rs_daily.get_row_data())
            if not daily_list: continue

            # 调用60分钟线 (仅当有新日线时)
            rs_hourly = bs.query_history_k_data_plus(
                code, "date,time,open,high,low,close",
                start_date=start_date, end_date=today, frequency="60", adjustflag="3"
            )
            api_calls += 1
            hourly_map = {}
            if rs_hourly.error_code == '0':
                h_list = []
                while rs_hourly.next(): h_list.append(rs_hourly.get_row_data())
                if h_list:
                    df_h = pd.DataFrame(h_list, columns=['date','time','open','high','low','close'])
                    for date_str, grp in df_h.groupby('date'):
                        hourly_map[date_str] = grp.sort_values('time').head(4).to_dict('records')

            df_d = pd.DataFrame(daily_list, columns=['date','open','high','low','close','preclose','volume','amount','turn','pctChg'])

            for _, row in df_d.iterrows():
                date_str = row['date']
                preclose = float(row['preclose'] or 0) or float(row['close'] or 0)
                if preclose == 0: continue

                o, h, l, c_val = float(row['open']), float(row['high']), float(row['low']), float(row['close'])
                vol, amt, turn, pct = float(row['volume'] or 0), float(row['amount'] or 0), float(row['turn'] or 0), float(row['pctChg'] or 0)

                o_r, c_r, h_r, l_r = round((o-preclose)/preclose*100,2), round((c_val-preclose)/preclose*100,2), \
                                       round((h-preclose)/preclose*100,2), round((l-preclose)/preclose*100,2)

                h_rows = hourly_map.get(date_str, [])
                hr_vals = []
                for i in range(4):
                    if i < len(h_rows):
                        hr = h_rows[i]
                        ho, hc, hh, hl = float(hr.get('open',0)), float(hr.get('close',0)), float(hr.get('high',0)), float(hr.get('low',0))
                        hr_vals.extend([round((ho-preclose)/preclose*100,2), round((hc-preclose)/preclose*100,2),
                                        round((hh-preclose)/preclose*100,2), round((hl-preclose)/preclose*100,2)])
                    else:
                        hr_vals.extend([0.0]*4)

                batch_data.append((date_str, code, code_name, o, h, l, c_val, preclose, vol, amt, turn, pct,
                                   o_r, c_r, h_r, l_r, *hr_vals))
                inserted_count += 1

            # 🔧 修改：每处理 10 只股票打印一次进度
            if (idx + 1) % 10 == 0:
                print(f"   📈 进度: {idx+1}/{total} | 已收集 {len(batch_data)} 条 | API调用: {api_calls}")
                time.sleep(0.1)  # 保持 Baostock 防封限速

        except Exception:
            pass

    # 2. 批量写入 SQLite (500条/批)
    if batch_data:
        print(f"\n💾 正在批量写入 {len(batch_data)} 条记录到数据库...")
        sql = """INSERT OR IGNORE INTO stock_daily_k
                 (date, code, code_name, open, high, low, close, preclose, volume, amount, turn, pctChg,
                  open_rate, close_rate, high_rate, low_rate,
                  hour1_open_rate, hour1_close_rate, hour1_high_rate, hour1_low_rate,
                  hour2_open_rate, hour2_close_rate, hour2_high_rate, hour2_low_rate,
                  hour3_open_rate, hour3_close_rate, hour3_high_rate, hour3_low_rate,
                  hour4_open_rate, hour4_close_rate, hour4_high_rate, hour4_low_rate)
                 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
        for i in range(0, len(batch_data), 500):
            c.executemany(sql, batch_data[i:i+500])
            conn.commit()
        print(f"✅ 批量写入完成!")
    else:
        print("ℹ️ 无新数据需要写入")

    conn.close()
    bs.logout()
    print(f"\n🎉 UPDATE FINISHED! 新增记录: {inserted_count} | API总调用: {api_calls}")


if __name__ == "__main__":
    import traceback
    print("="*50)
    print("🤖 AIWealth Auto Data Sync Started")
    print("="*50)
    try:
        update_basics()
        update_daily_k(days_to_update=3)
        print("\n✅ All tasks completed successfully.")
    except Exception as e:
        print(f"\n❌ Script failed: {e}")
        traceback.print_exc()
        sys.exit(1)
