import sqlite3
import pandas as pd
from datetime import datetime, timedelta
from ..db import get_conn

def calculate_limit_up_price(prev_close, code):
    """精确计算理论涨停价"""
    if pd.isna(prev_close) or prev_close == 0:
        return 0

    code_str = str(code)
    num_part = code_str.split('.')[1] if '.' in code_str else code_str

    ratio = 0.20 if (num_part.startswith('3') or num_part.startswith('68')) else 0.10
    increment = prev_close * ratio
    rounded_increment = round(increment, 2)
    limit_price = prev_close + rounded_increment
    return round(limit_price, 2)

def is_limit_up_actual(close, prev_close, code):
    """判断当日是否涨停"""
    if pd.isna(close) or pd.isna(prev_close) or prev_close == 0:
        return False
    limit_price = calculate_limit_up_price(prev_close, code)
    return close >= (limit_price - 0.001)

def build_core_pool():
    print(">>> Starting Core Pool Construction (Added: Last 5 Days Limit Up)...")

    conn = get_conn()
    c = conn.cursor()

    today = datetime.now().strftime("%Y-%m-%d")
    one_year_ago_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")

    c.execute("DELETE FROM core_pool")
    print("🧹 Cleared old core_pool.")

    c.execute("""
        SELECT code, code_name, ipo_date
        FROM stock_basic
        WHERE is_st = 0
    """)
    candidates = c.fetchall()

    print(f"🔍 Found {len(candidates)} non-ST candidates. Analyzing...")

    core_stocks = []
    processed = 0

    for code, name, ipo_date in candidates:
        processed += 1
        if processed % 500 == 0:
            print(f"   Progress: {processed}/{len(candidates)}...")

        try:
            # 获取最近 250 个交易日数据
            sql_k = """
                SELECT date, close, preclose, amount, turn, pctChg
                FROM stock_daily_k
                WHERE code = ?
                ORDER BY date DESC
                LIMIT 250
            """
            c.execute(sql_k, (code,))
            k_data = c.fetchall()

            if not k_data or len(k_data) < 20:
                continue

            df = pd.DataFrame(k_data, columns=['date', 'close', 'preclose', 'amount', 'turn', 'pctChg'])

            # --- 上市时间校验 ---
            valid_ipo = False
            if ipo_date and ipo_date != '' and ipo_date <= one_year_ago_date:
                valid_ipo = True
            else:
                c.execute("SELECT MIN(date) FROM stock_daily_k WHERE code=?", (code,))
                min_date_res = c.fetchone()[0]
                if min_date_res and min_date_res <= one_year_ago_date:
                    valid_ipo = True

            if not valid_ipo:
                continue

            # --- 流通市值校验 (50-700 亿) ---
            latest = df.iloc[0]
            if latest['turn'] == 0 or latest['turn'] is None or pd.isna(latest['turn']):
                continue

            circ_market_cap = (latest['amount'] / latest['turn']) * 100
            circ_market_cap_yi = circ_market_cap / 100000000.0

            if not (50 <= circ_market_cap_yi <= 700):
                continue

            # --- 板块判断与阈值设定 ---
            code_str = str(code).split('.')[-1] if '.' in str(code) else str(code)
            is_star_chinext = code_str.startswith('3') or code_str.startswith('68')

            if is_star_chinext:
                t_1y, t_1m, t_2m, t_cons = 8, 2, 4, 3
                board_name = "Star/ChiNext"
            else:
                t_1y, t_1m, t_2m, t_cons = 20, 4, 8, 7
                board_name = "Main"

            # --- 计算涨停标记 ---
            limit_flags = []
            for _, row in df.iterrows():
                if is_limit_up_actual(row['close'], row['preclose'], code):
                    limit_flags.append(True)
                else:
                    limit_flags.append(False)

            df['is_limit'] = limit_flags

            # --- 统计各项指标 ---
            count_1y = sum(limit_flags)             # 1 年

            df_1m = df.head(21)                     # 1 月
            count_1m = df_1m['is_limit'].sum()

            df_2m = df.head(42)                     # 2 月
            count_2m = df_2m['is_limit'].sum()

            df_5d = df.head(5)                      # [新增] 最近 5 天
            count_5d = df_5d['is_limit'].sum()

            # 最大连续涨停
            max_consecutive = 0
            current_consecutive = 0
            for flag in limit_flags:
                if flag:
                    current_consecutive += 1
                    max_consecutive = max(max_consecutive, current_consecutive)
                else:
                    current_consecutive = 0

            # --- 判定入选 (满足任一即可) ---
            passed = False
            reasons = []

            if count_1y >= t_1y:
                passed = True
                reasons.append(f"1Y:{int(count_1y)}(Req:{t_1y})")

            if count_1m >= t_1m:
                passed = True
                reasons.append(f"1M:{int(count_1m)}(Req:{t_1m})")

            if count_2m >= t_2m:
                passed = True
                reasons.append(f"2M:{int(count_2m)}(Req:{t_2m})")

            if max_consecutive >= t_cons:
                passed = True
                reasons.append(f"Cons:{max_consecutive}(Req:{t_cons})")

            # [新增逻辑] 最近 5 天有涨停
            if count_5d >= 1:
                passed = True
                reasons.append(f"5D:{int(count_5d)}(Req:1)")

            if not passed:
                continue

            reason_str = f"[{board_name}] " + ", ".join(reasons)

            core_stocks.append({
                'code': code,
                'code_name': name,
                'market_cap': round(circ_market_cap_yi, 2),
                'max_consecutive_limit': max_consecutive,
                'total_limit_ups_1y': int(count_1y),
                'total_limit_ups_1m': int(count_1m),
                'total_limit_ups_2m': int(count_2m),
                'total_limit_ups_5d': int(count_5d), # 可选：如果需要存库可加字段，这里仅用于逻辑判断
                'reason': reason_str,
                'last_verified_date': today
            })

        except Exception as e:
            pass

    # 写入数据库
    if core_stocks:
        for item in core_stocks:
            # 注意：core_pool 表结构未变，只存入 reason 体现 5D 信息
            c.execute("""
                INSERT OR REPLACE INTO core_pool
                (code, code_name, market_cap, max_consecutive_limit, total_limit_ups_1y, total_limit_ups_1m, total_limit_ups_2m, reason, last_verified_date, is_active)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
            """, (
                item['code'], item['code_name'], item['market_cap'], item['max_consecutive_limit'],
                item['total_limit_ups_1y'], item['total_limit_ups_1m'], item['total_limit_ups_2m'],
                item['reason'], item['last_verified_date']
            ))
        conn.commit()

    print(f"\n✅ Core Pool Construction Finished!")
    print(f"   Total Stocks Selected: {len(core_stocks)}")

    if len(core_stocks) > 0:
        print("\n   🔥 Top 5 Examples (Prioritizing Recent Activity - 5D):")
        # 排序策略：优先看最近 5 天是否有涨停，其次看 2 个月次数
        # key: (count_5d desc, count_2m desc)
        sorted_stocks = sorted(core_stocks, key=lambda x: (x.get('total_limit_ups_5d', 0), x['total_limit_ups_2m']), reverse=True)[:5]
        for s in sorted_stocks:
            d5 = s.get('total_limit_ups_5d', 0)
            marker = "🔥" if d5 > 0 else "⏳"
            print(f"   {marker} {s['code']} ({s['code_name']}): Cap={s['market_cap']}B | 5D={d5} | {s['reason']}")
    else:
        print("   ⚠️ No stocks matched.")

    conn.close()

if __name__ == "__main__":
    build_core_pool()
