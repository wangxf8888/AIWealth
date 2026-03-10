import sqlite3
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

# --- 1. 内置数据库连接与初始化逻辑 (不再依赖外部 db.py) ---

BASE_DIR = Path(__file__).resolve().parent.parent # services -> app
DB_PATH = BASE_DIR / "wealth.db"

def get_conn():
    """获取数据库连接"""
    if not DB_PATH.exists():
        raise FileNotFoundError(f"Database not found at {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def init_db():
    """确保核心表存在"""
    conn = get_conn()
    c = conn.cursor()
    # 只创建我们需要的那个历史表，其他表假设已存在
    c.execute('''CREATE TABLE IF NOT EXISTS core_pool_history (
        trade_date TEXT,
        code TEXT,
        code_name TEXT,
        market_cap REAL,
        max_consecutive_limit INTEGER,
        total_limit_ups_1y INTEGER,
        total_limit_ups_1m INTEGER,
        total_limit_ups_2m INTEGER,
        total_limit_ups_5d INTEGER,
        reason TEXT,
        PRIMARY KEY (trade_date, code)
    )''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_cph_date ON core_pool_history(trade_date)')
    conn.commit()
    conn.close()

# --- 2. 核心算法逻辑 ---

def calculate_limit_up_price(prev_close, code):
    if pd.isna(prev_close) or prev_close == 0: return 0
    code_str = str(code).split('.')[-1] if '.' in str(code) else str(code)
    ratio = 0.20 if (code_str.startswith('3') or code_str.startswith('68')) else 0.10
    return round(prev_close + round(prev_close * ratio, 2), 2)

def is_limit_up_actual(close, preclose, code):
    if pd.isna(close) or pd.isna(preclose) or preclose == 0: return False
    return close >= (calculate_limit_up_price(preclose, code) - 0.001)

def check_single_stock_eligibility(conn, code, target_date):
    """检查单只股票在 target_date 是否符合核心池标准"""
    c = conn.cursor()

    # 1. 基础信息
    c.execute("SELECT code_name, ipo_date, is_st FROM stock_basic WHERE code=?", (code,))
    basic = c.fetchone()
    if not basic: return None
    name, ipo_date, is_st = basic
    if is_st: return None

    # 校验上市时间
    limit_date = (datetime.strptime(target_date, "%Y-%m-%d") - timedelta(days=365)).strftime("%Y-%m-%d")
    valid_ipo = False
    if ipo_date and ipo_date <= limit_date:
        valid_ipo = True
    else:
        c.execute("SELECT MIN(date) FROM stock_daily_k WHERE code=?", (code,))
        min_d = c.fetchone()[0]
        if min_d and min_d <= limit_date: valid_ipo = True
    if not valid_ipo: return None

    # 2. 获取 K 线
    c.execute("""
        SELECT date, close, preclose, amount, turn
        FROM stock_daily_k
        WHERE code = ? AND date <= ?
        ORDER BY date DESC
        LIMIT 250
    """, (code, target_date))
    rows = c.fetchall()
    if len(rows) < 20: return None

    df = pd.DataFrame(rows, columns=['date', 'close', 'preclose', 'amount', 'turn'])

    # 3. 市值计算
    latest = df.iloc[0]
    if latest['turn'] == 0 or pd.isna(latest['turn']): return None
    circ_cap = (latest['amount'] / latest['turn']) * 100 / 100000000.0
    if not (50 <= circ_cap <= 700): return None

    # 4. 涨停统计
    code_suffix = str(code).split('.')[-1]
    is_star_chinext = code_suffix.startswith('3') or code_suffix.startswith('68')

    t_1y, t_1m, t_2m, t_cons, t_5d = (5, 2, 5, 3, 1) if is_star_chinext else (30, 5, 10, 7, 1)

    flags = [is_limit_up_actual(r['close'], r['preclose'], code) for _, r in df.iterrows()]

    count_1y = sum(flags)
    count_1m = sum(flags[:21])
    count_2m = sum(flags[:42])
    count_5d = sum(flags[:5])

    max_cons, curr = 0, 0
    for f in flags:
        if f: curr += 1; max_cons = max(max_cons, curr)
        else: curr = 0

    passed = False
    reasons = []
    board = "Star/ChiNext" if is_star_chinext else "Main"

    checks = [
        (count_1y >= t_1y, f"1Y:{count_1y}"),
        (count_1m >= t_1m, f"1M:{count_1m}"),
        (count_2m >= t_2m, f"2M:{count_2m}"),
        (max_cons >= t_cons, f"Cons:{max_cons}"),
        (count_5d >= t_5d, f"5D:{count_5d}")
    ]

    for ok, label in checks:
        if ok:
            passed = True
            req_val = {'1Y':t_1y, '1M':t_1m, '2M':t_2m, 'Cons':t_cons, '5D':t_5d}[label.split(':')[0]]
            reasons.append(f"{label}(Req:{req_val})")

    if not passed: return None

    return {
        'code': code, 'code_name': name, 'market_cap': round(circ_cap, 2),
        'max_consecutive_limit': max_cons, 'total_limit_ups_1y': count_1y,
        'total_limit_ups_1m': count_1m, 'total_limit_ups_2m': count_2m,
        'total_limit_ups_5d': count_5d, 'reason': f"[{board}] " + ", ".join(reasons)
    }

# --- 3. 主流程 ---

def build_core_pool_history_auto():
    print(f"🚀 Starting Historical Core Pool Construction (Standalone Mode)...")

    conn = get_conn()
    c = conn.cursor()

    # 1. 自动探测数据范围
    c.execute("SELECT MIN(date), MAX(date) FROM stock_daily_k")
    min_date, max_date = c.fetchone()

    if not min_date or not max_date:
        print("❌ No data found in stock_daily_k!")
        return

    print(f"📊 Data Range Detected: {min_date} to {max_date}")

    # 2. 计算有效回测起点
    start_date_obj = datetime.strptime(min_date, "%Y-%m-%d") + timedelta(days=365)
    start_date_str = start_date_obj.strftime("%Y-%m-%d")
    end_date_str = max_date

    if start_date_str > end_date_str:
        print(f"❌ Error: Not enough data history.")
        return

    print(f"✅ Effective Backtest Range: {start_date_str} to {end_date_str}")

    # 3. 获取交易日历
    c.execute("""
        SELECT DISTINCT date FROM stock_daily_k
        WHERE date >= ? AND date <= ?
        ORDER BY date ASC
    """, (start_date_str, end_date_str))
    trade_dates = [row[0] for row in c.fetchall()]

    if not trade_dates:
        print("❌ No trade dates found.")
        return

    total_days = len(trade_dates)
    print(f"📅 Total trading days to process: {total_days}")
    print(f"   Estimated time: ~{total_days * 2} seconds\n")

    yesterday_pool_codes = set()

    for i, t_date in enumerate(trade_dates):
        if i % 50 == 0:
            print(f"[{i+1}/{total_days}] Processing Date: {t_date} ...")

        candidates = set(yesterday_pool_codes)

        c.execute("""
            SELECT code FROM stock_daily_k
            WHERE date = ? AND pctChg > 9.5
        """, (t_date,))
        today_limitups = set([r[0] for r in c.fetchall()])
        candidates.update(today_limitups)

        day_pool_data = []
        for code in candidates:
            res = check_single_stock_eligibility(conn, code, t_date)
            if res:
                day_pool_data.append(res)

        if day_pool_data:
            values = [
                (t_date, item['code'], item['code_name'], item['market_cap'],
                 item['max_consecutive_limit'], item['total_limit_ups_1y'],
                 item['total_limit_ups_1m'], item['total_limit_ups_2m'],
                 item['total_limit_ups_5d'], item['reason'])
                for item in day_pool_data
            ]
            c.executemany("""
                INSERT OR REPLACE INTO core_pool_history
                (trade_date, code, code_name, market_cap, max_consecutive_limit,
                 total_limit_ups_1y, total_limit_ups_1m, total_limit_ups_2m,
                 total_limit_ups_5d, reason)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, values)
            conn.commit()

        yesterday_pool_codes = set([item['code'] for item in day_pool_data])

    conn.close()
    print("\n🎉 Historical Core Pool Construction Finished!")
    print(f"   Valid Range: {start_date_str} to {end_date_str}")
    print(f"   Data saved to: core_pool_history")

if __name__ == "__main__":
    init_db()
    build_core_pool_history_auto()
