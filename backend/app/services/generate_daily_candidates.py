import sqlite3
import pandas as pd
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

def generate_daily_candidates():
    """
    任务：从核心池中筛选明日候选
    逻辑：
    1. 取出核心池所有股票
    2. 获取今日日 K (涨跌幅 -2% ~ 2%, 换手率企稳)
    3. 分析最近 20 日趋势 (龙抬头：均线多头，缩量回调)
    4. 结合板块轮动 (简化版：检查所属板块今日是否强势)
    """
    print(">>> Generating Daily Candidates from Core Pool...")

    conn = get_conn()
    c = conn.cursor()
    today = datetime.now().strftime("%Y-%m-%d")
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    # 1. 获取核心池股票列表
    c.execute("SELECT code, code_name FROM core_pool WHERE is_active = 1")
    core_stocks = c.fetchall()

    candidates = []

    for code, name in core_stocks:
        # 2. 获取最近 20 日 K 线
        c.execute("""
            SELECT date, open, high, low, close, volume, turn, pctChg
            FROM daily_k
            WHERE code = ? AND date <= ?
            ORDER BY date DESC
            LIMIT 20
        """, (code, today))

        rows = c.fetchall()
        if len(rows) < 5: continue # 数据不足

        # 转为 DataFrame 方便计算
        df = pd.DataFrame(rows, columns=['date','open','high','low','close','volume','turn','pctChg'])
        df = df.iloc[::-1] # 反转为正序时间

        latest = df.iloc[-1]
        prev_days = df.iloc[:-1]

        # --- 策略判断逻辑 ---

        # 条件 A: 今日涨跌幅在 -2% 到 2% 之间 (蓄势)
        if not (-2.0 <= float(latest['pctChg']) <= 2.0):
            continue

        # 条件 B: 换手率企稳 (今日换手 > 5% 且 不过分夸张)
        if float(latest['turn']) < 5.0:
            continue

        # 条件 C: 龙抬头形态 (简化版：5 日线 > 10 日线，且股价在 5 日线附近)
        # 计算 MA5, MA10
        df['MA5'] = df['close'].rolling(5).mean()
        df['MA10'] = df['close'].rolling(10).mean()

        if len(df) < 10: continue

        ma5 = df['MA5'].iloc[-1]
        ma10 = df['MA10'].iloc[-1]
        close = float(latest['close'])

        if ma5 <= ma10: # 必须是多头排列
            continue

        # 股价回踩 5 日线或在其上方不远处
        if close < ma5 * 0.98:
            continue

        # 条件 D: 板块轮动 (简化：假设如果今日大盘好，或者该股所属概念今日涨幅前 10，则加分)
        # 此处暂略复杂板块计算，默认核心池股票自带属性

        score = 80.0
        reason = f"龙抬头形态确认 (MA5>MA10), 今日震荡 ({latest['pctChg']}%), 换手活跃 ({latest['turn']}%)"

        # 额外加分项：如果是缩量回调
        if float(latest['volume']) < float(df['volume'].iloc[-2]):
            score += 10
            reason += "; 缩量回调"

        candidates.append({
            "code": code,
            "name": name,
            "score": score,
            "reason": reason,
            "sector": "Unknown", # 需补充板块映射
            "date": today
        })

    # 排序并入库
    candidates.sort(key=lambda x: x['score'], reverse=True)

    # 清空今日候选
    c.execute("DELETE FROM daily_candidates WHERE trade_date = ?", (today,))

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for cand in candidates[:20]: # 只取前 20 名
        c.execute("""
            INSERT INTO daily_candidates (trade_date, code, score, reason, sector, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (cand['date'], cand['code'], cand['score'], cand['reason'], cand['sector'], now_str))

    conn.commit()
    conn.close()
    print(f">>> Daily Candidates Generated: {len(candidates)} selected.")
    return candidates

if __name__ == "__main__":
    # generate_daily_candidates()
    pass

