import sys
import logging
from pathlib import Path
from datetime import datetime

# 路径修复
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from db import get_conn

# 日志配置
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')

def generate_candidates_for_strategy(strategy, strategy_key: str, top_n: int = 5):
    logging.info(f"🚀 开始为策略 [{strategy_key}] 生成候选池...")
    conn = get_conn()
    c = conn.cursor()

    today = datetime.now().strftime("%Y-%m-%d")

    # 🔧 获取实际最后一个交易日
    c.execute("SELECT MAX(date) FROM stock_daily_k WHERE date < ?", (today,))
    last_trading_day = c.fetchone()[0]
    if not last_trading_day:
        logging.warning("⚠️ 数据库中无历史K线数据，跳过候选生成")
        conn.close()
        return

    logging.info(f"📅 基准交易日: {last_trading_day} (今日: {today})")

    # 1. 获取核心池
    c.execute("SELECT code, code_name FROM core_pool WHERE is_active=1")
    pool_rows = c.fetchall()
    if not pool_rows:
        logging.warning("⚠️ 核心池为空，跳过候选生成")
        conn.close()
        return

    pool_data = [{"code": r[0], "name": r[1]} for r in pool_rows]
    codes = [p["code"] for p in pool_data]

    # 2. 批量预取历史K线（按日期倒序）
    placeholders = ','.join('?' * len(codes))
    c.execute(f"""
        SELECT code, date, open, high, low, close, volume, amount, turn, pctChg, preclose
        FROM stock_daily_k
        WHERE code IN ({placeholders}) AND date <= ?
        ORDER BY code, date DESC
    """, codes + [last_trading_day])

    raw_k_map = {}
    for row in c.fetchall():
        raw_k_map.setdefault(row['code'], []).append(dict(row))

    # 🔑 核心修复：拆分为 T日(字典) 与 T-1/T-2/T-3(列表)，严格匹配策略签名
    latest_k_map = {}
    history_map = {}
    for code, rows in raw_k_map.items():
        if rows:
            latest_k_map[code] = rows[0]   # T日最新行情 (供策略读取 open_rate 等)
            history_map[code] = rows[1:]   # 历史行情 (供策略读取 turn_1, turn_2, turn_3)

    # 3. 调用策略选股逻辑
    try:
        candidate_codes = strategy.select_candidates(last_trading_day, pool_data, latest_k_map, history_map)
    except Exception as e:
        logging.error(f"❌ 策略 {strategy_key} 选股失败: {e}")
        conn.close()
        return

    # 4. 提取策略内部缓存的评分与理由
    scored_cands = []
    for c_code in candidate_codes:
        info = next((x for x in strategy.yesterday_candidates if x['code'] == c_code), None)
        if info:
            score = info.get('ratio_shrink', info.get('score', 0))
            reason = f"{info.get('name','')} | 缩{info.get('ratio_shrink',0)*100:.1f}% | T1:{info.get('turn_1',0):.2f}%"
            scored_cands.append({
                "code": c_code,
                "score": round(score * 100, 2),
                "reason": reason,
                "sector": "Unknown"
            })

    # 5. 排序取 Top N
    scored_cands.sort(key=lambda x: x['score'], reverse=True)
    top_candidates = scored_cands[:top_n]

    # 6. 安全入库
    c.execute("DELETE FROM daily_candidates WHERE trade_date=? AND strategy_name=?", (today, strategy_key))
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for cand in top_candidates:
        c.execute("""
            INSERT INTO daily_candidates (trade_date, strategy_name, code, score, reason, sector, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (today, strategy_key, cand['code'], cand['score'], cand['reason'], cand['sector'], now_str))
    conn.commit()
    conn.close()

    logging.info(f"✅ [{strategy_key}] 候选池已更新: {[c['code'] for c in top_candidates]}")

if __name__ == "__main__":
    from services.strategies.turnover_shrink import TurnoverShrinkStrategy
    strategy = TurnoverShrinkStrategy(use_core_pool=True)
    generate_candidates_for_strategy(strategy, strategy_key="turnover_shrink", top_n=5)
