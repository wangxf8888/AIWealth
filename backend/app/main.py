# backend/app/main.py
from fastapi import FastAPI, HTTPException, Query, Body
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime
import uvicorn
from apscheduler.schedulers.background import BackgroundScheduler
import requests, re
import sqlite3
import os

from .db import init_db, get_conn
from .services.sync_data import update_basics, update_daily_k
from .services.build_core_pool import build_core_pool
from .services.generate_daily_candidates import generate_daily_candidates

app = FastAPI(title="AIWealth Pro")

# 允许跨域
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# 初始化 DB
init_db()

# 定时任务调度器
scheduler = BackgroundScheduler()

# ... (原有的定时任务代码保持不变) ...
@scheduler.scheduled_job('cron', day=1, hour=2, minute=0)
def job_rebuild_core_pool():
    try: build_core_pool()
    except Exception as e: print(f"Error: {e}")

@scheduler.scheduled_job('cron', hour=19, minute=0)
def job_daily_update():
    try:
        update_basics()
        update_daily_k()
    except Exception as e: print(f"Error: {e}")

@scheduler.scheduled_job('cron', hour=20, minute=0)
def job_evening_analysis():
    try:
        generate_daily_candidates()
        conn = get_conn()
        c = conn.cursor()
        content = f"【盘后复盘 {datetime.now().strftime('%Y-%m-%d')}】\n今日市场情绪分析...\n核心池个股表现...\n明日策略：关注龙抬头形态个股。"
        c.execute("INSERT INTO analysis_reports (report_date, period, content, created_at) VALUES (?, ?, ?, ?)",
                  (datetime.now().strftime("%Y-%m-%d"), "post", content, datetime.now()))
        conn.commit()
        conn.close()
    except Exception as e: print(f"Error: {e}")

scheduler.start()

# --- API 接口 ---

@app.get("/api/health")
def health():
    return {"status": "ok", "time": datetime.now()}

@app.get("/api/realtime/{code}")
def get_realtime(code: str):
    prefix = "sh" if code.startswith('6') else "sz"
    # 自动补全前缀逻辑
    if '.' not in code:
        url = f"http://hq.sinajs.cn/list={prefix}{code}"
    else:
        url = f"http://hq.sinajs.cn/list={code}"

    try:
        res = requests.get(url, timeout=2)
        res.encoding = 'gbk'
        text = res.text
        match = re.search(r'="([^"]+)"', text)
        if match:
            data = match.group(1).split(',')
            if len(data) < 4: return {"error": "data incomplete"}
            name = data[0]
            pre = float(data[2])
            price = float(data[3])
            pct = ((price - pre) / pre) * 100 if pre != 0 else 0
            vol = int(float(data[8])) if data[8] else 0
            return {
                "code": code, "name": name, "price": round(price, 2),
                "change_pct": round(pct, 2), "volume": vol, "alert": abs(pct) > 3.0
            }
    except Exception as e:
        pass
    return {"error": "fetch failed"}

@app.get("/api/focus/list")
def get_focus_list():
    conn = get_conn()
    c = conn.cursor()
    c.execute("SELECT code, code_name, reason, total_limit_ups_1y FROM core_pool WHERE is_active=1 ORDER BY total_limit_ups_1y DESC LIMIT 50")
    rows = c.fetchall()
    conn.close()
    return [{"code": r[0], "name": r[1], "reason": r[2], "ups_1y": r[3]} for r in rows]

@app.get("/api/candidates/today")
def get_today_candidates():
    conn = get_conn()
    c = conn.cursor()
    today = datetime.now().strftime("%Y-%m-%d")
    c.execute("SELECT code, score, reason, sector FROM daily_candidates WHERE trade_date=? ORDER BY score DESC", (today,))
    rows = c.fetchall()
    conn.close()
    return [{"code": r[0], "score": r[1], "reason": r[2], "sector": r[3]} for r in rows]

@app.get("/api/analysis/{period}")
def get_analysis(period: str):
    conn = get_conn()
    c = conn.cursor()
    c.execute("SELECT content, created_at FROM analysis_reports WHERE period=? ORDER BY id DESC LIMIT 1", (period,))
    row = c.fetchone()
    conn.close()
    if row:
        return {"content": row[0], "time": row[1]}
    return {"content": "暂无分析报告", "time": ""}

@app.get("/api/kline/{code}")
def get_kline(code: str, days: int = 20):
    conn = get_conn()
    c = conn.cursor()
    # 修复表名
    c.execute("""
        SELECT date, open, high, low, close, volume, turn, pctChg
        FROM stock_daily_k
        WHERE code=?
        ORDER BY date DESC
        LIMIT ?
    """, (code, days))
    rows = c.fetchall()
    conn.close()
    data = [{
        "date": r[0], "open": r[1], "high": r[2], "low": r[3], "close": r[4],
        "volume": r[5], "turn": r[6], "pct": r[7]
    } for r in reversed(rows)]
    return data

@app.get("/api/backtest/{strategy_key}")
def get_backtest_data(strategy_key: str):
    """
    获取指定策略的回测数据，并将买卖记录合并为完整交易回合
    """
    strategy_map = {
        "dragon": "DragonHead",
        "chase": "Chase_",
        "break": "BreakOneWord",
        "shrink": "Turnover_Shrink"
    }

    if strategy_key not in strategy_map:
        raise HTTPException(status_code=404, detail="Strategy key not found")

    search_term = strategy_map[strategy_key]
    conn = get_conn()
    c = conn.cursor()

    # 1. 查询资金曲线
    c.execute("""
        SELECT trade_date, total_value, cash
        FROM backtest_daily_snapshot
        WHERE strategy_name LIKE ?
        ORDER BY trade_date ASC
    """, (f"%{search_term}%",))
    curve_rows = c.fetchall()
    equity_curve = [{"date": r[0], "value": r[1]} for r in curve_rows]

    # 2. 查询所有交易记录 (包含 BUY 和 SELL)
    c.execute("""
        SELECT trade_date, code, action, price, profit_loss, reason
        FROM backtest_trades
        WHERE strategy_name LIKE ?
        ORDER BY trade_date ASC, code ASC
    """, (f"%{search_term}%",))
    all_trade_rows = c.fetchall()

    # 3. 【核心逻辑】将买卖记录配对成完整交易回合，并查询股票名称
    buys_pending = {}
    completed_trades = []
    total_realized_profit = 0.0

    for r in all_trade_rows:
        t_date = r[0]
        t_code = r[1]
        t_action = r[2]
        t_price = r[3]
        t_profit = r[4] if r[4] else 0.0
        t_reason = r[5]

        # 【新增】查询股票名称 (先查 stock_basic，再查 core_pool_history)
        stock_name = None
        c.execute("SELECT code_name FROM stock_basic WHERE code=?", (t_code,))
        res = c.fetchone()
        if res and res['code_name']:
            stock_name = res['code_name']
        else:
            c.execute("SELECT code_name FROM core_pool_history WHERE code=? LIMIT 1", (t_code,))
            res = c.fetchone()
            if res and res['code_name']:
                stock_name = res['code_name']

        # 如果都没查到，就用代码本身，或者截取代码后半部分
        display_name = stock_name if stock_name else t_code

        if t_action == 'BUY':
            if t_code not in buys_pending:
                buys_pending[t_code] = []
            buys_pending[t_code].append({
                "date": t_date,
                "price": t_price,
                "reason": t_reason,
                "name": display_name # 保存名称
            })

        elif t_action == 'SELL':
            matched_buy = None
            if t_code in buys_pending and len(buys_pending[t_code]) > 0:
                matched_buy = buys_pending[t_code].pop(0)

            if matched_buy:
                completed_trades.append({
                    "code": t_code,
                    "name": matched_buy["name"], # 使用买入时查到的名称
                    "buy_date": matched_buy["date"],
                    "buy_price": matched_buy["price"],
                    "sell_date": t_date,
                    "sell_price": t_price,
                    "profit": t_profit,
                    "reason": f"{matched_buy['reason']} -> {t_reason}"
                })
                total_realized_profit += t_profit
            else:
                completed_trades.append({
                    "code": t_code,
                    "name": display_name,
                    "buy_date": "未知",
                    "buy_price": 0,
                    "sell_date": t_date,
                    "sell_price": t_price,
                    "profit": t_profit,
                    "reason": t_reason
                })
                total_realized_profit += t_profit

    conn.close()

    # 4. 计算统计数据
    total_return = 0.0
    win_rate = 0.0
    initial_capital = 100000.0

    # A. 如果有曲线数据，以曲线为准
    if len(equity_curve) > 0:
        start_val = equity_curve[0]['value']
        end_val = equity_curve[-1]['value']
        total_return = ((end_val - start_val) / start_val) * 100

        if len(completed_trades) > 0:
            win_count = sum(1 for t in completed_trades if t['profit'] > 0)
            win_rate = (win_count / len(completed_trades)) * 100

    # B. 如果没有曲线数据，基于实现盈亏估算
    else:
        total_return = (total_realized_profit / initial_capital) * 100

        if len(completed_trades) > 0:
            win_count = sum(1 for t in completed_trades if t['profit'] > 0)
            win_rate = (win_count / len(completed_trades)) * 100

    return {
        "strategy_name": search_term,
        "total_return": round(total_return, 2),
        "total_profit": round(total_realized_profit, 2),
        "win_rate": round(win_rate, 2),
        "trade_count": len(completed_trades), # 这里指完整的交易回合数
        "curve": equity_curve,
        "trades": completed_trades, # 返回合并后的完整交易列表
        "message": "" if len(equity_curve) > 0 or len(completed_trades) > 0 else "未找到该策略的回测数据。"
    }


# ... (原有的持仓接口保持不变) ...
class PositionUpdate(BaseModel):
    user_id: int
    code: str
    volume: int
    price: float
    action: str

@app.post("/api/position")
def update_position(p: PositionUpdate):
    conn = get_conn()
    c = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if p.action == 'buy':
        c.execute("SELECT hold_volume, cost_price FROM positions WHERE user_id=? AND code=?", (p.user_id, p.code))
        existing = c.fetchone()
        if existing:
            old_vol, old_cost = existing
            new_vol = old_vol + p.volume
            new_cost = ((old_vol * old_cost) + (p.volume * p.price)) / new_vol
            c.execute("UPDATE positions SET hold_volume=?, cost_price=?, update_time=? WHERE user_id=? AND code=?",
                      (new_vol, new_cost, now, p.user_id, p.code))
        else:
            c.execute("INSERT INTO positions (user_id, code, hold_volume, cost_price, update_time) VALUES (?, ?, ?, ?, ?)",
                      (p.user_id, p.code, p.volume, p.price, now))
    elif p.action == 'sell':
        c.execute("SELECT hold_volume FROM positions WHERE user_id=? AND code=?", (p.user_id, p.code))
        res = c.fetchone()
        if res and res[0] >= p.volume:
            c.execute("UPDATE positions SET hold_volume = hold_volume - ?, update_time = ? WHERE user_id=? AND code=?",
                      (p.volume, now, p.user_id, p.code))
        else:
            conn.close()
            raise HTTPException(status_code=400, detail="Insufficient holdings")
    conn.commit()
    conn.close()
    return {"status": "success"}

@app.get("/api/positions/{user_id}")
def get_positions(user_id: int):
    conn = get_conn()
    c = conn.cursor()
    c.execute("SELECT code, hold_volume, cost_price, update_time FROM positions WHERE user_id=?", (user_id,))
    rows = c.fetchall()
    conn.close()
    result = []
    for r in rows:
        code, vol, cost, time = r
        realtime = get_realtime(code.split('.')[-1] if '.' in code else code) # 简单处理
        curr_price = realtime.get('price', cost)
        profit = (curr_price - cost) * vol
        profit_rate = ((curr_price - cost) / cost) * 100 if cost != 0 else 0
        result.append({
            "code": code, "volume": vol, "cost": cost, "current_price": curr_price,
            "profit": round(profit, 2), "profit_rate": round(profit_rate, 2), "update_time": time
        })
    return result

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
