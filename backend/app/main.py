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

from .db import init_db, get_conn
# 导入所有服务模块
from .services.sync_data import update_basics, update_daily_k, check_focus_strategy
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

# 任务 1：每月 1 号凌晨重建核心池 (耗时较长)
@scheduler.scheduled_job('cron', day=1, hour=2, minute=0)
def job_rebuild_core_pool():
    print("[Scheduler] Rebuilding Core Pool...")
    try:
        build_core_pool()
    except Exception as e:
        print(f"Error rebuilding core pool: {e}")

# 任务 2: 每天 18:00 更新数据
@scheduler.scheduled_job('cron', hour=18, minute=0)
def job_daily_update():
    print("[Scheduler] Starting daily update...")
    try:
        update_basics()
        update_daily_k()
        # check_focus_strategy() # 这个函数在 sync_data 里有点旧，建议用新的候选池生成逻辑替代或整合
    except Exception as e:
        print(f"Error updating data: {e}")

# 任务 3: 每天 19:00 生成明日候选池 & 盘后分析
@scheduler.scheduled_job('cron', hour=19, minute=0)
def job_evening_analysis():
    print("[Scheduler] Generating evening analysis and candidates...")
    try:
        # 生成候选池
        generate_daily_candidates()

        # 生成盘后报告
        conn = get_conn()
        c = conn.cursor()
        content = f"【盘后复盘 {datetime.now().strftime('%Y-%m-%d')}】\n今日市场情绪分析...\n核心池个股表现...\n明日策略：关注龙抬头形态个股。"
        c.execute("INSERT INTO analysis_reports (report_date, period, content, created_at) VALUES (?, ?, ?, ?)",
                  (datetime.now().strftime("%Y-%m-%d"), "post", content, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Error generating analysis: {e}")

scheduler.start()

# --- API 接口 ---

@app.get("/api/health")
def health():
    return {"status": "ok", "time": datetime.now()}

@app.get("/api/realtime/{code}")
def get_realtime(code: str):
    """新浪实时接口"""
    # 自动补全前缀
    prefix = "sh" if code.startswith('6') else "sz"
    url = f"http://hq.sinajs.cn/list={prefix}{code}"
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
                "code": code,
                "name": name,
                "price": round(price, 2),
                "change_pct": round(pct, 2),
                "volume": vol,
                "alert": abs(pct) > 3.0
            }
    except Exception as e:
        pass
    return {"error": "fetch failed"}

@app.get("/api/focus/list")
def get_focus_list():
    """获取核心池股票 (用于前端展示重点监控)"""
    conn = get_conn()
    c = conn.cursor()
    # 这里我们直接返回 core_pool 作为重点监控池，或者你可以创建一个单独的 focus_stock 表
    c.execute("SELECT code, code_name, reason, total_limit_ups_1y FROM core_pool WHERE is_active=1 ORDER BY total_limit_ups_1y DESC LIMIT 50")
    rows = c.fetchall()
    conn.close()
    return [{"code": r[0], "name": r[1], "reason": r[2], "ups_1y": r[3]} for r in rows]

@app.get("/api/candidates/today")
def get_today_candidates():
    """获取今日生成的候选池"""
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
    # period: pre, intra, post
    c.execute("SELECT content, created_at FROM analysis_reports WHERE period=? ORDER BY id DESC LIMIT 1", (period,))
    row = c.fetchone()
    conn.close()
    if row:
        return {"content": row[0], "time": row[1]}
    return {"content": "暂无分析报告，请稍后或联系管理员生成。", "time": ""}

@app.get("/api/kline/{code}")
def get_kline(code: str, days: int = 20):
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        SELECT date, open, high, low, close, volume, turn, pctChg
        FROM daily_k
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
        # 检查是否已有持仓
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
            # 可选：如果数量为 0 则删除记录
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

    # 获取实时价格计算盈亏
    result = []
    for r in rows:
        code, vol, cost, time = r
        # 调用内部逻辑获取实时价 (简化处理，生产环境应批量获取)
        realtime = get_realtime(code)
        curr_price = realtime.get('price', cost)
        profit = (curr_price - cost) * vol
        profit_rate = ((curr_price - cost) / cost) * 100 if cost != 0 else 0

        result.append({
            "code": code,
            "volume": vol,
            "cost": cost,
            "current_price": curr_price,
            "profit": round(profit, 2),
            "profit_rate": round(profit_rate, 2),
            "update_time": time
        })
    return result

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
