# backend/app/main.py
from fastapi import FastAPI, HTTPException, Query, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime
import uvicorn
from apscheduler.schedulers.background import BackgroundScheduler
import requests, re
from pathlib import Path

from .db import init_db, get_conn
from .services.sync_data import update_basics, update_daily_k
from .services.build_core_pool import build_core_pool
from .services.generate_strategy_candidates import generate_candidates_for_strategy
from .services.strategies.turnover_shrink import TurnoverShrinkStrategy
from .services.intraday_monitor import engine

app = FastAPI(title="AIWealth Pro")

app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)

def get_tencent_quotes(codes: list) -> dict:
    """批量获取腾讯实时行情，返回 {6位代码: {"price": float, "pct": float}}"""
    if not codes: return {}
    # 统一转为腾讯格式: sz002454, sh600682
    fmt_codes = []
    for c in codes:
        suffix = c.split('.')[-1] if '.' in c else c
        fmt_codes.append(f"sz{suffix}" if not suffix.startswith('6') else f"sh{suffix}")
    fmt_codes = list(dict.fromkeys(fmt_codes)) # 去重保序

    url = f"http://qt.gtimg.cn/q={','.join(fmt_codes)}"
    try:
        res = requests.get(url, timeout=3)
        res.encoding = 'gbk'
        quotes = {}
        for line in res.text.strip().split(';'):
            if not line or '=' not in line: continue
            parts = line.split('="')[1].strip('"').split('~')
            if len(parts) > 4:
                code6 = parts[2]  # 腾讯返回的6位纯数字代码
                price = float(parts[3])
                preclose = float(parts[4])
                pct = round(((price - preclose) / preclose) * 100, 2) if preclose != 0 else 0.0
                quotes[code6] = {"price": price, "pct": pct}
        return quotes
    except:
        return {}


init_db()
scheduler = BackgroundScheduler()

@scheduler.scheduled_job('cron', day=1, hour=2, minute=0)
def job_rebuild_core_pool():
    try: build_core_pool()
    except Exception as e: print(f"Error: {e}")

@scheduler.scheduled_job('cron', hour=19, minute=0)
def job_daily_update():
    try: update_basics(); update_daily_k()
    except Exception as e: print(f"Error: {e}")

@scheduler.scheduled_job('cron', hour=19, minute=0)
def job_generate_candidates():
    try:
        strategy = TurnoverShrinkStrategy(use_core_pool=True)
        generate_candidates_for_strategy(strategy, strategy_key="turnover_shrink", top_n=5)
    except Exception as e: print(f"Error generating candidates: {e}")

scheduler.start()

@app.get("/api/health")
def health(): return {"status": "ok", "time": datetime.now()}

@app.get("/api/realtime/{code}")
def get_realtime(code: str):
    prefix = "sh" if code.startswith('6') else "sz"
    url = f"http://hq.sinajs.cn/list={prefix}{code}" if '.' not in code else f"http://hq.sinajs.cn/list={code}"
    try:
        res = requests.get(url, timeout=2); res.encoding = 'gbk'; text = res.text
        match = re.search(r'="([^"]+)"', text)
        if match:
            data = match.group(1).split(',')
            if len(data) < 4: return {"error": "data incomplete"}
            name, pre, price = data[0], float(data[2]), float(data[3])
            pct = ((price - pre) / pre) * 100 if pre != 0 else 0
            vol = int(float(data[8])) if data[8] else 0
            return {"code": code, "name": name, "price": round(price, 2), "change_pct": round(pct, 2), "volume": vol, "alert": abs(pct) > 3.0}
    except: pass
    return {"error": "fetch failed"}

@app.get("/api/focus/list")
def get_focus_list():
    conn = get_conn(); c = conn.cursor()
    c.execute("SELECT code, code_name, reason, total_limit_ups_1y FROM core_pool WHERE is_active=1 ORDER BY total_limit_ups_1y DESC LIMIT 50")
    rows = c.fetchall(); conn.close()
    return [{"code": r[0], "name": r[1], "reason": r[2], "ups_1y": r[3]} for r in rows]

@app.get("/api/candidates/today")
def get_today_candidates(strategy: str = Query("turnover_shrink")):
    conn = get_conn(); c = conn.cursor()
    today = datetime.now().strftime("%Y-%m-%d")
    c.execute("SELECT code, score, reason, sector FROM daily_candidates WHERE trade_date=? AND strategy_name=? ORDER BY score DESC", (today, strategy))
    rows = c.fetchall(); conn.close()
    return [{"code": r[0], "score": r[1], "reason": r[2], "sector": r[3]} for r in rows]

@app.get("/api/analysis/{period}")
def get_analysis(period: str):
    conn = get_conn(); c = conn.cursor()
    c.execute("SELECT content, created_at FROM analysis_reports WHERE period=? ORDER BY id DESC LIMIT 1", (period,))
    row = c.fetchone(); conn.close()
    return {"content": row[0], "time": row[1]} if row else {"content": "暂无分析报告", "time": ""}

@app.get("/api/kline/{code}")
def get_kline(code: str, days: int = 20):
    conn = get_conn(); c = conn.cursor()
    c.execute("SELECT date, open, high, low, close, volume, turn, pctChg FROM stock_daily_k WHERE code=? ORDER BY date DESC LIMIT ?", (code, days))
    rows = c.fetchall(); conn.close()
    return [{"date": r[0], "open": r[1], "high": r[2], "low": r[3], "close": r[4], "volume": r[5], "turn": r[6], "pct": r[7]} for r in reversed(rows)]

@app.get("/api/backtest/{strategy_key}")
def get_backtest_data(strategy_key: str):
    strategy_map = {"dragon": "DragonHead", "chase": "Chase_", "break": "BreakOneWord", "shrink": "Turnover_Shrink"}
    if strategy_key not in strategy_map: raise HTTPException(status_code=404, detail="Strategy key not found")
    search_term = strategy_map[strategy_key]
    conn = get_conn(); c = conn.cursor()
    c.execute("SELECT trade_date, total_value, cash FROM backtest_daily_snapshot WHERE strategy_name LIKE ? ORDER BY trade_date ASC", (f"%{search_term}%",))
    equity_curve = [{"date": r[0], "value": r[1]} for r in c.fetchall()]
    c.execute("SELECT trade_date, code, action, price, profit_loss, reason FROM backtest_trades WHERE strategy_name LIKE ? ORDER BY trade_date ASC, code ASC", (f"%{search_term}%",))
    all_trade_rows = c.fetchall()
    buys_pending, completed_trades, total_realized_profit = {}, [], 0.0

    for r in all_trade_rows:
        t_date, t_code, t_action, t_price, t_profit, t_reason = r[0], r[1], r[2], r[3], r[4] or 0.0, r[5]

        # 🔧 修复：sqlite3 默认返回元组，使用 res[0] 安全访问
        stock_name = None
        c.execute("SELECT code_name FROM stock_basic WHERE code=?", (t_code,)); res = c.fetchone()
        if res and res[0]: stock_name = res[0]
        else:
            c.execute("SELECT code_name FROM core_pool_history WHERE code=? LIMIT 1", (t_code,)); res = c.fetchone()
            if res and res[0]: stock_name = res[0]

        display_name = stock_name if stock_name else t_code

        if t_action == 'BUY':
            if t_code not in buys_pending: buys_pending[t_code] = []
            buys_pending[t_code].append({"date": t_date, "price": t_price, "reason": t_reason, "name": display_name})
        elif t_action == 'SELL':
            matched_buy = buys_pending[t_code].pop(0) if t_code in buys_pending and buys_pending[t_code] else None
            if matched_buy:
                completed_trades.append({"code": t_code, "name": matched_buy["name"], "buy_date": matched_buy["date"], "buy_price": matched_buy["price"], "sell_date": t_date, "sell_price": t_price, "profit": t_profit, "reason": f"{matched_buy['reason']} -> {t_reason}"})
                total_realized_profit += t_profit
            else:
                completed_trades.append({"code": t_code, "name": display_name, "buy_date": "未知", "buy_price": 0, "sell_date": t_date, "sell_price": t_price, "profit": t_profit, "reason": t_reason})
                total_realized_profit += t_profit

    for t_code, buy_list in buys_pending.items():
        for buy in buy_list:
            completed_trades.append({"code": t_code, "name": buy["name"], "buy_date": buy["date"], "buy_price": buy["price"], "sell_date": "持仓中", "sell_price": 0.0, "profit": 0.0, "reason": f"{buy['reason']} -> 持仓中"})

    conn.close()
    total_return, win_rate, initial_capital = 0.0, 0.0, 100000.0
    if equity_curve:
        start_val, end_val = equity_curve[0]['value'], equity_curve[-1]['value']
        total_return = ((end_val - start_val) / start_val) * 100
    else: total_return = (total_realized_profit / initial_capital) * 100
    if completed_trades: win_rate = (sum(1 for t in completed_trades if t['profit'] > 0) / len(completed_trades)) * 100

    return {"strategy_name": search_term, "total_return": round(total_return, 2), "total_profit": round(total_realized_profit, 2), "win_rate": round(win_rate, 2), "trade_count": len(completed_trades), "curve": equity_curve, "trades": completed_trades, "message": "" if equity_curve or completed_trades else "未找到该策略的回测数据。"}

class PositionUpdate(BaseModel):
    user_id: int
    code: str
    volume: int
    price: float
    action: str
    strategy_name: str = "manual"

@app.post("/api/position")
def update_position(p: PositionUpdate):
    conn = get_conn(); c = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if p.action == 'buy':
        c.execute("SELECT hold_volume, cost_price FROM positions WHERE user_id=? AND code=? AND strategy_name=?", (p.user_id, p.code, p.strategy_name))
        existing = c.fetchone()
        if existing:
            old_vol, old_cost = existing
            new_vol = old_vol + p.volume
            new_cost = ((old_vol * old_cost) + (p.volume * p.price)) / new_vol
            c.execute("UPDATE positions SET hold_volume=?, cost_price=?, update_time=? WHERE user_id=? AND code=? AND strategy_name=?", (new_vol, new_cost, now, p.user_id, p.code, p.strategy_name))
        else:
            c.execute("INSERT INTO positions (user_id, code, hold_volume, cost_price, update_time, strategy_name) VALUES (?, ?, ?, ?, ?, ?)", (p.user_id, p.code, p.volume, p.price, now, p.strategy_name))
    elif p.action == 'sell':
        c.execute("SELECT hold_volume FROM positions WHERE user_id=? AND code=? AND strategy_name=?", (p.user_id, p.code, p.strategy_name))
        res = c.fetchone()
        if res and res[0] >= p.volume:
            c.execute("UPDATE positions SET hold_volume = hold_volume - ?, update_time = ? WHERE user_id=? AND code=? AND strategy_name=?", (p.volume, now, p.user_id, p.code, p.strategy_name))
        else:
            conn.close(); raise HTTPException(status_code=400, detail="Insufficient holdings")
    conn.commit(); conn.close()
    return {"status": "success"}

@app.get("/api/positions/{user_id}")
def get_positions(user_id: int):
    conn = get_conn(); c = conn.cursor()
    c.execute("SELECT code, hold_volume, cost_price, update_time, strategy_name FROM positions WHERE user_id=?", (user_id,))
    rows = c.fetchall(); conn.close()

    if not rows: return []

    # 1. 收集所有持仓代码
    hold_codes = [r[0] for r in rows]
    # 2. 批量请求腾讯行情
    quotes = get_tencent_quotes(hold_codes)

    result = []
    for r in rows:
        code, vol, cost, time, strategy = r
        code6 = code.split('.')[-1] if '.' in code else code
        q = quotes.get(code6)

        if q:
            curr_price = q["price"]
            profit_rate = q["pct"]
            profit = (curr_price - cost) * vol
        else:
            curr_price = None
            profit_rate = 0.0
            profit = 0.0

        result.append({
            "code": code, "volume": vol, "cost": cost,
            "current_price": round(curr_price, 2) if curr_price else None,
            "profit": round(profit, 2), "profit_rate": round(profit_rate, 2),
            "update_time": time, "strategy_name": strategy
        })
    return result


@app.get("/api/intraday/signals")
def get_intraday_signals():
    now = datetime.now()
    weekday = now.weekday()
    t = now.hour * 60 + now.minute
    today_str = now.strftime("%Y-%m-%d")

    # 1. 时段判断
    if weekday >= 5: phase, mode = "盘后复盘 (周末)", "post"
    elif t < 555: phase, mode = "盘前准备 (09:15前)", "pre"
    elif 555 <= t < 690: phase, mode = "H2 早盘博弈 (09:30-11:30)", "trading"
    elif 690 <= t < 780: phase, mode = "午间休市 (11:30-13:00)", "post"
    elif 780 <= t <= 900: phase, mode = "H3 午后确认 (13:00-15:00)", "trading"
    else: phase, mode = "盘后复盘 (收盘后)", "post"

    # 2. 实时指数
    sh_pct = 0.0
    try:
        res = requests.get("http://qt.gtimg.cn/q=sh000001", timeout=2)
        res.encoding = 'gbk'
        if 'v_sh000001' in res.text:
            parts = res.text.split('="')[1].strip('";').split('~')
            if len(parts) > 4:
                price, preclose = float(parts[3]), float(parts[4])
                sh_pct = round(((price - preclose) / preclose) * 100, 2) if preclose != 0 else 0.0
    except: pass

    # 3. 情绪分级
    if sh_pct < -1.5: sentiment = "极弱"
    elif sh_pct < -0.5: sentiment = "偏弱"
    elif sh_pct < 0.5: sentiment = "震荡"
    elif sh_pct < 1.5: sentiment = "偏强"
    else: sentiment = "强势"

    # 4. 🔑 核心：直连 TurnoverShrinkStrategy 实时评估（零中间表）
    pos_count, cand_count = 0, 0
    holding_alerts = []
    try:
        conn = get_conn(); c = conn.cursor()
        pos_count = c.execute("SELECT COUNT(*) FROM positions WHERE user_id=1").fetchone()[0]
        cand_count = c.execute("SELECT COUNT(*) FROM daily_candidates WHERE trade_date=?", (today_str,)).fetchone()[0]

        if pos_count > 0:
            holdings = c.execute("SELECT code, hold_volume, cost_price, update_time FROM positions WHERE user_id=1").fetchall()
            codes = [h[0] for h in holdings]
            quotes = get_tencent_quotes(codes)

            from app.services.strategies.turnover_shrink import TurnoverShrinkStrategy
            strategy = TurnoverShrinkStrategy()

            for code, vol, cost, update_time in holdings:
                code6 = code.split('.')[-1] if '.' in code else code
                q = quotes.get(code6)
                if not q: continue
                curr = q["price"]
                profit_rate = ((curr - cost) / cost) * 100

                # 获取今日K线（含 hour*_rate，供策略读取）
                k_row = c.execute("SELECT * FROM stock_daily_k WHERE code=? AND date=? LIMIT 1", (code, today_str)).fetchone()
                k_dict = dict(zip([desc[0] for desc in c.description], k_row)) if k_row else {}

                # 计算持仓天数
                try:
                    buy_dt = datetime.strptime(update_time.split(' ')[0], "%Y-%m-%d") if update_time else datetime.now()
                    days_held = max(1, (datetime.now() - buy_dt).days + 1)
                except: days_held = 1

                # ⚡ 严格调用策略原生卖出判断
                try:
                    should_sell, reason, _ = strategy.check_sell_condition(
                        hold_code=code, buy_price=cost, current_date=today_str,
                        current_k_row=k_dict, profit_rate=profit_rate, days_held=days_held
                    )
                    if should_sell:
                        holding_alerts.append(f"⚠️ {code}: 策略触发【{reason}】，建议卖出。")
                    else:
                        holding_alerts.append(f"📈 {code}: {reason} (浮盈{profit_rate:.1f}%)，继续持有。")
                except Exception as e:
                    # 🛡️ 降级方案：若策略调用失败，严格使用策略参数判断
                    if profit_rate <= strategy.stop_loss_rate:
                        holding_alerts.append(f"⚠️ {code}: 浮亏{profit_rate:.1f}% (≤{strategy.stop_loss_rate}%)，建议止损。")
                    elif profit_rate >= strategy.take_profit_rate:
                        holding_alerts.append(f"🎯 {code}: 浮盈{profit_rate:.1f}% (≥{strategy.take_profit_rate}%)，建议止盈。")
                    elif days_held >= strategy.max_hold_days:
                        holding_alerts.append(f"⏱️ {code}: 持仓{days_held}天 (≥{strategy.max_hold_days})，建议时间止盈。")
                    else:
                        holding_alerts.append(f"📈 {code}: 持有中 (浮盈{profit_rate:.1f}%)，未触发卖出条件。")
        conn.close()
    except Exception as e:
        holding_alerts.append(f"⚠️ 数据层异常: {str(e)[:50]}")

    # 5. 组装看板建议
    base = f"{phase} | 市场{sentiment}。持仓{pos_count}只，监控中。"
    if sh_pct > 1.0: base += "指数强势，持股待涨。"
    elif sh_pct < -0.5: base += "指数偏弱，严控仓位。"
    else: base += "震荡市高抛低吸。"
    if cand_count == 0: base += " | 候选池空仓，建议观望。"

    advice = base
    if holding_alerts: advice += "\n📌 策略持仓指引：" + " | ".join(holding_alerts)
    else: advice += "\n📌 策略持仓指引：无持仓，按纪律跟踪。"

    return {
        "signals": [],  # 实时信号由策略引擎生成，此处按需扩展
        "analysis": {
            "phase": phase, "mode": mode, "sh_pct": sh_pct,
            "market_sentiment": sentiment, "advice": advice,
            "review": "", "outlook": ""
        },
        "last_update": now.strftime("%H:%M:%S")
    }


# 🔽 静态文件服务必须放在所有路由之后，避免拦截 /api/ 请求
frontend_path = Path(__file__).resolve().parent.parent.parent / "frontend"
if frontend_path.exists():
    app.mount("/", StaticFiles(directory=str(frontend_path), html=True), name="frontend")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
