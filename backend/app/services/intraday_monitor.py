import re
import logging
import requests
from datetime import datetime, timezone, timedelta
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from db import get_conn
from services.strategies.turnover_shrink import TurnoverShrinkStrategy

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')

class IntradaySignalEngine:
    def __init__(self):
        self.strategy = TurnoverShrinkStrategy(use_core_pool=True)
        self.cache = {"signals": [], "analysis": {}, "last_update": None}
        # 📊 缩量回调策略盘中纪律（可随策略调整）
        self.RULES = {
            "max_hold_days": 3,
            "stop_loss_pct": -3.0,
            "take_profit_pct": 5.0,
            "intraday_break_pct": -1.5,  # 盘中跌破开盘价1.5%视为弱势
            "h4_exit_if_flat": True      # 尾盘若仍横盘/微利，强制离场
        }

    def _get_current_phase(self):
        now = datetime.now(timezone(timedelta(hours=8)))
        t = now.hour * 60 + now.minute
        if 570 <= t < 600: return "H1 早盘观察 (09:30-10:00)"
        elif 600 <= t < 690: return "H2 核心决策 (10:00-11:30)"
        elif 780 <= t < 840: return "H3 午后确认 (13:00-14:00)"
        elif 840 <= t < 900: return "H4 尾盘定局 (14:00-15:00)"
        return "非交易时段"

    def _get_holding_days(self, buy_date_str):
        if not buy_date_str: return 0
        try:
            buy_dt = datetime.strptime(buy_date_str.split()[0], "%Y-%m-%d")
            now_dt = datetime.now(timezone(timedelta(hours=8)))
            return (now_dt - buy_dt).days
        except: return 0

    def _fetch_quotes(self, codes):
        if not codes: return {}
        tencent_codes = [c.replace('.', '') for c in codes]
        url = f"http://qt.gtimg.cn/q={','.join(tencent_codes)}"
        headers = {"Referer": "http://finance.qq.com", "User-Agent": "Mozilla/5.0"}
        try:
            res = requests.get(url, headers=headers, timeout=3)
            res.encoding = 'gbk'
            stocks = {}
            pattern = re.compile(r'v_(\w+)="([^"]+)"')
            for match in pattern.finditer(res.text):
                raw_code, data_str = match.groups()
                code = f"{raw_code[:2]}.{raw_code[2:]}"
                d = data_str.split('~')
                if len(d) < 6: continue
                try:
                    price = float(d[3]); yestclose = float(d[4])
                    pct = round((price - yestclose) / yestclose * 100, 2) if yestclose > 0 else 0.0
                    stocks[code] = {
                        "name": d[1], "price": price, "preclose": yestclose,
                        "open": float(d[5]) if d[5] else price,
                        "high": float(d[33]) if len(d) > 33 and d[33] else price,
                        "low": float(d[34]) if len(d) > 34 and d[34] else price,
                        "pct": pct, "vol": float(d[6]) if d[6] else 0.0
                    }
                except (ValueError, IndexError): continue
            return stocks
        except Exception as e:
            logging.error(f"行情请求异常: {e}")
            return {}

    def generate_signals(self):
        conn = get_conn(); c = conn.cursor()
        c.execute("SELECT code, hold_volume, cost_price, strategy_name, buy_date FROM positions WHERE user_id=1")
        holdings = {r[0]: {"vol": r[1], "cost": r[2], "strategy": r[3], "buy_date": r[4]} for r in c.fetchall()}
        c.execute("SELECT code FROM daily_candidates WHERE trade_date=? AND strategy_name='turnover_shrink'", (datetime.now(timezone(timedelta(hours=8))).strftime("%Y-%m-%d"),))
        candidates = [r[0] for r in c.fetchall()]
        watch_codes = list(set(list(holdings.keys()) + candidates + ["sh.000001", "sz.399001"]))
        realtime = self._fetch_quotes(watch_codes)
        phase = self._get_current_phase()
        watchlist, signals = [], []

        # 🔍 持仓股实时策略评估
        for code, pos in holdings.items():
            s = realtime.get(code, {})
            if not s: continue
            pct, price, cost = s["pct"], s["price"], pos["cost"]
            profit = (price - cost) / cost * 100 if cost > 0 else 0
            hold_days = self._get_holding_days(pos["buy_date"])
            action, reason, trigger = "HOLD", "未触发策略条件", ""

            # 1. 止损触发
            if profit <= self.RULES["stop_loss_pct"]:
                action, reason, trigger = "SELL_NOW", f"触及止损线({self.RULES['stop_loss_pct']}%)，立即离场", f"现价{price} <= 成本{cost}*(1+{self.RULES['stop_loss_pct']}/100)"
            # 2. 止盈触发
            elif profit >= self.RULES["take_profit_pct"]:
                action, reason, trigger = "SELL_NOW", f"触及止盈线({self.RULES['take_profit_pct']}%)，分批落袋", f"现价{price} >= 成本{cost}*(1+{self.RULES['take_profit_pct']}/100)"
            # 3. 时间止盈（超3天）
            elif hold_days >= self.RULES["max_hold_days"] and profit < self.RULES["take_profit_pct"]:
                action, reason, trigger = "SELL_NOW", f"持仓超{self.RULES['max_hold_days']}天未达目标，强制止盈/止损", f"持仓{hold_days}天 | 盈亏{profit:.1f}%"
            # 4. 盘中弱势破位（H3/H4阶段）
            elif phase.startswith("H3") or phase.startswith("H4"):
                if pct < self.RULES["intraday_break_pct"] and profit < 0:
                    action, reason, trigger = "SELL_NOW", "盘中跌破开盘价且浮亏，趋势转弱", f"今日{pct}% | 浮亏{profit:.1f}%"
                elif self.RULES["h4_exit_if_flat"] and phase.startswith("H4") and abs(profit) < 1.0:
                    action, reason, trigger = "SELL_NOW", "尾盘横盘无资金接力，避免过夜风险", f"浮盈{profit:.1f}% | 尾盘无放量"
                else:
                    action, reason = "HOLD", "持有观察，未触发离场条件"
            else:
                action, reason = "HOLD", "持有观察，未触发离场条件"

            watchlist.append({
                "code": code, "name": s["name"], "type": "持仓", "strategy": pos["strategy"],
                "price": price, "pct": pct, "cost": cost, "profit": profit, "hold_days": hold_days,
                "action": action, "reason": reason, "trigger": trigger, "status": "🔴 卖出" if action=="SELL_NOW" else "🟢 持有"
            })
            if action == "SELL_NOW":
                signals.append({"code": code, "name": s["name"], "type": "SELL", "price": price, "profit_rate": profit, "reason": reason})

        # 🔍 候选股实时评估
        for code in candidates:
            if code in holdings: continue
            s = realtime.get(code, {})
            if not s: continue
            pct = s["pct"]
            action, reason = "WAIT", f"当前{pct}%，未进入理想介入区间"
            if -1.5 <= pct <= 1.5:
                action, reason = "BUY_NOW", f"价格进入策略区间，量价健康可试错建仓(仓位≤20%)"
            elif pct > 3.0:
                action, reason = "WAIT", f"涨幅{pct}%已脱离缩量逻辑，放弃"
            elif pct < -3.0:
                action, reason = "WAIT", f"跌幅过大形态破坏，移出观察"

            watchlist.append({
                "code": code, "name": s["name"], "type": "候选", "strategy": "turnover_shrink",
                "price": s["price"], "pct": pct, "cost": 0, "profit": 0, "hold_days": 0,
                "action": action, "reason": reason, "trigger": "", "status": "🔵 买入" if action=="BUY_NOW" else "⏸ 等待"
            })
            if action == "BUY_NOW":
                signals.append({"code": code, "name": s["name"], "type": "BUY", "price": s["price"], "profit_rate": 0, "reason": reason})

        sh, sz = realtime.get("sh.000001", {}), realtime.get("sz.399001", {})
        sh_pct, sz_pct = sh.get("pct", 0), sz.get("pct", 0)
        sentiment = "偏强" if sh_pct > 0.5 and sz_pct > 0.5 else ("偏弱" if sh_pct < -0.5 and sz_pct < -0.5 else "震荡")
        now_str = datetime.now(timezone(timedelta(hours=8))).strftime("%H:%M:%S")

        self.cache = {
            "watchlist": watchlist, "signals": signals,
            "analysis": {
                "phase": phase, "market_sentiment": sentiment,
                "sh_pct": sh_pct, "sz_pct": sz_pct,
                "hold_count": len(holdings), "candidate_count": len(candidates),
                "advice": f"{phase} | 市场{sentiment}。持仓{len(holdings)}只，候选{len(candidates)}只。" +
                         (f" 发现 {len(signals)} 个强信号！" if signals else " 暂无触发信号，按纪律跟踪。")
            },
            "last_update": now_str
        }
        conn.close()
        return self.cache

engine = IntradaySignalEngine()
