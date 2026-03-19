# backend/app/services/backtest_engine.py
import sqlite3
from pathlib import Path
import random
import os
import csv
from datetime import datetime

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "wealth.db"
SNAPSHOT_DIR = BASE_DIR / "trade_snapshots"
os.makedirs(SNAPSHOT_DIR, exist_ok=True)

random.seed(42)

def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

class BacktestEngine:
    def __init__(self, strategy, start_date, end_date, initial_capital=100000):
        self.strategy = strategy
        self.start_date = start_date
        self.end_date = end_date
        self.capital = initial_capital
        self.initial_capital = initial_capital
        self.position = None
        self.trades = []
        self.daily_snapshots = []
        self.name_cache = {}
        self.trade_count = 0

        # 临时存储当前交易信息用于生成报告
        self.current_trade_buy_reason = ""
        self.current_trade_buy_date = ""
        self.current_trade_buy_price = 0.0
        self.current_trade_buy_price_rate = 1.0 # 新增：记录买入时的价格比率
        self.last_sell_reason = ""

        self.conn = get_conn()
        self.c = self.conn.cursor()

        # 缓存股票名称
        self.c.execute("SELECT code, code_name FROM stock_basic")
        for row in self.c.fetchall():
            self.name_cache[row['code']] = row['code_name']

    def get_stock_display_name(self, code):
        name = self.name_cache.get(code, "")
        if not name:
            self.c.execute("SELECT code_name FROM core_pool_history WHERE code=? LIMIT 1", (code,))
            res = self.c.fetchone()
            if res: name = res['code_name']
        return f"{name} ({code})" if name else code

    def _calc_rate(self, val, preclose):
        """辅助函数：计算涨跌幅百分比"""
        if not preclose or preclose == 0: return 0.0
        try:
            return (float(val) - preclose) / preclose * 100
        except:
            return 0.0

    def _format_hour_data(self, row, prefix):
        """格式化单个时段的数据为字符串"""
        o = row.get(f'{prefix}_open_rate')
        c = row.get(f'{prefix}_close_rate')
        h = row.get(f'{prefix}_high_rate')
        l = row.get(f'{prefix}_low_rate')
        if o is None: return "-"
        try:
            return f"O:{float(o):>5.1f}/C:{float(c):>5.1f}/H:{float(h):>5.1f}/L:{float(l):>5.1f}"
        except:
            return "-"

    def _generate_hourly_psychology(self, row, hour, is_buy_day, is_sell_day, buy_ratio_pct):
        """
        【单小时专用】生成详细的心理分析和操作指令
        原则：基于当前小时及之前的数据，给出即时反馈和下一步预案。
        """
        h_prefix = f'hour{hour}'
        h_open = float(row.get(f'{h_prefix}_open_rate') or 0)
        h_close = float(row.get(f'{h_prefix}_close_rate') or 0)
        h_high = float(row.get(f'{h_prefix}_high_rate') or 0)
        h_low = float(row.get(f'{h_prefix}_low_rate') or 0)
        turn = float(row.get('turn', 0))

        notes = []

        # --- 场景 1: 买入日 (重点分析 H1 观察 -> H2 决策) ---
        if is_buy_day:
            if hour == 1:
                notes.append(f"[H1 观察] 开盘 {h_open:.1f}%，下探 {h_low:.1f}%。")
                if h_low < -2.0:
                    notes.append("🧠 心理：恐慌盘杀出，机会可能在下方。")
                    notes.append("👉 指令：紧盯 H2，若快速收回 -1% 以上，准备开枪；若持续低于 -2%，放弃。")
                elif h_open > 1.0:
                    notes.append("🧠 心理：强势高开，谨防获利盘兑现。")
                    notes.append("👉 指令：若 H2 不破开盘价，可轻仓试错；否则观望。")
                else:
                    notes.append("🧠 心理：平淡开局，方向不明。")
                    notes.append("👉 指令：等待 H2 选择方向，不见兔子不撒鹰。")

            elif hour == 2:
                notes.append(f"[H2 决策] 低 {h_low:.1f}%，收 {h_close:.1f}%。")
                if h_low < -2.0 and h_close > h_low + 1.0:
                    notes.append("🧠 心理：典型‘黄金坑’！急杀后有大单承接。")
                    notes.append("🚀 动作：【买入触发】立即进场！")
                    notes.append(f"🛡️ 预案：止损设在今日低点 {h_low:.1f}% 下方；目标盈利 +3% 减半。")
                elif h_close > 2.0:
                    notes.append("🧠 心理：多头发动攻击。")
                    notes.append("🚀 动作：【追涨买入】确认强势，跟随主力。")
                    notes.append("🛡️ 预案：若 H3 回落破 H2 开盘价，立即止损。")
                else:
                    notes.append("🧠 心理：走势犹豫，无量震荡。")
                    notes.append("👉 指令：【放弃】不符合模式，管住手，等下一只。")

            elif hour == 3:
                current_profit_approx = h_close - buy_ratio_pct
                notes.append(f"[H3 验证] 浮盈/亏约 {current_profit_approx:.1f}%。")
                if h_close > h_open:
                    notes.append("🧠 心理：买入即涨，逻辑正确，心态轻松。")
                    notes.append("👉 指令：持股不动。若冲高至 +5% 以上，可将止损上移至成本价，确保不败。")
                else:
                    notes.append("🧠 心理：有点慌，但在预期范围内。")
                    notes.append("👉 指令：只要不跌破 H2 低点，就死拿。跌破则认赔出局。")

            elif hour == 4:
                notes.append(f"[H4 收盘] 今日收 {h_close:.1f}%。")
                if h_close > 0:
                    notes.append("🧠 心理：完美收官，过夜无忧。")
                    notes.append("👉 指令：设置明日预警：低开 -2% 准备跑，高开 +2% 准备止盈。")
                else:
                    notes.append("🧠 心理：小幅被套，焦虑。")
                    notes.append("👉 指令：今晚复盘找原因。明日若不能反包，果断离场。")

        # --- 场景 2: 卖出日 (重点分析何时跑) ---
        elif is_sell_day:
            if hour == 1:
                notes.append(f"[H1 竞价] 开盘 {h_open:.1f}%。")
                if h_open > 2.0:
                    notes.append("🧠 心理：大肉！主力给面子。")
                    notes.append("👉 指令：【准备止盈】若 H2 不能封板，坚决卖出，落袋为安。")
                elif h_open < -2.0:
                    notes.append("🧠 心理：不及预期，甚至核按钮。")
                    notes.append("👉 指令：【准备止损】反弹翻红无力即卖出，保住本金第一。")
                else:
                    notes.append("🧠 心理：正常波动。")
                    notes.append("👉 指令：观察 H2 方向，严格执行时间止盈/损纪律。")

            elif hour == 2:
                notes.append(f"[H2 执行] 高 {h_high:.1f}%，低 {h_low:.1f}%。")
                if h_high > 4.0:
                    notes.append("🚨 触发：冲高达标！")
                    notes.append("🚀 动作：【卖出执行】不要贪婪，此时卖出最稳妥。")
                elif h_low < -3.0:
                    notes.append("🚨 触发：跳水破位！")
                    notes.append("🚀 动作：【卖出执行】割肉离场，留得青山在。")
                else:
                    notes.append("👉 指令：继续持有到 H3/H4，等待时间窗口结束。")

            elif hour == 3:
                notes.append(f"[H3 尾盘前] 收 {h_close:.1f}%。")
                notes.append("🧠 心理：时间差不多了，不管盈亏都要走了。")
                notes.append("👉 指令：挂单准备，H4 一开盘或收盘前最后一分钟清仓。")

            elif hour == 4:
                notes.append(f"[H4 终结] 策略结束。")
                notes.append("🧠 心理：如释重负。无论结果如何，执行力就是生命力。")
                notes.append("✅ 动作：【清仓完毕】寻找下一个目标。")

        # --- 场景 3: 持仓中 (煎熬与观察) ---
        else:
            notes.append(f"[H{hour} 持仓] 波动区间 [{h_low:.1f}%, {h_high:.1f}%]。")
            if abs(h_close) < 1.0:
                notes.append("🧠 心理：织布机行情，磨人。")
                notes.append("👉 指令：关掉软件，不要看盘，避免情绪化操作。相信策略。")
            elif h_close > 3.0:
                notes.append("🧠 心理：爽！利润奔跑。")
                notes.append("👉 指令：设好移动止盈（例如回撤 1% 就走），让市场把你踢出来。")
            elif h_close < -3.0:
                notes.append("🧠 心理：难受，想卖。")
                notes.append("👉 指令：检查是否触及策略止损线。没到就死扛，到了就砍。")

            if turn > 15.0:
                notes.append("⚠️ 警报：巨量换手！主力可能在出货或对倒，高度警惕！")

        return " | ".join(notes)

    def export_trade_snapshot(self, code, action, price, date, reason, is_sell=False):
        """
        【修复版】导出交易快照：卖出后不再打印多余的心理逻辑
        """
        if not is_sell:
            if action == 'BUY':
                self.current_trade_buy_reason = reason
                self.current_trade_buy_date = date
                self.current_trade_buy_price = price
            return

        self.trade_count += 1
        safe_code = code.replace('.', '_')
        timestamp = datetime.now().strftime("%H%M%S")
        filename = f"{SNAPSHOT_DIR}/{date}_{action}_{safe_code}_{timestamp}.csv"
        self.last_sell_reason = reason

        # 获取全量 K 线数据
        self.c.execute("""
            SELECT date, open, high, low, close, volume, turn, pctChg, preclose,
                   hour1_open_rate, hour1_close_rate, hour1_high_rate, hour1_low_rate,
                   hour2_open_rate, hour2_close_rate, hour2_high_rate, hour2_low_rate,
                   hour3_open_rate, hour3_close_rate, hour3_high_rate, hour3_low_rate,
                   hour4_open_rate, hour4_close_rate, hour4_high_rate, hour4_low_rate
            FROM stock_daily_k WHERE code = ? ORDER BY date ASC
        """, (code,))
        all_rows = self.c.fetchall()
        if not all_rows: return

        data_list = [dict(r) for r in all_rows]

        buy_idx = next((i for i, r in enumerate(data_list) if r['date'] == self.current_trade_buy_date), -1)
        sell_idx = next((i for i, r in enumerate(data_list) if r['date'] == date), -1)

        if sell_idx == -1: return

        # 【修改点 1】截取窗口：只到卖出日为止，不再包含卖出后的日期
        # 原来这里是 sell_idx + 2，现在改为 sell_idx + 1 (即包含卖出日当天)
        start_idx = max(0, buy_idx - 2)
        end_idx = sell_idx + 1  # 关键修复：卖出后即停止，不包含后续日期
        snapshot_data = data_list[start_idx:end_idx]

        fieldnames = [
            'Date', 'Hour', 'Time_Range',
            'Open%', 'Close%', 'High%', 'Low%', 'Vol', 'Turn%',
            'Action_Mark',
            '🧠 实时心理与操作指令'
        ]

        with open(filename, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()

            for i, row in enumerate(snapshot_data):
                current_date = row['date']
                is_buy_day = (current_date == self.current_trade_buy_date)
                is_sell_day = (current_date == date)

                # 如果当前行已经是卖出日之后的（理论上不会发生，因为切片改了，但加个保险）
                if current_date > date:
                    continue

                buy_ratio = getattr(self, 'current_trade_buy_price_rate', 1.0)
                buy_ratio_pct = (buy_ratio - 1.0) * 100

                for h in range(1, 5):
                    h_prefix = f'hour{h}'
                    h_open = float(row.get(f'{h_prefix}_open_rate') or 0)
                    h_close = float(row.get(f'{h_prefix}_close_rate') or 0)
                    h_high = float(row.get(f'{h_prefix}_high_rate') or 0)
                    h_low = float(row.get(f'{h_prefix}_low_rate') or 0)

                    t_range = f"H{h} Segment"

                    action_mark = ""
                    # 只在特定的小时标记动作
                    if is_buy_day and h == 2:
                        action_mark = "🟢 BUY"
                    elif is_sell_day and h == 1:
                        action_mark = "🔴 SELL"

                    # 【修改点 2】如果是卖出日的 H1 之后，或者已经卖出了，后续小时不再生成复杂的持仓心理
                    # 但为了完整性，卖出日当天的 H1-H4 还是会显示“执行卖出”或“已完成”的逻辑
                    psychology_text = self._generate_hourly_psychology(
                        row=row,
                        hour=h,
                        is_buy_day=is_buy_day,
                        is_sell_day=is_sell_day,
                        buy_ratio_pct=buy_ratio_pct
                    )

                    writer.writerow({
                        'Date': current_date,
                        'Hour': f"H{h}",
                        'Time_Range': t_range,
                        'Open%': f"{h_open:.2f}",
                        'Close%': f"{h_close:.2f}",
                        'High%': f"{h_high:.2f}",
                        'Low%': f"{h_low:.2f}",
                        'Vol': int(row['volume']),
                        'Turn%': f"{float(row.get('turn', 0)):.2f}",
                        'Action_Mark': action_mark,
                        '🧠 实时心理与操作指令': psychology_text
                    })

        # 控制台打印精简版
        print("\n" + "="*180)
        print(f"📊 [HOURLY BREAKDOWN] {code} | Buy:{self.current_trade_buy_date} @ {self.current_trade_buy_price:.2f} | Sell:{date} @ {price:.2f}")
        print(f"   Profit: {reason.split('|')[-1].strip()}")
        print("="*180)
        print(f"{'Date':<10} | {'Hr':<3} | {'Act':<6} | {'O%':>5} | {'C%':>5} | {'H/L%':>10} | 🧠 核心心理与指令")
        print("-" * 180)

        for i, row in enumerate(snapshot_data):
            current_date = row['date']
            # 再次确保不处理卖出日之后的数据
            if current_date > date:
                break

            is_buy_day = (current_date == self.current_trade_buy_date)
            is_sell_day = (current_date == date)

            for h in range(1, 5):
                h_prefix = f'hour{h}'
                h_open = float(row.get(f'{h_prefix}_open_rate') or 0)
                h_close = float(row.get(f'{h_prefix}_close_rate') or 0)
                h_high = float(row.get(f'{h_prefix}_high_rate') or 0)
                h_low = float(row.get(f'{h_prefix}_low_rate') or 0)

                act = ""
                if is_buy_day and h == 2: act = "🟢BUY"
                elif is_sell_day and h == 1: act = "🔴SELL"

                full_note = self._generate_hourly_psychology(row, h, is_buy_day, is_sell_day, buy_ratio_pct)
                short_note = full_note.replace('\n', ' ')
                if len(short_note) > 75: short_note = short_note[:72] + "..."

                print(f"{current_date:<10} | H{h:<2} | {act:<6} | {h_open:>5.1f} | {h_close:>5.1f} | {h_high:>4.1f}/{h_low:>4.1f} | {short_note}")

        print("-" * 180)
        print(f"💾 完整详细报告已存档：{filename}")
        print("="*180 + "\n")


    def save_to_db(self):
        """将回测结果保存到数据库"""
        try:
            for snap in self.daily_snapshots:
                self.c.execute("""
                    INSERT OR REPLACE INTO backtest_daily_snapshot
                    (trade_date, strategy_name, total_value, cash, update_time)
                    VALUES (?, ?, ?, ?, ?)
                """, (snap['date'], self.strategy.name, snap['total_value'], snap['cash'], datetime.now()))

            for trade in self.trades:
                self.c.execute("""
                    INSERT INTO backtest_trades
                    (strategy_name, trade_date, code, action, price, profit_loss, reason, create_time)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (self.strategy.name, trade['date'], trade['code'], trade['action'],
                      trade['price'], trade['profit'], trade.get('reason', ''), datetime.now()))

            self.conn.commit()
            print("💾 回测数据已保存至数据库。")
        except Exception as e:
            print(f"❌ 保存数据库失败：{e}")

    def run(self):
        print(f"🚀 开始回测策略：[{self.strategy.name}] (高精度成交版)")
        print(f"   时间范围：{self.start_date} 至 {self.end_date}")
        print("=" * 100)

        self.c.execute("DELETE FROM backtest_trades WHERE strategy_name = ?", (self.strategy.name,))
        self.c.execute("DELETE FROM backtest_daily_snapshot WHERE strategy_name = ?", (self.strategy.name,))
        self.conn.commit()

        self.c.execute("SELECT DISTINCT date FROM stock_daily_k WHERE date >= ? AND date <= ? ORDER BY date ASC", (self.start_date, self.end_date))
        trade_dates = [row[0] for row in self.c.fetchall()]

        if len(trade_dates) < 2:
            print("❌ 数据不足。")
            return

        days_held = 0

        for i, date in enumerate(trade_dates):
            prev_date = trade_dates[i-1] if i > 0 else date

            # --- A. 处理卖出 ---
            if self.position:
                code = self.position['code']
                buy_price = self.position['buy_price']
                display_name = self.get_stock_display_name(code)

                self.c.execute("""
                    SELECT close, pctChg, open, low, high, preclose, turn,
                    hour1_open_rate, hour1_close_rate, hour1_high_rate, hour1_low_rate,
                    hour2_open_rate, hour2_close_rate, hour2_high_rate, hour2_low_rate,
                    hour3_open_rate, hour3_close_rate, hour3_high_rate, hour3_low_rate,
                    hour4_open_rate, hour4_close_rate, hour4_high_rate, hour4_low_rate
                    FROM stock_daily_k WHERE code=? AND date=?
                """, (code, date))
                row = self.c.fetchone()

                if row:
                    row_dict = dict(row)
                    current_price = row_dict['close']
                    profit_rate = (current_price - buy_price) / buy_price
                    days_held += 1

                    should_sell = False; reason = ""; sell_price = current_price; sell_ratio = 0.0

                    open_p = row_dict['open']
                    if days_held == 1 and open_p > 0 and (open_p - buy_price) / buy_price <= -0.03:
                        should_sell = True; reason = "开盘强止损 (-3%)"; sell_price = open_p

                    if not should_sell:
                        res = self.strategy.check_sell_condition(code, buy_price, date, row_dict, profit_rate, days_held)
                        if len(res) == 3: should_sell, reason, sell_ratio = res
                        else: should_sell, reason = res; sell_ratio = 0.0

                        if should_sell and sell_ratio > 0: sell_price = row_dict['preclose'] * sell_ratio
                        elif should_sell: sell_price = current_price

                    if days_held >= self.strategy.max_hold_days and not should_sell:
                        should_sell = True; reason = f"时间止损 ({self.strategy.max_hold_days}天)"; sell_price = current_price

                    if should_sell:
                        sell_value = sell_price * self.position['shares']
                        net_sell = sell_value * (1 - 0.0003)
                        buy_cost = buy_price * self.position['shares'] * (1 + 0.0003)
                        trade_profit = net_sell - buy_cost

                        self.capital += net_sell

                        # 记录买入比率供心理分析使用
                        self.current_trade_buy_price_rate = buy_price / row_dict['preclose'] if row_dict['preclose'] else 1.0

                        self.export_trade_snapshot(code, 'SELL', sell_price, date, f"{reason} | 盈亏:{trade_profit:+.1f}", is_sell=True)

                        status_icon = "✅ 盈利" if trade_profit > 0 else "❌ 亏损"
                        print(f"   💥 [{date}] {status_icon} 卖出 {display_name} | 盈亏:{trade_profit:+,.2f} | 累计:{self.capital - self.initial_capital:+,.2f}")

                        self.trades.append({'date': date, 'code': code, 'action': 'SELL', 'price': sell_price, 'profit': trade_profit, 'reason': reason})
                        self.position = None
                        days_held = 0

            # --- B. 选股 (T-1) ---
            candidate_codes = []
            scan_all_market = getattr(self.strategy, 'use_core_pool', True) == False

            pool_data = []
            codes = []

            if scan_all_market:
                self.c.execute("SELECT DISTINCT code FROM stock_daily_k WHERE date = ?", (prev_date,))
                codes = [r[0] for r in self.c.fetchall()]
                pool_data = [{'code': c, 'name': 'Unknown'} for c in codes]
            else:
                self.c.execute("SELECT code, code_name FROM core_pool_history WHERE trade_date=?", (date,))
                pool_rows = self.c.fetchall()
                if pool_rows:
                    pool_data = [{'code': r['code'], 'name': r['code_name']} for r in pool_rows]
                    codes = [p['code'] for p in pool_data]

            if codes and not self.position:
                placeholders = ','.join('?' * len(codes))
                limit_days = 5 if scan_all_market else 25
                history_map = {}

                BATCH_SIZE = 500
                for j in range(0, len(codes), BATCH_SIZE):
                    batch_codes = codes[j:j+BATCH_SIZE]
                    ph = ','.join('?' * len(batch_codes))
                    for c_code in batch_codes:
                        self.c.execute("SELECT close, high, low, open, volume, amount, turn, pctChg, preclose, date FROM stock_daily_k WHERE code=? AND date <= ? ORDER BY date DESC LIMIT ?", (c_code, prev_date, limit_days))
                        rows = self.c.fetchall()
                        if rows: history_map[c_code] = [dict(r) for r in rows]

                prev_k_map = {}
                if codes:
                    for j in range(0, len(codes), BATCH_SIZE):
                        batch_codes = codes[j:j+BATCH_SIZE]
                        ph = ','.join('?' * len(batch_codes))
                        self.c.execute(f"""
                            SELECT code, close, preclose, turn, pctChg, open, low, high,
                                   hour1_open_rate, hour1_close_rate, hour1_high_rate, hour1_low_rate,
                                   hour2_open_rate, hour2_close_rate, hour2_high_rate, hour2_low_rate,
                                   hour3_open_rate, hour3_close_rate, hour3_high_rate, hour3_low_rate,
                                   hour4_open_rate, hour4_close_rate, hour4_high_rate, hour4_low_rate
                            FROM stock_daily_k WHERE date=? AND code IN ({ph})
                        """, [prev_date] + batch_codes)
                        for r in self.c.fetchall(): prev_k_map[r['code']] = dict(r)

                if hasattr(self.strategy, 'select_candidates'):
                    candidate_codes = self.strategy.select_candidates(prev_date, pool_data, prev_k_map, history_map)

            # --- C. 买入 (T) ---
            if not self.position and candidate_codes:
                candidate_rows = {}
                BATCH_SIZE = 500

                for i_batch in range(0, len(candidate_codes), BATCH_SIZE):
                    batch_codes = candidate_codes[i_batch : i_batch + BATCH_SIZE]
                    placeholders = ','.join('?' * len(batch_codes))

                    try:
                        self.c.execute(f"""
                            SELECT code, open, preclose, hour3_close_rate, hour4_open_rate, hour4_close_rate, pctChg,
                                   hour1_high_rate, hour2_high_rate, hour3_high_rate, hour3_low_rate,
                                   hour1_open_rate, hour1_low_rate, hour2_open_rate
                            FROM stock_daily_k
                            WHERE date=? AND code IN ({placeholders})
                        """, [date] + batch_codes)

                        for row in self.c.fetchall():
                            candidate_rows[row['code']] = dict(row)

                    except Exception as e:
                        print(f"⚠️ 查询批次失败 (日期:{date}, 数量:{len(batch_codes)}): {e}")
                        continue

                report_result = self.strategy.generate_h3_analysis_report(date, candidate_codes, candidate_rows, {})

                best_code = None
                best_reason = ""
                best_buy_ratio = 0.0

                if isinstance(report_result, tuple):
                    if len(report_result) == 3: best_code, best_reason, best_buy_ratio = report_result
                    elif len(report_result) == 2: best_code, best_reason = report_result
                else: best_code = report_result if report_result else None

                if best_code and best_code in candidate_rows:
                    k_row = candidate_rows[best_code]
                    should_buy = False; buy_ratio = 0.0; reason = best_reason

                    if best_buy_ratio > 0:
                        should_buy = True; buy_ratio = best_buy_ratio
                    else:
                        signal_res = self.strategy.generate_buy_signal(date, best_code, k_row, is_candidate=True)
                        if len(signal_res) >= 3: should_buy, buy_ratio, reason = signal_res
                        elif len(signal_res) == 2: should_buy, buy_ratio = signal_res

                    if should_buy and buy_ratio > 0:
                        preclose = k_row.get('preclose', 0)
                        buy_price = preclose * buy_ratio
                        display_name = self.get_stock_display_name(best_code)

                        fee = self.capital * 0.0003
                        shares = int((self.capital - fee) // buy_price / 100) * 100

                        if shares > 0:
                            cost = shares * buy_price + fee
                            self.capital -= cost
                            self.position = {'code': best_code, 'buy_date': date, 'buy_price': buy_price, 'shares': shares, 'highest_price': buy_price}
                            days_held = 0

                            self.current_trade_buy_reason = reason
                            self.current_trade_buy_date = date
                            self.current_trade_buy_price = buy_price
                            self.current_trade_buy_price_rate = buy_ratio # 记录买入比率

                            self.trades.append({
                                'date': date,
                                'code': best_code,
                                'action': 'BUY',
                                'price': buy_price,
                                'profit': 0.0,
                                'reason': reason
                            })

                            print(f"   🚀 [{date}] ✅ 买入 {display_name} @ {buy_price:.2f} ({reason})")

            # --- D. 记录快照 ---
            current_total_value = self.capital
            if self.position:
                self.c.execute("SELECT close FROM stock_daily_k WHERE code=? AND date=?", (self.position['code'], date))
                res = self.c.fetchone()
                if res: current_total_value += res[0] * self.position['shares']
            self.daily_snapshots.append({'date': date, 'total_value': current_total_value, 'cash': self.capital})

        self.save_to_db()
        self.report_cn()
        self.conn.close()


    def report_cn(self):
        final_value = self.daily_snapshots[-1]['total_value'] if self.daily_snapshots else self.capital
        total_return = (final_value - self.initial_capital) / self.initial_capital
        win_trades = sum(1 for t in self.trades if t['action']=='SELL' and t['profit']>0)
        total_trades = sum(1 for t in self.trades if t['action']=='SELL')
        win_rate = win_trades/total_trades if total_trades>0 else 0

        max_drawdown = 0.0
        peak = self.initial_capital
        for snap in self.daily_snapshots:
            val = snap['total_value']
            if val > peak: peak = val
            dd = (peak - val) / peak
            if dd > max_drawdown: max_drawdown = dd

        print("\n" + "="*60)
        print(f"📊 回测报告总结：{self.strategy.name}")
        print(f"   总收益率：{total_return*100:.2f}%")
        print(f"   交易次数：{total_trades} | 胜率：{win_rate*100:.2f}%")
        print(f"   最大回撤：{max_drawdown*100:.2f}%")
        print("="*60)
