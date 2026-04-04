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
        self.current_trade_buy_price_rate = 1.0
        self.last_sell_reason = ""
        self.last_sell_price = 0.0

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
        if not preclose or preclose == 0: return 0.0
        try:
            return (float(val) - preclose) / preclose * 100
        except:
            return 0.0

    def _format_hour_data(self, row, prefix):
        o = row.get(f'{prefix}_open_rate')
        c = row.get(f'{prefix}_close_rate')
        h = row.get(f'{prefix}_high_rate')
        l = row.get(f'{prefix}_low_rate')
        if o is None: return "-"
        try:
            return f"O:{float(o):>5.1f}/C:{float(c):>5.1f}/H:{float(h):>5.1f}/L:{float(l):>5.1f}"
        except:
            return "-"

    def _generate_hourly_psychology(self, row, hour, is_buy_day, is_sell_day, buy_ratio_pct, already_sold=False, sell_price=0.0, preclose=0.0, buy_price=0.0):
        """
        【单小时专用】生成详细的心理分析和操作指令
        :param buy_price: 买入价，用于计算真实的总收益对比
        """
        h_prefix = f'hour{hour}'
        h_open_rate = float(row.get(f'{h_prefix}_open_rate') or 0)
        h_close_rate = float(row.get(f'{h_prefix}_close_rate') or 0)
        h_high_rate = float(row.get(f'{h_prefix}_high_rate') or 0)
        h_low_rate = float(row.get(f'{h_prefix}_low_rate') or 0)

        # 计算具体价格
        h_open_price = preclose * (1 + h_open_rate / 100)
        h_close_price = preclose * (1 + h_close_rate / 100)
        h_high_price = preclose * (1 + h_high_rate / 100)
        h_low_price = preclose * (1 + h_low_rate / 100)

        turn = float(row.get('turn', 0))
        current_price = float(row.get('close', 0.0))

        notes = []

        # ==========================================
        # 全局优先判断：如果已卖出，进入“旁观验证模式”
        # 【核心修复】基于总收益对比，而非瞬时价格对比
        # ==========================================
        if already_sold:
            notes.append(f"[H{hour} 旁观] 已离场观察。")

            if sell_price > 0 and buy_price > 0 and current_price > 0:
                # 1. 计算实际落袋收益率
                real_profit_pct = (sell_price - buy_price) / buy_price * 100

                # 2. 计算假设持有到现在的收益率
                hypothetical_profit_pct = (current_price - buy_price) / buy_price * 100

                # 3. 计算差异 (正数代表卖飞了/少赚了，负数代表躲过了/少亏了)
                diff_pct = hypothetical_profit_pct - real_profit_pct

                if diff_pct > 2.0:
                    notes.append(f"🧠 验证：真·卖飞了！若持有到现在将**多赚 {diff_pct:.1f}%**。")
                    notes.append(f"💰 数据：实赚 {real_profit_pct:.1f}% vs 持有可能赚 {hypothetical_profit_pct:.1f}%。")
                    notes.append(f"   (卖出价 {sell_price:.2f} vs 现价 {current_price:.2f})。虽有小憾，但纪律重于单笔利润。")
                elif diff_pct < -2.0:
                    notes.append(f"🧠 验证：神操作！成功**规避 {abs(diff_pct):.1f}% 的潜在回撤**。")
                    notes.append(f"💰 数据：实赚 {real_profit_pct:.1f}% vs 持有将亏/少赚 {hypothetical_profit_pct:.1f}%。")
                    notes.append(f"   (卖出价 {sell_price:.2f} vs 现价 {current_price:.2f})。庆幸严格执行了纪律！")
                else:
                    notes.append(f"🧠 验证：卖点合理，综合收益差异不大 ({diff_pct:+.1f}%)。")
                    notes.append(f"💰 数据：实赚 {real_profit_pct:.1f}% vs 持有 {hypothetical_profit_pct:.1f}%。落袋为安，资金效率最大化。")
            else:
                notes.append("🧠 心态：交易已结束，保持客观旁观。")

            return " | ".join(notes)

        # --- 场景 1: 买入日 ---
        if is_buy_day:
            if hour == 1:
                notes.append(f"[H1 观察] 开盘 {h_open_price:.2f} ({h_open_rate:.1f}%)，下探 {h_low_price:.2f} ({h_low_rate:.1f}%)。")
                if h_low_rate < -2.0:
                    notes.append("🧠 心理：恐慌盘杀出，机会可能在下方。")
                    notes.append("👉 指令：紧盯 H2，若快速收回 -1% 以上，准备开枪。")
                elif h_open_rate > 1.0:
                    notes.append("🧠 心理：强势高开，谨防获利盘兑现。")
                    notes.append("👉 指令：若 H2 不破开盘价，可轻仓试错。")
                else:
                    notes.append("🧠 心理：平淡开局，方向不明。")
                    notes.append("👉 指令：等待 H2 选择方向。")

            elif hour == 2:
                notes.append(f"[H2 决策] 低 {h_low_price:.2f} ({h_low_rate:.1f}%)，收 {h_close_price:.2f} ({h_close_rate:.1f}%)。")
                if h_low_rate < -2.0 and h_close_rate > h_low_rate + 1.0:
                    notes.append("🧠 心理：典型‘黄金坑’！急杀后有大单承接。")
                    notes.append(f"🚀 动作：【买入触发】立即进场！价格约 {h_close_price:.2f}。")
                    notes.append(f"🛡️ 预案：止损设在 {h_low_price:.2f} 下方。")
                elif h_close_rate > 2.0:
                    notes.append("🧠 心理：多头发动攻击。")
                    notes.append(f"🚀 动作：【追涨买入】确认强势。价格约 {h_close_price:.2f}。")
                else:
                    notes.append("🧠 心理：走势犹豫，无量震荡。")
                    notes.append("👉 指令：【放弃】不符合模式，管住手。")

            elif hour == 3:
                current_profit_approx = h_close_rate - buy_ratio_pct
                notes.append(f"[H3 验证] 浮盈/亏约 {current_profit_approx:.1f}%。")
                if h_close_rate > h_open_rate:
                    notes.append("🧠 心理：买入即涨，逻辑正确。")
                    notes.append("👉 指令：持股不动。若冲高至 +5% 以上，可将止损上移至成本价。")
                else:
                    notes.append("🧠 心理：有点慌，但在预期范围内。")
                    notes.append(f"👉 指令：只要不跌破 {h_low_price:.2f} (H2 低点)，就死拿。")

            elif hour == 4:
                notes.append(f"[H4 收盘] 今日收 {h_close_price:.2f} ({h_close_rate:.1f}%)。")
                if h_close_rate > 0:
                    notes.append("🧠 心理：完美收官，过夜无忧。")
                else:
                    notes.append("🧠 心理：小幅被套，焦虑。")
                    notes.append("👉 指令：明日若不能反包，果断离场。")

        # --- 场景 2: 卖出日 ---
        elif is_sell_day:
            if already_sold:
                notes.append(f"[H{hour} 盘后] 已止盈/损离场。")
                if sell_price > 0 and buy_price > 0:
                     # 同样使用总收益逻辑
                    real_profit_pct = (sell_price - buy_price) / buy_price * 100
                    hypothetical_profit_pct = (current_price - buy_price) / buy_price * 100
                    diff_pct = hypothetical_profit_pct - real_profit_pct

                    if diff_pct > 2.0:
                        notes.append(f"🧠 验证：卖飞了！若持有到现在将多赚 {diff_pct:.1f}%。")
                        notes.append(f"💰 对比：实赚 {real_profit_pct:.1f}% vs 持有 {hypothetical_profit_pct:.1f}%。")
                    elif diff_pct < -2.0:
                        notes.append(f"🧠 验证：神操作！成功躲过 {abs(diff_pct):.1f}% 的回撤。")
                        notes.append(f"💰 对比：实赚 {real_profit_pct:.1f}% vs 持有 {hypothetical_profit_pct:.1f}%。")
                    else:
                        notes.append(f"🧠 验证：卖点合理，后续波动影响不大。")
                return " | ".join(notes)

            # 未卖出前的逻辑
            if hour == 1:
                notes.append(f"[H1 竞价] 开盘 {h_open_price:.2f} ({h_open_rate:.1f}%)。")
                if h_open_rate > 2.0:
                    notes.append("🧠 心理：大肉！主力给面子。")
                    notes.append("👉 指令：【准备止盈】若 H2 不能封板，坚决卖出。")
                elif h_open_rate < -2.0:
                    notes.append("🧠 心理：不及预期，甚至核按钮。")
                    notes.append("👉 指令：【准备止损】反弹翻红无力即卖出。")
                else:
                    notes.append("🧠 心理：正常波动。")
                    notes.append("👉 指令：观察 H2 方向。")

            elif hour == 2:
                notes.append(f"[H2 执行] 高 {h_high_price:.2f} ({h_high_rate:.1f}%)，低 {h_low_price:.2f} ({h_low_rate:.1f}%)。")
                if h_high_rate > 4.0:
                    notes.append(f"🚨 触发：冲高达标！(阈值 >4.0%, 实际 {h_high_rate:.1f}%)。")
                    notes.append(f"🚀 动作：【卖出执行】价格约 {h_high_price:.2f}。不要贪婪，此时卖出最稳妥。")
                elif h_low_rate < -3.0:
                    notes.append(f"🚨 触发：跳水破位！(阈值 <-3.0%, 实际 {h_low_rate:.1f}%)。")
                    notes.append(f"🚀 动作：【卖出执行】价格约 {h_low_price:.2f}。割肉离场，留得青山在。")
                else:
                    notes.append("👉 指令：继续持有到 H3/H4。")

            elif hour == 3:
                notes.append(f"[H3 尾盘前] 收 {h_close_price:.2f} ({h_close_rate:.1f}%)。")
                notes.append("🧠 心理：时间差不多了，不管盈亏都要走了。")
                notes.append("👉 指令：挂单准备，最后一分钟清仓。")

            elif hour == 4:
                notes.append(f"[H4 终结] 策略结束。")
                notes.append("🧠 心理：如释重负。执行力就是生命力。")
                notes.append("✅ 动作：【清仓完毕】寻找下一个目标。")

        # --- 场景 3: 持仓中 ---
        else:
            notes.append(f"[H{hour} 持仓] 波动区间 [{h_low_price:.2f}, {h_high_price:.2f}] ({h_low_rate:.1f}% ~ {h_high_rate:.1f}%)。")
            if abs(h_close_rate) < 1.0:
                notes.append("🧠 心理：织布机行情，磨人。")
                notes.append("👉 指令：关掉软件，不要看盘。")
            elif h_close_rate > 3.0:
                notes.append("🧠 心理：爽！利润奔跑。")
                notes.append("👉 指令：设好移动止盈（回撤 1% 就走）。")
            elif h_close_rate < -3.0:
                notes.append("🧠 心理：难受，想卖。")
                notes.append(f"👉 指令：检查是否触及止损线 (现价 {h_close_price:.2f})。没到就死扛。")

            if turn > 15.0:
                notes.append("⚠️ 警报：巨量换手！主力可能在出货，高度警惕！")

        return " | ".join(notes)

    def export_trade_snapshot(self, code, action, price, date, reason, is_sell=False):
        """
        【终极版】导出交易快照：包含具体价格和触发细节
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
        self.last_sell_price = price

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

        start_idx = max(0, buy_idx - 2)
        end_idx = min(len(data_list), sell_idx + 3)
        snapshot_data = data_list[start_idx:end_idx]

        fieldnames = [
            'Date', 'Hour', 'Act',
            'O%', 'Open', 'C%', 'Close', 'H/L%',
            '🧠 核心心理与验证 (含具体价格/阈值)'
        ]

        with open(filename, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()

            for i, row in enumerate(snapshot_data):
                current_date = row['date']
                is_buy_day = (current_date == self.current_trade_buy_date)
                is_sell_day = (current_date == date)

                buy_ratio = getattr(self, 'current_trade_buy_price_rate', 1.0)
                buy_ratio_pct = (buy_ratio - 1.0) * 100
                preclose = float(row.get('preclose', 1.0))

                sold_flag = False

                for h in range(1, 5):
                    h_prefix = f'hour{h}'
                    h_open_rate = float(row.get(f'{h_prefix}_open_rate') or 0)
                    h_close_rate = float(row.get(f'{h_prefix}_close_rate') or 0)
                    h_high_rate = float(row.get(f'{h_prefix}_high_rate') or 0)
                    h_low_rate = float(row.get(f'{h_prefix}_low_rate') or 0)

                    h_open_price = preclose * (1 + h_open_rate / 100)
                    h_close_price = preclose * (1 + h_close_rate / 100)
                    h_high_price = preclose * (1 + h_high_rate / 100)
                    h_low_price = preclose * (1 + h_low_rate / 100)

                    is_future_day = (current_date > date)
                    already_sold_flag = sold_flag or is_future_day

                    psychology_text = self._generate_hourly_psychology(
                        row=row,
                        hour=h,
                        is_buy_day=is_buy_day,
                        is_sell_day=is_sell_day,
                        buy_ratio_pct=buy_ratio_pct,
                        already_sold=already_sold_flag,
                        sell_price=self.last_sell_price,
                        preclose=preclose,
                        buy_price=self.current_trade_buy_price
                    )

                    action_mark = ""
                    if is_buy_day and h == 2:
                        action_mark = "🟢 BUY"

                    if is_sell_day and not sold_flag:
                        if "【卖出执行】" in psychology_text:
                            action_mark = "🔴 SELL"
                            sold_flag = True
                        elif h == 1 and ("准备止盈" in psychology_text or "准备止损" in psychology_text):
                            action_mark = "⚠️ WAIT"
                        elif h == 4 and "【清仓完毕】" in psychology_text:
                            action_mark = "🏁 END"
                            sold_flag = True

                    if is_future_day:
                        action_mark = ""

                    writer.writerow({
                        'Date': current_date,
                        'Hour': f"H{h}",
                        'Act': action_mark,
                        'O%': f"{h_open_rate:.1f}",
                        'Open': f"{h_open_price:.2f}",
                        'C%': f"{h_close_rate:.1f}",
                        'Close': f"{h_close_price:.2f}",
                        'H/L%': f"{h_high_rate:.1f}/{h_low_rate:.1f}",
                        '🧠 核心心理与验证 (含具体价格/阈值)': psychology_text
                    })

        # 控制台打印
        print("\n" + "="*200)
        print(f"📊 [HOURLY BREAKDOWN] {code} | Buy:{self.current_trade_buy_date} @ {self.current_trade_buy_price:.2f} | Sell:{date} @ {price:.2f}")
        print(f"   Profit: {reason.split('|')[-1].strip()}")
        print("="*200)
        print(f"{'Date':<10} | {'Hr':<3} | {'Act':<6} | {'O%':>5} | {'Open':>7} | {'C%':>5} | {'Close':>7} | {'H/L%':>10} | 🧠 核心心理与验证")
        print("-" * 200)

        for i, row in enumerate(snapshot_data):
            current_date = row['date']
            is_buy_day = (current_date == self.current_trade_buy_date)
            is_sell_day = (current_date == date)
            preclose = float(row.get('preclose', 1.0))

            sold_flag_print = False

            for h in range(1, 5):
                h_prefix = f'hour{h}'
                h_open_rate = float(row.get(f'{h_prefix}_open_rate') or 0)
                h_close_rate = float(row.get(f'{h_prefix}_close_rate') or 0)
                h_high_rate = float(row.get(f'{h_prefix}_high_rate') or 0)
                h_low_rate = float(row.get(f'{h_prefix}_low_rate') or 0)

                h_open_price = preclose * (1 + h_open_rate / 100)
                h_close_price = preclose * (1 + h_close_rate / 100)
                h_high_price = preclose * (1 + h_high_rate / 100)
                h_low_price = preclose * (1 + h_low_rate / 100)

                is_future_day = (current_date > date)
                already_sold_flag = sold_flag_print or is_future_day

                full_note = self._generate_hourly_psychology(row, h, is_buy_day, is_sell_day, buy_ratio_pct, already_sold_flag, self.last_sell_price, preclose, self.current_trade_buy_price)

                act = ""
                if is_buy_day and h == 2: act = "🟢BUY"

                if is_sell_day and not sold_flag_print:
                    if "【卖出执行】" in full_note:
                        act = "🔴SELL"
                        sold_flag_print = True
                    elif act == "" and h == 1 and ("准备止盈" in full_note or "准备止损" in full_note):
                        act = "⚠️WAIT"
                    elif act == "" and h == 4 and "【清仓完毕】" in full_note:
                        act = "🏁END"
                        sold_flag_print = True

                short_note = full_note.replace('\n', ' ')
                if len(short_note) > 60: short_note = short_note[:57] + "..."

                print(f"{current_date:<10} | H{h:<2} | {act:<6} | {h_open_rate:>5.1f} | {h_open_price:>7.2f} | {h_close_rate:>5.1f} | {h_close_price:>7.2f} | {h_high_rate:>4.1f}/{h_low_rate:>4.1f} | {short_note}")

        print("-" * 200)
        print(f"💾 完整详细报告已存档 (含所有价格细节): {filename}")
        print("="*200 + "\n")

    def save_to_db(self):
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
        print(f"🚀 开始回测策略：[{self.strategy.name}] (高精度成交版 - 分时资金校验 & 严格天数控制)")
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

            # 【关键修复 1】无论今天是否卖出，只要持仓过夜，持有天数必须先 +1
            # 放在最前面，确保天数统计准确，解决“因未卖出导致天数不增加”的死循环
            if self.position:
                days_held += 1

            # --- 阶段 1: 确定今日是否必须卖出 (包含时间止损优先逻辑) ---
            should_sell_today = False
            sell_reason_preview = ""
            sell_price_preview = 0.0
            sell_triggered_hour = 4 # 默认尾盘

            if self.position:
                code = self.position['code']
                buy_price = self.position['buy_price']

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

                    # A. 最高优先级：时间止损 (一旦到期，无视其他信号，强制卖出)
                    if days_held >= self.strategy.max_hold_days:
                        should_sell_today = True
                        sell_reason_preview = f"时间止损 ({self.strategy.max_hold_days}天)"
                        sell_price_preview = current_price
                        sell_triggered_hour = 4
                    else:
                        # B. 其次：开盘强止损 (仅在第一天有效)
                        open_p = row_dict['open']
                        if days_held == 1 and open_p > 0 and (open_p - buy_price) / buy_price <= -0.03:
                            should_sell_today = True
                            sell_reason_preview = "开盘强止损 (-3%)"
                            sell_price_preview = open_p
                            sell_triggered_hour = 1

                        # C. 最后：策略信号 (冲高/跳水等)
                        if not should_sell_today:
                            res = self.strategy.check_sell_condition(code, buy_price, date, row_dict, profit_rate, days_held)
                            if len(res) == 3:
                                s_flag, s_reason, s_ratio = res
                                if s_flag:
                                    should_sell_today = True
                                    sell_reason_preview = s_reason
                                    if s_ratio > 0:
                                        sell_price_preview = row_dict['preclose'] * s_ratio
                                        # 估算时段：冲高通常发生在 H2/H3
                                        if s_ratio > 1.04: sell_triggered_hour = 2
                                        else: sell_triggered_hour = 4
                                    else:
                                        sell_price_preview = current_price
                                        sell_triggered_hour = 4
                            elif len(res) == 2:
                                s_flag, s_reason = res
                                if s_flag:
                                    should_sell_today = True
                                    sell_reason_preview = s_reason
                                    sell_price_preview = current_price
                                    sell_triggered_hour = 4

            # --- 阶段 2: 执行卖出 (如果预演决定要卖) ---
            executed_sell = False
            actual_sell_hour = 4

            if self.position and should_sell_today:
                 code = self.position['code']
                 buy_price = self.position['buy_price']
                 display_name = self.get_stock_display_name(code)

                 sell_price = sell_price_preview
                 reason = sell_reason_preview

                 # 修正 sell_triggered_hour 用于资金校验
                 if reason.startswith("开盘"): actual_sell_hour = 1
                 elif "冲高" in reason or "跳水" in reason: actual_sell_hour = 2
                 else: actual_sell_hour = 4

                 sell_value = sell_price * self.position['shares']
                 net_sell = sell_value * (1 - 0.0003)
                 buy_cost = buy_price * self.position['shares'] * (1 + 0.0003)
                 trade_profit = net_sell - buy_cost

                 self.capital += net_sell

                 self.current_trade_buy_price_rate = buy_price / row_dict['preclose'] if row_dict['preclose'] else 1.0
                 self.export_trade_snapshot(code, 'SELL', sell_price, date, f"{reason} | 盈亏:{trade_profit:+.1f}", is_sell=True)

                 status_icon = "✅ 盈利" if trade_profit > 0 else "❌ 亏损"
                 print(f"   💥 [{date}] {status_icon} 卖出 {display_name} @ {sell_price:.2f} (持有{days_held}天) | 盈亏:{trade_profit:+,.2f}")

                 self.trades.append({'date': date, 'code': code, 'action': 'SELL', 'price': sell_price, 'profit': trade_profit, 'reason': reason})
                 self.position = None
                 days_held = 0 # 重置持有天数
                 executed_sell = True
                 sell_triggered_hour = actual_sell_hour # 更新全局变量供买入使用

            # --- 阶段 3: 选股与买入 (带资金时序校验) ---
            if not self.position:
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

                if codes:
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
                    for j in range(0, len(codes), BATCH_SIZE):
                        batch_codes = codes[j:j+BATCH_SIZE]
                        ph = ','.join('?' * len(batch_codes))
                        self.c.execute(f"""SELECT code, close, preclose, turn, pctChg, open, low, high,
                               hour1_open_rate, hour1_close_rate, hour1_high_rate, hour1_low_rate,
                               hour2_open_rate, hour2_close_rate, hour2_high_rate, hour2_low_rate,
                               hour3_open_rate, hour3_close_rate, hour3_high_rate, hour3_low_rate,
                               hour4_open_rate, hour4_close_rate, hour4_high_rate, hour4_low_rate
                            FROM stock_daily_k WHERE date=? AND code IN ({ph})""", [prev_date] + batch_codes)
                        for r in self.c.fetchall(): prev_k_map[r['code']] = dict(r)

                    if hasattr(self.strategy, 'select_candidates'):
                        candidate_codes = self.strategy.select_candidates(prev_date, pool_data, prev_k_map, history_map)

                if candidate_codes:
                    candidate_rows = {}
                    for i_batch in range(0, len(candidate_codes), BATCH_SIZE):
                        batch_codes = candidate_codes[i_batch : i_batch + BATCH_SIZE]
                        placeholders = ','.join('?' * len(batch_codes))
                        try:
                            self.c.execute(f"""SELECT code, open, preclose, hour3_close_rate, hour4_open_rate, hour4_close_rate, pctChg,
                                   hour1_high_rate, hour2_high_rate, hour3_high_rate, hour3_low_rate,
                                   hour1_open_rate, hour1_low_rate, hour2_open_rate
                            FROM stock_daily_k WHERE date=? AND code IN ({placeholders})""", [date] + batch_codes)
                            for row in self.c.fetchall(): candidate_rows[row['code']] = dict(row)
                        except Exception as e:
                            continue

                    report_result = self.strategy.generate_h3_analysis_report(date, candidate_codes, candidate_rows, {})
                    best_code = None; best_reason = ""; best_buy_ratio = 0.0

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
                            # 【资金时序校验】
                            buy_triggered_hour = 4
                            if "H2Open" in reason or (k_row.get('hour2_open_rate', 0) > -1 and k_row.get('hour2_open_rate', 0) < 2):
                                buy_triggered_hour = 2
                            elif "H1" in reason:
                                buy_triggered_hour = 1
                            elif "H3" in reason:
                                buy_triggered_hour = 3

                            capital_available_hour = 1
                            if executed_sell:
                                capital_available_hour = sell_triggered_hour

                            if buy_triggered_hour >= capital_available_hour:
                                preclose = k_row.get('preclose', 0)
                                final_buy_price = preclose * buy_ratio
                                display_name = self.get_stock_display_name(best_code)

                                fee = self.capital * 0.0003
                                shares = int((self.capital - fee) // final_buy_price / 100) * 100

                                if shares > 0:
                                    cost = shares * final_buy_price + fee
                                    self.capital -= cost
                                    self.position = {'code': best_code, 'buy_date': date, 'buy_price': final_buy_price, 'shares': shares, 'highest_price': final_buy_price}
                                    days_held = 0 # 新买入，重置天数

                                    self.current_trade_buy_reason = reason
                                    self.current_trade_buy_date = date
                                    self.current_trade_buy_price = final_buy_price
                                    self.current_trade_buy_price_rate = buy_ratio

                                    self.trades.append({
                                        'date': date, 'code': best_code, 'action': 'BUY',
                                        'price': final_buy_price, 'profit': 0.0, 'reason': reason
                                    })
                                    print(f"   🚀 [{date}] ✅ 买入 {display_name} @ {final_buy_price:.2f} (时段:H{buy_triggered_hour})")
                                else:
                                    print(f"   ⚠️ [{date}] 资金不足")
                            else:
                                print(f"   ⏳ [{date}] 跳过 {best_code}: 买点 (H{buy_triggered_hour}) 早于资金释放 (H{capital_available_hour})")
                    else:
                         if candidate_codes: print(f"   ℹ️ [{date}] 无符合买点标的")

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
