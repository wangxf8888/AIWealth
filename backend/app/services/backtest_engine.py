# backend/app/services/backtest_engine.py
import sqlite3
from pathlib import Path
import random
import os
import csv
from datetime import datetime
from .strategies.dragon_head_turnover import DragonHeadTurnoverStrategy

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "wealth.db"
# 创建交易快照目录
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
        self.current_position_t0_logs = []

        # 交易计数器，用于文件名
        self.trade_count = 0

        self.conn = get_conn()
        self.c = self.conn.cursor()

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

    def export_trade_snapshot(self, code, action, price, date, reason, window_days=15):
        """
        导出交易时刻附近的 K 线数据到 CSV，并打印文本图
        :param code: 股票代码
        :param action: 'BUY' or 'SELL'
        :param price: 成交价格
        :param date: 交易日期
        :param reason: 交易理由
        :param window_days: 前后截取天数
        """
        self.trade_count += 1
        safe_code = code.replace('.', '_')
        timestamp = datetime.now().strftime("%H%M%S")
        filename = f"{SNAPSHOT_DIR}/{date}_{action}_{safe_code}_{timestamp}.csv"

        # 1. 查询 K 线数据 (当前日期往前推 window_days，往后推 2 天以便观察后续走势)
        # 注意：由于是回测，我们只能查到当前数据库里有的数据。
        # 如果是卖出操作，我们可以查到卖出日之后的数据（因为数据库里有历史全量）。
        # 为了简化，我们统一查询：以交易日期为中心，前后各取一部分。
        # 但 SQLite 查询通常基于 <= date。
        # 策略：查询从 (date - 10 天) 到 (date + 5 天) 的数据。
        # 由于我们不知道未来的日期具体是哪几个交易日，我们先查出所有日期排序，再切片。

        # 简单做法：查出该股票所有数据，然后在内存中筛选附近日期
        # 为了性能，我们只查最近 30 天相对于该日期的数据（假设回测是顺序执行的，后面的数据也在库里）
        # 修正：直接查该股票在 [start_date, end_date] 范围内的所有数据可能会慢。
        # 优化：查该日期前后各 10 个交易日。

        # 先获取该日期前后的日期范围 (粗略估计)
        # 这里为了准确，我们一次性取出该股票在回测区间的所有数据，然后定位 (数据量不大，可接受)
        # 或者：利用索引查特定范围。

        # 最稳健方法：查该日期之前 12 天 和 之后 5 天
        # 由于不知道具体交易日间隔，我们多查一点然后过滤

        self.c.execute("""
            SELECT date, open, high, low, close, volume, turn, pctChg
            FROM stock_daily_k
            WHERE code = ?
            ORDER BY date ASC
        """, (code,))

        all_rows = self.c.fetchall()
        if not all_rows:
            return

        # 转为列表方便处理
        data_list = [dict(r) for r in all_rows]

        # 找到交易日期所在的索引
        target_index = -1
        for i, row in enumerate(data_list):
            if row['date'] == date:
                target_index = i
                break

        if target_index == -1:
            return # 没找到当天数据

        # 切片：前 10 天，后 5 天
        start_idx = max(0, target_index - 10)
        end_idx = min(len(data_list), target_index + 6) # 包含当天再加 5 天

        snapshot_data = data_list[start_idx:end_idx]

        if not snapshot_data:
            return

        # 2. 写入 CSV
        with open(filename, 'w', newline='', encoding='utf-8-sig') as f:
            fieldnames = ['date', 'open', 'high', 'low', 'close', 'pctChg', 'volume', 'Signal', 'Price', 'Reason']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()

            for row in snapshot_data:
                signal = ""
                sig_price = ""
                sig_reason = ""

                if row['date'] == date:
                    signal = action
                    sig_price = f"{price:.2f}"
                    sig_reason = reason[:50] # 截断过长理由

                writer.writerow({
                    'date': row['date'],
                    'open': f"{row['open']:.2f}",
                    'high': f"{row['high']:.2f}",
                    'low': f"{row['low']:.2f}",
                    'close': f"{row['close']:.2f}",
                    'pctChg': f"{row['pctChg']:.2f}",
                    'volume': int(row['volume']),
                    'Signal': signal,
                    'Price': sig_price,
                    'Reason': sig_reason
                })

        # 3. 打印简易文本 K 线图到日志
        print(f"\n📊 [{'='*20}] 交易快照：{action} {code} @ {date} [{'='*20}]")
        print(f"   理由：{reason}")
        print(f"   数据已保存至：{filename}")
        print("-" * 80)
        print(f"{'Date':<12} | {'Close':>8} | {'Pct%':>6} | {'Vol':>10} | {'Signal':>8} | {'K-Line (Text)'}")
        print("-" * 80)

        # 计算缩放比例用于画图
        closes = [float(r['close']) for r in snapshot_data]
        min_c, max_c = min(closes), max(closes)
        range_c = max_c - min_c if max_c > min_c else 1.0
        width = 40 # 图表宽度

        for row in snapshot_data:
            c = float(row['close'])
            o = float(row['open'])
            h = float(row['high'])
            l = float(row['low'])
            pct = float(row['pctChg'])

            # 确定信号标记
            sig_mark = ""
            if row['date'] == date:
                sig_mark = f"<<<{action}>>>" if action == 'BUY' else f"<<<{action}>>>"

            # 简单的 ASCII K 线逻辑
            # 归一化位置
            norm_low = int((l - min_c) / range_c * width)
            norm_high = int((h - min_c) / range_c * width)
            norm_close = int((c - min_c) / range_c * width)
            norm_open = int((o - min_c) / range_c * width)

            # 构建行
            line_chars = [' '] * (width + 1)
            # 画高低点连线 (简化为实体)
            start = min(norm_low, norm_high)
            end = max(norm_low, norm_high)

            # 画实体
            body_start = min(norm_close, norm_open)
            body_end = max(norm_close, norm_open)

            for k in range(start, end + 1):
                if 0 <= k <= width:
                    line_chars[k] = '|'

            # 实体加粗 (用 # 表示)
            color = '+' if pct >= 0 else '-'
            for k in range(body_start, body_end + 1):
                if 0 <= k <= width:
                    line_chars[k] = '#' if color == '+' else '='

            k_line_str = "".join(line_chars)

            # 颜色指示 (文本模拟)
            trend = "🔺" if pct > 0 else ("🔻" if pct < 0 else "➖")

            print(f"{row['date']:<12} | {c:>8.2f} | {pct:>5.1f}% {trend} | {int(row['volume']):>10} | {sig_mark:>8} | {k_line_str}")

        print("-" * 80)
        print(f"[{'='*58}]\n")

    def run(self):
        print(f"🚀 开始回测策略：[{self.strategy.name}]")
        print(f"   时间范围：{self.start_date} 至 {self.end_date}")
        print(f"   初始资金：{self.initial_capital:,.2f} 元")
        print(f"   📂 交易快照将保存至：{SNAPSHOT_DIR}")
        print("=" * 100)

        self.c.execute("""
            SELECT DISTINCT date FROM stock_daily_k
            WHERE date >= ? AND date <= ? ORDER BY date ASC
        """, (self.start_date, self.end_date))
        trade_dates = [row[0] for row in self.c.fetchall()]

        if len(trade_dates) < 2:
            print("❌ 数据不足，无法回测。")
            return

        days_held = 0

        for i, date in enumerate(trade_dates):
            sell_triggered = False
            sell_reason = ""
            sell_price = 0.0

            # --- A. 处理持仓卖出逻辑 ---
            if self.position:
                code = self.position['code']
                buy_price = self.position['buy_price']
                display_name = self.get_stock_display_name(code)

                self.c.execute("""
                    SELECT close, pctChg, open, low, high, preclose,
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

                    current_high = float(row_dict['high'])
                    if current_high > self.position.get('highest_price', buy_price):
                        self.position['highest_price'] = current_high

                    # --- 卖出判断 ---
                    open_p = row_dict['open']
                    should_sell = False
                    reason = ""

                    # 检查开盘强止损
                    if days_held == 1 and open_p > 0:
                        open_profit = (open_p - buy_price) / buy_price
                        if open_profit <= -0.03: # 临时硬编码开盘止损，也可调策略
                             should_sell = True
                             reason = f"开盘强止损"
                             sell_price = open_p

                    if not should_sell:
                        should_sell, reason = self.strategy.check_sell_condition(
                            code, self.position['buy_date'], buy_price,
                            date, row_dict, profit_rate, days_held
                        )
                        sell_price = current_price

                    if days_held >= self.strategy.max_hold_days and not should_sell:
                        should_sell = True
                        reason = f"时间止损"

                    if should_sell:
                        sell_triggered = True
                        sell_reason = reason

                        sell_value = sell_price * self.position['shares']
                        fee = sell_value * 0.0003
                        net_sell = sell_value - fee

                        buy_cost = buy_price * self.position['shares']
                        buy_fee = buy_cost * 0.0003

                        total_t0_profit = sum(self.current_position_t0_logs)
                        trade_profit = (net_sell - (buy_cost + buy_fee)) + total_t0_profit
                        total_invested = buy_cost + buy_fee
                        trade_return_rate = (trade_profit / total_invested) * 100

                        self.capital += net_sell

                        # 📸 【关键】导出卖出快照
                        self.export_trade_snapshot(
                            code, 'SELL', sell_price, date,
                            f"{reason} | 盈亏:{trade_profit:+.1f}({trade_return_rate:.1f}%)",
                            window_days=15
                        )

                        status_icon = "✅ 盈利" if trade_profit > 0 else "❌ 亏损"
                        print(f"   💥 [{date}] {status_icon} 卖出 {display_name} @ {sell_price:.2f}")
                        print(f"      单笔盈亏:{trade_profit:+,.2f} | 累计:{self.capital - self.initial_capital:+,.2f}")
                        print(f"      原因：{reason}")
                        print("=" * 100)

                        self.trades.append({
                            'date': date, 'code': code, 'action': 'SELL',
                            'price': sell_price, 'profit': trade_profit, 'return_rate': trade_return_rate, 'reason': reason
                        })

                        self.position = None
                        days_held = 0
                        self.current_position_t0_logs = []

            # --- B. T-1 日：选股 ---
            # (此处省略部分日志打印以保持整洁，逻辑不变)
            self.c.execute("SELECT code, code_name FROM core_pool_history WHERE trade_date=?", (date,))
            pool_rows = self.c.fetchall()

            if pool_rows and not self.position: # 只有空仓时才选股 (简化逻辑，也可改为多只)
                 # 注意：原逻辑是 T-1 选，T 买。这里保持原引擎逻辑
                 pass

            # 重新获取候选池逻辑 (适配原引擎结构)
            # 原引擎逻辑是在 T 日检查 T-1 选的票。
            # 我们需要确保 strategy.yesterday_candidates 已经被填充。
            # 在原引擎中，这部分逻辑在 "B. T-1 日" 块中调用 select_candidates。
            # 让我们复现那部分逻辑以确保 candidates 存在

            if pool_rows:
                pool_data = [{'code': r['code'], 'name': r['code_name']} for r in pool_rows]
                codes = [p['code'] for p in pool_data]
                if codes:
                    placeholders = ','.join('?' * len(codes))
                    # 获取当日数据用于选股分析 (虽然选股是基于 T-1 收盘，但引擎里是在 T 日跑 T-1 的逻辑)
                    # 注意：原引擎逻辑有点混淆，它是在 T 日循环里，用 T 日的数据去跑 select_candidates?
                    # 不，原引擎是：
                    # 1. 查 core_pool_history (这是 T-1 日的池子)
                    # 2. 获取这些股票在 T 日的实时/收盘数据 (k_data_map)
                    # 3. 获取历史数据 (history_map) -> 这里的 history_map 应该截止到 T-1 日才对！
                    # **重大逻辑修正提示**：
                    # 在回测中，当我们在 T 日运行时，select_candidates 应该使用 T-1 日及之前的数据。
                    # 但原引擎传入的是 T 日的 k_data_map 和 history_map (包含 T 日)。
                    # 这会导致未来函数！
                    # **但是**，为了保持和你当前代码兼容，且你的策略里 `select_candidates` 主要是看形态。
                    # 你的策略 `evaluate_realtime_strength` 是用 T 日数据决定买不买。
                    # 而 `select_candidates` 是在 T-1 日晚上的逻辑。
                    # 在回测引擎中，我们通常在 T 日开盘前，用 T-1 日的数据跑一遍 `select_candidates`。

                    # 获取 T-1 日的数据来跑选股 (模拟昨晚的工作)
                    prev_date = trade_dates[i-1] if i > 0 else date
                    # 构造 history_map (截止到 prev_date)
                    history_map = {}
                    for c_code in codes:
                        self.c.execute("""
                            SELECT close, high, low, open, volume, amount, turn, pctChg, preclose, date
                            FROM stock_daily_k
                            WHERE code=? AND date <= ?
                            ORDER BY date DESC
                            LIMIT 25
                        """, (c_code, prev_date))
                        rows = self.c.fetchall()
                        history_map[c_code] = [dict(r) for r in rows]

                    # 构造 k_data_map (prev_date 的数据，作为"昨天"的收盘)
                    # 注意：select_candidates 只需要 prev_date 的收盘数据来判断形态
                    # 但原引擎逻辑似乎是把 prev_date 当作"今天"传给 select_candidates?
                    # 让我们看原引擎：它传的是 `today_map` (其实是当前循环日期 date 的数据？)
                    # 不，原引擎代码中：
                    # self.c.execute(... WHERE date=? ... [date] + codes) -> 这是 T 日数据
                    # 然后传给 select_candidates(date, pool, today_map, history_map)
                    # 这意味着你的 select_candidates 看到的是 T 日的数据！这是未来函数！

                    # **修正方案**：
                    # 我们必须手动构造 T-1 日的数据快照传给 select_candidates。

                    prev_k_map = {}
                    self.c.execute(f"""
                        SELECT code, close, preclose, turn, pctChg, open, low, high
                        FROM stock_daily_k WHERE date=? AND code IN ({placeholders})
                    """, [prev_date] + codes)
                    for r in self.c.fetchall():
                        prev_k_map[r['code']] = dict(r)

                    # 运行选股 (使用 T-1 日数据)
                    if hasattr(self.strategy, 'select_candidates'):
                        # 注意：这里传入的 date 应该是 prev_date，代表选股发生的日期
                        candidate_codes = self.strategy.select_candidates(prev_date, pool_data, prev_k_map, history_map)
                    else:
                        candidate_codes = []
                else:
                    candidate_codes = []
            else:
                candidate_codes = []

            # --- C. T 日：检查候选股并买入 ---
            if not self.position and candidate_codes:
                # 获取 T 日 (当前 date) 的实时数据用于买入判断
                placeholders = ','.join('?' * len(candidate_codes))
                self.c.execute(f"""
                    SELECT code, open, preclose, hour3_close_rate, hour4_open_rate, hour4_close_rate, pctChg,
                           hour1_high_rate, hour2_high_rate, hour3_high_rate, hour3_low_rate
                    FROM stock_daily_k
                    WHERE date=? AND code IN ({placeholders})
                """, [date] + candidate_codes)

                candidate_rows = {r['code']: dict(r) for r in self.c.fetchall()}

                # 需要构建一个简化的 history_map 给 evaluate 用吗？
                # evaluate_realtime_strength 主要看分时和当日涨跌，不太依赖长历史
                # 但为了安全，我们传入空的或简单的

                best_code, best_reason = self.strategy.generate_h3_analysis_report(
                    date,
                    candidate_codes,
                    candidate_rows,
                    {} # 买入决策主要看当天分时，暂不需要长历史
                )

                if best_code and best_code in candidate_rows:
                    k_row = candidate_rows[best_code]
                    should_buy, buy_ratio, reason = self.strategy.generate_buy_signal(
                        date, best_code, k_row, is_candidate=True
                    )

                    if should_buy:
                        preclose = k_row.get('preclose', 0)
                        buy_price = preclose * buy_ratio

                        display_name = self.get_stock_display_name(best_code)
                        print(f"   🚀 [{date}] ✅ 买入：{display_name} @ {buy_price:.2f}")
                        print(f"      决策依据：{best_reason}")

                        # 📸 【关键】导出买入快照
                        self.export_trade_snapshot(
                            best_code, 'BUY', buy_price, date,
                            best_reason,
                            window_days=15
                        )

                        fee = self.capital * 0.0003
                        shares = int((self.capital - fee) // buy_price / 100) * 100

                        if shares > 0:
                            cost = shares * buy_price + fee
                            self.capital -= cost
                            self.position = {
                                'code': best_code,
                                'buy_date': date,
                                'buy_price': buy_price,
                                'shares': shares,
                                'highest_price': buy_price
                            }
                            days_held = 0
                            self.current_position_t0_logs = []
                            print("-" * 80)

            # --- D. 记录快照 ---
            current_total_value = self.capital
            if self.position:
                self.c.execute("SELECT close FROM stock_daily_k WHERE code=? AND date=?", (self.position['code'], date))
                res = self.c.fetchone()
                if res:
                    current_total_value += res[0] * self.position['shares']

            self.daily_snapshots.append({
                'date': date,
                'total_value': current_total_value,
                'cash': self.capital
            })

        self.conn.close()
        self.report_cn()

    def report_cn(self):
        final_value = self.daily_snapshots[-1]['total_value'] if self.daily_snapshots else self.capital
        total_return = (final_value - self.initial_capital) / self.initial_capital

        win_trades = sum(1 for t in self.trades if t['action']=='SELL' and t['profit']>0)
        total_trades = sum(1 for t in self.trades if t['action']=='SELL')
        win_rate = win_trades/total_trades if total_trades>0 else 0

        max_drawdown = 0.0
        if len(self.daily_snapshots) > 0:
            peak = self.initial_capital
            for snap in self.daily_snapshots:
                val = snap['total_value']
                if val > peak: peak = val
                dd = (peak - val) / peak
                if dd > max_drawdown: max_drawdown = dd

        print("\n" + "="*60)
        print(f"📊 回测报告总结：{self.strategy.name}")
        print("="*60)
        print(f"   回测周期：{self.start_date} 至 {self.end_date}")
        print(f"   初始资金：{self.initial_capital:,.2f} 元")
        print(f"   最终资金：{final_value:,.2f} 元")
        print(f"   总收益率：{total_return*100:.2f}%")
        print(f"   最大回撤：{max_drawdown*100:.2f}%")
        print(f"   交易次数：{total_trades} 次")
        print(f"   胜率：{win_rate*100:.2f}%")
        print(f"   📂 所有交易 K 线快照已保存至：{SNAPSHOT_DIR}")
        print("="*60)
