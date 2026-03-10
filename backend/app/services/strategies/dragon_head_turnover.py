# backend/app/services/strategies/dragon_head_turnover.py
from .base_strategy import BaseStrategy

class DragonHeadTurnoverStrategy(BaseStrategy):
    def __init__(self):
        super().__init__("DragonHead_DoubleBottom_V4_Aggressive")

        # --- 核心策略参数 (用户定制版) ---
        self.buy_max_pct = 2.0          # 买入时当日涨幅不得超过 2%
        self.h3_buy_range_low = -2.0    # H3 收盘价下限 -2%
        self.h3_buy_range_high = 2.0    # H3 收盘价上限 2%

        self.t0_sell_threshold = 7.0    # H1/H2/H3 冲高 7% 卖出 (提高止盈)
        self.stop_loss = -0.04          # 硬止损 -4% (收紧止损)
        self.max_hold_days = 3          # 最大持有 3 天 (快速周转)

        # 做 T 开关 (暂时关闭)
        self.enable_t0 = False
        self.t0_capital_ratio = 0.5

        self.yesterday_candidates = []
        self.highest_price_since_buy = 0.0

    def _is_main_board(self, code):
        if not code: return False
        code_num = code.split('.')[-1] if '.' in code else code
        if code_num.startswith('300') or code_num.startswith('301'): return False
        if code_num.startswith('688') or code_num.startswith('689'): return False
        return True

    def _safe_float(self, val, default=0.0):
        if val is None: return default
        try: return float(val)
        except: return default

    def select_candidates(self, date, pool_data, k_data_map, history_map=None):
        """
        V4.0 选股逻辑：双底结构 (N 字回调)
        """
        candidates = []
        print(f"\n🔍 [{date}] 扫描双底结构强势股 (V4.0)...")

        for item in pool_data:
            code = item['code']
            name = item.get('name', '')

            if not self._is_main_board(code): continue
            if code not in history_map: continue

            hist_rows = history_map[code]
            if len(hist_rows) < 12: continue

            # 数据提取 (倒序：0=今天 T-1, 1=昨天 T-2, ...)
            closes = [self._safe_float(r['close']) for r in hist_rows]
            lows = [self._safe_float(r['low']) for r in hist_rows]
            highs = [self._safe_float(r['high']) for r in hist_rows]
            pct_chgs = [self._safe_float(r.get('pctChg', 0)) for r in hist_rows]

            # --- 1. 基因检查 ---
            has_limit_1m = any(p > 9.5 for p in pct_chgs[:20])
            if not has_limit_1m:
                continue

            # --- 2. 第二次企稳检查 (T-1, T-2) ---
            stable_2d = True
            for i in [0, 1]:
                if pct_chgs[i] < -2.5 or pct_chgs[i] > 1.5:
                    stable_2d = False
                    break
            if not stable_2d:
                continue

            # --- 3. 下杀幅度检查 (3% ~ 5%) ---
            first_base_highs = highs[4:8]
            if not first_base_highs: continue
            peak_after_first_base = max(first_base_highs)

            drop_period_lows = lows[2:5]
            if not drop_period_lows: continue
            bottom_of_drop = min(drop_period_lows)

            if peak_after_first_base == 0: continue
            drop_ratio = (bottom_of_drop - peak_after_first_base) / peak_after_first_base

            # 核心条件：下跌 2.5% ~ 5.5%
            if not (-0.055 <= drop_ratio <= -0.025):
                continue

            # --- 4. 破位检查 ---
            if closes[0] < bottom_of_drop * 0.99:
                continue

            # --- 5. 第一次企稳检查 (T-6 ~ T-8) ---
            first_base_stable = False
            for i in [5, 6, 7]:
                if -1.5 <= pct_chgs[i] <= 1.5:
                    first_base_stable = True
                    break
            if not first_base_stable:
                continue

            current_drawdown = (closes[0] - peak_after_first_base) / peak_after_first_base

            candidates.append({
                'code': code,
                'name': name,
                'reason': f"双底结构 (下杀{drop_ratio:.1%}), 二次企稳",
                'drop_ratio': drop_ratio,
                'current_price': closes[0],
                'drawdown': current_drawdown
            })

        self.yesterday_candidates = candidates
        if candidates:
            print(f"✅ 选中 {len(candidates)} 只双底牛股：{[c['name'] for c in candidates[:5]]}")
        else:
            print("⚠️ 今日无符合双底结构的标的。")
        return [c['code'] for c in candidates]

    def evaluate_realtime_strength(self, code, name, k_row, cand_info):
        if not isinstance(k_row, dict):
            k_row = dict(k_row)

        today_pct = self._safe_float(k_row.get('pctChg'), 0)
        h3_close = self._safe_float(k_row.get('hour3_close_rate'), today_pct)

        reasons = []
        should_buy = False
        score = 0

        if today_pct > self.buy_max_pct:
            return -1, f"涨幅过大 ({today_pct:.1f}%) > {self.buy_max_pct}%, 放弃", False

        if self.h3_buy_range_low <= h3_close <= self.h3_buy_range_high:
            score = 100
            reasons.append(f"H3 企稳 ({h3_close:.1f}%)")
            reasons.append(f"双底确认 ({today_pct:.1f}%)")
            should_buy = True
        else:
            if h3_close < self.h3_buy_range_low:
                return -1, f"H3 走弱 ({h3_close:.1f}%) < {self.h3_buy_range_low}%, 放弃", False
            elif h3_close > self.h3_buy_range_high:
                return -1, f"H3 过强 ({h3_close:.1f}%) > {self.h3_buy_range_high}%, 放弃", False

        return score, "; ".join(reasons), should_buy

    def generate_h3_analysis_report(self, date, candidate_codes, k_data_map, history_map):
        print(f"\n⏰ [{date} 14:00] 生成双底决策报告...")
        print("="*80)

        best_stock = None
        best_score = -1
        best_reason = ""

        for code in candidate_codes:
            if code not in k_data_map: continue
            cand_info = next((c for c in self.yesterday_candidates if c['code'] == code), None)
            if not cand_info: continue

            name = cand_info['name']
            k_row = k_data_map[code]

            score, reason, should_buy = self.evaluate_realtime_strength(code, name, k_row, cand_info)

            status_icon = "❌"
            if should_buy:
                status_icon = "✅"
                if score > best_score:
                    best_score = score
                    best_stock = code
                    best_reason = reason

            h3_val = self._safe_float(k_row.get('hour3_close_rate'), self._safe_float(k_row.get('pctChg'), 0))
            print(f"{status_icon} {code} ({name}): H3={h3_val:+.1f}% | {reason}")

        print("-" * 80)
        if best_stock:
            cand_name = next((c['name'] for c in self.yesterday_candidates if c['code'] == best_stock), "")
            print(f"🚀【最终决策】H4 开盘买入：{best_stock} ({cand_name})")
            print(f"   理由：{best_reason}")
        else:
            print("🛑【最终决策】无符合双底条件的标的，空仓。")
        print("="*80 + "\n")

        return best_stock, best_reason

    def generate_buy_signal(self, date, code, k_row, is_candidate=False):
        if not is_candidate: return False, 0.0, "不在池"

        cand_info = next((c for c in self.yesterday_candidates if c['code'] == code), None)
        if not cand_info: return False, 0.0, "信息缺失"

        score, reason, should_buy = self.evaluate_realtime_strength(code, cand_info['name'], k_row, cand_info)
        if not should_buy:
            return False, 0.0, reason

        h4_open_rate = self._safe_float(k_row.get('hour4_open_rate'), 0)
        if h4_open_rate == 0:
             h4_open_rate = self._safe_float(k_row.get('open_rate'), 0)

        if h4_open_rate > 3.0:
            return False, 0.0, f"H4 开盘过高 ({h4_open_rate:.1f}%)"

        buy_ratio = 1.0 + (h4_open_rate / 100.0)
        return True, buy_ratio, f"双底确认：{reason}"

    def check_sell_condition(self, hold_code, buy_date, buy_price, current_date, current_k_row, profit_rate, days_held):
        if not isinstance(current_k_row, dict):
            current_k_row = dict(current_k_row)

        h1_high = self._safe_float(current_k_row.get('hour1_high_rate'), 0)
        h2_high = self._safe_float(current_k_row.get('hour2_high_rate'), 0)
        h3_high = self._safe_float(current_k_row.get('hour3_high_rate'), 0)
        current_low = self._safe_float(current_k_row.get('low'), 0)

        # 取全天前 3 个小时的最高涨幅
        max_h1h2h3_pct = max(h1_high, h2_high, h3_high)

        # 1. 冲高止盈 (7%)
        if max_h1h2h3_pct >= self.t0_sell_threshold:
            return True, f"H1-H3 冲高 {max_h1h2h3_pct:.1f}% >= {self.t0_sell_threshold}%, 执行止盈"

        # 2. 硬止损 (-4%)
        if current_low > 0 and (current_low - buy_price) / buy_price <= self.stop_loss:
            return True, f"触及硬止损 {self.stop_loss:.1%}"

        # 3. 时间止损 (3 天)
        if days_held >= self.max_hold_days:
            return True, f"达到最大持有天数 ({self.max_hold_days}天)"

        return False, "持有观察 (双底反弹中)"

    def calculate_intraday_profit(self, k_row, buy_price, shares):
        return 0.0, "未开启", []

    def get_candidate_name(self, code):
        target = next((c for c in self.yesterday_candidates if c['code'] == code), None)
        return target['name'] if target else ""

    def select_target(self, date, pool_data, k_data_map):
        return None
