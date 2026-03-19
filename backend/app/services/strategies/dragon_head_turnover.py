# backend/app/services/strategies/dragon_head_turnover.py
from .base_strategy import BaseStrategy
import random

class DragonHeadTurnoverStrategy(BaseStrategy):
    def __init__(self):
        super().__init__("DragonHead_DeepPullback_V8_Transparent")

        # --- 核心策略参数 ---
        self.buy_max_pct = 2.0          # 买入时当日涨幅不得超过 2%
        self.h3_buy_range_low = -2.0    # H3 收盘价下限
        self.h3_buy_range_high = 2.0    # H3 收盘价上限
        self.t0_sell_threshold = 8.0    # 冲高止盈阈值
        self.stop_loss = -0.05          # 硬止损 -5%
        self.max_hold_days = 4          # 最大持有天数

        self.enable_t0 = False
        self.yesterday_candidates = []

    def _is_main_board(self, code):
        """判断是否为主板"""
        if not code: return False
        code_num = code.split('.')[-1] if '.' in code else code
        if code_num.startswith('300') or code_num.startswith('301'): return False
        if code_num.startswith('688') or code_num.startswith('689'): return False
        return True

    def _safe_float(self, val, default=0.0):
        """安全浮点转换"""
        if val is None: return default
        try: return float(val)
        except: return default

    def select_candidates(self, date, pool_data, k_data_map, history_map=None):
        """
        V8.0 选股逻辑：透明化显示高点日期和价格
        """
        candidates = []

        # 打印表头
        print(f"\n🔍 [{date}] 扫描深度双底候选池 (V8 透明版)...")
        print("-" * 130)
        header = f"{'Rank':<4} | {'Code':<10} | {'Name':<10} | {'高点日期':>10} | {'高点价':>8} | {'现价':>8} | {'回撤%':>8} | {'量比%':>8} | {'评分':>6}"
        print(header)
        print("-" * 130)

        for item in pool_data:
            code = item['code']
            name = item.get('name', '')

            if not self._is_main_board(code): continue
            if code not in history_map: continue

            hist_rows = history_map[code]
            if len(hist_rows) < 15: continue

            # 提取数据 (0=今天 T-1, 1=昨天 T-2, ...)
            dates = [r['date'] for r in hist_rows]
            closes = [self._safe_float(r['close']) for r in hist_rows]
            lows = [self._safe_float(r['low']) for r in hist_rows]
            highs = [self._safe_float(r['high']) for r in hist_rows]
            volumes = [self._safe_float(r['volume']) for r in hist_rows]
            pct_chgs = [self._safe_float(r.get('pctChg', 0)) for r in hist_rows]

            # --- 1. 基因检查 (必须有涨停) ---
            has_limit_1m = any(p > 9.5 for p in pct_chgs[:20])
            if not has_limit_1m: continue

            # --- 2. 当日状态检查 ---
            today_pct = pct_chgs[0]
            if today_pct < -2.5 or today_pct > 2.0: continue

            # --- 3. 动态寻找高点 (核心逻辑) ---
            # 搜索范围：过去第 3 天 到 第 12 天 (避开最近 2 天的波动，寻找之前的峰值)
            search_start_idx = 3
            search_end_idx = min(12, len(highs))

            if search_end_idx <= search_start_idx: continue

            peak_window = highs[search_start_idx:search_end_idx]
            if not peak_window: continue

            recent_peak = max(peak_window)
            # 找到高点的具体索引和日期
            peak_local_index = peak_window.index(recent_peak)
            peak_real_index = search_start_idx + peak_local_index
            peak_date = dates[peak_real_index]

            current_close = closes[0]
            if recent_peak == 0: continue

            drop_ratio = (current_close - recent_peak) / recent_peak

            # 【标准】回撤 6% ~ 15% (确保是深蹲)
            if not (-0.15 <= drop_ratio <= -0.06):
                continue

            # --- 4. 缩量检查 ---
            vol_window = volumes[search_start_idx:search_end_idx]
            if not vol_window or max(vol_window) == 0: continue

            recent_avg_vol = sum(volumes[0:3]) / 3.0
            peak_max_vol = max(vol_window)
            vol_ratio = recent_avg_vol / peak_max_vol

            # 要求缩量到高峰期的 50% 以下
            if vol_ratio > 0.50:
                continue

            # --- 5. 支撑验证 (不破前低) ---
            first_base_lows = lows[12:18]
            if not first_base_lows: continue
            first_base_min = min(first_base_lows)

            if current_close < first_base_min * 0.97:
                continue

            # --- 6. 评分系统 ---
            score = 0
            # 越接近 -8% 回撤分越高
            ideal_drop = -0.08
            score += max(0, 50 - int(abs(drop_ratio - ideal_drop) * 1000))
            # 越接近 30% 量比分越高
            ideal_vol = 0.30
            score += max(0, 50 - int(abs(vol_ratio - ideal_vol) * 1000))

            candidates.append({
                'code': code,
                'name': name,
                'drop_ratio': drop_ratio,
                'vol_ratio': vol_ratio,
                'score': score,
                'peak_date': peak_date,
                'peak_price': recent_peak,
                'current_price': current_close
            })

        # 按评分排序
        candidates.sort(key=lambda x: x['score'], reverse=True)

        # 打印详细信息 (包含高点日期和价格)
        for i, c in enumerate(candidates[:10]):
            row_str = f"{i+1:<4} | {c['code']:<10} | {c['name']:<10} | {c['peak_date']:>10} | {c['peak_price']:>8.2f} | {c['current_price']:>8.2f} | {c['drop_ratio']:>8.1%} | {c['vol_ratio']:>8.1%} | {c['score']:>6}"
            print(row_str)

        if not candidates:
            print("⚠️ 今日无符合【深蹲 + 极致缩量】标准的标的。")
        print("-" * 130)

        self.yesterday_candidates = candidates
        return [c['code'] for c in candidates]

    def evaluate_realtime_strength(self, code, name, k_row, cand_info):
        """盘中实时强度评估"""
        if not isinstance(k_row, dict):
            k_row = dict(k_row)

        today_pct = self._safe_float(k_row.get('pctChg'), 0)
        h3_close = self._safe_float(k_row.get('hour3_close_rate'), today_pct)

        reasons = []
        should_buy = False
        score = 0

        if today_pct > self.buy_max_pct:
            return -1, f"涨幅过大 ({today_pct:.1f}%)", False

        if self.h3_buy_range_low <= h3_close <= self.h3_buy_range_high:
            score = 100
            reasons.append(f"H3 企稳 ({h3_close:.1f}%)")
            reasons.append(f"较{cand_info['peak_date']}高点回撤{cand_info['drop_ratio']:.1%}")
            should_buy = True
        else:
            if h3_close < self.h3_buy_range_low:
                return -1, f"H3 走弱 ({h3_close:.1f}%)", False
            elif h3_close > self.h3_buy_range_high:
                return -1, f"H3 过强 ({h3_close:.1f}%)", False

        return score, "; ".join(reasons), should_buy

    def generate_h3_analysis_report(self, date, candidate_codes, k_data_map, history_map):
        """生成 H3 决策报告"""
        best_stock = None
        best_score = -1
        best_reason = ""

        valid_candidates = [c for c in self.yesterday_candidates if c['code'] in candidate_codes]

        for cand in valid_candidates:
            code = cand['code']
            if code not in k_data_map: continue

            name = cand['name']
            k_row = k_data_map[code]

            score, reason, should_buy = self.evaluate_realtime_strength(code, name, k_row, cand)

            if should_buy:
                if score > best_score:
                    best_score = score
                    best_stock = code
                    best_reason = reason

        if best_stock:
            cand_name = next((c['name'] for c in self.yesterday_candidates if c['code'] == best_stock), "")
            peak_info = next((c for c in self.yesterday_candidates if c['code'] == best_stock), {})
            p_date = peak_info.get('peak_date', '?')
            p_price = peak_info.get('peak_price', 0)

            print(f"\n🚀【最终决策】选中：{best_stock} ({cand_name})")
            print(f"   理由：{best_reason}")
            print(f"   溯源：从 {p_date} 的高点 {p_price:.2f} 回落至此，博取反弹。")
        else:
            print(f"\n🛑【最终决策】候选池中无符合 H3 买入条件的标的，空仓休息。")

        return best_stock, best_reason

    def generate_buy_signal(self, date, code, k_row, is_candidate=False):
        """生成买入信号"""
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
        return True, buy_ratio, f"深蹲反转：{reason}"

    def check_sell_condition(self, hold_code, buy_date, buy_price, current_date, current_k_row, profit_rate, days_held):
        """卖出条件判断"""
        if not isinstance(current_k_row, dict):
            current_k_row = dict(current_k_row)

        h1_high = self._safe_float(current_k_row.get('hour1_high_rate'), 0)
        h2_high = self._safe_float(current_k_row.get('hour2_high_rate'), 0)
        h3_high = self._safe_float(current_k_row.get('hour3_high_rate'), 0)
        current_low = self._safe_float(current_k_row.get('low'), 0)
        max_h1h2h3_pct = max(h1_high, h2_high, h3_high)

        if max_h1h2h3_pct >= self.t0_sell_threshold:
            return True, f"冲高 {max_h1h2h3_pct:.1f}% 止盈"
        if current_low > 0 and (current_low - buy_price) / buy_price <= self.stop_loss:
            return True, f"触及硬止损 {self.stop_loss:.1%}"
        if days_held >= self.max_hold_days:
            return True, f"时间止损 ({self.max_hold_days}天)"
        return False, "持有观察"

    def calculate_intraday_profit(self, k_row, buy_price, shares):
        return 0.0, "未开启", []

    def get_candidate_name(self, code):
        target = next((c for c in self.yesterday_candidates if c['code'] == code), None)
        return target['name'] if target else ""

    def select_target(self, date, pool_data, k_data_map):
        return None
