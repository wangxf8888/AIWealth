# backend/app/services/strategies/n_word_rebound.py
from .base_strategy import BaseStrategy
from typing import List, Dict, Tuple, Optional

class NWordReboundStrategy(BaseStrategy):
    def __init__(self, use_core_pool=True):
        self.use_core_pool = use_core_pool
        suffix = "Core" if use_core_pool else "All"
        super().__init__(f"N_Word_Rebound_{suffix}")

        # --- 策略参数 ---
        # T-3：涨停日
        self.limit_up_min_pct = 9.5     # 涨停最低涨幅（9.5%）

        # T-2到T-1：回调期标准
        self.pullback_max_pct = -5.0    # 回调最大跌幅（-5%）

        # T（今天）：买入标准
        self.buy_min_open = -5.0        # 买入开盘下限（-5%）
        self.buy_max_open = 2.0         # 买入开盘上限（2%）

        # 止盈止损
        self.max_hold_days = 3          # 最大持股3天
        self.take_profit_rate = 5.0     # 止盈5%
        self.stop_loss_rate = -3.0      # 止损-3%

        self.yesterday_candidates = []

    def _to_float(self, val, default=0.0):
        if val is None: return default
        try: return float(val)
        except: return default

    def select_candidates(self, date, pool_data, k_data_map, history_map):
        """
        选股逻辑：扫描符合N字反弹形态的股票

        形态要求：
        1. T-3：涨停（≥9.5%）
        2. T-2到T-1：回调≥5%（2天累计跌幅）
        3. T（今天）：等待买入信号（开盘-5%~2%）
        """
        candidates = []
        mode_str = '核心池' if self.use_core_pool else '全市场'
        print(f"\n🔍 [{date}] 扫描【N字反弹】候选股 (模式：{mode_str})...")

        items_to_scan = []
        if self.use_core_pool:
            sorted_pool = sorted(pool_data, key=lambda x: x['code'])
            for item in sorted_pool:
                if item['code'] in k_data_map:
                    items_to_scan.append({'code': item['code'], 'name': item['name'], 'row': k_data_map[item['code']]})
        else:
            for code, row in k_data_map.items():
                items_to_scan.append({'code': code, 'name': 'Unknown', 'row': row})

        print(f"   📊 待检查数量：{len(items_to_scan)}")
        valid_count = 0

        for item in items_to_scan:
            code = item['code']
            hist_list = history_map.get(code, [])

            # 需要至少3天历史数据（T-3, T-2, T-1）
            if len(hist_list) < 3:
                continue

            # hist_list[0] = T-1, hist_list[1] = T-2, hist_list[2] = T-3
            row_t_minus_1 = hist_list[0]   # T-1：回调第2天
            row_t_minus_2 = hist_list[1]   # T-2：回调第1天
            row_t_minus_3 = hist_list[2]   # T-3：涨停日

            # --- 1. 检查T-3：涨停日 ---
            pct_t_minus_3 = self._to_float(row_t_minus_3.get('pctChg'), 0)
            if pct_t_minus_3 < self.limit_up_min_pct:
                continue

            # 记录涨停日的收盘价
            limit_up_close = self._to_float(row_t_minus_3.get('close'), 0)
            if limit_up_close == 0:
                continue

            # --- 2. 检查T-2到T-1：回调期（2天累计跌幅≥5%）---
            close_t_minus_1 = self._to_float(row_t_minus_1.get('close'), 0)  # T-1收盘

            if close_t_minus_1 == 0:
                continue

            # 从涨停收盘到T-1收盘的跌幅
            pullback_pct = (close_t_minus_1 - limit_up_close) / limit_up_close * 100

            if pullback_pct > self.pullback_max_pct:  # 例如：-3% > -5%，不满足（跌幅不够）
                continue

            # --- 3. 检查今天（T）：开盘符合预期 ---
            row_today = item['row']
            open_rate = self._to_float(row_today.get('open_rate'), 999)

            # 开盘不在目标范围，跳过
            if not (self.buy_min_open <= open_rate <= self.buy_max_open):
                continue

            valid_count += 1
            candidates.append({
                'code': code,
                'name': item['name'],
                'limit_up_pct': pct_t_minus_3,
                'pullback_pct': pullback_pct,
                'limit_up_close': limit_up_close,
                'today_open_rate': open_rate
            })

        # 排序：回调越深越好，涨停越强越好
        candidates.sort(key=lambda x: (x['pullback_pct'], -x['limit_up_pct']))
        self.yesterday_candidates = candidates

        if valid_count > 0:
            print(f"   ✅ 发现 {valid_count} 只符合N字反弹形态。")
            print(f"   🔥 首选：{candidates[0]['code']} ({candidates[0]['name']}) [回调{candidates[0]['pullback_pct']:.1f}% | 涨停{candidates[0]['limit_up_pct']:.1f}%]")
            for i, c in enumerate(candidates[:3]):
                print(f"      #{i+1} {c['code']}: 回调{c['pullback_pct']:.1f}% | 涨停{c['limit_up_pct']:.1f}% | 今开{c['today_open_rate']:.1f}%")
        else:
            print(f"   💡 结果：无符合条件标的。")

        return [c['code'] for c in candidates]

    def check_buy_signal(self, date, candidate_codes, k_data_map):
        """
        【标准接口】买入判断：从候选股中选择最佳标的
        返回: (best_code, reason, buy_ratio)
        """
        if not candidate_codes:
            return None, "", 0.0

        best_code = None
        best_reason = ""
        best_buy_ratio = 0.0

        for code in candidate_codes:
            info = next((s for s in self.yesterday_candidates if s['code'] == code), {})
            row_today = k_data_map.get(code, {})
            preclose = self._to_float(row_today.get('preclose'), 0)

            if preclose == 0:
                continue

            h2_open_rate = self._to_float(row_today.get('hour2_open_rate'), 999)
            h4_open_rate = self._to_float(row_today.get('hour4_open_rate'), 999)

            bought = False

            # --- 尝试 H2 买入 ---
            if h2_open_rate != 999:
                if self.buy_min_open <= h2_open_rate <= self.buy_max_open:
                    best_code = code
                    buy_ratio = 1.0 + (h2_open_rate / 100.0)
                    best_buy_ratio = buy_ratio
                    best_reason = f"N字反弹 (涨停{info['limit_up_pct']:.1f}% 回调{info['pullback_pct']:.1f}%) | H2Open:{h2_open_rate:.1f}%"
                    bought = True

            # --- 尝试 H4 买入 ---
            if not bought and h4_open_rate != 999:
                if self.buy_min_open <= h4_open_rate <= self.buy_max_open:
                    best_code = code
                    buy_ratio = 1.0 + (h4_open_rate / 100.0)
                    best_buy_ratio = buy_ratio
                    best_reason = f"N字反弹 (涨停{info['limit_up_pct']:.1f}% 回调{info['pullback_pct']:.1f}%) | H4Open:{h4_open_rate:.1f}%"
                    bought = True

            if bought:
                print(f"\n🚀【选中】{code} -> {best_reason}")
                break

        if not best_code:
            return None, "", 0.0

        return best_code, best_reason, best_buy_ratio

    def generate_buy_signal(self, date, code, k_row, is_candidate=False):
        if not is_candidate: return False, 0.0, "No"
        return False, 0.0, "Logic in Report"

    def check_sell_condition(self, hold_code, buy_price, current_date, current_k_row, profit_rate, days_held, is_last_day=False):
        """
        卖出判断
        【非到期日】检查H1-H4，触发即卖
        【到期日】检查H1-H3，触发用触发价，未触发用H4 Open
        """
        if not isinstance(current_k_row, dict):
            current_k_row = dict(current_k_row)

        preclose = self._to_float(current_k_row.get('preclose'), 0.0)
        if preclose == 0:
            return False, "No PreClose", 0.0

        # 根据是否为到期日，决定检测范围
        if is_last_day:
            check_hours = range(1, 4)  # H1-H3
        else:
            check_hours = range(1, 5)  # H1-H4

        # 收集指定时段的High/Low
        h_highs = []
        h_lows = []
        triggered_hour = 0

        for i in check_hours:
            h_highs.append(self._to_float(current_k_row.get(f'hour{i}_high_rate'), 0))
            h_lows.append(self._to_float(current_k_row.get(f'hour{i}_low_rate'), 0))

        max_h = max(h_highs) if h_highs else 0
        min_l = min(h_lows) if h_lows else 0

        # 止盈：任意时段冲高到阈值
        if max_h >= self.take_profit_rate:
            for i in check_hours:
                h_high = self._to_float(current_k_row.get(f'hour{i}_high_rate'), 0)
                if h_high >= self.take_profit_rate:
                    triggered_hour = i
                    break

            sell_ratio = 1.0 + (self.take_profit_rate * 0.95 / 100)
            return True, f"N字冲高止盈 (H{triggered_hour}{max_h:.1f}%)", sell_ratio

        # 止损：任意时段跌破阈值
        if min_l <= self.stop_loss_rate:
            for i in check_hours:
                h_low = self._to_float(current_k_row.get(f'hour{i}_low_rate'), 0)
                if h_low <= self.stop_loss_rate:
                    triggered_hour = i
                    break

            sell_ratio = 1.0 + (self.stop_loss_rate / 100)
            return True, f"跌破止损 (H{triggered_hour}{min_l:.1f}%)", sell_ratio

        # 时间止损：持有到期
        if is_last_day:
            h4_open_rate = self._to_float(current_k_row.get('hour4_open_rate'), 0)
            if h4_open_rate != 0:
                sell_ratio = 1.0 + (h4_open_rate / 100.0)
                return True, f"N字失效 ({days_held}天, H4Open:{h4_open_rate:.1f}%)", sell_ratio
            else:
                close_r = self._to_float(current_k_row.get('close_rate'), 0)
                return True, f"N字失效 ({days_held}天)", 1.0 + (close_r/100)

        return False, "Hold", 0.0

    def calculate_intraday_profit(self, k_row, buy_price, shares):
        return 0.0, "Disabled", []

    def get_candidate_name(self, code):
        t = next((s for s in self.yesterday_candidates if s['code'] == code), None)
        return t['name'] if t else ""

    def select_target(self, date, pool_data, k_data_map):
        return None
