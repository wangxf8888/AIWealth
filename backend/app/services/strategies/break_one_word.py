# backend/app/services/strategies/break_one_word.py
from .base_strategy import BaseStrategy

class BreakOneWordStrategy(BaseStrategy):
    def __init__(self, use_core_pool=True):
        self.use_core_pool = use_core_pool
        suffix = "Core" if use_core_pool else "All"
        super().__init__(f"BreakOneWord_Fixed_{suffix}")

        self.buy_target_rate = -2.0
        self.buy_max_drop_limit = -10.0
        self.min_open_rate = 9.0
        self.limit_tolerance = 0.1  # 涨跌幅比较，允许 0.1% 的误差 (因为数据源可能有微小精度差异)

        self.max_hold_days = 1
        self.take_profit_rate = 5.0

        self.yesterday_candidates = []

    def _to_float(self, val, default=0.0):
        if val is None: return default
        if isinstance(val, (int, float)): return float(val)
        if isinstance(val, str):
            val = val.strip()
            if not val: return default
            try: return float(val)
            except ValueError: return default
        return default

    def get_theoretical_limit_rate(self, code):
        """
        【核心修复】获取理论涨停涨跌幅 (%)
        而不是计算涨停价格
        """
        code_str = str(code)
        num_part = code_str.split('.')[-1] if '.' in code_str else code_str

        # 创业板 (300/301)、科创板 (688) 为 20%，其他 10%
        # 注意：ST 股是 5%，这里暂不做特殊处理，如需精确可加判断
        if num_part.startswith('3') or num_part.startswith('68'):
            return 20.0
        else:
            return 10.0

    def is_strict_one_word(self, row, preclose, code):
        """
        严格判断一字板：
        比较的是【理论涨停涨跌幅】与【实际小时线涨跌幅】
        """
        if not preclose: return False

        # 获取理论涨停幅度 (例如 10.0 或 20.0)
        theoretical_limit_rate = self.get_theoretical_limit_rate(code)

        hours = ['hour1', 'hour2', 'hour3', 'hour4']
        metrics = ['_open_rate', '_close_rate', '_high_rate', '_low_rate']

        for h in hours:
            for m in metrics:
                key = f"{h}{m}"
                # 从数据库获取实际的涨跌幅 (%)
                actual_rate = self._to_float(row.get(key), -999.0)

                # 如果数据缺失
                if actual_rate == -999.0:
                    return False

                # 【核心修复】比较两个百分比数值
                # 例如：理论 10.0% vs 实际 9.98%
                if abs(actual_rate - theoretical_limit_rate) > self.limit_tolerance:
                    return False

        return True

    def select_candidates(self, date, pool_data, k_data_map, history_map=None):
        candidates = []
        mode_str = '核心池' if self.use_core_pool else '全市场'
        print(f"\n🔍 [{date}] 扫描【严格一字板】 (模式：{mode_str})...")

        valid_count = 0
        items_to_scan = []

        if self.use_core_pool:
            core_codes = set([item['code'] for item in pool_data])
            core_name_map = {item['code']: item['name'] for item in pool_data}
            for code in core_codes:
                if code in k_data_map:
                    items_to_scan.append({'code': code, 'name': core_name_map[code], 'row': k_data_map[code]})
        else:
            for code, row in k_data_map.items():
                name = "Unknown"
                for item in pool_data:
                    if item['code'] == code:
                        name = item['name']
                        break
                items_to_scan.append({'code': code, 'name': name, 'row': row})

        print(f"   📊 待检查数量：{len(items_to_scan)}")

        for item in items_to_scan:
            code = item['code']
            row_yesterday = item['row']
            preclose = self._to_float(row_yesterday.get('preclose'), 0.0)
            if preclose == 0: continue

            if not self.is_strict_one_word(row_yesterday, preclose, code):
                continue

            valid_count += 1
            # 计算昨天的实际涨停价用于后续参考 (可选)
            limit_rate = self.get_theoretical_limit_rate(code)
            limit_price = round(preclose * (1 + limit_rate/100.0), 2)

            candidates.append({
                'code': code,
                'name': item['name'],
                'limit_price': limit_price,
                'preclose': preclose
            })

        self.yesterday_candidates = candidates

        if valid_count > 0:
            print(f"   ✅ 发现 {valid_count} 只严格一字板。重点：{[c['code'] for c in candidates[:5]]}")
        else:
            status = "无符合定义的一字板" if len(items_to_scan) > 0 else "无数据可查"
            print(f"   💡 结果：{status}")

        return [c['code'] for c in candidates]

    def evaluate_buy_signal(self, code, k_row_today, candidate_info):
        if not isinstance(k_row_today, dict): k_row_today = dict(k_row_today)
        preclose = self._to_float(candidate_info.get('preclose'), 0.0)
        if preclose == 0: preclose = self._to_float(k_row_today.get('preclose'), 0.0)
        if preclose == 0: return False, "缺少昨收", 0.0

        h1_open = self._to_float(k_row_today.get('hour1_open_rate'), 0.0)
        if h1_open < self.min_open_rate: return False, f"开盘不强 ({h1_open:.1f}%)", 0.0

        h1_low = self._to_float(k_row_today.get('hour1_low_rate'), 100.0)
        h2_low = self._to_float(k_row_today.get('hour2_low_rate'), 100.0)
        min_low_rate = min(h1_low, h2_low)

        if min_low_rate >= 5.0: return False, f"未有效破板 (Low:{min_low_rate:.1f}%)", 0.0
        if min_low_rate > self.buy_target_rate: return False, f"回落不够 (Low:{min_low_rate:.1f}%)", 0.0
        if min_low_rate < self.buy_max_drop_limit: return False, f"抛压过大 (Low:{min_low_rate:.1f}%)", 0.0

        return True, f"一字开板低吸 (Open:{h1_open:.1f}% Low:{min_low_rate:.1f}%)", 1.0 + (self.buy_target_rate / 100.0)

    def generate_h3_analysis_report(self, date, candidate_codes, k_data_map, history_map):
        best_stock = None
        best_reason = ""
        best_buy_ratio = 0.0
        valid_pool = set(candidate_codes) & set([s['code'] for s in self.yesterday_candidates])

        for code in valid_pool:
            if code not in k_data_map: continue
            info = next((s for s in self.yesterday_candidates if s['code'] == code), {})
            should_buy, reason, ratio = self.evaluate_buy_signal(code, k_data_map[code], info)
            if should_buy:
                best_stock = code
                best_reason = reason
                best_buy_ratio = ratio
                print(f"\n🚀【选中】{code} ({info['name']}) -> {reason}")
                break

        if not best_stock:
            print(f"\n🛑【空仓】无符合条件标的。")
        return best_stock, best_reason

    def generate_buy_signal(self, date, code, k_row, is_candidate=False):
        if not is_candidate: return False, 0.0, "No"
        info = next((s for s in self.yesterday_candidates if s['code'] == code), {})
        ok, reason, ratio = self.evaluate_buy_signal(code, k_row, info)
        return (True, ratio, reason) if ok else (False, 0.0, reason)

    def check_sell_condition(self, hold_code, buy_price, current_date, current_k_row, profit_rate, days_held):
        if not isinstance(current_k_row, dict): current_k_row = dict(current_k_row)
        if days_held >= 1:
            h_rates = []
            for i in range(1, 5):
                h_rates.extend([
                    self._to_float(current_k_row.get(f'hour{i}_high_rate'), 0.0),
                    self._to_float(current_k_row.get(f'hour{i}_low_rate'), 0.0)
                ])
            if not h_rates: return True, "Data Error", 1.0
            max_h, min_l = max(h_rates), min(h_rates)
            if max_h >= self.take_profit_rate: return True, f"冲高止盈 ({max_h:.1f}%)", 1.0 + (max_h * 0.95 / 100.0)
            if min_l <= -5.0: return True, f"急跌止损 ({min_l:.1f}%)", 1.0 + (min_l / 100.0)
            close_r = self._to_float(current_k_row.get('close_rate'), 0.0)
            return True, f"T+1 离场", 1.0 + (close_r/100)
        return False, "Hold", 0.0

    def calculate_intraday_profit(self, k_row, buy_price, shares): return 0.0, "Disabled", []
    def get_candidate_name(self, code):
        t = next((s for s in self.yesterday_candidates if s['code'] == code), None)
        return t['name'] if t else ""
    def select_target(self, date, pool_data, k_data_map): return None
