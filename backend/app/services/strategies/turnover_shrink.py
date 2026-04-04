# backend/app/services/strategies/turnover_shrink.py
from .base_strategy import BaseStrategy

class TurnoverShrinkStrategy(BaseStrategy):
    def __init__(self, use_core_pool=True):
        self.use_core_pool = use_core_pool
        suffix = "Core" if use_core_pool else "All"
        super().__init__(f"Turnover_Shrink_{suffix}")

        # --- 策略参数 ---
        self.vol_increase_threshold = 0.25   # T-2 比 T-3 增长 25%
        self.vol_shrink_threshold = 0.25     # T-1 比 T-2 缩小 25%
        self.open_range_min = -2.0
        self.open_range_max = 2.0
        self.h1_close_range_min = -2.0
        self.h1_close_range_max = 2.0

        self.max_hold_days = 2
        self.take_profit_rate = 3.0
        self.stop_loss_rate = -2.0

        # 【新增】H2 买入条件
        self.h2_buy_min_open = -2.0
        self.h2_buy_max_open = 4.0

        # 【新增】H4 买入条件 (允许下午补票)
        # 默认与 H2 一致，也可以设置得更宽泛或更严格
        self.h4_buy_min_open = -2.0
        self.h4_buy_max_open = 3.0

        self.yesterday_candidates = []

    def _is_main_board(self, code):
        if not code: return False
        code_num = code.split('.')[-1] if '.' in code else code
        if code_num.startswith('300') or code_num.startswith('301'): return False
        if code_num.startswith('688') or code_num.startswith('689'): return False
        return True

    def _to_float(self, val, default=0.0):
        if val is None: return default
        if isinstance(val, (int, float)): return float(val)
        if isinstance(val, str):
            val = val.strip()
            if not val: return default
            try: return float(val)
            except ValueError: return default
        return default

    def select_candidates(self, date, pool_data, k_data_map, history_map):
        candidates = []
        mode_str = '核心池' if self.use_core_pool else '全市场'
        print(f"\n🔍 [{date}] 扫描【缩量回调】候选股 (模式：{mode_str})...")

        valid_count = 0
        items_to_scan = []

        # 【修复 1】如果来自 core_pool，先将 pool_data 转为列表并保持原有顺序或按代码排序
        if self.use_core_pool:
            sorted_pool = sorted(pool_data, key=lambda x: x['code'])
            for item in sorted_pool:
                code = item['code']
                if code in k_data_map:
                    items_to_scan.append({'code': code, 'name': item['name'], 'row': k_data_map[code]})
        else:
            sorted_codes = sorted(k_data_map.keys())
            for code in sorted_codes:
                row = k_data_map[code]
                name = "Unknown"
                for item in pool_data:
                    if item['code'] == code:
                        name = item['name']
                        break
                items_to_scan.append({'code': code, 'name': name, 'row': row})

        print(f"   📊 待检查数量：{len(items_to_scan)}")

        for item in items_to_scan:
            code = item['code']
            row_today = item['row']

            if not self._is_main_board(code): continue

            # 2. 检查 T 日全天开盘范围 (作为初步筛选)
            open_rate = self._to_float(row_today.get('open_rate'), 999.0)
            if open_rate == 999.0:
                preclose = self._to_float(row_today.get('preclose'), 0.0)
                open_p = self._to_float(row_today.get('open'), 0.0)
                if preclose > 0:
                    open_rate = (open_p - preclose) / preclose * 100
                else:
                    continue

            # 初步筛选放宽一点，只要 H2 或 H4 有一个满足即可，或者直接用全天开盘做粗筛
            # 这里保持原逻辑，用全天开盘做第一道过滤
            if not (self.open_range_min <= open_rate <= self.open_range_max):
                continue

            # 3. 检查 H1 收盘范围
            h1_close_rate = self._to_float(row_today.get('hour1_close_rate'), 999.0)
            if h1_close_rate == 999.0: continue
            if not (self.h1_close_range_min <= h1_close_rate <= self.h1_close_range_max):
                continue

            # 4. 获取历史数据
            hist_list = history_map.get(code, [])
            if len(hist_list) < 3: continue

            row_t_minus_1 = hist_list[0]
            row_t_minus_2 = hist_list[1]
            row_t_minus_3 = hist_list[2]

            turn_1 = self._to_float(row_t_minus_1.get('turn'), -1.0)
            turn_2 = self._to_float(row_t_minus_2.get('turn'), -1.0)
            turn_3 = self._to_float(row_t_minus_3.get('turn'), -1.0)

            if turn_1 < 0 or turn_2 < 0 or turn_3 < 0: continue
            if turn_2 == 0 or turn_3 == 0: continue

            # 5. 换手率条件计算
            ratio_inc = (turn_2 - turn_3) / turn_3
            if ratio_inc < self.vol_increase_threshold:
                continue

            ratio_shrink = (turn_2 - turn_1) / turn_2
            if ratio_shrink < self.vol_shrink_threshold:
                continue

            valid_count += 1
            candidates.append({
                'code': code,
                'name': item['name'],
                'turn_1': turn_1,
                'turn_2': turn_2,
                'turn_3': turn_3,
                'open_rate': open_rate,
                'ratio_shrink': ratio_shrink,
                'ratio_inc': ratio_inc
            })

        # 【修复 2】核心排序逻辑
        candidates.sort(key=lambda x: (-x['ratio_shrink'], -x['ratio_inc'], x['code']))

        self.yesterday_candidates = candidates

        if valid_count > 0:
            print(f"   ✅ 发现 {valid_count} 只符合缩量回调形态。已按缩量程度排序。")
            print(f"   🔥 首选标的：{candidates[0]['code']} ({candidates[0]['name']}) [缩量:{candidates[0]['ratio_shrink']:.1%}]")
            for i, c in enumerate(candidates[:3]):
                print(f"      #{i+1} {c['code']}: 缩{c['ratio_shrink']:.1%} | 放{c['ratio_inc']:.1%} | Open:{c['open_rate']:.1f}%")
        else:
            print(f"   💡 结果：无符合条件标的。")

        return [c['code'] for c in candidates]

    def generate_h3_analysis_report(self, date, candidate_codes, k_data_map, history_map):
        """
        【核心修改】支持 H2 和 H4 双重买入时机
        逻辑：
        1. 优先尝试 H2 买入。
        2. 如果 H2 不满足，尝试 H4 买入（作为补票机会）。
        返回格式：(code, reason, buy_ratio)
        注意：具体的资金校验由 engine 完成，这里只需返回“如果资金允许，我想在这个价位买”的信号。
        """
        if not candidate_codes:
            print(f"\n🛑【空仓】无候选股。")
            return None, "", 0.0

        best_code = None
        best_reason = ""
        best_buy_ratio = 0.0
        selected_hour = 0 # 记录选中的时段，用于 debug 或后续扩展

        # 因为传入的 candidate_codes 已经是排序好的，直接遍历
        for code in candidate_codes:
            info = next((s for s in self.yesterday_candidates if s['code'] == code), {})
            row_today = k_data_map.get(code, {})

            preclose = self._to_float(row_today.get('preclose'), 0.0)
            if preclose == 0: continue

            # 获取 H2 和 H4 的开盘涨幅
            h2_open_rate = self._to_float(row_today.get('hour2_open_rate'), 999.0)
            h4_open_rate = self._to_float(row_today.get('hour4_open_rate'), 999.0)

            # 如果没有 H4 数据（某些老数据可能缺失），用 H3 收盘或 H4 收盘近似，或者直接跳过 H4 逻辑
            # 这里假设数据完整，必须显式有 hour4_open_rate
            if h4_open_rate == 999.0:
                # 尝试用 hour4_close_rate 或者 hour3_close_rate 作为备选？
                # 严格来说，H4 Open 是 13:00 的价格。如果缺失，我们保守起见只检查 H2
                h4_open_rate = None

            bought = False

            # --- 尝试 1: H2 买入 ---
            if h2_open_rate != 999.0:
                if self.h2_buy_min_open <= h2_open_rate <= self.h2_buy_max_open:
                    best_code = code
                    buy_price = preclose * (1 + h2_open_rate / 100.0)
                    best_buy_ratio = buy_price / preclose
                    best_reason = f"缩量回调 ({info['turn_2']:.2f}%-> {info['turn_1']:.2f}%) | H2Open:{h2_open_rate:.1f}%"
                    selected_hour = 2
                    bought = True

            # --- 尝试 2: H4 买入 (如果 H2 没买成，且 H4 数据存在且符合) ---
            if not bought and h4_open_rate is not None:
                if self.h4_buy_min_open <= h4_open_rate <= self.h4_buy_max_open:
                    best_code = code
                    buy_price = preclose * (1 + h4_open_rate / 100.0)
                    best_buy_ratio = buy_price / preclose
                    best_reason = f"缩量回调 ({info['turn_2']:.2f}%-> {info['turn_1']:.2f}%) | H4Open:{h4_open_rate:.1f}%"
                    selected_hour = 4
                    bought = True

            if bought:
                print(f"\n🚀【选中】{code} ({info['name']}) -> {best_reason} (时段:H{selected_hour})")
                break # 找到一只就停止

        if not best_code:
            if len(candidate_codes) > 0:
                print(f"\n🛑【空仓】所有候选股 H2/H4 开盘均不符合条件。")
            else:
                print(f"\n🛑【空仓】无候选股。")
            return None, "", 0.0

        return best_code, best_reason, best_buy_ratio

    def generate_buy_signal(self, date, code, k_row, is_candidate=False):
        """
        通用买入信号生成，支持 H2 或 H4
        """
        if not is_candidate: return False, 0.0, "No"

        preclose = self._to_float(k_row.get('preclose'), 0.0)
        if preclose == 0: return False, 0.0, "No PreClose"

        h2_open_rate = self._to_float(k_row.get('hour2_open_rate'), 999.0)
        h4_open_rate = self._to_float(k_row.get('hour4_open_rate'), 999.0)

        chosen_rate = None
        chosen_hour = 0

        # 优先 H2
        if self.h2_buy_min_open <= h2_open_rate <= self.h2_buy_max_open:
            chosen_rate = h2_open_rate
            chosen_hour = 2
        # 其次 H4
        elif h4_open_rate != 999.0 and self.h4_buy_min_open <= h4_open_rate <= self.h4_buy_max_open:
            chosen_rate = h4_open_rate
            chosen_hour = 4

        if chosen_rate is None:
            return False, 0.0, f"H2/H4 Open out of range (H2:{h2_open_rate:.1f}%, H4:{h4_open_rate if h4_open_rate else 'N/A'}%)"

        buy_ratio = 1.0 + (chosen_rate / 100.0)
        reason = f"缩量回调低吸 (H{chosen_hour}:{chosen_rate:.1f}%)"
        return True, buy_ratio, reason

    def check_sell_condition(self, hold_code, buy_price, current_date, current_k_row, profit_rate, days_held):
        if not isinstance(current_k_row, dict):
            current_k_row = dict(current_k_row)
        h_rates = []
        for i in range(1, 5):
            h_high = self._to_float(current_k_row.get(f'hour{i}_high_rate'), 0.0)
            h_low = self._to_float(current_k_row.get(f'hour{i}_low_rate'), 0.0)
            h_rates.extend([h_high, h_low])

        if h_rates:
            max_h = max(h_rates)
            min_l = min(h_rates)
            if max_h >= self.take_profit_rate:
                sell_ratio = 1.0 + (self.take_profit_rate * 0.95 / 100.0)
                return True, f"止盈 ({max_h:.1f}%)", sell_ratio
            if min_l <= self.stop_loss_rate:
                sell_ratio = 1.0 + (self.stop_loss_rate / 100.0)
                return True, f"止损 ({min_l:.1f}%)", sell_ratio

        if days_held >= self.max_hold_days:
            close_r = self._to_float(current_k_row.get('close_rate'), 0.0)
            return True, f"时间到 ({days_held}天)", 1.0 + (close_r/100)
        return False, "Hold", 0.0

    def calculate_intraday_profit(self, k_row, buy_price, shares):
        return 0.0, "Disabled", []

    def get_candidate_name(self, code):
        t = next((s for s in self.yesterday_candidates if s['code'] == code), None)
        return t['name'] if t else ""

    def select_target(self, date, pool_data, k_data_map):
        return None
