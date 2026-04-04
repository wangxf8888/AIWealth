# backend/app/services/strategies/n_word_rebound.py
from .base_strategy import BaseStrategy

class NWordReboundStrategy(BaseStrategy):
    def __init__(self, use_core_pool=True):
        self.use_core_pool = use_core_pool
        suffix = "Core" if use_core_pool else "All"
        super().__init__(f"N_Word_Rebound_{suffix}")

        # --- 策略参数 ---
        # T-2: 第一天强势标准
        self.day1_min_pct = 5.0       # 最小涨幅 5%
        self.day1_is_limit_up = False # 是否强制要求涨停 (True 则必须涨停)

        # T-1: 第二天回调标准
        self.day2_max_pct = 1.0       # 最大涨幅 (超过则视为继续强攻，非回调)
        self.day2_min_pct = -4.0      # 最小跌幅 (跌太深说明走弱)
        self.vol_shrink_ratio = 0.8   # 缩量比例：今日换手 < 昨日换手 * 0.8
        self.break_mid_line = False   # 是否允许跌破第一天实体中位线 (False=严格不破)

        # T: 今天买入标准
        self.buy_break_yesterday_high = True # 必须突破昨天高点
        self.h2_buy_min_open = -2.0   # H2 开盘下限
        self.h2_buy_max_open = 3.0    # H2 开盘上限 (避免高开太多追高)

        # 止盈止损
        self.max_hold_days = 3
        self.take_profit_rate = 6.0
        self.stop_loss_rate = -3.0
        self.ma5_stop_loss = True     # 跌破 5 日线止损

        self.yesterday_candidates = []

    def _to_float(self, val, default=0.0):
        if val is None: return default
        try: return float(val)
        except: return default

    def _calc_ma5(self, history_list):
        """计算简单的 5 日均线 (收盘价)"""
        if len(history_list) < 5: return None
        closes = [self._to_float(h.get('close'), 0) for h in history_list[:5]]
        if 0 in closes: return None
        return sum(closes) / 5

    def select_candidates(self, date, pool_data, k_data_map, history_map):
        candidates = []
        mode_str = '核心池' if self.use_core_pool else '全市场'
        print(f"\n🔍 [{date}] 扫描【N 字反包】候选股 (模式：{mode_str})...")

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
            row_today = item['row'] # T 日数据
            hist_list = history_map.get(code, [])

            # 需要至少 3 天数据 (T, T-1, T-2) + 5 天算 MA5
            if len(hist_list) < 5: continue

            row_t_minus_1 = hist_list[0] # T-1
            row_t_minus_2 = hist_list[1] # T-2
            row_t_minus_3 = hist_list[2] # T-3 (用于参考)

            # --- 1. 检查 T-2 (第一天): 大阳线/涨停 ---
            pct_2 = self._to_float(row_t_minus_2.get('pctChg'), 0)
            open_2 = self._to_float(row_t_minus_2.get('open'), 0)
            close_2 = self._to_float(row_t_minus_2.get('close'), 0)

            if pct_2 < self.day1_min_pct: continue
            if close_2 <= open_2: continue # 必须是阳线

            # 计算第一天实体中位线
            mid_point_2 = (open_2 + close_2) / 2

            # --- 2. 检查 T-1 (第二天): 缩量回调 ---
            pct_1 = self._to_float(row_t_minus_1.get('pctChg'), 0)
            low_1 = self._to_float(row_t_minus_1.get('low'), 0)
            turn_1 = self._to_float(row_t_minus_1.get('turn'), 0)
            turn_2 = self._to_float(row_t_minus_2.get('turn'), 0)

            # 2.1 涨跌幅范围
            if not (self.day2_min_pct <= pct_1 <= self.day2_max_pct): continue

            # 2.2 缩量检查 (关键！)
            if turn_2 == 0 or turn_1 > (turn_2 * self.vol_shrink_ratio):
                continue # 没有缩量，或者放量了

            # 2.3 支撑位检查 (不破中位线 & 不破 5 日线)
            if self.break_mid_line == False and low_1 < mid_point_2:
                continue # 跌破了第一天实体中位

            # 检查 5 日线
            ma5 = self._calc_ma5(hist_list)
            if ma5 and low_1 < ma5 * 0.98: # 允许轻微击穿，但不能太深
                continue

            # --- 3. 检查 T (今天): 初步筛选 (开盘符合预期) ---
            open_rate = self._to_float(row_today.get('open_rate'), 999)
            if not (self.h2_buy_min_open <= open_rate <= self.h2_buy_max_open):
                continue

            valid_count += 1
            candidates.append({
                'code': code,
                'name': item['name'],
                'day1_pct': pct_2,
                'day2_pct': pct_1,
                'shrink_ratio': turn_1 / turn_2 if turn_2 > 0 else 0,
                'ma5': ma5,
                'yesterday_high': self._to_float(row_t_minus_1.get('high'), 0)
            })

        # 排序：缩量越明显越好，第一天越强越好
        candidates.sort(key=lambda x: (x['shrink_ratio'], -x['day1_pct']))
        self.yesterday_candidates = candidates

        if valid_count > 0:
            print(f"   ✅ 发现 {valid_count} 只符合 N 字反包形态。")
            print(f"   🔥 首选：{candidates[0]['code']} ({candidates[0]['name']}) [缩{candidates[0]['shrink_ratio']:.2f} | D1:{candidates[0]['day1_pct']:.1f}%]")
            for i, c in enumerate(candidates[:3]):
                print(f"      #{i+1} {c['code']}: 缩{c['shrink_ratio']:.2f} | D1:{c['day1_pct']:.1f}% | D2:{c['day2_pct']:.1f}%")
        else:
            print(f"   💡 结果：无符合条件标的。")

        return [c['code'] for c in candidates]

    def generate_h3_analysis_report(self, date, candidate_codes, k_data_map, history_map):
        if not candidate_codes:
            return None, "", 0.0

        best_code = None
        best_reason = ""
        best_buy_ratio = 0.0
        selected_hour = 0

        for code in candidate_codes:
            info = next((s for s in self.yesterday_candidates if s['code'] == code), {})
            row_today = k_data_map.get(code, {})
            preclose = self._to_float(row_today.get('preclose'), 0)
            if preclose == 0: continue

            yesterday_high = info['yesterday_high']
            target_price = yesterday_high * 1.005 # 突破价略高于昨日高点

            h2_open_rate = self._to_float(row_today.get('hour2_open_rate'), 999)
            h2_high_rate = self._to_float(row_today.get('hour2_high_rate'), -999)
            h4_open_rate = self._to_float(row_today.get('hour4_open_rate'), 999)
            h4_high_rate = self._to_float(row_today.get('hour4_high_rate'), -999)

            bought = False

            # --- 尝试 H2 买入 (突破确认) ---
            # 条件：开盘符合 + H2 最高价已经突破昨日高点
            if h2_open_rate != 999 and h2_high_rate != -999:
                if self.h2_buy_min_open <= h2_open_rate <= self.h2_buy_max_open:
                    if h2_high_rate >= (self._to_float(row_today.get('hour2_open_rate'), 0) * 0.0 + (yesterday_high/preclose-1)*100):
                        # 简化判断：只要 H2 最高涨幅对应的价格 > 昨日高价
                        current_h2_high_price = preclose * (1 + h2_high_rate/100)
                        if current_h2_high_price >= yesterday_high:
                            best_code = code
                            buy_price = preclose * (1 + h2_open_rate/100) # 按开盘或现价买
                            best_buy_ratio = buy_price / preclose
                            best_reason = f"N 字反包 (D1:{info['day1_pct']:.1f}% D2:缩{info['shrink_ratio']:.2f}) | 突破昨日高"
                            selected_hour = 2
                            bought = True

            # --- 尝试 H4 买入 (尾盘确认) ---
            if not bought and h4_open_rate != 999:
                 if self.h2_buy_min_open <= h4_open_rate <= self.h2_buy_max_open: # 复用开盘范围逻辑，或者单独设 H4 范围
                    current_h4_high_price = preclose * (1 + h4_high_rate/100) if h4_high_rate != -999 else preclose * (1 + h4_open_rate/100)
                    if current_h4_high_price >= yesterday_high:
                        best_code = code
                        buy_price = preclose * (1 + h4_open_rate/100)
                        best_buy_ratio = buy_price / preclose
                        best_reason = f"N 字反包 (D1:{info['day1_pct']:.1f}% D2:缩{info['shrink_ratio']:.2f}) | H4 突破"
                        selected_hour = 4
                        bought = True

            if bought:
                print(f"\n🚀【选中】{code} -> {best_reason} (时段:H{selected_hour})")
                break

        if not best_code:
            return None, "", 0.0

        return best_code, best_reason, best_buy_ratio

    def generate_buy_signal(self, date, code, k_row, is_candidate=False):
        # 逻辑同上，用于实盘信号生成
        if not is_candidate: return False, 0.0, "No"
        # ... (省略重复代码，逻辑同 generate_h3_analysis_report) ...
        # 为简洁起见，实际项目中应提取公共方法
        return False, 0.0, "Logic in Report"

    def check_sell_condition(self, hold_code, buy_price, current_date, current_k_row, profit_rate, days_held):
        if not isinstance(current_k_row, dict):
            current_k_row = dict(current_k_row)

        # 1. 获取当前价格和均线
        current_close = self._to_float(current_k_row.get('close'), 0)
        current_low = min([self._to_float(current_k_row.get(f'hour{i}_low_rate'), 0) for i in range(1,5)])

        # 简单估算 MA5 (实际需要传入历史数据，这里简化处理，假设策略外部维护或仅用收盘价估)
        # 在真实引擎中，这里应该能访问到该股票的历史列表来计算实时 MA5
        # 此处暂用固定逻辑：如果利润回撤严重也卖
        close_rate = self._to_float(current_k_row.get('close_rate'), 0)

        # 2. 止盈止损
        max_h = max([self._to_float(current_k_row.get(f'hour{i}_high_rate'), 0) for i in range(1,5)])
        min_l = min([self._to_float(current_k_row.get(f'hour{i}_low_rate'), 0) for i in range(1,5)])

        if max_h >= self.take_profit_rate:
            return True, f"N 字冲高止盈 ({max_h:.1f}%)", 1.0 + (self.take_profit_rate * 0.95 / 100)

        if min_l <= self.stop_loss_rate:
            return True, f"跌破止损 ({min_l:.1f}%)", 1.0 + (self.stop_loss_rate / 100)

        # 3. 时间止损 (N 字失败，通常 3 天不涨就是弱)
        if days_held >= self.max_hold_days:
            return True, f"N 字失效 ({days_held}天)", 1.0 + (close_rate/100)

        # 4. 特殊逻辑：如果买入后当天就跌破昨日低点 (N 字结构破坏)
        # 需要传入昨日低点数据，此处简化

        return False, "Hold", 0.0

    def calculate_intraday_profit(self, k_row, buy_price, shares):
        return 0.0, "Disabled", []

    def get_candidate_name(self, code):
        t = next((s for s in self.yesterday_candidates if s['code'] == code), None)
        return t['name'] if t else ""

    def select_target(self, date, pool_data, k_data_map):
        return None
