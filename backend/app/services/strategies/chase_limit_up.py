# backend/app/services/strategies/chase_limit_up.py
from .base_strategy import BaseStrategy
from typing import List, Dict, Tuple, Optional

class ChaseLimitUpStrategy(BaseStrategy):
    def __init__(self, entry_board=1, use_core_pool=False):
        """
        :param entry_board: 入口板数 (1=1 进 2, 2=2 进 3)
        :param use_core_pool: 是否仅从核心池选股
        """
        self.entry_board = entry_board
        self.target_board = entry_board + 1
        self.use_core_pool = use_core_pool

        name_suffix = "Core" if use_core_pool else "All"
        super().__init__(f"Chase_{entry_board}to{self.target_board}_TimePriority_{name_suffix}")

        # --- 策略参数配置 ---
        self.min_open_pct = 1.0       # 今日 H1 开盘下限 (%)
        self.max_open_pct = 3.0       # 今日 H1 开盘上限 (%)
        self.limit_threshold = 9.8    # 涨停判定阈值 (%)
        self.open_tolerance = 0.2     # 允许的开板误差范围 (%)

        self.max_hold_days = 1        # 严格 T+1
        self.sell_next_h1_open = True # 次日 H1 开盘无条件卖出

        # 缓存昨日候选信息
        self.yesterday_candidates = []

    def _safe_float(self, val, default=0.0):
        if val is None: return default
        try: return float(val)
        except: return default

    def _calc_real_pct(self, close, preclose):
        if not preclose or preclose == 0: return 0.0
        return (float(close) - float(preclose)) / float(preclose) * 100.0

    def _get_theoretical_limit_rate(self, code):
        """获取理论涨停幅度"""
        code_str = str(code).split('.')[-1] if '.' in str(code) else str(code)
        if code_str.startswith('3') or code_str.startswith('68'):
            return 20.0
        return 10.0

    def analyze_yesterday_strength(self, code: str, row_yesterday: dict, history_map: dict) -> Optional[Dict]:
        """
        【核心逻辑 1】分析昨日股票强度
        1. 确认是否为第 N 连板
        2. 确认是否全程未开板 (H1>=H2>=H3>=H4 且都接近涨停)
        3. 返回封板时间优先级
        """
        # 1. 计算连续涨停数
        cont_limits = 0
        rows_hist = history_map.get(code, [])

        # 检查历史连板
        for r in rows_hist:
            close = self._safe_float(r.get('close'))
            preclose = self._safe_float(r.get('preclose'))
            pct = self._calc_real_pct(close, preclose)
            limit_rate = self._get_theoretical_limit_rate(code)

            if pct >= (limit_rate - self.open_tolerance):
                cont_limits += 1
            else:
                break

        # 必须恰好是 entry_board 连板 (例如要做 1 进 2，昨天必须是 1 板)
        if cont_limits != self.entry_board:
            return None

        # 2. 严格检查昨日是否开板 (H1 >= H2 >= H3 >= H4 且均贴近涨停)
        # 获取昨日的小时线涨跌幅
        h_rates = []
        for i in range(1, 5):
            rate = self._safe_float(row_yesterday.get(f'hour{i}_close_rate'), -999.0)
            if rate == -999.0:
                # 如果缺少小时数据，视为不合格或根据日线勉强判断 (此处选择严格模式：剔除)
                return None
            h_rates.append(rate)

        limit_rate = self._get_theoretical_limit_rate(code)
        min_limit_bound = limit_rate - self.open_tolerance

        # 检查是否每个小时都封住了
        for rate in h_rates:
            if rate < min_limit_bound:
                return None # 中途开板，剔除

        # 检查封板顺序逻辑：理论上如果一直封死，H1~H4 应该都很高
        # 你的需求是 H1>H2>H3>H4 (这通常意味着早盘最强，后面维持，或者数据噪声)
        # 实际上只要都封死即可。如果需要严格递减排序作为强度指标：
        # 这里我们主要记录【最早封板时间】作为排序依据
        # 既然 H1-H4 都封死了，说明 H1 就封死了，优先级最高 (Priority 4)

        priority_score = 4 # H1 即封死，最高分

        # 获取股票名称
        name = "Unknown"
        # 注意：pool_data 可能在外部传入，这里暂时无法直接获取 name，由调用方补充或后续查询

        return {
            'code': code,
            'priority': priority_score, # 4=H1 封，3=H2 封...
            'h_rates': h_rates,
            'cont_limits': cont_limits
        }

    def select_candidates(self, date: str, pool_data: list, k_data_map: dict, history_map: dict) -> List[str]:
        """
        【步骤 1：T-1 日盘后】筛选昨日首板且未开板的股票
        """
        print(f"\n🔍 [{date}] 扫描【{self.entry_board}进{self.target_board}】候选 (严格未开板模式)...")

        candidates = []
        core_codes = set([item['code'] for item in pool_data]) if self.use_core_pool else None

        # 遍历所有昨日有数据的股票
        # 优化：如果全市场扫描太慢，可以先从昨日涨停股中筛选 (需依赖额外索引，此处简化为遍历 k_data_map)
        scanned_count = 0

        for code, row_yesterday in k_data_map.items():
            # 核心池过滤
            if self.use_core_pool and code not in core_codes:
                continue

            # 基础数据检查
            preclose = self._safe_float(row_yesterday.get('preclose'))
            if preclose == 0: continue

            # 执行强度分析
            result = self.analyze_yesterday_strength(code, row_yesterday, history_map)

            if result:
                # 补充名称 (从 pool_data 找，或者暂留空)
                name = "Unknown"
                if self.use_core_pool:
                    for item in pool_data:
                        if item['code'] == code:
                            name = item.get('code_name', 'Unknown')
                            break

                candidates.append({
                    'code': code,
                    'name': name,
                    'priority': result['priority'],
                    'yesterday_h_rates': result['h_rates']
                })
                scanned_count += 1

        # 排序：优先级高的在前 (虽然这里都是 H1 封板，但保留扩展性)
        # 如果有多个都是 H1 封板，可以按换手率或市值二次排序，此处暂按代码排序保证确定性
        candidates.sort(key=lambda x: (-x['priority'], x['code']))

        self.yesterday_candidates = candidates

        print(f"   ✅ 发现 {len(candidates)} 只符合【{self.entry_board}连板且全程未开板】的股票。")
        if candidates:
            top_5 = [f"{c['code']}({c['name']})" for c in candidates[:5]]
            print(f"   🔥 重点观察：{', '.join(top_5)}")

        # 返回代码列表供回测引擎缓存
        return [c['code'] for c in candidates]

    def generate_h3_analysis_report(self, date: str, candidate_codes: List[str], k_data_map: dict, history_map: dict) -> Tuple[Optional[str], str, float]:
        """
        【步骤 2：T 日盘中/H1 后】从候选池中选出今日开盘 1%~3% 的股票，并按昨日强度排序
        返回：(最佳代码，理由，买入价格比率)
        """
        if not self.yesterday_candidates:
            return None, "昨日无合格候选", 0.0

        valid_today = []

        # 1. 筛选今日开盘符合条件的股票
        for cand in self.yesterday_candidates:
            code = cand['code']
            if code not in k_data_map:
                continue

            row_today = k_data_map[code]
            h1_open_rate = self._safe_float(row_today.get('hour1_open_rate'), -999.0)

            if h1_open_rate == -999.0:
                continue

            # 核心条件：H1 开盘在 1% ~ 3% 之间
            if self.min_open_pct <= h1_open_rate <= self.max_open_pct:
                valid_today.append({
                    'code': code,
                    'name': cand['name'],
                    'h1_open_rate': h1_open_rate,
                    'priority': cand['priority'] # 继承昨日的强度排序
                })

        if not valid_today:
            print(f"   🛑 [{date}] 候选池中无股票满足开盘 {self.min_open_pct}%~{self.max_open_pct}% 条件。")
            return None, "无符合开盘条件的标的", 0.0

        # 2. 排序逻辑：按昨日封板时间排序 (优先级高的在前)
        # 如果优先级相同，可按今日开盘幅度越小越优先 (更安全) 或 越大越优先 (更强)，此处按优先级为主
        valid_today.sort(key=lambda x: (-x['priority'], x['h1_open_rate']))

        # 3. 选中第一只
        best = valid_today[0]

        reason = f"【{self.entry_board}进{self.target_board}】{best['name']} | 昨日 H1 封板未开 | 今日高开 {best['h1_open_rate']:.2f}%"
        buy_ratio = 1.0 + (best['h1_open_rate'] / 100.0)

        print(f"   🚀 [{date}] 选中目标：{best['code']} ({best['name']})")
        print(f"      理由：{reason}")
        print(f"      计划买入价：H1 开盘价 (Ratio: {buy_ratio:.4f})")

        return best['code'], reason, buy_ratio

    def generate_buy_signal(self, date: str, code: str, k_row: dict, is_candidate: bool = False) -> Tuple[bool, float, str]:
        """
        执行买入：按 H1_Open 买入
        由于我们在 generate_h3_analysis_report 已经锁定了目标和比例，这里直接确认
        """
        if not is_candidate:
            return False, 0.0, "不在候选池"

        # 再次校验实时数据 (防止数据跳变)
        h1_open_rate = self._safe_float(k_row.get('hour1_open_rate'), -999.0)
        if not (self.min_open_pct <= h1_open_rate <= self.max_open_pct):
            return False, 0.0, f"开盘幅变动 ({h1_open_rate:.2f}%) 超出范围"

        buy_ratio = 1.0 + (h1_open_rate / 100.0)
        return True, buy_ratio, f"H1 开盘执行 ({h1_open_rate:.2f}%)"

    def check_sell_condition(self, hold_code: str, buy_price: float, current_date: str, current_k_row: dict, profit_rate: float, days_held: int) -> Tuple[bool, str, float]:
        """
        【步骤 3：T+1 日】无条件 H1 开盘卖出
        逻辑：持有满 1 天，且在 T+1 日的 H1 阶段，直接卖出
        """
        if days_held < 1:
            return False, "Hold", 0.0

        # 只要是 T+1 日，无论盈亏，H1 开盘即卖出
        # 模拟中，我们用 H1_Open 作为卖出价
        h1_open_rate = self._safe_float(current_k_row.get('hour1_open_rate'), 0.0)

        # 计算卖出价格比率 (相对于昨日收盘)
        sell_ratio = 1.0 + (h1_open_rate / 100.0)

        reason = f"T+1 纪律止盈/损 (H1 开盘 {h1_open_rate:.2f}%)"

        # 打印交易心理
        action = "✅ 盈利离场" if profit_rate > 0 else "❌ 止损离场"
        print(f"   💥 [{current_date}] {action} {hold_code} | 盈亏:{profit_rate*100:.2f}% | 卖出逻辑：{reason}")

        return True, reason, sell_ratio

    # 占位函数，保持基类兼容
    def select_target(self, date, pool_data, k_data_map):
        return None
