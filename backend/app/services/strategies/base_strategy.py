# backend/app/services/strategies/base_strategy.py
from abc import ABC, abstractmethod
from typing import List, Tuple, Optional

class BaseStrategy(ABC):
    """策略抽象基类 - 统一接口规范"""

    def __init__(self, name):
        self.name = name

    @abstractmethod
    def select_candidates(self, date: str, pool_data: list, k_data_map: dict, history_map: dict) -> List[str]:
        """
        【选股】在 date 这一天，从股票池中筛选符合条件的候选股
        :param date: 交易日期 (str)
        :param pool_data: 股票池列表 (list of dict)
        :param k_data_map: 当日所有股票的 K 线数据字典 {code: row}
        :param history_map: 历史K线数据 {code: [rows]}
        :return: 候选股代码列表 [code1, code2, ...]
        """
        pass

    @abstractmethod
    def check_buy_signal(self, date: str, candidate_codes: List[str], k_data_map: dict) -> Tuple[Optional[str], str, float]:
        """
        【买入判断】从候选股中选择最佳标的，并确定买入价格
        :param date: 交易日期
        :param candidate_codes: 候选股代码列表
        :param k_data_map: 当日K线数据
        :return: (best_code, reason, buy_ratio)
                 buy_ratio 是相对于 preclose 的倍数，用于计算具体买入价
                 例如: buy_ratio=1.02 表示买入价 = preclose * 1.02
        """
        pass

    @abstractmethod
    def check_sell_condition(self, hold_code: str, buy_price: float, current_date: str,
                           current_k_row: dict, profit_rate: float, days_held: int,
                           is_last_day: bool = False) -> Tuple[bool, str, float]:
        """
        【卖出判断】检查持仓是否该卖出
        :param hold_code: 持仓代码
        :param buy_price: 买入价格
        :param current_date: 当前日期
        :param current_k_row: 当前日K线数据
        :param profit_rate: 当前收益率 (current_close - buy_price) / buy_price
        :param days_held: 持仓天数（交易日）
        :param is_last_day: 是否为到期日（max_hold_days）
        :return: (should_sell, reason, sell_ratio)
                 sell_ratio 是相对于 preclose 的倍数，用于计算卖出价
                 sell_ratio=0 表示用收盘价

        【重要规则】
        - 非到期日：检查H1-H4，触发即卖（用触发时段价格）
        - 到期日：只检查H1-H3，触发用触发价，未触发用H4 Open
        """
        pass

