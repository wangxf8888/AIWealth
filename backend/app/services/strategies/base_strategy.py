# backend/app/services/strategies/base_strategy.py
from abc import ABC, abstractmethod

class BaseStrategy(ABC):
    """策略抽象基类"""
    
    def __init__(self, name):
        self.name = name

    @abstractmethod
    def select_target(self, date, pool_data, k_data_map):
        """
        【选股】在 date 这一天，从 core_pool 中选出一只最强的股票。
        :param date: 交易日期 (str)
        :param pool_data: 当日核心池列表 (list of dict)
        :param k_data_map: 当日所有股票的 K 线数据字典 {code: row}
        :return: code (str) 或 None (若无合适标的)
        """
        pass

    @abstractmethod
    def generate_buy_signal(self, date, code, k_row):
        """
        【买入判断】针对选中的股票，判断是否在 Hour4 买入。
        :return: (should_buy: bool, buy_price_ratio: float) 
                 buy_price_ratio 是相对于 preclose 的倍数，用于计算具体买入价
        """
        pass

    @abstractmethod
    def check_sell_condition(self, hold_code, buy_date, buy_price, current_date, current_k_row, profit_rate):
        """
        【卖出判断】
        :param profit_rate: 当前收益率 (current_close - buy_price) / buy_price
        :return: (should_sell: bool, reason: str)
        """
        pass

