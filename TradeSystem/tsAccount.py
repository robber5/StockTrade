# coding=utf-8

import pandas as pd


class Account(object):
    """基金账户"""

    def __init__(self, _capital_base, _current_date, _num):
        # 净值
        self.fundvalue = 1
        # 上一K线结束后的现金头寸
        self.cash = _capital_base
        # 当前交易日
        self.current_date = _current_date
        self.num = _num  # 最大持仓股票数量
        # 净值记录dataframe,包含日期,基金净值,指数净值
        self.dfFundvalue = pd.DataFrame(columns=('date', 'fundvalue', 'benchmarkvalue'))
        # 调仓记录dataframe，包含日期,操作方向,股票代码,股票名称,买入卖出价格(不复权),操作手数
        self.dfOperate = pd.DataFrame(columns=('date', 'stockcode', 'operatetype', 'referencenum',
                                               'referenceprice'))
        # 持仓dataframe，包含股票代码，前复权的股数价格
        self.dfPosition = pd.DataFrame(columns=('stockcode', 'referencenum'))
        self.buylist = []
        self.benchmarkvalue = 0

    def get_fundvalue(self):
        """获取资金净值"""
        pass
