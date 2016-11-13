# coding=utf-8

import pandas as pd


class Account(object):
    """基金账户"""

    def __init__(self, _capital_base, _start_date, _num):
        # 净值
        self.fundvalue = 1
        # 基准净值
        self.benchmarkvalue = 1
        # 上一K线结束后的现金头寸
        self.cash = _capital_base
        # 当前交易日
        self.current_date = _start_date
        # 最大持仓股票数量
        self.num = _num
        # 买入股票池
        self.buylist = []
        # 净值记录表,包含日期,基金净值,指数净值
        self.dfFundvalue = pd.DataFrame(columns=('date', 'fundvalue', 'benchmarkvalue'))
        # 调仓记录dataframe,包含日期,股票代码,股票名称,操作方向,操作手数,操作价格(前复权)
        self.dfOperate = pd.DataFrame(columns=('date', 'stockcode', 'operatetype', 'referencenum',
                                               'referenceprice'))
        # 持仓dataframe,包含股票代码,前复权的股数价格
        self.dfPosition = pd.DataFrame(columns=('stockcode', 'referencenum'))

    def get_fundvalue(self):
        """获取资金净值"""
        print('获取资金净值')
        # @todo 获取资金净值

    def get_postion(self):
        """获取当前持仓"""
        print("获取当前持仓")
        # @todo 获取当前持仓
