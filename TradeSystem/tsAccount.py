# coding=utf-8

import datetime
import csv


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
        # 净值记录表,包含日期,基金净值,指数净值,仓位
        self.list_fundvalue = []
        # 调仓记录dataframe,包含日期,股票代码,股票名称,操作方向,操作手数,操作价格(前复权)
        self.list_operate = []
        # 持仓dataframe,包含股票代码,前复权的股数价格
        self.list_position = {}
        # 账户初始资金
        self.capital_base = _capital_base

    def get_postion(self):
        """获取当前持仓"""
        writer = csv.writer(open('D:/position_log/' + datetime.datetime.strftime(self.current_date, '%Y-%m-%d') + '-position.csv', 'wb'))
        writer.writerow(['stockcode', 'referencenum', 'buy_price', 'new_price'])
        for item in self.list_position:
            row = [item, self.list_position[item]['referencenum'], self.list_position[item]['buy_price'], self.list_position[item]['new_price']]
            writer.writerow(row)
