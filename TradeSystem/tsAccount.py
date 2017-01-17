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
        # 买卖列表
        self.buy_list = {}
        self.sell_list = []
        # 净值记录表,包含日期,基金净值,指数净值,zig仓位,alpha仓位
        self.list_fundvalue = []
        # 调仓记录dataframe,包含日期,股票代码,股票名称,操作方向,操作手数,操作价格(前复权)
        self.list_operate = []
        # 持仓dataframe,包含股票代码,前复权的股数价格
        self.list_position = {}
        # 账户初始资金
        self.capital_base = _capital_base
        # 各部分头寸的比例
        self.zig_position_ratio = 0
        self.alpha_position_ratio = 0
        # 对冲相关
        self.hedge_deposit = 0
        self.hedge_position = []
        self.hedge_last_month_change = 0
        # 期货相关
        self.futures_leverage = 10
        # 开仓后高点的记录表
        self.dic_high_stk_position = {}

    def get_postion(self):
        """获取当前持仓"""
        writer = csv.writer(open('D:/position_log/' + datetime.datetime.strftime(self.current_date, '%Y-%m-%d') + '-position.csv', 'wb'))
        writer.writerow(['stockcode', 'referencenum', 'buy_price', 'new_price'])
        for item in self.list_position:
            row = [item, self.list_position[item]['referencenum'], self.list_position[item]['buy_price'], self.list_position[item]['new_price']]
            writer.writerow(row)
