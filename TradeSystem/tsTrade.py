# coding=utf-8

from tsAccount import Account
from tsFunction import *
import json


class Trade(object):
    """交易控制"""

    def __init__(self):
        """初始化"""
        # 总股票池
        self.stock_pool = []
        # 基准指数
        self.benchmark = ""
        # 回测起始时间
        self.start_day = None
        # 回测结束时间
        self.end_day = None
        # 调仓频率
        self.refresh_rate = 1
        # 调仓周期 d-日，m-月，y-年
        self.freq = "d"
        # 初始资金
        self.capital_base = 1000000
        # 持仓股票数量
        self.chosen_num = 10
        # 滑点
        self.slippage = 0.003
        # 买入手续费
        self.buy_commission = 0.0003
        # 卖出手续费
        self.sell_commission = 0.0013

    def print_info(self):
        """信息显示"""
        print('股票池:{0}'.format(str(self.stock_pool)))
        print('基准指数:{0}'.format(self.benchmark))
        print('回测起始时间:' + self.start_day)
        print('回测结束时间:' + self.end_day)
        print('调仓频率:{0}'.format(str(self.refresh_rate)))
        print('调仓周期:{0}'.format(self.freq))
        print('初始资金:{0}'.format(str(self.capital_base)))
        print('持仓股票数量:{0}'.format(str(self.chosen_num)))
        print('滑点:{0}'.format(str(self.slippage)))
        print('买入手续费:{0}'.format(str(self.buy_commission)))
        print('卖出手续费:{0}'.format(str(self.sell_commission)))

    def back_test(self):
        """回测"""
        account = Account(self.capital_base, self.start_day, self.chosen_num)
        account.current_date = self.start_day

        output(u'开始回测')

        while account.current_date <= self.end_day:
            self.handle_date(account)

        output(u'回测结束')

    def handle_date(self, _account):
        print("每日调仓变化")
        pass

    def load_setting(self):
        """读取配置"""
        setting = json.load(file("Trade_Setting.json"))
        if setting['benchmark'] is None:
            self.benchmark = setting["benchmark"]
        if setting['start_day'] is None:
            self.start_day = setting["start_day"]
        if setting['end_day'] is None:
            self.end_day = setting["end_day"]
        if setting['capital_base'] is None and str(setting['capital_base']).isdigit():
            self.capital_base = int(str(setting['capital_base']))
        if setting['chosen_num'] is None and str(setting['chosen_num']).isdigit():
            self.chosen_num = int(str(setting["chosen_num"]))
        if setting['slippage'] is None and is_num(setting['slippage']):
            self.slippage = setting["slippage"]
        if setting['buy_commission'] is None and is_num(setting['buy_commission']):
            self.buy_commission = setting["buy_commission"]
        if setting['sell_commission'] is None and is_num(setting["sell_commission"]):
            self.sell_commission = setting["sell_commission"]
        if setting['stock_pool'] is None:
            self.stock_pool = self.get_stock_pool_by_mark(str(setting['stock_pool']))

    @staticmethod
    def get_stock_pool_by_mark(value):
        stock_pool = []
        print("获取股票池" + value)
        return stock_pool






