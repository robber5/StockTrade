# coding=utf-8

from tsAccount import Account
from tsFunction import *


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

    def setSlippage(self, _slippage):
        """设置滑点点数"""
        self.slippage = _slippage

    def setBuy_commission(self, _buy_commission):
        """设置买入手续费"""
        self.buy_commission = _buy_commission

    def setSell_commission(self, _sell_commission):
        """设置卖出点数"""
        self.sell_commission = _sell_commission


