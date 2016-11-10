# coding=utf-8

from tsAccount import Account
from tsFunction import *
import json
import pandas as pd
import matplotlib.pyplot as plt


class Trade(object):
    """交易控制"""

    def __init__(self, _main_engine):
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
        self.capital_base = 10000000
        # 持仓股票数量
        self.chosen_num = 10
        # 滑点
        self.slippage = 0.003
        # 买入手续费
        self.buy_commission = 0.0003
        # 卖出手续费
        self.sell_commission = 0.0013
        # 数据获取
        self.mssql = _main_engine.mssqlDB

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
        if 'benchmark' in setting:
            self.benchmark = str(setting["benchmark"])
        if 'start_day' in setting:
            self.start_day = datetime.strptime(setting["start_day"], '%Y-%m-%d')
        if 'end_day' in setting:
            self.end_day = datetime.strptime(setting["end_day"], '%Y-%m-%d')
        if 'capital_base' in setting and str(setting['capital_base']).isdigit():
            self.capital_base = int(str(setting['capital_base']))
        if 'chosen_num' in setting and str(setting['chosen_num']).isdigit():
            self.chosen_num = int(str(setting["chosen_num"]))
        if 'slippage' in setting and is_num(setting['slippage']):
            self.slippage = setting["slippage"]
        if 'buy_commission' in setting and is_num(setting['buy_commission']):
            self.buy_commission = setting["buy_commission"]
        if 'sell_commission' in setting and is_num(setting["sell_commission"]):
            self.sell_commission = setting["sell_commission"]
        if 'stock_pool' in setting:
            self.stock_pool = self.get_stock_pool_by_mark(str(setting['stock_pool']))

    def get_stock_pool_by_mark(self, key):
        stock_pool = self.mssql.get_stock_pool(key)
        return stock_pool

    def order_to(self, account, _stockcode, _referencenum, _price):
        """买卖操作"""
        if _stockcode in account.dfPosition.index:
            have_num = account.dfPosition.loc[_stockcode].values[0]
        else:
            have_num = 0

        if _referencenum >= have_num:
            _operatetype = '买入'
            account.cash = account.cash - (_referencenum - have_num) * _price - abs(
                (_referencenum - have_num) * _price) * (self.slippage + self.buy_commission)
        else:
            _operatetype = '卖出'
            account.cash = account.cash - (_referencenum - have_num) * _price - abs(
                (_referencenum - have_num) * _price) * (self.slippage + self.sell_commission)

        row = pd.DataFrame([dict(date=account.current_date, stockcode=_stockcode, operatetype=_operatetype,
                                 referencenum=(_referencenum - have_num), referenceprice=_price), ])

        account.dfOperate = account.dfOperate.append(row)

    def run(self):
        """主运行启动"""
        self.load_setting()

        account = Account(self.capital_base, self.start_day, self.chosen_num)

        # @todo 进行各种类型转换

        # 按日执行策略
        while account.current_date <= self.end_day:
            self.handle_date(account)
            account.current_date = account.current_date + datetime.timedelta(days=1)

        # 输出到csv
        account.dfFundvalue.to_csv('fundvalue.csv', index=False, encoding='gbk')

        # 绘制净值曲线
        plt.plotfile('fundvalue.csv', ('date', 'fundvalue', 'benchmarkvalue'), subplots=False)
        plt.show()

        # 打印各项参数
        self.print_info()
