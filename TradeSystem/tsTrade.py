# coding=utf-8

import csv
import json
from datetime import timedelta
import matplotlib.pyplot as plt
import numpy as np
from TradeSystem.tradeSystemBase.tsFunction import *
from TradeSystem.tsAccount import Account


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
        # 数据获取
        self.mssql = _main_engine.mssqlDB
        # 基准指数基础值
        self.benchmark_base_value = 0
        # 交易日历
        self.tradecalendars = []
        # 调仓日历
        self.changecalendars = []
        # sqlite数据库连接
        self.sqlite = _main_engine.sqliteDB
        # zig的记录表
        self.list_zig_log = []
        # 读取用户设置
        self.load_setting()
        # 在策略中初始化账户
        self.account = Account(self.capital_base, self.start_day, self.chosen_num)
        # 开仓后高点的记录表
        self.dic_high_stk_position = {}
        # 引入风控引擎
        self.riskEngine = _main_engine.riskEngine
        # 引入仓控引擎
        self.positionEngine = _main_engine.positionEngine
        self.positionEngine.target_pool = self.stock_pool
        self.positionEngine.stock_risk_ration_init()

    def print_info(self):
        """信息显示"""
        print('股票池:{0}'.format(str(self.stock_pool)))
        print('基准指数:{0}'.format(self.benchmark))
        print('回测起始时间:' + str(self.start_day))
        print('回测结束时间:' + str(self.end_day))
        print('调仓频率:{0}'.format(str(self.refresh_rate)))
        print('调仓周期:{0}'.format(self.freq))
        print('初始资金:{0}'.format(str(self.capital_base)))
        print('持仓股票数量:{0}'.format(str(self.chosen_num)))

    def back_test(self):
        """回测"""

        self.account.current_date = self.start_day

        output(u'开始回测')

        while self.account.current_date <= self.end_day:
            self.handle_date()

        output(u'回测结束')

    def handle_date(self):
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
        if 'stock_pool' in setting:
            self.stock_pool = self.get_stock_pool_by_mark(str(setting['stock_pool']))

    def get_stock_pool_by_mark(self, key):
        """根据获取基础股票池"""
        stock_pool = self.mssql.get_stock_pool(key)
        return stock_pool

    def get_history(self, column_list, current_day):
        """获取历史行情"""
        return self.mssql.get_history(column_list, self.tradecalendars[self.tradecalendars.index(current_day) + 1])

    def buy(self, buy_list):
        """买入操作"""
        buy_result = self.riskEngine.buy(self.account, buy_list, self.positionEngine)
        for s in buy_result:
            if s not in self.dic_high_stk_position.keys():
                self.dic_high_stk_position[s] = dict(high_price=0, buy_date=self.account.current_date)

    def sell(self, sell_list):
        """卖出操作"""
        sell_result = self.riskEngine.sell(self.account, sell_list)
        for s in sell_result:
            if sell_result[s] == 0:
                self.dic_high_stk_position.pop(s)

    def get_fundvalue(self, _account):
        """获取资金净值"""
        # 算净值
        _account.fundvalue = 0

        for item in _account.list_position:
            _account.fundvalue += _account.list_position[item]['referencenum'] * _account.list_position[item][
                'new_price']

        _account.fundvalue = (_account.fundvalue + _account.cash) / self.capital_base

        # 算基准值
        history_index = self.mssql.get_index(self.benchmark, _account.current_date, 'close')

        if _account.fundvalue != 1:
            if _account.benchmarkvalue == 1:
                # 获取指数起始值
                self.benchmark_base_value = self.mssql.get_index(self.benchmark, _account.current_date, 'open')

            _account.benchmarkvalue = history_index / self.benchmark_base_value
        else:
            _account.benchmarkvalue = 1

        _account.list_fundvalue.append([_account.current_date, _account.fundvalue, _account.benchmarkvalue])

    def run(self):
        """主运行启动"""

        self.account.current_date = self.start_day

        # 时间预处理
        calendar_end_str = datetime.strftime(self.end_day, '%Y-%m-%d')

        # 创建日历
        sql_str = "SELECT DISTINCT [date] FROM index_data WHERE [date]<='" + calendar_end_str + "' order by [date] desc"
        calendars = self.mssql.execquery(sql_str)
        for d in calendars:
            self.tradecalendars.append(d[0])

        # 将净值起始设为1
        self.account.list_fundvalue.append([self.account.current_date, 1, 1])

        # 按日执行策略
        while self.account.current_date <= self.end_day:
            print(self.account.current_date)
            # @todo 加入仓控的内容——大盘择时的部分---> α和股票择时两个系统的仓位
            if self.account.current_date in self.tradecalendars:
                print(self.account.current_date)
                self.positionEngine.position_manage(self.account.current_date)
                self.handle_date()
            self.account.current_date = self.account.current_date + timedelta(days=1)

        # 输出到csv
        writer = csv.writer(open('operate.csv', 'wb'))
        writer.writerow(['date', 'stockcode', 'operatetype', 'referencenum', 'referenceprice'])
        for item in self.account.list_operate:
            item[0] = datetime.strftime(item[0], '%Y-%m-%d')
            writer.writerow(item)

        writer = csv.writer(open('fundvalue.csv', 'wb'))
        writer.writerow(['date', 'fundvalue', 'benchmarkvalue'])
        for item in self.account.list_fundvalue:
            item[0] = datetime.strftime(item[0], '%Y-%m-%d')
            writer.writerow(item)

        writer = csv.writer(open('zig_log.csv', 'wb'))
        writer.writerow(['code', 'date', 'price'])
        for item in self.list_zig_log:
            item[1] = datetime.strftime(item[1], '%Y-%m-%d')
            writer.writerow(item)

        # 绘制净值曲线
        plt.plotfile('fundvalue.csv', ('date', 'fundvalue', 'benchmarkvalue'), subplots=False)
        plt.show()

        # 打印各项参数
        self.print_info()
