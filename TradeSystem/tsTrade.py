# coding=utf-8

import csv
import json
from datetime import timedelta
import matplotlib.pyplot as plt
from tradeSystemBase.tsFunction import *
from tsAccount import Account
from tradeControl.riskManage.tsRiskManage import RiskEngine
import pandas as pd


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
        # 引入风控引擎
        self.riskEngine = None
        # 引入仓控引擎
        self.positionEngine = _main_engine.positionEngine
        self.positionEngine.target_pool = self.stock_pool
        self.positionEngine.stock_risk_ration_init()
        # 前一个交易日的行情信息
        self.df_last_hist = pd.DataFrame()

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

    def timing_strategy(self):
        """行情循环推送推送（必须由用户继承实现）"""
        raise NotImplementedError

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

    def get_fundvalue(self):
        """获取资金净值"""
        # 算基准值
        history_index = self.mssql.get_index(self.benchmark, self.account.current_date, 'close')

        if self.account.fundvalue != 1:
            if self.account.benchmarkvalue == 1:
                # 获取指数起始值
                self.benchmark_base_value = self.mssql.get_index(self.benchmark, self.account.current_date, 'open')

            self.account.benchmarkvalue = history_index / self.benchmark_base_value
        else:
            self.account.benchmarkvalue = 1.0

        # 算净值
        zig_fundvalue = 0

        for item in self.account.list_position:
            zig_fundvalue += self.account.list_position[item]['referencenum'] * self.account.list_position[item]['new_price']

        alpha_stock_fundvalue = self.positionEngine.get_alpha_fundvalue()

        self.account.fundvalue = zig_fundvalue + self.account.cash + alpha_stock_fundvalue + self.account.hedge_deposit

        self.account.zig_fundvalue = zig_fundvalue
        self.account.alpha_stock_fundvalue = alpha_stock_fundvalue
        self.account.alpha_stop_value = self.positionEngine.get_alpha_stop_fundvalue()

        # 算zig突破的持仓比例: position_ratio
        position_ratio = float(zig_fundvalue / self.account.fundvalue)

        # 算alpha的持仓: alpha_fundvalue
        alpha_ratio = float((alpha_stock_fundvalue + self.account.hedge_deposit + (alpha_stock_fundvalue / self.account.futures_leverage)) / self.account.fundvalue)

        self.account.fundvalue = float(self.account.fundvalue / self.capital_base)

        self.account.list_fundvalue.append([self.account.current_date, self.account.fundvalue, self.account.benchmarkvalue, position_ratio, alpha_ratio, alpha_stock_fundvalue, self.account.hedge_deposit, self.account.cash])

    def update_timing_position_price(self):
        # 信息更新
        df_close = self.mssql.get_close_price(self.account.current_date)

        df_close = df_close.set_index('code')

        df_close = df_close.dropna(how='any')

        df_close_all = df_close.copy()

        df_position = pd.DataFrame(self.account.dic_high_stk_position.keys())
        if not df_position.empty:
            df_position = df_position.set_index(0)

        df_close = pd.merge(df_close, df_position, left_index=True, right_index=True, how='inner')

        dict_close = df_close.to_dict()

        for stk in df_close.index.values.tolist():
            a = dict_close['adjust_price_f'][stk]
            b = self.account.dic_high_stk_position[stk]['high_price']

            if a >= b:
                self.account.dic_high_stk_position[stk]['high_price'] = dict_close['adjust_price_f'][stk]
                self.account.dic_high_stk_position[stk]['buy_date'] = self.account.current_date

        dic_close_all = df_close_all.to_dict()

        list_df_close_all_index = set(df_close_all.index.values.tolist())

        for stk in self.account.list_position.keys():
            if stk in list_df_close_all_index:
                self.account.list_position[stk]['new_price'] = dic_close_all['adjust_price_f'][stk]

    def run(self):
        """主运行启动"""

        self.account.current_date = self.start_day

        # 时间预处理
        calendar_end_str = get_format_date_str(self.end_day)

        # 创建日历
        sql_str = "SELECT DISTINCT [date] FROM index_data WHERE [date]<='" + calendar_end_str + "' order by [date] desc"
        calendars = self.mssql.execquery(sql_str)
        for d in calendars:
            self.tradecalendars.append(d[0])

        # 将净值起始设为1
        self.account.list_fundvalue.append([self.account.current_date, 1, 1, 0, 0])

        # 按日执行策略
        while self.account.current_date <= self.end_day:
            if self.account.current_date in self.tradecalendars:
                print(self.account.current_date)
                # 重置择时系统的buy_list和sell_list
                self.account.buy_list = {}
                self.account.sell_list = []

                # @todo 加入仓控的内容——大盘择时的部分---> α和股票择时两个系统的仓位
                self.positionEngine.position_manage(self.account.current_date)

                # 择时获取买卖列表
                self.timing_strategy()
                self.account.dic_ATR = self.dic_ATR

                self.riskEngine = RiskEngine(self.positionEngine, self.account)
                self.riskEngine.operate()

                # 根据收盘价进行择时价格更新
                self.update_timing_position_price()

                if self.account.current_date > get_format_date(self.positionEngine.alpha_start_date):
                    # 根据收盘价进行alpha价格更新
                    self.positionEngine.update_alpha_position_price(self.account.current_date)
                    # 收盘对冲结算
                    self.riskEngine.future_close_settlement()

                # 输出持仓列表
                self.account.get_zig_position()
                self.positionEngine.get_alpha_position(self.account.current_date)
                self.account.get_hedge_position()

                # 计算净值变化
                if self.account.list_position or self.positionEngine.alpha_position_list:
                    self.get_fundvalue()

            self.account.current_date = self.account.current_date + timedelta(days=1)

        # 输出到csv
        writer = csv.writer(open('zig_operate.csv', 'wb'))
        writer.writerow(['date', 'stockcode', 'operatetype', 'referencenum', 'referenceprice'])
        for item in self.account.list_operate:
            item[0] = get_format_date_str(item[0])
            writer.writerow(item)

        writer = csv.writer(open('alpha_operate.csv', 'wb'))
        writer.writerow(['date', 'stockcode', 'operatetype', 'referencenum', 'referenceprice'])
        for item in self.positionEngine.alpha_operate_list:
            item[0] = get_format_date_str(item[0])
            writer.writerow(item)

        writer = csv.writer(open('hedge_operate.csv', 'wb'))
        writer.writerow(['date', 'code', 'number'])
        for item in self.account.hedge_operate_list:
            writer.writerow(item)

        writer = csv.writer(open('fundvalue.csv', 'wb'))
        writer.writerow(['date', 'fundvalue', 'benchmarkvalue', 'timing_position_ratio', 'alpha_position_ratio', 'alpha_stock', 'deposit', 'cash'])
        for item in self.account.list_fundvalue:
            item[0] = get_format_date_str(item[0])
            writer.writerow(item)

        writer = csv.writer(open('zig_log.csv', 'wb'))
        writer.writerow(['code', 'date', 'price'])
        for item in self.list_zig_log:
            item[1] = get_format_date_str(item[1])
            writer.writerow(item)

        # 绘制净值曲线
        df = pd.read_csv('fundvalue.csv')
        df = df.loc[:, ['date', 'fundvalue', 'benchmarkvalue', 'timing_position_ratio', 'alpha_position_ratio']]
        df.to_csv('fundvalue_plot.csv')
        plt.plotfile('fundvalue_plot.csv', ('date', 'fundvalue', 'benchmarkvalue', 'timing_position_ratio', 'alpha_position_ratio'), subplots=False)
        plt.show()

        # 打印各项参数
        self.print_info()
