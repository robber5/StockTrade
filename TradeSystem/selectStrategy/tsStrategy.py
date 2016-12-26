# coding=utf-8

"""
可以使用回测或者实盘
所有未标注的close，high，low都是前复权结果
"""

import numpy as np
import pandas as pd
from TradeSystem.tsTrade import Trade


def b_in_list(stock_list, stock):
    if stock in stock_list:
        b_in = True
    else:
        b_in = False
    return b_in


class StrategyTest(Trade):
    """策略"""

    def __init__(self, _main_engine):
        super(StrategyTest, self).__init__(_main_engine)

        # 自高点回撤止损倍数（n*ATR）
        self.down_ATR = 13
        # Zig最大使用数量
        self.zig_count = 8
        # n个zig点为低点卖出
        self.zig_sell = 6
        # ATR的参数
        self.ATR_base = 120
        # zig记录的标准(n*ATR)
        self.zig_limit = 4.5
        # 前一个交易日的行情信息
        self.df_last_hist = pd.DataFrame()
        # Zig记录表
        self.list_zig_log = []
        # TR记录字典
        self.dic_TR = {}
        for s in self.stock_pool:
            self.dic_TR[s] = np.ones(self.ATR_base) * -1
        # ATR的表(日期，还有当日的ATR值)
        self.dic_ATR = {}
        # zig的字典，字典的值是一个list
        self.dic_Zig_price = {}
        self.dic_Zig_Type = {}
        self.dic_Zig_Date = {}
        for s in self.stock_pool:
            self.dic_Zig_price[s] = np.zeros(self.zig_count)
            self.dic_Zig_Type[s] = np.zeros(self.zig_count)   # 0,1,-1
            self.dic_Zig_Date[s] = np.zeros(self.zig_count)   # 时间戳
        # Zig的记录表(高低点信息)
        self.dic_Zig_base = {}

    def handle_date(self):
        try:
            # 信号处理
            zig_day = self.tradecalendars[self.tradecalendars.index(self.account.current_date) + 1]

            hist = self.get_history('adjust_price_f,adjust_price_f/[close]*[open]',
                                    self.account.current_date)                             # 2.4

            hist.columns = ['code', 'close', 'open']

            hist = hist.dropna(how='any')

            hist = hist.set_index('code')

            dict_hist = hist.to_dict()

            dict_last_hist = self.df_last_hist.to_dict()

            sell_list = []

            buy_list = {}

            list_last_hist_keys = set(self.df_last_hist.index.values.tolist())
            list_zig_base_keys = set(self.dic_Zig_base.keys())
            list_atr_keys = set(self.dic_ATR.keys())

            b_In_list_zig_base_keys = False

            for s in hist.index.values:
                newPrice = dict_hist['close'][s]

                if list_zig_base_keys:
                    b_In_list_zig_base_keys = b_in_list(list_zig_base_keys, s)

                if list_zig_base_keys and b_In_list_zig_base_keys:
                    if s in list_last_hist_keys:
                        stock_TR = max(abs(dict_hist['open'][s] - dict_hist['close'][s]),
                                       abs(dict_last_hist['close'][s] - dict_hist['close'][s]))

                        self.dic_TR[s][0:self.ATR_base - 1] = self.dic_TR[s][1:self.ATR_base]
                        self.dic_TR[s][-1] = stock_TR

                        if self.dic_TR[s][0] != -1:
                            newATR = self.dic_TR[s].sum() / self.dic_TR[s].size

                            if s in list_atr_keys:
                                self.dic_ATR[s]['ATR'] = newATR
                            else:
                                self.dic_ATR[s] = dict(ATR=newATR)

                            if self.dic_Zig_base[s]['bLastHigh'] is False and newPrice >= self.dic_Zig_base[s]['high']:
                                self.dic_Zig_base[s]['high'] = newPrice
                                self.dic_Zig_base[s]['high_position'] = zig_day
                                self.dic_Zig_base[s]['low'] = newPrice
                                self.dic_Zig_base[s]['low_position'] = zig_day

                            elif self.dic_Zig_base[s]['bLastHigh'] is False and newPrice < self.dic_Zig_base[s]['low']:
                                self.dic_Zig_base[s]['low'] = newPrice
                                self.dic_Zig_base[s]['low_position'] = zig_day
                                if self.dic_Zig_base[s]['high'] - self.dic_Zig_base[s]['low'] >= self.zig_limit * newATR:
                                    self.list_zig_log.append([s, self.dic_Zig_base[s]['high_position'], self.dic_Zig_base[s]['high']])

                                    self.dic_Zig_base[s]['bLastHigh'] = True

                                    self.dic_Zig_price[s][0:self.zig_count - 1] = self.dic_Zig_price[s][1:self.zig_count]
                                    self.dic_Zig_Type[s][0:self.zig_count - 1] = self.dic_Zig_Type[s][1:self.zig_count]
                                    # self.dic_Zig_Date[s][0:self.zig_count - 1] = self.dic_Zig_Date[s][1:self.zig_count]

                                    self.dic_Zig_price[s][-1] = self.dic_Zig_base[s]['high']
                                    self.dic_Zig_Type[s][-1] = 1
                                    # self.dic_Zig_Date[s][-1] = self.dic_Zig_base[s]['high_position']

                                    self.dic_Zig_base[s]['high'] = newPrice
                                    self.dic_Zig_base[s]['high_position'] = zig_day

                            elif self.dic_Zig_base[s]['bLastHigh'] is True and newPrice <= self.dic_Zig_base[s]['low']:
                                self.dic_Zig_base[s]['high'] = newPrice
                                self.dic_Zig_base[s]['high_position'] = zig_day
                                self.dic_Zig_base[s]['low'] = newPrice
                                self.dic_Zig_base[s]['low_position'] = zig_day

                            elif self.dic_Zig_base[s]['bLastHigh'] is True and newPrice > self.dic_Zig_base[s]['high']:
                                self.dic_Zig_base[s]['high'] = newPrice
                                self.dic_Zig_base[s]['high_position'] = zig_day
                                if self.dic_Zig_base[s]['high'] - self.dic_Zig_base[s]['low'] >= self.zig_limit * newATR:
                                    self.list_zig_log.append([s, self.dic_Zig_base[s]['low_position'], self.dic_Zig_base[s]['low']])

                                    self.dic_Zig_base[s]['bLastHigh'] = False

                                    self.dic_Zig_price[s][0:self.zig_count - 1] = self.dic_Zig_price[s][1:self.zig_count]
                                    self.dic_Zig_Type[s][0:self.zig_count - 1] = self.dic_Zig_Type[s][1:self.zig_count]
                                    # self.dic_Zig_Date[s][0:self.zig_count - 1] = self.dic_Zig_Date[s][1:self.zig_count]

                                    self.dic_Zig_price[s][-1] = self.dic_Zig_base[s]['low']
                                    self.dic_Zig_Type[s][-1] = -1
                                    # self.dic_Zig_Date[s][-1] = self.dic_Zig_base[s]['low_position']

                                    self.dic_Zig_base[s]['low'] = newPrice
                                    self.dic_Zig_base[s]['low_position'] = zig_day

                    if self.dic_Zig_price[s][0] != 0:
                        if s in self.dic_high_stk_position:
                            # 在持仓列表里面只管平仓
                            min_zig_temp = min(self.dic_Zig_price[s][-self.zig_sell:])
                            stop_line_temp = self.dic_high_stk_position[s]['high_price'] - self.down_ATR * self.dic_ATR[s]['ATR']
                            if newPrice < min_zig_temp or newPrice < stop_line_temp:
                                sell_list.append(s)
                        else:
                            # 不在持仓列表只管开仓
                            max_zig_temp = max(self.dic_Zig_price[s])
                            min_zig_temp = min(self.dic_Zig_price[s][-self.zig_sell:])
                            risk_temp = self.down_ATR * self.dic_ATR[s]['ATR']
                            if newPrice > max_zig_temp:
                                buy_list[s] = dict(code=s, risk=min(newPrice - min_zig_temp, risk_temp))
                else:
                    self.dic_Zig_base[s] = dict(high=dict_hist["close"][s], high_position=self.account.current_date,
                                                low=dict_hist["close"][s], low_position=self.account.current_date,
                                                bLastHigh=False)

            if len(buy_list) > 0:
                self.buy(buy_list)

            if len(sell_list) > 0:
                self.sell(sell_list)

            # 信息更新
            df_close = self.mssql.get_close_price(self.account.current_date)

            df_close = df_close.set_index('code')

            df_close = df_close.dropna(how='any')

            df_close_all = df_close.copy()

            df_position = pd.DataFrame(self.dic_high_stk_position.keys())
            if not df_position.empty:
                df_position = df_position.set_index(0)

            df_close = pd.merge(df_close, df_position, left_index=True, right_index=True, how='inner')

            dict_close = df_close.to_dict()

            for stk in df_close.index.values.tolist():
                a = dict_close['adjust_price_f'][stk]
                b = self.dic_high_stk_position[stk]['high_price']

                if a >= b:
                    self.dic_high_stk_position[stk]['high_price'] = dict_close['adjust_price_f'][stk]
                    self.dic_high_stk_position[stk]['buy_date'] = self.account.current_date

            dic_close_all = df_close_all.to_dict()

            list_df_close_all_index = set(df_close_all.index.values.tolist())

            for stk in self.account.list_position.keys():
                if stk in list_df_close_all_index:
                    self.account.list_position[stk]['new_price'] = dic_close_all['adjust_price_f'][stk]

            self.account.get_postion()

            # 计算净值变化
            if self.account.list_position:
                self.get_fundvalue(self.account)

            self.df_last_hist = hist

        except Exception, e:
            print(e)
