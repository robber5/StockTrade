# encoding: UTF-8

from TradeSystem.tradeSystemBase.tsMssql import MSSQL
from TradeSystem.tradeControl.operateManage.tsOperateManage import OperateManage
from TradeSystem.tradeControl.HedgeManage.tsHedgeEngine import HedgeEngine
from TradeSystem.tradeSystemBase.tsFunction import *


class RiskEngine:
    def __init__(self, _position_engine, _account):
        # 是否启动风控
        self.active = True
        # 风控的引擎
        self.position_engine = _position_engine
        # 账户内容
        self.account = _account
        # 数据库连接MSSQL
        self.sql_conn = MSSQL(host='127.0.0.1', user='sa', pwd='windows-999', db='stocks')
        alpha_max_position = self.account.list_fundvalue[-1][1] * self.account.capital_base * self.position_engine.alpha_position_ratio
        # 减去停牌的个股后进行对冲
        self.hedge_engine = HedgeEngine(self.account, alpha_max_position, self.position_engine.get_alpha_stop_fundvalue())

    def buy(self, _buy_list):
        """买入"""
        dic_buy_result = {}

        buy_price = self.sql_conn.get_open_price(self.account.current_date, _buy_list)
        buy_price = buy_price.set_index('stockcode')
        buy_price = buy_price[buy_price['high'] != buy_price['low']]
        dic_buy_price = buy_price.to_dict()

        for stk in buy_price.index.values:
            referencenum = int(
                self.position_engine.dic_risk_ratio[stk] * self.account.capital_base / _buy_list[stk]['risk'])

            if self.active:
                referencenum = self.buy_check(stk, referencenum, dic_buy_price['open'][stk])

                OperateManage().order_to(self.account, stk, referencenum, dic_buy_price['open'][stk])
                dic_buy_result[stk] = referencenum

        return dic_buy_result

    def sell(self, _sell_list):
        """卖出"""
        dic_sell_result = {}

        sell_price = self.sql_conn.get_open_price(self.account.current_date, _sell_list)
        sell_price = sell_price.set_index('stockcode')
        sell_price = sell_price[sell_price['high'] != sell_price['low']]
        dic_sell_price = sell_price.to_dict()

        for stk in sell_price.index.values:
            referencenum = 0

            if self.active:
                referencenum = self.sell_check(stk, 0)

            OperateManage().order_to(self.account, stk, referencenum, dic_sell_price['open'][stk])
            dic_sell_result[stk] = referencenum

        return dic_sell_result

    def sell_check(self, stk, referencenum):
        """卖出控制"""
        # todo 这里增加逻辑
        return 0

    def buy_check(self, stk, referencenum, price):
        """买入控制"""
        cash_use = referencenum * price
        rest_position = (self.account.list_fundvalue[-1][1] - self.account.list_fundvalue[-1][
            3]) * self.account.capital_base * self.position_engine.break_position_ratio
        if self.account.cash >= cash_use and rest_position >= cash_use:
            buy_referencenum = referencenum
        else:
            buy_referencenum = min(int(rest_position / price), int(self.account.cash / price))
        return buy_referencenum

    def switch_Engine_Status(self):
        """控制风控引擎开关"""
        self.active = not self.active

    def operate(self):
        """整体仓位控制"""
        # 择时部分的买卖操作
        if len(self.account.sell_list):
            sell_result = self.sell(self.account.sell_list)
            for s in sell_result:
                if sell_result[s] == 0:
                    self.account.dic_high_stk_position.pop(s)

        if self.account.list_fundvalue:
            zig_fundvalue = self.account.list_fundvalue[-1][1]
        else:
            zig_fundvalue = 1

        require_zig_fundvalue = zig_fundvalue * self.account.capital_base * self.position_engine.break_position_ratio

        real_zig_fundvalue = 0

        for item in self.account.list_position:
            real_zig_fundvalue += self.account.list_position[item]['referencenum'] * self.account.list_position[item]['new_price']

        if real_zig_fundvalue > require_zig_fundvalue:
            print('zig持仓超标，等比例减少')

            sell_price = self.sql_conn.get_open_price(self.account.current_date, self.account.list_position)

            sell_price = sell_price.set_index('stockcode')
            sell_price = sell_price[sell_price['high'] != sell_price['low']]
            dic_sell_price = sell_price.to_dict()

            for s in sell_price.index.values:
                referencenum = self.account.list_position[s]['referencenum'] * 0.9   # 非停牌股票等比例减少10%
                OperateManage().order_to(self.account, s, referencenum, dic_sell_price['open'][s])

        elif len(self.account.buy_list):
            buy_result = self.buy(self.account.buy_list)
            for s in buy_result:
                if s not in self.account.dic_high_stk_position.keys():
                    self.account.dic_high_stk_position[s] = dict(high_price=0, buy_date=self.account.current_date)

        if self.account.current_date > get_format_date(self.position_engine.alpha_start_date):
            # alpha部分的股票的买、期货的卖操作
            hedge_position_new = self.hedge_engine.est_hedge_position()
            self.alpha_buy_stock(hedge_position_new)  # alpha 股票买操作
            self.hedge_engine.open_hedge_trade(hedge_position_new, len(self.position_engine.alpha_position_list))

    def future_close_settlement(self):
        self.hedge_engine.future_close_settlement()

    def alpha_buy_stock(self, hedge_position_new):
        # hedge_position_new是手数
        """alpha买卖"""
        buy_list = self.position_engine.alpha_buy_list
        position_list = self.position_engine.alpha_position_list

        if len(buy_list) != 0:
            operate_price = self.sql_conn.get_open_price(self.account.current_date, set(buy_list).union(set(position_list.keys())))
            operate_price = operate_price.set_index('stockcode')
            operate_price = operate_price[operate_price['high'] != operate_price['low']]
            dic_operate_price = operate_price.to_dict()

            operate_dic = {}

            operate_buy_list = []
            for s in buy_list:
                if s in operate_price.index.values.tolist():
                    operate_buy_list.append(s)

            self.position_engine.alpha_stop_list = [s for s in position_list if s not in buy_list]

            for s in operate_buy_list:
                # 将alpha_position_list当中，停牌的股票减去，因为无法调仓（已经不在新一期的alpha_buy_list中的股票不计算在hedge中）
                position_list_stop = [s for s in position_list if s not in operate_price and s not in self.position_engine.alpha_stop_list]
                position_list_stop_value = 0
                for item in position_list_stop:
                    position_list_stop_value += self.position_engine.alpha_position_list[item]['referencenum'] * self.position_engine.alpha_position_list[item]['new_price']
                stock_cash = (hedge_position_new * self.account.hedge_position[1] * 300 - position_list_stop_value) / len(operate_buy_list)
                referencenum = stock_cash / dic_operate_price['open'][s]
                operate_dic[s] = referencenum

            for s in position_list.keys():
                if s in operate_price.index.values.tolist() and s not in operate_dic.keys():
                    operate_dic[s] = 0

            for s in operate_dic.keys():
                OperateManage().alpha_order_to(self.account, self.position_engine, s, operate_dic[s], dic_operate_price['open'][s])

        if self.account.cash < 0:
            print self.position_engine.get_alpha_fundvalue()
            print('alpha调仓导致cash不足')
