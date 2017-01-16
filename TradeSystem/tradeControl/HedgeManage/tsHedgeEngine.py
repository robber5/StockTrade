# encoding: UTF-8

from TradeSystem.tradeSystemBase.tsMssql import MSSQL


class HedgeEngine(object):
    """对冲引擎"""

    def __init__(self, _account, _alpha_max_position):
        self.slippage = 0.8
        # 数据库连接MSSQL
        self.sql_conn = MSSQL(host='127.0.0.1', user='sa', pwd='windows-999', db='stocks')
        self.account = _account
        self.alpha_max_position = _alpha_max_position
        self.futures_max_ratio = 0.1
        self.futures_min_cash_ratio = 0.1
        self.stock_max_ratio = 1.0 - self.futures_max_ratio - self.futures_min_cash_ratio

    def est_hedge_position(self):
        # 计算应持仓单数
        hedge_position_new = 0

        if self.account.hedge_last_month_change != 0:
            max_future_num = int(self.alpha_max_position * self.stock_max_ratio / (self.account.hedge_position[1] * 300))
            future_budget = int(self.alpha_max_position * self.futures_max_ratio * self.account.futures_leverage / (self.account.hedge_position[1] * 300))

            hedge_position_new = min(max_future_num, future_budget)

            # todo 判断 account.cash 够不够整体的alpha操作(股票, 期货, 现金预留)

        return hedge_position_new

    def open_hedge_trade(self, _hedge_position_new, len_alpha_position_list):
        futures_open_today = self.sql_conn.get_futures('IF01', self.account.current_date, 'open')
        index_open_today = self.sql_conn.get_index('sh000300', self.account.current_date, 'open')

        # account初始化
        if self.account.hedge_last_month_change == 0:
            # self.account.hedge_cash = (self.futures_max_ratio + self.futures_min_cash_ratio) * self.alpha_max_position - self.account.hedge_deposit
            self.account.cash -= self.account.hedge_deposit
            if futures_open_today != 0:
                self.account.hedge_position = ['futures', futures_open_today, _hedge_position_new]
            else:
                self.account.hedge_position = ['index', futures_open_today, _hedge_position_new]
            self.account.hedge_last_month_change = self.account.current_date.month

        if len_alpha_position_list != 0:    # alpha 股票买入成功以后， 才进行hedge操作

            # self.account.cash += (self.account.hedge_deposit + self.account.hedge_cash)

            # 调整实际持仓
            self.account.hedge_position[2] = _hedge_position_new
            # 每天早晨根据alpha的仓位调整
            trade_cost = 300 * self.slippage * abs(_hedge_position_new - self.account.hedge_position[2])

            if futures_open_today != 0:
                self.account.hedge_deposit = (_hedge_position_new - self.account.hedge_position[2]) * futures_open_today + self.account.hedge_deposit
                # self.account.hedge_cash = self.account.hedge_cash - (_hedge_position_new - self.account.hedge_position[2]) * futures_open_today - trade_cost
            else:
                self.account.hedge_deposit = (_hedge_position_new - self.account.hedge_position[2]) * index_open_today + self.account.hedge_deposit
                # self.account.hedge_cash = self.account.hedge_cash - (_hedge_position_new - self.account.hedge_position[2]) * index_open_today - trade_cost
            if self.account.cash < 0:
                print('早晨调仓部分： 出现保证金把账户现金耗尽情况，需要重新思考仓位控制方案')

            # 每个月第一天需要增加一次成本
            if self.account.current_date.month != self.account.hedge_last_month_change:
                # self.account.hedge_cash -= 300 * self.slippage * self.account.hedge_position[2]
                self.account.hedge_last_month_change = self.account.current_date.month

    def future_close_settlement(self):
        futures_value = self.sql_conn.get_futures('IF01', self.account.current_date, 'close')
        index_value = self.sql_conn.get_index('sh000300', self.account.current_date, 'close')

        if futures_value != 0:
            self.account.hedge_position[0] = 'futures'
            self.account.hedge_position[1] = futures_value
        else:
            self.account.hedge_position[0] = 'index'
            self.account.hedge_position[1] = index_value

        hedge_deposit_new = self.account.hedge_position[1] * self.account.hedge_position[2] * 300 / self.account.futures_leverage
        # self.account.hedge_cash = self.account.hedge_deposit + self.account.hedge_cash - hedge_deposit_new
        self.account.hedge_deposit = hedge_deposit_new

        if self.account.cash < 0:  #:
            print('收盘结算部分： 出现保证金把账户现金耗尽情况，需要重新思考仓位控制方案')