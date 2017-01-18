# encoding: UTF-8

from TradeSystem.tradeSystemBase.tsMssql import MSSQL


class HedgeEngine(object):
    """对冲引擎"""

    def __init__(self, _account, _alpha_max_position, _alpha_stop_fundvalue):
        self.slippage = 0.4
        # 数据库连接MSSQL
        self.sql_conn = MSSQL(host='127.0.0.1', user='sa', pwd='windows-999', db='stocks')
        self.account = _account
        self.alpha_max_position = _alpha_max_position
        self.futures_max_ratio = 0.1
        self.futures_min_cash_ratio = 0.1
        self.stock_max_ratio = 1.0 - self.futures_max_ratio - self.futures_min_cash_ratio
        self.alpha_stop_fundvalue = _alpha_stop_fundvalue

    def est_hedge_position(self):
        # 计算应持仓单数
        hedge_position_new = 0

        if self.account.hedge_last_month_change != 0:
            max_future_num = int((self.alpha_max_position - self.alpha_stop_fundvalue) * self.stock_max_ratio / (self.account.hedge_position[1] * 300))
            future_budget = int((self.alpha_max_position - self.alpha_stop_fundvalue) * self.futures_max_ratio * self.account.futures_leverage / (self.account.hedge_position[1] * 300))

            hedge_position_new = min(max_future_num, future_budget)

            de_cnt = 0
            tmp_cash = self.account.cash
            while tmp_cash - self.account.hedge_position[1] * hedge_position_new * 300 * 0.12 < 0:  # 现金如果不够期货持仓权益的12%, 也就是说几乎不能抵抗1天的涨停
                hedge_position_new -= 1
                tmp_cash += self.account.hedge_position[1] * 300 * (1.0 + 1.0 / self.account.futures_leverage)
                de_cnt += 1
            if de_cnt > 0:
                print('判断hedge仓位部分： 出现账户现金可能耗尽的情况，期望对冲仓位 hedge_position_new 减少' + str(de_cnt) + '张')
            if hedge_position_new > self.account.hedge_position[2]:
                print('判断hedge仓位部分： 实际对冲仓位变动: ' + str(hedge_position_new - self.account.hedge_position[2]) + '张')
            elif hedge_position_new < self.account.hedge_position[2]:
                print('判断hedge仓位部分： 实际对冲仓位变动: ' + str(hedge_position_new - self.account.hedge_position[2]) + '张')

        return hedge_position_new

    def open_hedge_trade(self, _hedge_position_new, len_alpha_position_list):
        futures_open_today = self.sql_conn.get_futures('IF01', self.account.current_date, 'open')
        index_open_today = self.sql_conn.get_index('sh000300', self.account.current_date, 'open')

        # account初始化
        if self.account.hedge_last_month_change == 0:
            if futures_open_today != 0:
                self.account.hedge_position = ['futures', futures_open_today, _hedge_position_new]
            else:
                self.account.hedge_position = ['index', futures_open_today, _hedge_position_new]
            self.account.hedge_last_month_change = self.account.current_date.month

        if len_alpha_position_list != 0:    # alpha 股票买入成功以后， 才进行hedge操作
            # 每天早晨根据alpha的仓位调整
            operate_hedge_position = _hedge_position_new - self.account.hedge_position[2]  # 需要变化的合约数

            trade_cost = 300 * self.slippage * abs(operate_hedge_position)

            if futures_open_today != 0:

                self.account.hedge_deposit += operate_hedge_position * futures_open_today
                self.account.cash -= operate_hedge_position * futures_open_today + trade_cost
            else:
                self.account.hedge_deposit += operate_hedge_position * index_open_today
                self.account.cash -= operate_hedge_position * index_open_today + trade_cost

            if self.account.cash < 0:
                print('早晨调仓部分： 出现保证金把账户现金耗尽情况')

            # 每个月第一天需要增加一次成本
            if self.account.current_date.month != self.account.hedge_last_month_change:
                self.account.cash -= 300 * (self.slippage * 2) * self.account.hedge_position[2]
                self.account.hedge_last_month_change = self.account.current_date.month

            # 调整实际持仓
            self.account.hedge_position[2] = _hedge_position_new

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
        self.account.cash = self.account.hedge_deposit + self.account.cash - hedge_deposit_new
        self.account.hedge_deposit = hedge_deposit_new

        if self.account.cash < 0:  #:
            print('收盘结算部分： 出现保证金把账户现金耗尽情况')