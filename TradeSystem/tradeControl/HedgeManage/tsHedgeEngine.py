# encoding: UTF-8

from TradeSystem.tradeSystemBase.tsMssql import MSSQL


class HedgeEngine(object):
    """对冲引擎"""

    def __init__(self, _account, _alpha_max_position, _alpha_change_stock_flag):
        self.slippage = 0.4
        # 数据库连接MSSQL
        self.sql_conn = MSSQL(host='127.0.0.1', user='sa', pwd='windows-999', db='stocks')
        self.account = _account
        self.alpha_max_position = _alpha_max_position
        self.alpha_change_stock_flag = _alpha_change_stock_flag
        self.futures_max_ratio = 0.1
        self.futures_min_cash_ratio = 0.1
        self.stock_max_ratio = 1.0 - self.futures_max_ratio - self.futures_min_cash_ratio
        self.future_position_change = 0
        self.future_open_price = 0.0

    def est_hedge_position(self, _alpha_stop_fundvalue):
        # 计算应持仓单数
        hedge_position_new = 0

        if self.account.hedge_last_month_change != 0:
            # @test@ 非alpha调仓期 不加仓
            if self.alpha_change_stock_flag:
                max_future_num = int((self.alpha_max_position - _alpha_stop_fundvalue) * self.stock_max_ratio / (self.account.hedge_position[1] * 300))
                future_budget = int((self.alpha_max_position - _alpha_stop_fundvalue) * self.futures_max_ratio * self.account.futures_leverage / (self.account.hedge_position[1] * 300))

                hedge_position_new = min(max_future_num, future_budget)

                # @test@ 价差估计
                futures_00 = self.sql_conn.get_futures('IF00', self.account.current_date, 'open')
                futures_01 = self.sql_conn.get_futures('IF01', self.account.current_date, 'open')
                futures_02 = self.sql_conn.get_futures('IF02', self.account.current_date, 'open')
                index_open_today = self.sql_conn.get_index('sh000300', self.account.current_date, 'open')
                if futures_00 != 0 and hedge_position_new > 1:
                    if futures_01 < futures_00:
                        hedge_position_new = int(hedge_position_new * 0.65)
                    if futures_00 < index_open_today:
                        hedge_position_new = int(hedge_position_new * 0.60)
                    if futures_00 / index_open_today > 1.02:
                        hedge_position_new = int(hedge_position_new * 0.55)
            else:
                hedge_position_new = self.account.hedge_position[2]

                # @test@ 价差估计
                futures_00 = self.sql_conn.get_futures('IF00', self.account.current_date, 'open')
                futures_01 = self.sql_conn.get_futures('IF01', self.account.current_date, 'open')
                futures_02 = self.sql_conn.get_futures('IF02', self.account.current_date, 'open')
                index_open_today = self.sql_conn.get_index('sh000300', self.account.current_date, 'open')
                if futures_00 != 0:
                    if futures_01 < futures_00 and hedge_position_new > 1:
                        hedge_position_new = int(hedge_position_new * 0.95)
                    if futures_00 < index_open_today:
                        hedge_position_new = int(hedge_position_new * 0.90)
                    if futures_00 / index_open_today > 1.02:
                        hedge_position_new = int(hedge_position_new * 0.85)

            de_cnt = 0
            tmp_cash = self.account.cash
            while tmp_cash - self.account.hedge_position[1] * hedge_position_new * 300 * 0.12 < 0:  # 现金如果不够期货持仓权益的12%, 也就是说几乎不能抵抗1天的涨停
                hedge_position_new -= 1
                tmp_cash += self.account.hedge_position[1] * 300 * (1.0 + 1.0 / self.account.futures_leverage)
                de_cnt += 1
            if de_cnt > 0:
                print('判断hedge仓位部分： 出现账户现金可能耗尽的情况，期望对冲仓位 hedge_position_new 减少'+str(de_cnt)+'张')

            self.future_position_change = hedge_position_new - self.account.hedge_position[2]
            if hedge_position_new > self.account.hedge_position[2]:
                print('判断hedge仓位部分： 实际对冲仓位变动: '+str(hedge_position_new - self.account.hedge_position[2])+'张')
            elif hedge_position_new < self.account.hedge_position[2]:
                print('判断hedge仓位部分： 实际对冲仓位变动: '+str(hedge_position_new - self.account.hedge_position[2])+'张')

        return [hedge_position_new, self.future_position_change]

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
            operate_hedge_position = self.future_position_change  # 需要变化的合约数

            trade_cost = 300 * self.slippage * abs(operate_hedge_position)

            if futures_open_today != 0:
                deposit_change = operate_hedge_position * futures_open_today * 300 / self.account.futures_leverage
                self.account.hedge_deposit += deposit_change
                self.account.cash -= deposit_change + trade_cost
                self.future_open_price = futures_open_today
            else:
                deposit_change = operate_hedge_position * index_open_today * 300 / self.account.futures_leverage
                self.account.hedge_deposit += deposit_change
                self.account.cash -= deposit_change + trade_cost
                self.future_open_price = index_open_today

            if self.account.cash < 0:
                print('早晨调仓部分： 出现保证金把账户现金耗尽情况')

            # 每个月第一天需要增加一次成本
            if self.account.current_date.month != self.account.hedge_last_month_change:
                self.account.cash -= 300 * (self.slippage * 2) * self.account.hedge_position[2]
                self.account.hedge_last_month_change = self.account.current_date.month

            # 计算一次早盘期货的盈亏 ( 在self.account.hedge_position[2]更新之前 )
            self.account.cash += (self.account.hedge_position[1] - self.future_open_price) * self.account.hedge_position[2] * 300

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

        # 计算保证金变化
        hedge_deposit_new = self.account.hedge_position[1] * self.account.hedge_position[2] * 300 / self.account.futures_leverage
        self.account.cash = self.account.hedge_deposit + self.account.cash - hedge_deposit_new
        self.account.hedge_deposit = hedge_deposit_new

        # 计算一次收盘期货的盈亏
        self.account.cash += (self.future_open_price - self.account.hedge_position[1]) * self.account.hedge_position[2] * 300

        if self.account.cash < 0:  #:
            print('收盘结算部分： 出现保证金把账户现金耗尽情况')