# encoding: UTF-8
from TradeSystem.tradeSystemBase.tsMssql import MSSQL


class RiskEngine:
    def __init__(self):
        # 是否启动风控
        self.active = False
        # 滑点
        self.slippage = 0.001
        # 买入手续费
        self.buy_commission = 0.0003
        # 卖出手续费
        self.sell_commission = 0.0013
        # 数据库连接MSSQL
        self.sql_conn = MSSQL(host='127.0.0.1', user='sa', pwd='windows-999', db='stocks')

    def order_to(self, _account, _stockcode, _referencenum, _price):
        """买卖操作"""
        new_num = _referencenum

        if _stockcode in _account.list_position.keys():
            old_num = _account.list_position[_stockcode]['referencenum']
            operate_num = new_num - old_num
            if operate_num >= 0:
                _operatetype = 'buy'
                _account.cash -= operate_num * _price * (1 + (self.slippage + self.buy_commission))
                old_cash_use = old_num * _account.list_position[_stockcode]['buy_price']
                new_cash_use = operate_num * _price
                _account.list_position[_stockcode]['referencenum'] = _referencenum
                _account.list_position[_stockcode]['buy_price'] = (old_cash_use + new_cash_use) * (1 + (self.slippage + self.buy_commission)) / new_num
            else:
                _operatetype = 'sell'
                _account.cash -= operate_num * _price * (1 - (self.slippage + self.sell_commission))
                if new_num == 0:
                    _account.list_position.pop(_stockcode)
                else:
                    buy_cash_use = old_num * _account.list_position[_stockcode]['buy_price'] * (1 + (self.slippage + self.buy_commission))
                    sell_cash_get = operate_num * _price * (1 - (self.slippage + self.sell_commission))
                    _account.list_position[_stockcode]['referencenum'] = _referencenum
                    _account.list_position[_stockcode]['buy_price'] = (buy_cash_use - sell_cash_get) / new_num
        else:
            _operatetype = 'buy'
            operate_num = new_num
            _account.cash -= operate_num * _price * (1 + (self.slippage + self.buy_commission))
            _account.list_position[_stockcode] = dict(referencenum=_referencenum, buy_price=_price, new_price=0)

        _account.list_operate.append([_account.current_date, _stockcode, _operatetype, operate_num, _price])

    def buy(self, _account, _buy_list, _position_engine):
        """买入"""
        dic_buy_result = {}

        buy_price = self.sql_conn.get_open_price(_account.current_date, _buy_list)
        buy_price = buy_price.set_index('stockcode')
        buy_price = buy_price[buy_price['high'] != buy_price['low']]
        dic_buy_price = buy_price.to_dict()

        for stk in buy_price.index.values:
            referencenum = int(_position_engine.dic_risk_ratio[stk] * _account.capital_base / _buy_list[stk]['risk'])

            if self.active:
                referencenum = self.buy_check(stk, referencenum)

            cash_use = referencenum * dic_buy_price['open'][stk]
            if _account.cash >= cash_use:
                self.order_to(_account, stk, referencenum, dic_buy_price['open'][stk])
                dic_buy_result[stk] = referencenum

        return dic_buy_result

    def sell(self, _account, _sell_list):
        """卖出"""
        dic_sell_result = {}

        sell_price = self.sql_conn.get_open_price(_account.current_date, _sell_list)
        sell_price = sell_price.set_index('stockcode')
        sell_price = sell_price[sell_price['high'] != sell_price['low']]
        dic_sell_price = sell_price.to_dict()

        for stk in sell_price.index.values:
            referencenum = 0

            if self.active:
                referencenum = self.sell_check(stk, 0)

            self.order_to(_account, stk, referencenum, dic_sell_price['open'][stk])
            dic_sell_result[stk] = referencenum

        return dic_sell_result

    def sell_check(self, stk, referencenum):
        """卖出控制"""
        # todo 这里增加逻辑
        return 0

    def buy_check(self, stk, referencenum):
        """买入控制"""
        # todo 这里增加逻辑
        return 0

    def switch_Engine_Status(self):
        """控制风控引擎开关"""
        self.active = not self.active


