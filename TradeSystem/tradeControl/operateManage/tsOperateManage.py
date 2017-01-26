# encoding: UTF-8
"""
这里控制真正的买卖操作，包括生成买卖单（excel)
"""


class OperateManage(object):
    """操作管理"""
    def __init__(self):
        # 滑点
        self.slippage = 0.001
        # 买入手续费
        self.buy_commission = 0.0003
        # 卖出手续费
        self.sell_commission = 0.0013

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
                _account.list_position[_stockcode]['buy_price'] = (old_cash_use + new_cash_use) * (1 + self.slippage + self.buy_commission) / new_num
            else:
                _operatetype = 'sell'
                _account.cash -= operate_num * _price * (1 - (self.slippage + self.sell_commission))
                if new_num == 0:
                    _account.list_position.pop(_stockcode)
                else:
                    buy_cash_use = old_num * _account.list_position[_stockcode]['buy_price'] * (1 + self.slippage + self.buy_commission)
                    sell_cash_get = operate_num * _price * (1 - (self.slippage + self.sell_commission))
                    _account.list_position[_stockcode]['referencenum'] = _referencenum
                    _account.list_position[_stockcode]['buy_price'] = (buy_cash_use - sell_cash_get) / new_num
        else:
            _operatetype = 'buy'
            operate_num = new_num
            _account.cash -= operate_num * _price * (1 + self.slippage + self.buy_commission)
            _account.list_position[_stockcode] = dict(referencenum=_referencenum, buy_price=_price, new_price=0)

        if _account.cash < 0:
            print('zig调仓导致cash不足,不应该出现')

        _account.list_operate.append([_account.current_date, _stockcode, _operatetype, operate_num, _price])

    def alpha_order_to(self, _account, _position_engine, _stockcode, _referencenum, _price):
        """买卖操作"""
        new_num = _referencenum

        if _stockcode in _position_engine.alpha_position_list.keys():
            old_num = _position_engine.alpha_position_list[_stockcode]['referencenum']
            operate_num = new_num - old_num
            if operate_num >= 0:
                _operatetype = 'buy'
                _account.cash -= operate_num * _price * (1 + (self.slippage + self.buy_commission))
                old_cash_use = old_num * _position_engine.alpha_position_list[_stockcode]['buy_price']
                new_cash_use = operate_num * _price
                _position_engine.alpha_position_list[_stockcode]['referencenum'] = _referencenum
                _position_engine.alpha_position_list[_stockcode]['buy_price'] = (old_cash_use + new_cash_use) * (1 + self.slippage + self.buy_commission) / new_num
            else:
                _operatetype = 'sell'
                _account.cash -= operate_num * _price * (1 - (self.slippage + self.sell_commission))
                if new_num == 0:
                    _position_engine.alpha_position_list.pop(_stockcode)
                else:
                    buy_cash_use = old_num * _position_engine.alpha_position_list[_stockcode]['buy_price'] * (1 + self.slippage + self.buy_commission)
                    sell_cash_get = operate_num * _price * (1 - (self.slippage + self.sell_commission))
                    _position_engine.alpha_position_list[_stockcode]['referencenum'] = _referencenum
                    _position_engine.alpha_position_list[_stockcode]['buy_price'] = (buy_cash_use - sell_cash_get) / new_num
        else:
            _operatetype = 'buy'
            operate_num = new_num
            _account.cash -= operate_num * _price * (1 + self.slippage + self.buy_commission)

            _position_engine.alpha_position_list[_stockcode] = dict(referencenum=_referencenum, buy_price=_price, new_price=0)

        _position_engine.alpha_operate_list.append([_account.current_date, _stockcode, _operatetype, operate_num, _price])
