# encoding: UTF-8

"""
可以使用回测或者实盘
"""

from datetime import *
import pandas as pd
from tsTrade import Trade
from tsAccount import Account
# from tsHedgeEngine import *


class StrategyTest(Trade):
    """策略"""
    def __init__(self, _main_engine):
        super(StrategyTest, self).__init__(_main_engine)

        # @todo 读取参数
        # @todo 设置当前策略需要的参数（涉及的参数有从高点回撤多少之后强制止损的，单只股票开仓亏损）

        # 单只股票最大亏损比例
        self.open_limit = 0.005
        # 高点回撤止损倍数（n*ATR）
        self.down_ATR = 1.5
        # zig数据
        self.df_zig = pd.read_csv('full.csv')
        # 开仓后高点的位置
        self.df_high_stk_position = pd.DataFrame()

    def handle_date(self, _account):

        # @Todo 遍历zig文件，根据4个点来计算突破买入的位置
        # @todo 对于开仓股票计算max[开仓以来的最高点-self.down_ATR*ATR值,zig(-1)],close<该值后清

        hist = self.get_history(1, ['close'], _account.current_date)

        # for stk in self.stock_pool:
        #     df_stock_zig = (self.stock_pool[
        #         (self.stock_pool['date'] <= _account.current_date) & (self.stock_pool['stock'] == stk)
        #     ]).tail(4)
        #
        #
        #
        #     if hist['close'] >= max(df_stock_zig['price'].tolist()):
        #         order_to(stk, amount)
        #
        #
        # for stk in self.df_high_stk_position['code'].values:
        #     if hist['close'] <= max[(self.df_high_stk_position[self.df_high_stk_position['code'] == stk]]).values[0] - self.down_ATR * ATR,df_stock_zig(-1)]






