# encoding: UTF-8

"""
可以使用回测或者实盘
"""

from datetime import *
# from tsHedgeEngine import *
import pandas as pd
from tsTrade import Trade


class StrategyTest(Trade):
    """策略"""
    def __init__(self, _main_engine):
        super(StrategyTest, self).__init__()
        # @todo 读取参数
        # @todo 设置当前策略需要的参数（涉及的参数有从高点回撤多少之后强制止损的，单只股票开仓亏损）
        # 单只股票最大亏损比例
        self.open_limit = 0.005
        # 高点回撤止损倍数（n*ATR）
        self.down_ATR = 1.5
        # 数据获取
        self.mssql = _main_engine.mssqlDB

    def handle_date(self, _account):
        # @todo 遍历zig文件，根据4个点来计算突破买入的位置
        # @todo 对于开仓股票计算max[开仓以来的最高点-self.down_ATR*ATR值,zig(-1)],close<该值后清仓
        # @todo 画图功能开发未完成，回撤相关开发未完成
        for

        # pass
