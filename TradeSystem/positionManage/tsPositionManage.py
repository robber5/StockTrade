# coding=utf-8

import math
from TradeSystem.alphaModel.tsAlphaWeight import StockScreener
from TradeSystem.tradeSystemBase.tsMssql import MSSQL


class PositionEngine:
    def __init__(self):
        # 单只股票最大可亏损比例
        self.risk_ratio_once = 0.0005
        self.dic_risk_ratio = {}
        self.target_pool = []
        self.active = True

        # alpha 相关的参数设置

        # alpha 模型开关
        self.alpha_active = True
        self.alpha_period = 'Month'   # todo 目前只支持 'Month'
        # alpha测算所用股票池
        self.alpha_stock_pool = 'All'
        # alpha模型建议持仓股票数量
        self.alpha_stock_num = 50
        self.alpha_start_date = '2007-04-01'
        # 取当日之前alpha_est_len个自然的数据做线性回归
        self.alpha_est_len = 365 * 2
        # alpha模型的建议持仓列表
        self.alpha_buy_list = []
        # alpha实际买卖操作
        self.alpha_operate_list = []
        # alpha实际持仓列表，包括实际最后买入的数量，买入的价格，当前的价格，一共3项
        self.alpha_position_list = {}
        self.alpha_risk_levels = 5.0
        self.stockScreener = StockScreener(self.alpha_stock_pool, self.alpha_period, self.alpha_start_date)
        self.sql_conn = MSSQL(host='127.0.0.1', user='sa', pwd='windows-999', db='stocks')

        # 大盘判断相关参数

        # 大盘趋势 模型开关
        self.market_trend_active = False
        self.alpha_position_ratio = 0.4
        self.break_position_ratio = 0.0
        self.future_short_ratio = 0.0

    def get_alpha_fundvalue(self):
        """alpha的净值"""
        alpha_fundvalue = 0
        for item in self.alpha_position_list:
            alpha_fundvalue += self.alpha_position_list[item]['referencenum'] * self.alpha_position_list[item][
                'new_price']
        return alpha_fundvalue

    def update_alpha_position_price(self, current_date):
        df_close = self.sql_conn.get_close_price(current_date)

        df_close = df_close.set_index('code')

        df_close = df_close.dropna(how='any')

        dic_close_all = df_close.to_dict()

        list_df_close_index = set(df_close.index.values.tolist())

        for stk in self.alpha_position_list.keys():
            if stk in list_df_close_index:
                self.alpha_position_list[stk]['new_price'] = dic_close_all['adjust_price_f'][stk]

    def position_manage(self, current_date):
        if self.active:
            if self.alpha_active:
                if self.stockScreener.handle_date(current_date):
                    df_stock_weight = self.stockScreener.get_weight_list(self.alpha_est_len)
                    # 生成alpha因子模型的持仓列表
                    df_tmp = df_stock_weight.head(self.alpha_stock_num)
                    self.alpha_buy_list = df_tmp['code'].values
                    # 生成alpha因子风险权重列表
                    risk_sort_list = df_stock_weight['code'].values.tolist()
                    cnt = 0
                    lenx = 1.0 / len(risk_sort_list)
                    for s in risk_sort_list:
                        risk_weight = math.ceil((1.0 - cnt * lenx) * self.alpha_risk_levels) / self.alpha_risk_levels
                        self.dic_risk_ratio[s] = risk_weight * self.risk_ratio_once
                        cnt += cnt
                    # print self.dic_risk_ratio
            # todo 根据大盘分级配置各模型仓位
            if self.market_trend_active:
                # todo 调用market_trend判断的模型
                # todo 调用持仓分配模型 <- 需传参数: self.alpha_active, market_trend判断结果
                # todo 调用持仓分配模型 -> 传出参数: self.*_ratio 各模型的持仓 ratio
                pass

    def stock_risk_ration_init(self):
        if self.alpha_active and not self.active:
            for s in self.target_pool:
                self.dic_risk_ratio[s] = self.risk_ratio_once / self.alpha_risk_levels
        else:
            for s in self.target_pool:
                self.dic_risk_ratio[s] = 1.0 * self.risk_ratio_once

    def switch_Engine_Status(self):
        """控制仓控引擎开关"""
        self.active = not self.active
