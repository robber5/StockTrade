# coding=utf-8

"""
可以使用回测或者实盘
"""

from datetime import *
import pandas as pd
from tsTrade import Trade
from math import isnan
# from tsHedgeEngine import *


class StrategyTest(Trade):
    """策略"""

    def __init__(self, _main_engine):
        super(StrategyTest, self).__init__(_main_engine)

        # 单只股票最大亏损比例
        self.open_limit = 0.0005
        # 高点回撤止损倍数（n*ATR）
        self.down_ATR = 1.5
        # # zig数据
        # self.df_zig = pd.read_csv('full.csv')
        # 开仓后高点的位置
        self.dic_high_stk_position = {}
        # 计算最大亏损额
        self.max_limit = self.capital_base * self.open_limit

    def get_zig_by_date(self, current_day_str):
        sql_tuple = self.mssql.execquery(
            "WITH StuRank(stock, d_date, price,rank_rn) AS (SELECT stock, [date], price,DENSE_RANK() OVER(PARTITION BY stock ORDER BY [date] DESC) FROM stock_zig where date<'" + str(
                current_day_str) + "') SELECT stock,max(price),min(price),count(1) FROM StuRank WHERE rank_rn <= 4 group by stock order by stock")

        df_zig = pd.DataFrame(sql_tuple, columns=('stock', 'max_zig', 'min_zig', 'count_zig'))
        return df_zig

    # @profile
    def handle_date(self, _account):
        try:
            if _account.current_date in self.tradecalendars:

                end_str = datetime.strftime(_account.current_date, '%Y-%m-%d')

                print(end_str)

                hist = self.get_history('adjust_price_f', _account.current_date)

                get_zig_by_date = self.get_zig_by_date(end_str)

                df_buylist = pd.merge(get_zig_by_date, hist, left_on='stock', right_on=0, how='inner')

                df_buylist = df_buylist[(df_buylist['max_zig'] - df_buylist[1] < 0) & (df_buylist['count_zig'] == 4)]

                df_buylist['stock_limit'] = df_buylist[1] - df_buylist['min_zig']

                df_buylist = df_buylist.set_index('stock')

                df_buylist = df_buylist.loc[:, ['stock_limit', 'min_zig']]

                if len(df_buylist) > 0:
                    buy_list = [stk for stk in df_buylist.index.values.tolist() if stk not in self.dic_high_stk_position.keys()]
                    if len(buy_list) > 0:
                        buy_price = self.mssql.get_open_history(_account.current_date, buy_list)
                        buy_price = buy_price.set_index('stockcode')

                        df_buylist['max_limit'] = self.max_limit

                        df_buylist['amount'] = df_buylist['max_limit'] / df_buylist['stock_limit'] / 100

                        df_buylist['amount'] = (df_buylist['amount'].astype('int')) * 100

                        # 创建一个新函数:
                        for stk in buy_price.index.values:
                            cash_user = df_buylist.ix[stk].values[3] * buy_price.ix[stk].values[0]
                            if _account.cash >= cash_user:
                                self.order_to(_account, stk, df_buylist.ix[stk].values[3], buy_price.ix[stk].values[0])
                                self.dic_high_stk_position[stk] = [0, _account.current_date, df_buylist.ix[stk].values[1]]

                if len(self.dic_high_stk_position) > 0:

                    df_high_stk_position = pd.DataFrame.from_dict(self.dic_high_stk_position).T

                    df_close = self.mssql.get_stock_close(df_high_stk_position, _account.current_date)

                    df_close = df_close.set_index('code')

                    # todo 小于当前时间的2个zig点的最小值位置

                    sql_tuple_ATR = self.mssql.execquery("select code, ATR_25 FROM stock_ATR where date ='" + end_str + "'")

                    df_ATR = pd.DataFrame(sql_tuple_ATR, columns=['code', 'ATR_25'])

                    df_ATR = df_ATR.set_index('code')

                    df_temp = pd.merge(df_high_stk_position, df_ATR, left_index=True, right_index=True, how='inner')

                    hist_with_index = hist.set_index(0)

                    hist_with_index['pre_close'] = hist_with_index[1]

                    df_temp = pd.merge(df_temp, hist_with_index, left_index=True, right_index=True, how='inner')

                    sql_tuple = self.mssql.execquery(
                        "WITH StuRank(stock, d_date, price,rank_rn) AS (SELECT stock, [date], price, DENSE_RANK() OVER(PARTITION BY stock ORDER BY [date] DESC) FROM stock_zig where type='low' and date<'" + str(
                            end_str) + "') SELECT stock, price FROM StuRank WHERE rank_rn = 1")

                    df_Zig_Low = pd.DataFrame(sql_tuple, columns=['code', 'limit_zig'])

                    df_Zig_Low = df_Zig_Low.set_index('code')

                    df_temp = pd.merge(df_temp, df_Zig_Low, left_index=True, right_index=True, how='inner')

                    df_temp['min_zig'] = df_temp[2].astype('float64')

                    df_temp['high'] = df_temp[0].astype('float64')

                    df_temp['ATR_25'] = df_temp['ATR_25'].astype('float64')

                    df_temp = df_temp.dropna(how='any')

                    df_temp['high_limit'] = df_temp['high'] - self.down_ATR * df_temp['ATR_25']

                    df_temp = df_temp.loc[:, ['min_zig', 'high_limit', 'pre_close', 'limit_zig']]

                    df_temp['position'] = df_temp.T.idxmax()

                    if not df_temp.empty:
                        df_temp = df_temp[df_temp['position'] != 'pre_close']

                        if not df_temp.empty:

                            df_sell_price = self.mssql.get_open_history(_account.current_date, df_temp.index.values)

                            df_sell_price = df_sell_price.dropna(how='any')

                            df_sell_price = df_sell_price.set_index('stockcode')

                            for stk in df_sell_price.index.values:
                                self.order_to(_account, stk, 0, df_sell_price.ix[stk].values[0])
                                # print('sell：'+ str(stk))
                                self.dic_high_stk_position.pop(stk)

                    for stk in self.dic_high_stk_position.keys():
                        if df_close.ix[stk].values[0] >= self.dic_high_stk_position[stk][0]:
                            self.dic_high_stk_position[stk][0] = df_close.ix[stk].values[0]
                            self.dic_high_stk_position[stk][1] = _account.current_date

                _account.get_postion()

                # 计算净值变化
                if not _account.dfPosition.empty:
                    self.get_fundvalue(_account)

        except Exception, e:
            print(e)
