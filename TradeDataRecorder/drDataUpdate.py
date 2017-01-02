# coding=utf-8

import datetime
import pymssql
import pandas as pd
import math
import os
from drDataBase import DrEngine
from statsmodels import regression
import statsmodels.api as sm
import numpy as np


class DataUpdate:
    def __init__(self, _main_engine):
        self.drEngine = DrEngine(_main_engine)

    def calculate_SERDP(self, x):  # 计算lags阶以内的自相关系数，返回lags个值，分别计算序列均值，标准差
        n = len(x)
        x = np.array(x)
        numrt = 0.0
        denmt = 0.0
        for i in range(0, n - 2):
            numrt += (x[i] + x[i + 1] + x[i + 2]) * (x[i] + x[i + 1] + x[i + 2])
            denmt += (x[i] * x[i] + x[i + 1] * x[i + 1] + x[i + 2] * x[i + 2])

        return numrt / denmt

    def run(self):
        # @todo 分别做对齐
        # @todo 需要考虑记录的手段，dict为主
        # @todo 计算的过程中尽量考虑用i和range来做循环
        # @todo 注意循环的时间复杂度，尽量一次把所有需要的数据集合起来
        # @todo 考虑把所有数据的对齐变为一次循环，分别处理
        # @todo 需要读取的东西——stock_list：股票代码表；

        ms_engine = self.drEngine.mssql
        sqlite_engine = self.drEngine.sqlite

        SQL_tuple = ms_engine.execquery("SELECT DISTINCT [股票代码] FROM [stocks].[dbo].[all_financial_data] ")
        stock_list = pd.DataFrame(SQL_tuple, columns=['stock_code'])

        cursor = pd.read_sql_query("select dateS from stock_800_1M_all_factors_for_roll order by dateS desc",
                                   sqlite_engine)

        last_date_str = '1900-01-01'

        # if cursor:
        #     last_date_str = str(cursor.ix[0].values)[3:13]
        # else:
        #     last_date_str = '1900-01-01'

        for stock_code in stock_list.stock_code:
            # @todo 分别处理日、周、月、季数据
            # @todo 验证是不是存在数据，最新的是啥时候的，按日、周、月、年的顺序
            if stock_code[0:2] == 'sh':
                index_code = 'sh000001'
            elif stock_code[0:2] == 'sz':
                index_code = 'sz399001'

            print stock_code + '---< ' + index_code

            # @todo 日数据处理
            SQL_tuple_day = ms_engine.execqueryparam("SELECT [股票代码], [报告类型], [报告日期], [营业总收入] \
                FROM [stocks].[dbo].[all_financial_data] WHERE 股票代码=%s AND MONTH(报告类型)=12 \
                AND 报告日期 IS NOT NULL ORDER BY 报告类型 DESC", stock_code)
            finance_data_day = pd.DataFrame(SQL_tuple_day, columns=['code', 'report_type', 'report_date', 'operating_revenue'])
            finance_data_day.set_index('report_type', drop=False, inplace=True)  # date列设为index

            SQL_tuple_day = ms_engine.execqueryparam("SELECT  [code] \
                                                  ,[date] \
                                                  ,[high] \
                                                  ,[low] \
                                                  ,[close] \
                                                  ,[change] \
                                                  ,[traded_market_value] \
                                                  ,[market_value] \
                                                  ,[turnover] \
                                                  ,[adjust_price] \
                                                  ,[report_type] \
                                                  ,[report_date] \
                                                  ,[PE_TTM] \
                                                  ,[adjust_price_f] \
                                                  ,[PB] \
              FROM [stocks].[dbo].[stock_data]\
              WHERE code=%s AND [date]>%s ORDER BY date DESC", (stock_code, last_date_str))

            stock_data_day = pd.DataFrame(SQL_tuple_day, columns=['code', 'date', 'high', 'low', 'close', 'change', \
                                                          'traded_market_value', 'market_value', 'turnover', \
                                                          'adjust_price', 'report_type', 'report_date', \
                                                          'PE_TTM', 'adjust_price_f', 'PB'])  # 转为DataFrame 并设定列名
            stock_data_day.set_index('date', drop=False, inplace=True)  # date列设为index
            stock_data_day['Hi_adj'] = (stock_data_day['high'] / stock_data_day['close']) * stock_data_day[
                'adjust_price']  # 非因子 计算后复权后的high
            stock_data_day['Lo_adj'] = (stock_data_day['low'] / stock_data_day['close']) * stock_data_day['adjust_price']  # 非因子 计算后复权后的low

            # _1     Earning Yield 因子项 ETOP: 最近12个月净利润/最新市值,即PE的倒数
            stock_data_day['ETOP'] = 1.0 / stock_data_day['PE_TTM']
            stock_data_day['BTOP'] = 1.0 / stock_data_day['PB']
            # _2
            dateList = list(stock_data_day.index)
            for i in range(len(dateList) - 1, -1, -1):
                dateNw = dateList[i]

                # 计算STOP, 需要最近一期的年报
                rp_typeNw = stock_data_day.loc[dateNw, 'report_type']
                rp_typeMatch = finance_data_day[finance_data_day.report_type <= rp_typeNw]['operating_revenue']
                # print str(rp_typeNw.date())
                # print str(rp_typeMatch.head(1).index)
                if len(rp_typeMatch) > 0:
                    operating_revenue = rp_typeMatch.head(1)[0]
                    stock_data_day.loc[dateNw, 'STOP'] = operating_revenue / stock_data_day['market_value'][i]

                # 计算日波动率
                if len(dateList) - i > 65:
                    rList = list(stock_data_day[i:i + 65]['change'])
                    # print rList
                    stock_data_day.loc[dateNw, 'DASTD'] = math.sqrt(23.0 * sum([c * c for c in rList]))  # 65天收益率的平方和乘以23,开根

                # 计算日变相关factor
                if stock_data_day['market_value'][i] > 0:
                    stock_data_day.loc[dateNw, 'LNCAP'] = math.log(stock_data_day['market_value'][i], math.e)
                stock_data_day.loc[dateNw, 'LPRI'] = math.log(stock_data_day['close'][i], math.e)

                m1st_date = stock_data_day[str(dateNw.year) + '-' + str(dateNw.month)]['date'].tail(1)[0]
                # stock_data.loc[dateNw, 'MRA'] = sum(stock_data[dateNw:m1st_date]['change'])  # # 非因子 计算当月累积收益
                stock_data_day.loc[dateNw, 'MRA'] = (stock_data_day.loc[dateNw, 'adjust_price'] / stock_data_day.loc[
                    m1st_date, 'adjust_price']) - 1.0  # # 非因子 计算当月累积收益
                stock_data_day.loc[dateNw, 'CMRA'] = math.log(
                    (1 + max(stock_data_day[dateNw:m1st_date]['MRA'])) / (1 + min(stock_data_day[dateNw:m1st_date]['MRA'])),
                    math.e)  # 计算当月累积收益范围 CMRA

                # 需一个月数据的相关factor
                # 一个月的起点(4周)
                date1M = stock_data_day['date'][i] + datetime.timedelta(days=-(7 * 4))
                if len(stock_data_day[stock_data_day.date <= date1M]['date']) == 0:  # 上市不满一个月, 跳过此次循环
                    continue
                else:
                    date1M = stock_data_day[stock_data_day.date >= date1M]['date'].tail(1)[0]
                    sttpos1M = dateList.index(date1M)
                # print str(dateNw.weekday())+'__'+str(date1M.weekday())
                # print str(dateNw.date())+'__'+str(date1M.date())

                stock_data_day.loc[dateNw, 'STO_1M'] = sum(stock_data_day[i:sttpos1M + 1]['turnover'])
                # stock_data.loc[dateNw, 'RSTR_1M'] = sum(stock_data[i:sttpos1M + 1]['change'])
                stock_data_day.loc[dateNw, 'RSTR_1M'] = (stock_data_day.loc[dateNw, 'adjust_price'] / stock_data_day.loc[
                    date1M, 'adjust_price']) - 1.0
                stock_data_day.loc[dateNw, 'HILO'] = math.log(
                    max(stock_data_day[i:sttpos1M + 1]['Hi_adj']) / min(stock_data_day[i:sttpos1M + 1]['Lo_adj']), math.e)

                # 需三个月数据的相关factor
                # 三个月的起点(13周)
                date3M = stock_data_day['date'][i] + datetime.timedelta(days=-(7 * 13))
                if len(stock_data_day[stock_data_day.date <= date3M]['date']) == 0:  # 上市不满3个月, 跳过此次循环
                    continue
                else:
                    date3M = stock_data_day[stock_data_day.date >= date3M]['date'].tail(1)[0]
                    sttpos3M = dateList.index(date3M)
                # print str(dateNw.date())+'__'+str(date3M.date())

                stock_data_day.loc[dateNw, 'STO_3M'] = sum(stock_data_day[i:sttpos3M + 1]['turnover'])
                # stock_data.loc[dateNw, 'RSTR_3M'] = sum(stock_data[i:sttpos3M + 1]['change'])
                stock_data_day.loc[dateNw, 'RSTR_3M'] = (stock_data_day.loc[dateNw, 'adjust_price'] / stock_data_day.loc[
                    date3M, 'adjust_price']) - 1.0

                # 需六个月数据的相关factor
                # 六个月的起点(26周)
                date6M = stock_data_day['date'][i] + datetime.timedelta(days=-(7 * 26))
                if len(stock_data_day[stock_data_day.date <= date6M]['date']) == 0:  # 上市不满6个月, 跳过此次循环
                    continue
                else:
                    date6M = stock_data_day[stock_data_day.date >= date6M]['date'].tail(1)[0]
                    sttpos6M = dateList.index(date6M)
                # print str(dateNw.date())+'__'+str(date6M.date())

                stock_data_day.loc[dateNw, 'STO_6M'] = sum(stock_data_day[i:sttpos6M + 1]['turnover'])
                # stock_data.loc[dateNw, 'RSTR_6M'] = sum(stock_data[i:sttpos6M + 1]['change'])
                stock_data_day.loc[dateNw, 'RSTR_6M'] = (stock_data_day.loc[dateNw, 'adjust_price'] / stock_data_day.loc[
                    date6M, 'adjust_price']) - 1.0

                # 需12个月数据的相关factor
                # 12个月的起点(52周)
                date12M = stock_data_day['date'][i] + datetime.timedelta(days=-(7 * 52))
                if len(stock_data_day[stock_data_day.date <= date12M]['date']) == 0:  # 上市不满12个月, 跳过此次循环
                    continue
                else:
                    date12M = stock_data_day[stock_data_day.date >= date12M]['date'].tail(1)[0]
                    sttpos12M = dateList.index(date12M)

                stock_data_day.loc[dateNw, 'STO_12M'] = sum(stock_data_day[i:sttpos12M + 1]['turnover'])
                # stock_data.loc[dateNw, 'RSTR_12M'] = sum(stock_data[i:sttpos12M + 1]['change'])
                stock_data_day.loc[dateNw, 'RSTR_12M'] = (stock_data_day.loc[dateNw, 'adjust_price'] / stock_data_day.loc[
                    date12M, 'adjust_price']) - 1.0

                # 需60个月数据的相关factor
                # 60个月的起点(260周)
                date60M = stock_data_day['date'][i] + datetime.timedelta(days=-(7 * 260))
                if len(stock_data_day[stock_data_day.date <= date60M]['date']) == 0:  # 上市不满5年, 跳过此次循环
                    continue
                else:
                    date60M = stock_data_day[stock_data_day.date >= date60M]['date'].tail(1)[0]
                    sttpos60M = dateList.index(date60M)

                stock_data_day.loc[dateNw, 'STO_60M'] = sum(stock_data_day[i:sttpos60M + 1]['turnover'])
                # stock_data.loc[dateNw, 'RSTR_60M'] = sum(stock_data[i:sttpos60M + 1]['change'])
                stock_data_day.loc[dateNw, 'RSTR_60M'] = (stock_data_day.loc[dateNw, 'adjust_price'] / stock_data_day.loc[
                    date60M, 'adjust_price']) - 1.0

            # @todo stock_data_day就是单只股票——日线的数据

            # @todo 周级别的数据

            SQL_tuple_week = ms_engine.execqueryparam("select * from [dbo].[V_stock_1w] where code=%s and date>%s order by date DESC", (stock_code, last_date_str))
            Stock_data_week = pd.DataFrame(SQL_tuple_week, columns=['code', 'dateS', 'adjust_price', 'moneyS'])
            Stock_data_week.set_index('dateS', drop=False, inplace=True)  # dateS 列设为index
            for i in range(0, len(Stock_data_week) - 1):
                Stock_data_week.loc[Stock_data_week.index[i], 'stock_change'] = Stock_data_week.adjust_price[i] / Stock_data_week.adjust_price[i + 1] - 1

            SQL_tuple_week = ms_engine.execqueryparam("select * from [dbo].[V_index_1w] where index_code=%s and date>%s order by date DESC", (index_code, last_date_str))
            Index_data_week = pd.DataFrame(SQL_tuple_week, columns=['index_code', 'dateI', 'close', 'moneyI'])
            Index_data_week.set_index('dateI', drop=False, inplace=True)  # dateI 列设为index
            for i in range(0, len(Index_data_week) - 1):
                Index_data_week.loc[Index_data_week.index[i], 'index_change'] = Index_data_week.close[i] / Index_data_week.close[i + 1] - 1

            Index_data_weekVSstock = Index_data_week.reindex(list(Stock_data_week.index),
                                                             ['dateI', 'moneyI', 'close', 'index_change'])

            for i in range(len(Stock_data_week) - 2, -1, -1):
                dateNow = Stock_data_week.dateS[i]
                dateNowStr = str(dateNow.date())
                if dateNow != Index_data_weekVSstock.dateI[i]:
                    tmpVolm = Index_data_week[Index_data_week.dateI > dateNow].tail(1)
                    Index_data_weekVSstock.loc[dateNowStr, 'dateI'] = tmpVolm['dateI'][0]
                    Index_data_weekVSstock.loc[dateNowStr, 'moneyI'] = tmpVolm['moneyI'][0]
                    Index_data_weekVSstock.loc[dateNowStr, 'close'] = tmpVolm['close'][0]
                    Index_data_weekVSstock.loc[dateNowStr, 'index_change'] = tmpVolm['index_change'][0]

                Index_data_weekVSstock.loc[dateNowStr, 'V_changeI'] = Index_data_weekVSstock.moneyI[i] / Index_data_weekVSstock.moneyI[i + 1] - 1
                Stock_data_week.loc[dateNowStr, 'V_changeS'] = Stock_data_week.moneyS[i] / Stock_data_week.moneyS[
                    i + 1] - 1
                Stock_data_week.loc[dateNowStr, 'dateI'] = Index_data_weekVSstock.dateI[dateNowStr]
                Stock_data_week.loc[dateNowStr, 'closeI'] = Index_data_weekVSstock.close[dateNowStr]
                Stock_data_week.loc[dateNowStr, 'index_change'] = Index_data_weekVSstock.index_change[dateNowStr]
                if np.isnan(Index_data_weekVSstock.moneyI[i] / Index_data_weekVSstock.moneyI[i + 1] - 1):
                    print 'Index-1W-nan----------------------------------------' + dateNowStr
                    print Index_data_weekVSstock.moneyI[i]
                    print Index_data_weekVSstock.moneyI[i + 1]
                if np.isnan(Stock_data_week.moneyS[i] / Stock_data_week.moneyS[i + 1] - 1):
                    print 'Stock-1W-nan----------------------------------------' + dateNowStr
                    print Stock_data_week.moneyS[i]
                    print Stock_data_week.moneyS[i + 1]

            i = len(Stock_data_week) - 1
            j = i - 60
            while j >= 0:
                X = Index_data_weekVSstock.V_changeI[j:i]
                Y = Stock_data_week.V_changeS[j:i]
                bench = sm.add_constant(X)
                model = regression.linear_model.OLS(Y, bench).fit()
                alpha = model.params[0]
                beta = model.params[1]
                Stock_data_week.loc[Stock_data_week.index[j], 'VOLBT'] = beta

                # 循环
                i -= 1
                j = i - 60

            # @todo Stock_data_week就是周数据

            # @todo 月数据记录
            # 获取股票和指数月收益率数据
            SQL_tuple_month = ms_engine.execqueryparam("select * from dbo.v_stock_1m a left join v_index_1m b on a.stock_date = b.index_date \
            where code =%s and index_code = %s and stock_date>%s order by a.stock_date DESC ", (stock_code, index_code, last_date_str))
            Stock_data_month = pd.DataFrame(SQL_tuple_month,
                                     columns=['code', 'stock_trade_date', 'adjust_price', 'stock_date', \
                                              'index_code', 'index_trade_dateI', 'index_close', 'index_date'])
            Stock_data_month.set_index('stock_date', drop=False, inplace=True)  # lastdate 列设为index
            # print Stock_data_month
            for i in range(0, len(Stock_data_month) - 1):
                Stock_data_month.loc[Stock_data_month.index[i], 'stock_change'] = Stock_data_month.adjust_price[i] / Stock_data_month.adjust_price[
                    i + 1] - 1
                Stock_data_month.loc[Stock_data_month.index[i], 'index_change'] = Stock_data_month.index_close[i] / Stock_data_month.index_close[
                    i + 1] - 1
                # print Stock_data_month.stock_date[i]

            # print Stock_data_month

            i = len(Stock_data_month)
            j = i - 60
            while j >= 0:
                # print Stock_data_month.index_change[j:i]
                X = Stock_data_month.index_change[j:i]
                Y = Stock_data_month.stock_change[j:i]
                bench = sm.add_constant(X)
                model = regression.linear_model.OLS(Y, bench).fit()
                # print Stock_data_month.index[j]
                alpha = model.params[0]
                beta = model.params[1]
                Stock_data_month.loc[Stock_data_month.index[j], 'ALPHA'] = alpha
                Stock_data_month.loc[Stock_data_month.index[j], 'BETA'] = beta

                Y_hat = X * beta + alpha
                residual = Y - Y_hat
                # print residual

                sigma = np.std(residual)
                Stock_data_month.loc[Stock_data_month.index[j], 'SIGMA'] = sigma
                Stock_data_month.loc[Stock_data_month.index[j], 'BTSG'] = sigma * beta
                Stock_data_month.loc[Stock_data_month.index[j], 'SERDP'] = self.calculate_SERDP(residual)

                # 循环
                i -= 1
                j = i - 60

            # @todo Stock_data_month就是个股的月度数据

            # @todo 记录季度数据
            SQL_tuple_qt = ms_engine.execqueryparam("select code,Year(date),  \
                (select top 1 market_value from stocks.dbo.stock_data b where b.code = a.code and Year(b.date)=year(a.date) order by date desc)  \
                from stocks.dbo.stock_data a where code=%s and date>%s  group by code,Year(date) order by code,Year(date) ", (stock_code, last_date_str))
            MkVl_1Y = pd.DataFrame(SQL_tuple_qt, columns=['code', 'year', 'market_value'])
            MkVl_1Y.set_index('year', drop=False, inplace=True)

            SQL_tuple_qt = ms_engine.execqueryparam("select code,Year(date),  \
                (select top 1 market_value from stocks.dbo.stock_data b where b.code = a.code and Year(b.date)=year(a.date) order by date desc)  \
                from stocks.dbo.stock_data a where code=%s and date>%s  group by code,Year(date) order by code,Year(date) ", (stock_code, last_date_str))
            MkVl_1Y = pd.DataFrame(SQL_tuple_qt, columns=['code', 'year', 'market_value'])
            MkVl_1Y.set_index('year', drop=False, inplace=True)  # year 列设为index
            # print MkVl_1Y

            # 获取年报的营业收入数据
            SQL_tuple = ms_engine.execqueryparam("SELECT [股票代码], [报告类型], [报告日期], [总市值], [营业总收入], [营业利润],  \
                [净利润], [资产总计], [负债合计], [长期负债合计], [其中：优先股]  \
                FROM [stocks].[dbo].[all_financial_data] WHERE 股票代码=%s and 报告日期>%s  AND 报告日期 IS NOT NULL  \
                AND 营业总收入 IS NOT NULL  \
                AND 净利润 IS NOT NULL  \
                ORDER BY 报告类型 DESC", (stock_code, last_date_str))
            stock_data_qt = pd.DataFrame(SQL_tuple,
                                        columns=['code', 'report_type', 'report_date', 'market_value', 'op_earning', 'op_profit', 'net_profit', 'total_assets', 'total_liabilities', 'L_term_liabilities', 'preference_share'])
            stock_data_qt.set_index('report_type', drop=False, inplace=True)  # date列设为index

            for i in range(0, len(stock_data_qt) - 1):
                typeNow = stock_data_qt.report_type[i]
                typePre = stock_data_qt.report_type[i + 1]
                stock_data_qt.loc[typeNow, 'report_term'] = round((typeNow - typePre).days / 30.0)
                if typePre.month == 12:
                    stock_data_qt.loc[typeNow, 'qt_net_pr   ofit'] = stock_data_qt.loc[typeNow, 'net_profit']
                    stock_data_qt.loc[typeNow, 'qt_op_profit'] = stock_data_qt.loc[typeNow, 'op_profit']
                    stock_data_qt.loc[typeNow, 'qt_op_earning'] = stock_data_qt.loc[typeNow, 'op_earning']
                else:
                    stock_data_qt.loc[typeNow, 'qt_net_profit'] = stock_data_qt['net_profit'][i] - stock_data_qt['net_profit'][i + 1]
                    stock_data_qt.loc[typeNow, 'qt_op_profit'] = stock_data_qt['op_profit'][i] - stock_data_qt['op_profit'][i + 1]
                    stock_data_qt.loc[typeNow, 'qt_op_earning'] = stock_data_qt['op_earning'][i] - stock_data_qt['op_earning'][i + 1]

            rpList = list(stock_data_qt.report_type)
            for i in range(0, len(stock_data_qt) - 1):
                # for i in range(0, 5):
                typeNow = stock_data_qt.report_type[i]
                flg_60months = 0
                flg_12months = 0
                L_months = 0
                L_m_to_5Y = 60
                L_m_to_12M = 12
                qt5Y_nt_prftList = []
                qt12M_nt_prftList = []
                qt12M_op_earnList = []
                qt12M_op_prftList = []
                tt_asstList = []
                rp_termList = []
                for j in range(i, len(stock_data_qt) - 1):
                    L_months += stock_data_qt['report_term'][j]
                    if L_months == 60 and flg_60months == 0:
                        # print str(stock_data_qt['report_type'][i].date())+'---'+str(stock_data_qt['report_type'][j].date())
                        qt5Y_nt_prftList.append(stock_data_qt.qt_net_profit[j])
                        tt_asstList.append(stock_data_qt.total_assets[j])
                        rp_termList.append(stock_data_qt.report_term[j])
                        flg_60months = 1
                    elif L_months > 60 and flg_60months == 0:
                        wt_upto60m = L_m_to_5Y / stock_data_qt['report_term'][j]
                        # print str(stock_data_qt['report_type'][i].date())+'---'+str(stock_data_qt['report_type'][j].date())+' xxxxx '+str(wt_upto60m)
                        qt5Y_nt_prftList.append(stock_data_qt.qt_net_profit[j] * wt_upto60m)
                        tt_asstList.append(stock_data_qt.total_assets[j])
                        rp_termList.append(stock_data_qt.report_term[j])
                        flg_60months = 1
                    elif flg_60months == 0:
                        qt5Y_nt_prftList.append(stock_data_qt.qt_net_profit[j])
                        tt_asstList.append(stock_data_qt.total_assets[j])
                        rp_termList.append(stock_data_qt.report_term[j])
                        L_m_to_5Y = 60 - L_months

                    if L_months == 12 and flg_12months == 0:
                        # print str(stock_data_qt['report_type'][i].date())+'---'+str(stock_data_qt['report_type'][j].date())
                        qt12M_nt_prftList.append(stock_data_qt.qt_net_profit[j])
                        qt12M_op_earnList.append(stock_data_qt.qt_op_earning[j])
                        qt12M_op_prftList.append(stock_data_qt.qt_op_profit[j])
                        flg_12months = 1
                    elif L_months > 12 and flg_12months == 0:
                        wt_upto12m = L_m_to_12M / stock_data_qt['report_term'][j]
                        # print str(stock_data_qt['report_type'][i].date())+'---'+str(stock_data_qt['report_type'][j].date())+' xxxxx '+str(wt_upto12m)
                        qt12M_nt_prftList.append(stock_data_qt.qt_net_profit[j] * wt_upto12m)
                        qt12M_op_earnList.append(stock_data_qt.qt_op_earning[j] * wt_upto12m)
                        qt12M_op_prftList.append(stock_data_qt.qt_op_profit[j] * wt_upto12m)
                        flg_12months = 1
                    elif flg_12months == 0:
                        qt12M_nt_prftList.append(stock_data_qt.qt_net_profit[j])
                        qt12M_op_earnList.append(stock_data_qt.qt_op_earning[j])
                        qt12M_op_prftList.append(stock_data_qt.qt_op_profit[j])
                        L_m_to_12M = 12 - L_months

                # 计算 ETP5 : 最近五年平均净利润/最近五年每年 12 月 31 日市
                yearNow = stock_data_qt.report_date[i].year
                if len(MkVl_1Y[MkVl_1Y.year == yearNow - 5]) > 0 and flg_60months == 1:
                    mean_nt_prft_5Y = sum(qt5Y_nt_prftList) * 0.2
                    mean_mkt_val_5Y = sum(MkVl_1Y.loc[yearNow - 5:yearNow - 1]['market_value']) * 0.2
                    stock_data_qt.loc[typeNow, 'ETP5'] = mean_nt_prft_5Y / mean_mkt_val_5Y

                # 计算 AGRO : 最近五年总资产的回归系数/最近五年平均总资产
                if flg_60months == 1 and len(tt_asstList) > 1 and len(rp_termList) > 1:  # 超过5年的研报才做回归
                    tt_asstList.reverse()
                    rp_termList.reverse()

                    mean_tt_asst_5Y = sum(tt_asstList) / len(tt_asstList)
                    # print mean_tt_asst_5Y
                    rp_termList[0] /= 12.0
                    tt_asstList[0] /= mean_tt_asst_5Y
                    for k in range(1, len(rp_termList)):
                        rp_termList[k] = (rp_termList[k] / 12.0 + rp_termList[k - 1])
                        tt_asstList[k] /= mean_tt_asst_5Y

                    Y = np.transpose(tt_asstList)
                    X = np.transpose(rp_termList)
                    # print Y
                    # print X

                    bench = sm.add_constant(X)
                    model = regression.linear_model.OLS(Y, bench).fit()
                    alpha = model.params[0]
                    beta = model.params[1]

                    stock_data_qt.loc[typeNow, 'AGRO'] = beta

                # 计算 最近 12 个月的滚动因子
                if flg_12months == 1:  # 研报覆盖期超过12个月
                    tot_asset = stock_data_qt.total_assets[i]
                    net_asset = tot_asset - stock_data_qt.total_liabilities[i]
                    op_earn_12M = sum(qt12M_op_earnList)
                    op_prft_12M = sum(qt12M_op_prftList)
                    ne_prft_12M = sum(qt12M_nt_prftList)

                    # 计算 T_GPM : 最近 12 个月毛利润/最近 12 个月营业收入
                    stock_data_qt.loc[typeNow, 'T_GPM'] = op_prft_12M / op_earn_12M
                    # 计算 T_NPM : 最近 12 个月净利润/最近 12 个月营业收入
                    stock_data_qt.loc[typeNow, 'T_NPM'] = ne_prft_12M / op_earn_12M
                    # 计算 T_ROE : 最近 12 个月净利润/最新报告期净资产
                    stock_data_qt.loc[typeNow, 'T_ROE'] = ne_prft_12M / net_asset
                    # 计算 T_ROA : 最近 12 个月净利润/最新报告期总资产
                    stock_data_qt.loc[typeNow, 'T_ROA'] = ne_prft_12M / tot_asset

                # 计算 单季 因子
                # 计算 MLEV : 市值杠杆 （市值+优先股权+长期负债）/市值
                L_term_lib = stock_data_qt.loc[typeNow, 'L_term_liabilities']
                prfr_share = stock_data_qt.loc[typeNow, 'preference_share']
                mkt_value = stock_data_qt.loc[typeNow, 'market_value']
                if str(L_term_lib) != 'None':
                    if np.isnan(L_term_lib) == False and L_term_lib > 0:
                        # print str(prfr_share)
                        if str(prfr_share) == 'None':
                            stock_data_qt.loc[typeNow, 'MLEV'] = 1.0 + L_term_lib / mkt_value
                        elif np.isnan(prfr_share):
                            stock_data_qt.loc[typeNow, 'MLEV'] = 1.0 + L_term_lib / mkt_value
                        else:
                            stock_data_qt.loc[typeNow, 'MLEV'] = 1.0 + (L_term_lib + prfr_share) / mkt_value

                # 计算 S_GPM : 单季毛利率 当年单季毛利润/当年单季营业收入
                stock_data_qt.loc[typeNow, 'S_GPM'] = stock_data_qt.qt_op_profit[i] / stock_data_qt.qt_op_earning[
                    i]
                # 计算 S_NPM : 单季净利率 当年单季净利润/当年单季营业收入
                stock_data_qt.loc[typeNow, 'S_NPM'] = stock_data_qt.qt_net_profit[i] / stock_data_qt.qt_op_earning[
                    i]
                # 计算 S_ROE :单季 ROE  当年单季净利润/最新报告期净资产
                stock_data_qt.loc[typeNow, 'S_ROE'] = stock_data_qt.qt_net_profit[i] / (stock_data_qt.total_assets[i] - stock_data_qt.total_liabilities[i])
                # 计算 S_ROA :单季 ROA 	当年单季净利润/最新报告期总资产
                stock_data_qt.loc[typeNow, 'S_ROA'] = stock_data_qt.qt_net_profit[i] / stock_data_qt.total_assets[
                    i]

                # 计算 C_GPM : 累计毛利率  当年累计毛利润/当年累计营业收入
                stock_data_qt.loc[typeNow, 'C_GPM'] = stock_data_qt.op_profit[i] / stock_data_qt.op_earning[i]
                # 计算 C_NPM : 累计净利率  当年累计净利润/当年累计营业收入
                stock_data_qt.loc[typeNow, 'C_NPM'] = stock_data_qt.net_profit[i] / stock_data_qt.op_earning[i]
                # 计算 C_ROE : 累计 ROE    当年累计净利润/最新报告期净资产
                stock_data_qt.loc[typeNow, 'C_ROE'] = stock_data_qt.net_profit[i] / (stock_data_qt.total_assets[i] - stock_data_qt.total_liabilities[i])
                # 计算 C_ROA : 累计 ROA    当年累计净利润/最新报告期总资产
                stock_data_qt.loc[typeNow, 'C_ROA'] = stock_data_qt.net_profit[i] / stock_data_qt.total_assets[i]

            # @todo stock_data_qt就是单只股票季度数据

            # @进行当月对齐
            # 读取周数据
            Wdata = Stock_data_week.loc[:, ['code', 'dateS', 'dateI', 'VOLBT', 'stock_change', 'index_change']]
            Wdata.set_index('dateS', drop=False, inplace=True)  # dateS 列设为index
            # print Wdata

            # 读取日数据
            Ddata = stock_data_day.loc[:, ['code', 'date', 'report_type', 'ETOP', 'STO_1M', 'STO_3M', 'STO_6M', 'STO_12M', 'STO_60M'  \
              , 'RSTR_1M', 'RSTR_3M', 'RSTR_6M', 'RSTR_12M', 'LNCAP', 'BTOP', 'STOP', 'HILO', 'DASTD', 'LPRI', 'CMRA']]
            Ddata.set_index('date', drop=False, inplace=True)

            # 读取月数据
            Mdata = Stock_data_month.loc[:, ['code', 'dateS', 'dateI', 'ALPHA', 'BETA', 'SIGMA', 'BTSG', 'SERDP', 'stock_change', 'index_change']]
            Mdata.set_index('dateS', drop=False, inplace=True)

            # 读取季数据
            Qdata = stock_data_qt.loc[:, ['code', 'report_type', 'ETP5', 'AGRO', 'MLEV', 'S_GPM', 'C_GPM', 'T_GPM' \
                                     , 'S_NPM', 'C_NPM', 'T_NPM', 'S_ROE', 'C_ROE', 'T_ROE', 'S_ROA', 'C_ROA',
                                          'T_ROA']]
            Qdata.set_index('report_type', drop=False, inplace=True)

            # 主时间轴 设为 "月"
            df = pd.DataFrame(Mdata.loc[:, ['code', 'dateS', 'dateI']], index=Mdata.dateS,
                              columns=['code', 'dateS', 'dateI', 'report_type', 'VOLBT' \
                                  , 'ETOP', 'STO_1M', 'STO_3M', 'STO_6M', 'STO_12M', 'STO_60M' \
                                  , 'RSTR_1M', 'RSTR_3M', 'RSTR_6M', 'RSTR_12M', 'LNCAP', 'BTOP', 'STOP', 'HILO',
                                       'DASTD', 'LPRI', 'CMRA' \
                                  , 'ALPHA', 'BTSG', 'SERDP', 'BETA', 'SIGMA' \
                                  , 'ETP5', 'AGRO', 'MLEV', 'S_GPM', 'C_GPM', 'T_GPM' \
                                  , 'S_NPM', 'C_NPM', 'T_NPM', 'S_ROE', 'C_ROE', 'T_ROE', 'S_ROA', 'C_ROA', 'T_ROA'
                                  , 'stock_change', 'index_change'])

            for i in range(0, len(Mdata) - 1):
                # print i
                dateNw = Mdata.dateS[i]
                datePre = Mdata.dateS[i + 1]
                # print str(dateNw.date()) + '-----' + str(datePre.date())
                rp_typeNw = Ddata.report_type[dateNw]
                rp_typePre = Ddata.report_type[datePre]
                # print str(rp_typeNw.date())

                df.loc[dateNw, 'report_type'] = rp_typePre.date()  # 对上一期的!!!!!!

                # 周factors 对齐
                tmpVolm = Wdata[Wdata.dateS <= datePre].head(1)  # 对上一期的!!!!!!
                if len(tmpVolm) > 0:
                    df.loc[dateNw, 'VOLBT'] = tmpVolm['VOLBT'][0]
                # 日factors 对齐
                # 与价格无关的因子, 对上一期的!!!!!!
                df.loc[dateNw, ['STO_1M', 'STO_3M', 'STO_6M', 'STO_12M', 'STO_60M']] \
                    = Ddata.loc[datePre, ['STO_1M', 'STO_3M', 'STO_6M', 'STO_12M', 'STO_60M']]
                # 与绝对价格有关的因子, 对齐上期, 但不受停牌影响
                df.loc[dateNw, ['ETOP', 'LNCAP', 'BTOP', 'STOP', 'LPRI']] \
                    = Ddata.loc[datePre, ['ETOP', 'LNCAP', 'BTOP', 'STOP', 'LPRI']]
                # 与收益率相关的因子, 对其上期, 同时受到停牌影响
                if ((dateNw - datePre).days < 57):
                    df.loc[dateNw, ['RSTR_1M', 'RSTR_3M', 'RSTR_6M', 'RSTR_12M', 'HILO', 'DASTD', 'CMRA']] \
                        = Ddata.loc[datePre, ['RSTR_1M', 'RSTR_3M', 'RSTR_6M', 'RSTR_12M', 'HILO', 'DASTD', 'CMRA']]

                # 月factors 对齐  全部都去对上一期的!!!!!! 除了 stock_change 和 index_change !!!!!!!
                df.loc[dateNw, ['ALPHA', 'BTSG', 'SERDP', 'BETA', 'SIGMA']] \
                    = Mdata.loc[datePre, ['ALPHA', 'BTSG', 'SERDP', 'BETA', 'SIGMA']]
                df.loc[dateNw, ['stock_change', 'index_change']] \
                    = Mdata.loc[dateNw, ['stock_change', 'index_change']]
                # 季factors 对齐  全部都去对上一期的!!!!!!
                tmpVolm = Qdata[Qdata.report_type == rp_typePre]  # 对上一期的!!!!!!
                if len(tmpVolm) > 0:
                    # print tmpVolm
                    df.loc[dateNw, ['ETP5', 'AGRO', 'MLEV', 'S_GPM', 'C_GPM', 'T_GPM' \
                        , 'S_NPM', 'C_NPM', 'T_NPM', 'S_ROE', 'C_ROE', 'T_ROE', 'S_ROA', 'C_ROA', 'T_ROA']] \
                        = tmpVolm.loc[tmpVolm.report_type[0], ['ETP5', 'AGRO', 'MLEV', 'S_GPM', 'C_GPM', 'T_GPM' \
                        , 'S_NPM', 'C_NPM', 'T_NPM', 'S_ROE', 'C_ROE', 'T_ROE', 'S_ROA', 'C_ROA', 'T_ROA']]

            # print df.T
            df.to_sql('stock_all_1M_all_factors_for_weight_new', sqlite_engine, if_exists='append', index=False,
                      chunksize=100000)

            for i in range(0, len(Mdata) - 1):
                # print i
                dateNw = Mdata.dateS[i]
                # datePre = Mdata.dateS[i+1]
                # print str(dateNw.date()) + '-----' + str(datePre.date())
                rp_typeNw = Ddata.report_type[dateNw]
                # print str(rp_typeNw.date())

                df.loc[dateNw, 'report_type'] = rp_typeNw.date()

                # 周factors 对齐
                tmpVolm = Wdata[Wdata.dateS <= dateNw].head(1)
                if len(tmpVolm) > 0:
                    df.loc[dateNw, 'VOLBT'] = tmpVolm['VOLBT'][0]
                # 日factors 对齐
                # 与价格无关的因子, 对齐当前因子
                df.loc[dateNw, ['STO_1M', 'STO_3M', 'STO_6M', 'STO_12M', 'STO_60M']] \
                    = Ddata.loc[dateNw, ['STO_1M', 'STO_3M', 'STO_6M', 'STO_12M', 'STO_60M']]
                # 与绝对价格有关的因子, 对齐当期, 但不受停牌影响
                df.loc[dateNw, ['ETOP', 'LNCAP', 'BTOP', 'STOP', 'LPRI']] \
                    = Ddata.loc[dateNw, ['ETOP', 'LNCAP', 'BTOP', 'STOP', 'LPRI']]
                # 与收益率相关的因子, 对其当期, 同时受到停牌影响
                df.loc[dateNw, ['RSTR_1M', 'RSTR_3M', 'RSTR_6M', 'RSTR_12M', 'HILO', 'DASTD', 'CMRA']] \
                    = Ddata.loc[dateNw, ['RSTR_1M', 'RSTR_3M', 'RSTR_6M', 'RSTR_12M', 'HILO', 'DASTD', 'CMRA']]

                # 月factors 对齐
                df.loc[dateNw, ['ALPHA', 'BTSG', 'SERDP', 'BETA', 'SIGMA', 'stock_change', 'index_change']] \
                    = Mdata.loc[dateNw, ['ALPHA', 'BTSG', 'SERDP', 'BETA', 'SIGMA', 'stock_change', 'index_change']]
                # 季factors 对齐
                tmpVolm = Qdata[Qdata.report_type == rp_typeNw]
                if len(tmpVolm) > 0:
                    # print tmpVolm
                    df.loc[dateNw, ['ETP5', 'AGRO', 'MLEV', 'S_GPM', 'C_GPM', 'T_GPM' \
                        , 'S_NPM', 'C_NPM', 'T_NPM', 'S_ROE', 'C_ROE', 'T_ROE', 'S_ROA', 'C_ROA', 'T_ROA']] \
                        = tmpVolm.loc[tmpVolm.report_type[0], ['ETP5', 'AGRO', 'MLEV', 'S_GPM', 'C_GPM', 'T_GPM' \
                        , 'S_NPM', 'C_NPM', 'T_NPM', 'S_ROE', 'C_ROE', 'T_ROE', 'S_ROA', 'C_ROA', 'T_ROA']]

            # print df.T
            df.to_sql('stock_all_1M_all_factors_for_roll_new', sqlite_engine, if_exists='append', index=False, chunksize=100000)






