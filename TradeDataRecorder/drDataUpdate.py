# coding=utf-8

from datetime import timedelta
import pymssql
import pandas as pd
import math
import os
from drDataBase import DrEngine
from statsmodels import regression
import statsmodels.api as sm
import numpy as np
from TradeSystem.tradeSystemBase.tsFunction import *
from WindPy import *
import sys
from sqlalchemy.types import String

def standardize_f(factor):
    # 因子1准备
    X0 = factor.copy()
    X1 = factor.copy()
    # #因子去极值处理
    m_X = np.median(X0)
    mad_X = np.median(abs(X0-m_X))
    thresh_max = m_X+15.0*mad_X
    thresh_min = m_X-15.0*mad_X
    X1[X1 > thresh_max] = thresh_max
    X1[X1 < thresh_min] = thresh_min
    # 因子值标准化
    u_X = np.mean(X1.dropna())
    std_X = np.std(X1.dropna())
    X1 = (X1-u_X)/std_X
    return X1


class DataUpdate:
    def __init__(self, _main_engine):
        self.drEngine = DrEngine(_main_engine)

        self.ms_engine = self.drEngine.mssql
        self.sqlite_engine = self.drEngine.sqlite
        SQL_tuple = self.ms_engine.execquery("SELECT DISTINCT [股票代码] FROM [stocks].[dbo].[all_financial_data] ")
        self.stock_list = pd.DataFrame(SQL_tuple, columns=['stock_code'])

    def calculate_SERDP(self, x):  # 计算lags阶以内的自相关系数，返回lags个值，分别计算序列均值，标准差
        n = len(x)
        x = np.array(x)
        numrt = 0.0
        denmt = 0.0
        for i in range(0, n - 2):
            numrt += (x[i] + x[i + 1] + x[i + 2]) * (x[i] + x[i + 1] + x[i + 2])
            denmt += (x[i] * x[i] + x[i + 1] * x[i + 1] + x[i + 2] * x[i + 2])

        return numrt / denmt

    def update_industry_change_table(self, table_name, table_type):

        ms_engine = self.ms_engine
        sqlite_engine = self.sqlite_engine

        query_line = ("select * from %(table_name)s order by [dateI] desc" % {'table_name': table_name})
        dfIndustryChange = pd.read_sql(query_line, sqlite_engine)
        stt_date = '2000-01-01'
        if len(dfIndustryChange) > 1:
            last_date_str = str(dfIndustryChange.ix[0].values)[3:13]
        else:
            last_date_str = stt_date

        if table_type is '1m':
            SQL_trade_date = ms_engine.execqueryparam("SELECT DISTINCT [trade_date] FROM [stocks].[dbo].[V_index_1m] \
                                                      where [trade_date]>'2000-01-01' order by [trade_date] ", '')
            trade_date_roll = pd.DataFrame(SQL_trade_date, columns=['dateI'])
            trade_date_roll.set_index('dateI', drop=False, inplace=True)
        elif table_type is '1w':
            index_code = 'sh000001'
            # SQL_trade_date = ms_engine.execqueryparam("SELECT DISTINCT [date] FROM [stocks].[dbo].[V_index_1w] \
            #                                           where [date]>'2000-01-01' order by [date] ", '')
            SQL_trade_date = ms_engine.execqueryparam("SELECT A.[date] FROM \
                    (SELECT V_index_1w.*,DATEPART(DW,date) as dw,row_number() OVER(order by index_code,date desc) r  FROM V_index_1w where index_code=%s) A, \
                    (SELECT V_index_1w.*,DATEPART(DW,date) as dw,row_number() OVER(order by index_code,date desc) r  FROM V_index_1w where index_code=%s) B  \
                    WHERE (A.r=B.r+1 and (B.dw<=A.dw or DATEDIFF(day,A.date,B.date)>=7)) or (A.r=1 and B.r=1) \
                    and A.[date]>'2000-01-01' order by date", (index_code, index_code))
            trade_date_roll = pd.DataFrame(SQL_trade_date, columns=['dateI'])
            trade_date_roll.set_index('dateI', drop=False, inplace=True)

        trade_date_recent = trade_date_roll[trade_date_roll.dateI > get_format_date(last_date_str)]
        if len(trade_date_recent) > 0:
            industry_list = pd.read_csv('./Industry_IndexCode.csv', encoding='utf-8')
            industry_list.columns = ['industry', 'index_code', 'until']
            end_date = trade_date_roll.dateI[len(trade_date_roll) - 1]
            date_from = get_format_date(last_date_str) + timedelta(days=-35)
            if date_from < get_format_date(stt_date):
                date_from = stt_date
            else:
                date_from = get_format_date_str(date_from)

            w.start()
            for j in range(0, len(industry_list)):
                clmnX = industry_list.industry[j]
                trade_date_roll[clmnX] = 0.0
                index_code = str(industry_list.index_code[j])
                print 'update_industry_change_table: ' + clmnX

                if table_type is '1m':
                    index_data = w.wsd(str(index_code), 'close', date_from, end_date, "Period=M")
                elif table_type is '1w':
                    index_data = w.wsd(str(index_code), 'close', date_from, end_date, "Period=W")
                if index_data.ErrorCode != 0:
                    print 'Wind Error: ' + str(index_data.ErrorCode)
                    exit(1)

                df_index_data = pd.DataFrame(index_data.Times, columns=['dateI'])
                df_index_data['close'] = pd.DataFrame(index_data.Data).T
                df_index_data.set_index('dateI', drop=False, inplace=True)
                for i in range(1, len(df_index_data)):
                    if i == len(df_index_data) - 1:  # 最新一日时, 当取改日信息
                        data0 = w.wsd(str(index_code), 'close', end_date, end_date)
                        if data0.ErrorCode != 0:
                            print 'Wind Error: ' + str(data0.ErrorCode)
                            exit(1)
                        df_index_data.loc[df_index_data.dateI[i], 'close'] = data0.Data[0][0]

                    tmp_r = df_index_data.close[i] / df_index_data.close[i - 1] - 1.0
                    df_index_data.loc[df_index_data.dateI[i], clmnX] = tmp_r
                    df_index_data.loc[df_index_data.dateI[i], 'dateI'] = df_index_data.dateI[i].date()
                    trade_date_roll.loc[df_index_data.dateI[i], clmnX] = tmp_r

            w.stop()

            trade_date_recent = trade_date_roll[trade_date_roll.dateI > get_format_date(last_date_str)].copy()
            trade_date_recent.to_sql(table_name, sqlite_engine, dtype={'dateI': String}, if_exists='append', index=False, chunksize=100000)
        print 'update_industry_change_table: len: ' + str(len(trade_date_recent))

        return 0

    def update_day_factors_table(self, table_name):
        ms_engine = self.ms_engine
        sqlite_engine = self.sqlite_engine

        query_line = ("select DISTINCT [date] from %(table_name)s where [date]>'2015-01-01' order by [date] desc"
                      % {'table_name': table_name})
        cursor = pd.read_sql(query_line, sqlite_engine)
        if len(cursor) > 1:
            last_date_str = str(cursor.ix[0].values)[3:13]
        else:
            last_date_str = '2000-01-01'

        for stock_code in self.stock_list.stock_code:
            SQL_tuple_day = ms_engine.execqueryparam("SELECT  [code],[date],[high],[low],[close],[change] \
                                                      ,[traded_market_value],[market_value],[turnover],[adjust_price] \
                                                      ,[report_type],[report_date],[PE_TTM],[adjust_price_f],[PB] \
                                                      FROM [stocks].[dbo].[stock_data] \
                                                      WHERE code=%s ORDER BY date DESC", stock_code)

            stock_data_day_all = pd.DataFrame(SQL_tuple_day, columns=['code', 'date', 'high', 'low', 'close', 'change',
                                                                      'traded_market_value', 'market_value', 'turnover',
                                                                      'adjust_price', 'report_type', 'report_date',
                                                                      'PE_TTM', 'adjust_price_f', 'PB'])  # 转为DataFrame 并设定列名
            stock_data_day_all.set_index('date', drop=False, inplace=True)  # date列设为index
            stock_data_day_all['Hi_adj'] = (stock_data_day_all['high'] / stock_data_day_all['close']) * stock_data_day_all['adjust_price']  # 非因子 计算后复权后的high
            stock_data_day_all['Lo_adj'] = (stock_data_day_all['low'] / stock_data_day_all['close']) * stock_data_day_all['adjust_price']  # 非因子 计算后复权后的low
            # 获得近期需要更新的部分
            stock_data_day_recent = stock_data_day_all[stock_data_day_all.date > get_format_date(last_date_str)].copy()

            if len(stock_data_day_recent) > 0:
                SQL_tuple_day = ms_engine.execqueryparam("SELECT [股票代码], [报告类型], [报告日期], [营业总收入] \
                                                      FROM [stocks].[dbo].[all_financial_data] WHERE 股票代码=%s \
                                                      AND MONTH(报告类型)=12 AND 报告日期 IS NOT NULL ORDER BY \
                                                      报告类型 DESC", stock_code)
                finance_data_day = pd.DataFrame(SQL_tuple_day, columns=['code', 'report_type', 'report_date', 'operating_revenue'])
                finance_data_day.set_index('report_type', drop=False, inplace=True)  # date列设为index

                # _1     Earning Yield 因子项 ETOP: 最近12个月净利润/最新市值,即PE的倒数
                stock_data_day_recent['ETOP'] = 1.0 / stock_data_day_recent['PE_TTM']
                stock_data_day_recent['BTOP'] = 1.0 / stock_data_day_recent['PB']
                # _2
                dateList_all    = list(stock_data_day_all.index)
                dateList_recent = list(stock_data_day_recent.index)
                for i in range(len(dateList_recent) - 1, -1, -1):
                    dateNw = dateList_recent[i]

                    # 计算STOP, 需要最近一期的年报
                    rp_typeNw = stock_data_day_all.loc[dateNw, 'report_type']
                    rp_typeMatch = finance_data_day[finance_data_day.report_type <= rp_typeNw]['operating_revenue']
                    # print str(rp_typeNw.date())
                    # print str(rp_typeMatch.head(1).index)
                    if len(rp_typeMatch) > 0:
                        operating_revenue = rp_typeMatch.head(1)[0]
                        stock_data_day_recent.loc[dateNw, 'STOP'] = operating_revenue / stock_data_day_recent['market_value'][i]

                    # 计算日波动率
                    if len(dateList_all) - i > 65:
                        rList = list(stock_data_day_all[i:i + 65]['change'])
                        # print rList
                        stock_data_day_recent.loc[dateNw, 'DASTD'] = math.sqrt(23.0 * sum([c * c for c in rList]))  # 65天收益率的平方和乘以23,开根

                    # 计算日变相关factor
                    if stock_data_day_recent['market_value'][i] > 0:
                        stock_data_day_recent.loc[dateNw, 'LNCAP'] = math.log(stock_data_day_recent['market_value'][i], math.e)
                    stock_data_day_recent.loc[dateNw, 'LPRI'] = math.log(stock_data_day_recent['close'][i], math.e)

                    # 需一个月数据的相关factor
                    # 一个月的起点(4周)
                    date1M = stock_data_day_recent['date'][i] + timedelta(days=-(7 * 4))
                    if len(stock_data_day_all[stock_data_day_all.date <= date1M]['date']) == 0:  # 上市不满一个月, 跳过此次循环
                        continue
                    else:
                        date1M = stock_data_day_all[stock_data_day_all.date >= date1M]['date'].tail(1)[0]
                        sttpos1M = dateList_all.index(date1M)
                    # print str(dateNw.weekday())+'__'+str(date1M.weekday())
                    # print str(dateNw.date())+'__'+str(date1M.date())

                    MRA_list = (stock_data_day_all[i:sttpos1M + 1]['adjust_price'] / stock_data_day_all.loc[date1M, 'adjust_price']) - 1.0
                    stock_data_day_recent.loc[dateNw, 'CMRA'] = math.log((1 + max(MRA_list)) / (1 + min(MRA_list)), math.e)  # 计算当月累积收益范围 CMRA

                    stock_data_day_recent.loc[dateNw, 'STO_1M'] = sum(stock_data_day_all[i:sttpos1M + 1]['turnover'])
                    # stock_data.loc[dateNw, 'RSTR_1M'] = sum(stock_data[i:sttpos1M + 1]['change'])
                    stock_data_day_recent.loc[dateNw, 'RSTR_1M'] = (stock_data_day_all.loc[dateNw, 'adjust_price'] / stock_data_day_all.loc[date1M, 'adjust_price']) - 1.0
                    stock_data_day_recent.loc[dateNw, 'HILO'] = math.log(max(stock_data_day_all[i:sttpos1M + 1]['Hi_adj']) / min(stock_data_day_all[i:sttpos1M + 1]['Lo_adj']), math.e)

                    # 需三个月数据的相关factor
                    # 三个月的起点(13周)
                    date3M = stock_data_day_recent['date'][i] + timedelta(days=-(7 * 13))
                    if len(stock_data_day_all[stock_data_day_all.date <= date3M]['date']) == 0:  # 上市不满3个月, 跳过此次循环
                        continue
                    else:
                        date3M = stock_data_day_all[stock_data_day_all.date >= date3M]['date'].tail(1)[0]
                        sttpos3M = dateList_all.index(date3M)
                    # print str(dateNw.date())+'__'+str(date3M.date())

                    stock_data_day_recent.loc[dateNw, 'STO_3M'] = sum(stock_data_day_all[i:sttpos3M + 1]['turnover'])
                    # stock_data.loc[dateNw, 'RSTR_3M'] = sum(stock_data[i:sttpos3M + 1]['change'])
                    stock_data_day_recent.loc[dateNw, 'RSTR_3M'] = (stock_data_day_all.loc[dateNw, 'adjust_price'] / stock_data_day_all.loc[date3M, 'adjust_price']) - 1.0

                    # 需六个月数据的相关factor
                    # 六个月的起点(26周)
                    date6M = stock_data_day_recent['date'][i] + timedelta(days=-(7 * 26))
                    if len(stock_data_day_all[stock_data_day_all.date <= date6M]['date']) == 0:  # 上市不满6个月, 跳过此次循环
                        continue
                    else:
                        date6M = stock_data_day_all[stock_data_day_all.date >= date6M]['date'].tail(1)[0]
                        sttpos6M = dateList_all.index(date6M)
                    # print str(dateNw.date())+'__'+str(date6M.date())

                    stock_data_day_recent.loc[dateNw, 'STO_6M'] = sum(stock_data_day_all[i:sttpos6M + 1]['turnover'])
                    # stock_data.loc[dateNw, 'RSTR_6M'] = sum(stock_data[i:sttpos6M + 1]['change'])
                    stock_data_day_recent.loc[dateNw, 'RSTR_6M'] = (stock_data_day_all.loc[dateNw, 'adjust_price'] / stock_data_day_all.loc[date6M, 'adjust_price']) - 1.0

                    # 需12个月数据的相关factor
                    # 12个月的起点(52周)
                    date12M = stock_data_day_recent['date'][i] + timedelta(days=-(7 * 52))
                    if len(stock_data_day_all[stock_data_day_all.date <= date12M]['date']) == 0:  # 上市不满12个月, 跳过此次循环
                        continue
                    else:
                        date12M = stock_data_day_all[stock_data_day_all.date >= date12M]['date'].tail(1)[0]
                        sttpos12M = dateList_all.index(date12M)

                    stock_data_day_recent.loc[dateNw, 'STO_12M'] = sum(stock_data_day_all[i:sttpos12M + 1]['turnover'])
                    # stock_data.loc[dateNw, 'RSTR_12M'] = sum(stock_data[i:sttpos12M + 1]['change'])
                    stock_data_day_recent.loc[dateNw, 'RSTR_12M'] = (stock_data_day_all.loc[dateNw, 'adjust_price'] / stock_data_day_all.loc[date12M, 'adjust_price']) - 1.0

                    # 需60个月数据的相关factor
                    # 60个月的起点(260周)
                    date60M = stock_data_day_recent['date'][i] + timedelta(days=-(7 * 260))
                    if len(stock_data_day_all[stock_data_day_all.date <= date60M]['date']) == 0:  # 上市不满5年, 跳过此次循环
                        continue
                    else:
                        date60M = stock_data_day_all[stock_data_day_all.date >= date60M]['date'].tail(1)[0]
                        sttpos60M = dateList_all.index(date60M)

                    stock_data_day_recent.loc[dateNw, 'STO_60M'] = sum(stock_data_day_all[i:sttpos60M + 1]['turnover'])
                    stock_data_day_recent.loc[dateNw, 'RSTR_60M'] = (stock_data_day_all.loc[dateNw, 'adjust_price'] / stock_data_day_all.loc[date60M, 'adjust_price']) - 1.0

                stock_data_day_recent.to_sql(table_name, sqlite_engine,
                                             dtype={'date': String, 'report_type': String, 'report_date': String},
                                             if_exists='append', index=False, chunksize=100000)
            print 'update day factors ' + stock_code + '   len: ' + str(len(stock_data_day_recent))
        return 0

    def update_week_factors_table(self, table_name):
        ms_engine = self.ms_engine
        sqlite_engine = self.sqlite_engine

        query_line = ("select distinct dateS from %(table_name)s where dateS > '2006-01-01' order by dateS desc"
                      % {'table_name': table_name})
        cursor = pd.read_sql_query(query_line, sqlite_engine)
        if len(cursor) > 1:
            last_date_str = str(cursor.ix[0].values)[3:13]
        else:
            last_date_str = '2000-01-01'

        for stock_code in self.stock_list.stock_code:
            if stock_code[0:2] == 'sh':
                index_code = 'sh000001'
            elif stock_code[0:2] == 'sz':
                index_code = 'sz399001'

            # SQL_tuple_week_all = ms_engine.execqueryparam("select * from [dbo].[V_stock_1w] where code=%s order by date DESC", stock_code)
            SQL_tuple_week_all = ms_engine.execqueryparam("SELECT A.[code],A.[date],A.[adjust_price],A.[money] FROM \
                (SELECT V_stock_1w.*,DATEPART(DW,date) as dw,row_number() OVER(order by code,date desc) r  FROM V_stock_1w where code=%s) A, \
                (SELECT V_stock_1w.*,DATEPART(DW,date) as dw,row_number() OVER(order by code,date desc) r  FROM V_stock_1w where code=%s) B  \
                WHERE (A.r=B.r+1 and (B.dw<=A.dw or DATEDIFF(day,A.date,B.date)>=7)) or (A.r=1 and B.r=1) \
                order by date desc", (stock_code, stock_code))

            Stock_data_week_all = pd.DataFrame(SQL_tuple_week_all, columns=['code', 'dateS', 'adjust_price', 'moneyS'])
            Stock_data_week_all.set_index('dateS', drop=False, inplace=True)  # dateS 列设为index

            Stock_data_week_recent = Stock_data_week_all[Stock_data_week_all.dateS > get_format_date(last_date_str)]

            if len(Stock_data_week_recent) > 0:
                # SQL_tuple_week_all = ms_engine.execqueryparam("select * from [dbo].[V_index_1w] where index_code=%s order by date DESC", index_code)
                SQL_tuple_week_all = ms_engine.execqueryparam("SELECT A.[index_code],A.[date],A.[close],A.[money] FROM \
                    (SELECT V_index_1w.*,DATEPART(DW,date) as dw,row_number() OVER(order by index_code,date desc) r  FROM V_index_1w where index_code=%s) A, \
                    (SELECT V_index_1w.*,DATEPART(DW,date) as dw,row_number() OVER(order by index_code,date desc) r  FROM V_index_1w where index_code=%s) B  \
                    WHERE (A.r=B.r+1 and (B.dw<=A.dw or DATEDIFF(day,A.date,B.date)>=7)) or (A.r=1 and B.r=1) \
                    order by date desc", (index_code, index_code))
                Index_data_week_all = pd.DataFrame(SQL_tuple_week_all, columns=['index_code', 'dateI', 'close', 'moneyI'])
                Index_data_week_all.set_index('dateI', drop=False, inplace=True)  # dateI 列设为index

                for i in range(0, len(Stock_data_week_recent) + 240):
                    if i < len(Stock_data_week_all) - 4:    # 算周change和月change
                        Stock_data_week_all.loc[Stock_data_week_all.index[i], 'stock_change'] = Stock_data_week_all.adjust_price[i] / Stock_data_week_all.adjust_price[i + 1] - 1
                        Stock_data_week_all.loc[Stock_data_week_all.index[i], 'stock_change_1m'] = Stock_data_week_all.adjust_price[i] / Stock_data_week_all.adjust_price[i + 4] - 1
                    elif i < len(Stock_data_week_all) - 1:  # 算周change
                        Stock_data_week_all.loc[Stock_data_week_all.index[i], 'stock_change'] = Stock_data_week_all.adjust_price[i] / Stock_data_week_all.adjust_price[i + 1] - 1

                for i in range(0, len(Index_data_week_all) - 1):
                    Index_data_week_all.loc[Index_data_week_all.index[i], 'index_change'] = Index_data_week_all.close[i] / Index_data_week_all.close[i + 1] - 1
                    if i < len(Index_data_week_all) - 4:    # 算月change
                        Index_data_week_all.loc[Index_data_week_all.index[i], 'index_change_1m'] = Index_data_week_all.close[i] / Index_data_week_all.close[i + 4] - 1

                Index_data_weekVSstock = Index_data_week_all.reindex(list(Stock_data_week_all.index), ['dateI', 'moneyI', 'close', 'index_change', 'index_change_1m'])

                sttpos = len(Stock_data_week_recent) + 240 - 1
                for i in range(sttpos, -1, -1):
                    if i < len(Stock_data_week_all) - 1:
                        dateNow = Stock_data_week_all.dateS[i]
                        dateNowStr = str(dateNow.date())
                        if dateNow != Index_data_weekVSstock.dateI[i]:
                            tmpVolm = Index_data_week_all[Index_data_week_all.dateI > dateNow].tail(1)
                            Index_data_weekVSstock.loc[dateNowStr, 'dateI'] = tmpVolm['dateI'][0]
                            Index_data_weekVSstock.loc[dateNowStr, 'moneyI'] = tmpVolm['moneyI'][0]
                            Index_data_weekVSstock.loc[dateNowStr, 'close'] = tmpVolm['close'][0]
                            Index_data_weekVSstock.loc[dateNowStr, 'index_change'] = tmpVolm['index_change'][0]
                            Index_data_weekVSstock.loc[dateNowStr, 'index_change_1m'] = tmpVolm['index_change_1m'][0]

                        Index_data_weekVSstock.loc[dateNowStr, 'V_changeI'] = Index_data_weekVSstock.moneyI[i] / Index_data_weekVSstock.moneyI[i + 1] - 1
                        Stock_data_week_all.loc[dateNowStr, 'V_changeS'] = Stock_data_week_all.moneyS[i] / Stock_data_week_all.moneyS[i + 1] - 1
                        Stock_data_week_all.loc[dateNowStr, 'dateI'] = Index_data_weekVSstock.dateI[i]
                        Stock_data_week_all.loc[dateNowStr, 'closeI'] = Index_data_weekVSstock.close[i]
                        Stock_data_week_all.loc[dateNowStr, 'index_change'] = Index_data_weekVSstock.index_change[i]
                        Stock_data_week_all.loc[dateNowStr, 'index_change_1m'] = Index_data_weekVSstock.index_change_1m[i]
                        if np.isnan(Index_data_weekVSstock.moneyI[i] / Index_data_weekVSstock.moneyI[i + 1] - 1):
                            print 'Index-1W-nan----------------------------------------' + dateNowStr
                            print Index_data_weekVSstock.moneyI[i]
                            print Index_data_weekVSstock.moneyI[i + 1]
                        if np.isnan(Stock_data_week_all.moneyS[i] / Stock_data_week_all.moneyS[i + 1] - 1):
                            print 'Stock-1W-nan----------------------------------------' + dateNowStr
                            print Stock_data_week_all.moneyS[i]
                            print Stock_data_week_all.moneyS[i + 1]

                Stock_data_week_recent = Stock_data_week_all[Stock_data_week_all.dateS > get_format_date(last_date_str)].copy()

                # 计算 VOLBT
                j = len(Stock_data_week_recent) - 1
                i = j + 60
                while j >= 0:
                    if i < len(Stock_data_week_all) - 1:
                        X = Index_data_weekVSstock.V_changeI[j:i]
                        Y = Stock_data_week_all.V_changeS[j:i]
                        bench = sm.add_constant(X)
                        model = regression.linear_model.OLS(Y, bench).fit()
                        alpha = model.params[0]
                        beta = model.params[1]
                        Stock_data_week_recent.loc[Stock_data_week_recent.index[j], 'VOLBT'] = beta

                    # 循环
                    i -= 1
                    j = i - 60

                # 计算 ALPHA, BETA, SIGMA, BTSG, SERDP
                j = len(Stock_data_week_recent) - 1
                i = j + 240
                j_to_i_list = [j]
                k = j + 4
                while k < i:
                    j_to_i_list.append(k)
                    k += 4
                one_list = np.ones(len(j_to_i_list), dtype=np.int).tolist()
                while j >= 0:
                    if i < len(Stock_data_week_all) - 4:
                        X = Stock_data_week_all.index_change_1m[j_to_i_list]
                        Y = Stock_data_week_all.stock_change_1m[j_to_i_list]
                        bench = sm.add_constant(X)
                        model = regression.linear_model.OLS(Y, bench).fit()
                        alpha = model.params[0]
                        beta = model.params[1]
                        Stock_data_week_recent.loc[Stock_data_week_recent.index[j], 'ALPHA'] = alpha
                        Stock_data_week_recent.loc[Stock_data_week_recent.index[j], 'BETA'] = beta

                        Y_hat = X * beta + alpha
                        residual = Y - Y_hat
                        # print residual

                        sigma = np.std(residual)
                        Stock_data_week_recent.loc[Stock_data_week_recent.index[j], 'SIGMA'] = sigma
                        Stock_data_week_recent.loc[Stock_data_week_recent.index[j], 'BTSG'] = sigma * beta
                        Stock_data_week_recent.loc[Stock_data_week_recent.index[j], 'SERDP'] = self.calculate_SERDP(residual)

                    # 循环
                    i -= 1
                    j = i - 240
                    j_to_i_list = list(map(lambda x: x[0]-x[1], zip(j_to_i_list, one_list)))

                Stock_data_week_recent.to_sql(table_name, sqlite_engine,
                                              dtype={'dateS': String, 'dateI': String},
                                              if_exists='append', index=False, chunksize=100000)
            print 'update week factors ' + stock_code + '   len: ' + str(len(Stock_data_week_recent))
        pass
        return 0

    def update_month_factors_table(self, table_name):

        ms_engine = self.ms_engine
        sqlite_engine = self.sqlite_engine

        query_line = ("select distinct stock_trade_date from %(table_name)s \
                       where stock_trade_date > '2006-01-01' order by stock_trade_date desc"
                      % {'table_name': table_name})
        cursor = pd.read_sql_query(query_line, sqlite_engine)
        if len(cursor) > 1:
            last_date_str = str(cursor.ix[0].values)[3:13]
        else:
            last_date_str = '1900-01-01'

        for stock_code in self.stock_list.stock_code:
            if stock_code[0:2] == 'sh':
                index_code = 'sh000001'
            elif stock_code[0:2] == 'sz':
                index_code = 'sz399001'

            # 获取股票和指数月收益率数据
            SQL_tuple_month_all = ms_engine.execqueryparam("select * from dbo.V_stock_1m a left join V_index_1m b \
                                                            on a.stock_date = b.index_date where code =%s and \
                                                            index_code = %s order by a.stock_date DESC ", (stock_code, index_code))
            Stock_data_month_all = pd.DataFrame(SQL_tuple_month_all,
                                     columns=['code', 'stock_trade_date', 'adjust_price', 'stock_date',
                                              'index_code', 'index_trade_dateI', 'index_close', 'index_date'])
            Stock_data_month_all.set_index('stock_date', drop=False, inplace=True)  # lastdate 列设为index
            Stock_data_month_recent = Stock_data_month_all[Stock_data_month_all.stock_trade_date > get_format_date(last_date_str)]

            if len(Stock_data_month_recent) > 0:
                # print Stock_data_month
                for i in range(0, len(Stock_data_month_recent) + 60 - 1):
                    if i < len(Stock_data_month_all) - 1:
                        Stock_data_month_all.loc[Stock_data_month_all.index[i], 'stock_change'] = Stock_data_month_all.adjust_price[i] / Stock_data_month_all.adjust_price[i + 1] - 1
                        Stock_data_month_all.loc[Stock_data_month_all.index[i], 'index_change'] = Stock_data_month_all.index_close[i] / Stock_data_month_all.index_close[i + 1] - 1
                        # print Stock_data_month.stock_date[i]

                # print Stock_data_month
                Stock_data_month_recent = Stock_data_month_all[Stock_data_month_all.stock_trade_date > get_format_date(last_date_str)].copy()
                j = len(Stock_data_month_recent) - 1
                i = j + 60
                while j >= 0:
                    if i < len(Stock_data_month_all) - 1:
                        X = Stock_data_month_all.index_change[j:i]
                        Y = Stock_data_month_all.stock_change[j:i]
                        bench = sm.add_constant(X)
                        model = regression.linear_model.OLS(Y, bench).fit()
                        # print Stock_data_month.index[j]
                        alpha = model.params[0]
                        beta = model.params[1]
                        Stock_data_month_recent.loc[Stock_data_month_recent.index[j], 'ALPHA'] = alpha
                        Stock_data_month_recent.loc[Stock_data_month_recent.index[j], 'BETA'] = beta

                        Y_hat = X * beta + alpha
                        residual = Y - Y_hat
                        # print residual

                        sigma = np.std(residual)
                        Stock_data_month_recent.loc[Stock_data_month_recent.index[j], 'SIGMA'] = sigma
                        Stock_data_month_recent.loc[Stock_data_month_recent.index[j], 'BTSG'] = sigma * beta
                        Stock_data_month_recent.loc[Stock_data_month_recent.index[j], 'SERDP'] = self.calculate_SERDP(residual)

                    # 循环
                    i -= 1
                    j = i - 60

                Stock_data_month_recent.to_sql(table_name, sqlite_engine,
                                               dtype={'stock_trade_date': String, 'stock_date': String,
                                                      'index_trade_dateI': String, 'index_date': String},
                                               if_exists='append', index=False, chunksize=100000)
            print 'update month factors ' + stock_code + '---< ' + index_code + '   len: ' + str(len(Stock_data_month_recent))
        pass
        return 0

    def update_quarter_factors_table(self, table_name):
        ms_engine = self.ms_engine
        sqlite_engine = self.sqlite_engine

        query_line = ("select distinct report_date from %(table_name)s \
                      where report_date > '2006-01-01' order by report_date desc"
                      % {'table_name': table_name})
        cursor = pd.read_sql_query(query_line, sqlite_engine)
        if len(cursor) > 1:
            last_date_str = str(cursor.ix[0].values)[3:13]
        else:
            last_date_str = '1900-01-01'

        for stock_code in self.stock_list.stock_code:

            SQL_tuple_qt_all = ms_engine.execqueryparam("select code,Year(date),  \
                (select top 1 market_value from stocks.dbo.stock_data b where b.code = a.code and Year(b.date)=year(a.date) order by date desc)  \
                from stocks.dbo.stock_data a where code=%s and Year(date)>=Year(%s)-5  group by code,Year(date) order by code,Year(date) ", (stock_code, last_date_str))
            MkVl_1Y = pd.DataFrame(SQL_tuple_qt_all, columns=['code', 'year', 'market_value'])
            MkVl_1Y.set_index('year', drop=False, inplace=True)
            # print MkVl_1Y

            # 获取年报的营业收入数据
            SQL_tuple_all = ms_engine.execqueryparam("SELECT [股票代码], [报告类型], [报告日期], [总市值], [营业总收入], \
                                                      [营业利润], [净利润], [资产总计], [负债合计], [长期负债合计], [其中：优先股]  \
                                                      FROM [stocks].[dbo].[all_financial_data] WHERE 股票代码=%s \
                                                      AND 报告日期 IS NOT NULL  \
                                                      AND 营业总收入 IS NOT NULL  \
                                                      AND 净利润 IS NOT NULL  \
                                                      ORDER BY 报告类型 DESC", stock_code)
            stock_data_qt_all = pd.DataFrame(SQL_tuple_all,
                                        columns=['code', 'report_type', 'report_date', 'market_value',
                                                 'op_earning', 'op_profit', 'net_profit', 'total_assets',
                                                 'total_liabilities', 'L_term_liabilities', 'preference_share'])
            stock_data_qt_all.set_index('report_type', drop=False, inplace=True)  # date列设为index

            if len(stock_data_qt_all) > 1:
                stock_data_qt_recent = stock_data_qt_all[stock_data_qt_all.report_date > get_format_date(last_date_str)]

                if len(stock_data_qt_recent) > 0:
                    for i in range(0, len(stock_data_qt_all)):
                        if i < len(stock_data_qt_all) - 1:
                            typeNow = stock_data_qt_all.report_type[i]
                            typePre = stock_data_qt_all.report_type[i + 1]
                            stock_data_qt_all.loc[typeNow, 'report_term'] = round((typeNow - typePre).days / 30.0)
                            if typePre.month == 12:
                                stock_data_qt_all.loc[typeNow, 'qt_net_profit'] = stock_data_qt_all.loc[typeNow, 'net_profit']
                                stock_data_qt_all.loc[typeNow, 'qt_op_profit'] = stock_data_qt_all.loc[typeNow, 'op_profit']
                                stock_data_qt_all.loc[typeNow, 'qt_op_earning'] = stock_data_qt_all.loc[typeNow, 'op_earning']
                            else:
                                stock_data_qt_all.loc[typeNow, 'qt_net_profit'] = stock_data_qt_all['net_profit'][i] - stock_data_qt_all['net_profit'][i + 1]
                                stock_data_qt_all.loc[typeNow, 'qt_op_profit'] = stock_data_qt_all['op_profit'][i] - stock_data_qt_all['op_profit'][i + 1]
                                stock_data_qt_all.loc[typeNow, 'qt_op_earning'] = stock_data_qt_all['op_earning'][i] - stock_data_qt_all['op_earning'][i + 1]

                    stock_data_qt_recent = stock_data_qt_all[stock_data_qt_all.report_date > get_format_date(last_date_str)].copy()
                    for i in range(0, len(stock_data_qt_recent)):
                        # for i in range(0, 5):
                        typeNow = stock_data_qt_recent.report_type[i]
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
                        for j in range(i, len(stock_data_qt_all)):
                            L_months += stock_data_qt_all['report_term'][j]
                            if L_months == 60 and flg_60months == 0:
                                # print str(stock_data_qt['report_type'][i].date())+'---'+str(stock_data_qt['report_type'][j].date())
                                qt5Y_nt_prftList.append(stock_data_qt_all.qt_net_profit[j])
                                tt_asstList.append(stock_data_qt_all.total_assets[j])
                                rp_termList.append(stock_data_qt_all.report_term[j])
                                flg_60months = 1
                            elif L_months > 60 and flg_60months == 0:
                                wt_upto60m = L_m_to_5Y / stock_data_qt_all['report_term'][j]
                                # print str(stock_data_qt['report_type'][i].date())+'---'+str(stock_data_qt['report_type'][j].date())+' xxxxx '+str(wt_upto60m)
                                qt5Y_nt_prftList.append(stock_data_qt_all.qt_net_profit[j] * wt_upto60m)
                                tt_asstList.append(stock_data_qt_all.total_assets[j])
                                rp_termList.append(stock_data_qt_all.report_term[j])
                                flg_60months = 1
                            elif flg_60months == 0:
                                qt5Y_nt_prftList.append(stock_data_qt_all.qt_net_profit[j])
                                tt_asstList.append(stock_data_qt_all.total_assets[j])
                                rp_termList.append(stock_data_qt_all.report_term[j])
                                L_m_to_5Y = 60 - L_months

                            if L_months == 12 and flg_12months == 0:
                                # print str(stock_data_qt['report_type'][i].date())+'---'+str(stock_data_qt['report_type'][j].date())
                                qt12M_nt_prftList.append(stock_data_qt_all.qt_net_profit[j])
                                qt12M_op_earnList.append(stock_data_qt_all.qt_op_earning[j])
                                qt12M_op_prftList.append(stock_data_qt_all.qt_op_profit[j])
                                flg_12months = 1
                            elif L_months > 12 and flg_12months == 0:
                                wt_upto12m = L_m_to_12M / stock_data_qt_all['report_term'][j]
                                # print str(stock_data_qt['report_type'][i].date())+'---'+str(stock_data_qt['report_type'][j].date())+' xxxxx '+str(wt_upto12m)
                                qt12M_nt_prftList.append(stock_data_qt_all.qt_net_profit[j] * wt_upto12m)
                                qt12M_op_earnList.append(stock_data_qt_all.qt_op_earning[j] * wt_upto12m)
                                qt12M_op_prftList.append(stock_data_qt_all.qt_op_profit[j] * wt_upto12m)
                                flg_12months = 1
                            elif flg_12months == 0:
                                qt12M_nt_prftList.append(stock_data_qt_all.qt_net_profit[j])
                                qt12M_op_earnList.append(stock_data_qt_all.qt_op_earning[j])
                                qt12M_op_prftList.append(stock_data_qt_all.qt_op_profit[j])
                                L_m_to_12M = 12 - L_months

                        # 计算 ETP5 : 最近五年平均净利润/最近五年每年 12 月 31 日市
                        yearNow = stock_data_qt_recent.report_date[i].year
                        if len(MkVl_1Y[MkVl_1Y.year == yearNow - 5]) > 0 and flg_60months == 1:
                            mean_nt_prft_5Y = sum(qt5Y_nt_prftList) * 0.2
                            mean_mkt_val_5Y = sum(MkVl_1Y.loc[yearNow - 5:yearNow - 1]['market_value']) * 0.2
                            stock_data_qt_recent.loc[typeNow, 'ETP5'] = mean_nt_prft_5Y / mean_mkt_val_5Y

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

                            stock_data_qt_recent.loc[typeNow, 'AGRO'] = beta

                        # 计算 最近 12 个月的滚动因子
                        if flg_12months == 1:  # 研报覆盖期超过12个月
                            tot_asset = stock_data_qt_all.total_assets[i]
                            net_asset = tot_asset - stock_data_qt_all.total_liabilities[i]
                            op_earn_12M = sum(qt12M_op_earnList)
                            op_prft_12M = sum(qt12M_op_prftList)
                            ne_prft_12M = sum(qt12M_nt_prftList)

                            # 计算 T_GPM : 最近 12 个月毛利润/最近 12 个月营业收入
                            stock_data_qt_recent.loc[typeNow, 'T_GPM'] = op_prft_12M / op_earn_12M
                            # 计算 T_NPM : 最近 12 个月净利润/最近 12 个月营业收入
                            stock_data_qt_recent.loc[typeNow, 'T_NPM'] = ne_prft_12M / op_earn_12M
                            # 计算 T_ROE : 最近 12 个月净利润/最新报告期净资产
                            stock_data_qt_recent.loc[typeNow, 'T_ROE'] = ne_prft_12M / net_asset
                            # 计算 T_ROA : 最近 12 个月净利润/最新报告期总资产
                            stock_data_qt_recent.loc[typeNow, 'T_ROA'] = ne_prft_12M / tot_asset

                        # 计算 单季 因子
                        # 计算 MLEV : 市值杠杆 （市值+优先股权+长期负债）/市值
                        L_term_lib = stock_data_qt_recent.loc[typeNow, 'L_term_liabilities']
                        prfr_share = stock_data_qt_recent.loc[typeNow, 'preference_share']
                        mkt_value  = stock_data_qt_recent.loc[typeNow, 'market_value']
                        if str(L_term_lib) != 'None':
                            if np.isnan(L_term_lib) == False and L_term_lib > 0:
                                # print str(prfr_share)
                                if str(prfr_share) == 'None':
                                    stock_data_qt_recent.loc[typeNow, 'MLEV'] = 1.0 + L_term_lib / mkt_value
                                elif np.isnan(prfr_share):
                                    stock_data_qt_recent.loc[typeNow, 'MLEV'] = 1.0 + L_term_lib / mkt_value
                                else:
                                    stock_data_qt_recent.loc[typeNow, 'MLEV'] = 1.0 + (L_term_lib + prfr_share) / mkt_value

                        # 计算 S_GPM : 单季毛利率 当年单季毛利润/当年单季营业收入
                        stock_data_qt_recent.loc[typeNow, 'S_GPM'] = stock_data_qt_recent.qt_op_profit[i] / stock_data_qt_recent.qt_op_earning[
                            i]
                        # 计算 S_NPM : 单季净利率 当年单季净利润/当年单季营业收入
                        stock_data_qt_recent.loc[typeNow, 'S_NPM'] = stock_data_qt_recent.qt_net_profit[i] / stock_data_qt_recent.qt_op_earning[
                            i]
                        # 计算 S_ROE :单季 ROE  当年单季净利润/最新报告期净资产
                        stock_data_qt_recent.loc[typeNow, 'S_ROE'] = stock_data_qt_recent.qt_net_profit[i] / (stock_data_qt_recent.total_assets[i] - stock_data_qt_recent.total_liabilities[i])
                        # 计算 S_ROA :单季 ROA 	当年单季净利润/最新报告期总资产
                        stock_data_qt_recent.loc[typeNow, 'S_ROA'] = stock_data_qt_recent.qt_net_profit[i] / stock_data_qt_recent.total_assets[i]

                        # 计算 C_GPM : 累计毛利率  当年累计毛利润/当年累计营业收入
                        stock_data_qt_recent.loc[typeNow, 'C_GPM'] = stock_data_qt_recent.op_profit[i] / stock_data_qt_recent.op_earning[i]
                        # 计算 C_NPM : 累计净利率  当年累计净利润/当年累计营业收入
                        stock_data_qt_recent.loc[typeNow, 'C_NPM'] = stock_data_qt_recent.net_profit[i] / stock_data_qt_recent.op_earning[i]
                        # 计算 C_ROE : 累计 ROE    当年累计净利润/最新报告期净资产
                        stock_data_qt_recent.loc[typeNow, 'C_ROE'] = stock_data_qt_recent.net_profit[i] / (stock_data_qt_recent.total_assets[i] - stock_data_qt_all.total_liabilities[i])
                        # 计算 C_ROA : 累计 ROA    当年累计净利润/最新报告期总资产
                        stock_data_qt_recent.loc[typeNow, 'C_ROA'] = stock_data_qt_recent.net_profit[i] / stock_data_qt_recent.total_assets[i]

                    stock_data_qt_recent.to_sql(table_name, sqlite_engine,
                                                dtype={'report_type': String, 'report_date': String,},
                                                if_exists='append', index=False, chunksize=100000)
                print 'update quarter factors ' + stock_code + '   len: ' + str(len(stock_data_qt_recent))

        return 0

    def update_all_factors_rolling_table(self, table_name, table_type):

        sqlite_engine = self.sqlite_engine

        query_line = ("select dateS from %(table_name)s order by dateS desc" % {'table_name': table_name})
        cursor = pd.read_sql_query(query_line, sqlite_engine)
        if len(cursor) > 1:
            last_date = get_format_date(str(cursor.ix[0].values)[3:13]) + timedelta(days=1)  # 加一天, 因为 sqlite 的[date]> 效果等于 [date]>=
            last_date_str = str(last_date.date())
        else:
            last_date_str = '2005-01-01'

        if table_type is '1m':
            query_line = ("select * from industry_change_1m where dateI>'%(dt)s' order by dateI desc" % {'dt': last_date_str})
        elif table_type is '1w':
            query_line = ("select * from industry_change_1w where dateI>'%(dt)s' order by dateI desc" % {'dt': last_date_str})
        industry_change_df = pd.read_sql_query(query_line, sqlite_engine)
        industry_change_df.set_index('dateI', drop=False, inplace=True)  # date 列设为index

        w.start()
        for stock_code in self.stock_list.stock_code:
            stock_code = 'sh600006'
            # 读取日factors
            query_line = ("select [code],[date],[report_type],[ETOP],[STO_1M],[STO_3M],[STO_6M],[STO_12M],[STO_60M]  \
                          ,[RSTR_1M],[RSTR_3M],[RSTR_6M],[RSTR_12M],[LNCAP],[BTOP],[STOP],[HILO],[DASTD],[LPRI],[CMRA]   \
                          from day_factors_update where code='%(cd)s' and [date]>'%(dt)s' order by date DESC" %
                          {'cd': stock_code, 'dt': last_date_str})
            Ddata = pd.read_sql_query(query_line, sqlite_engine)
            Ddata.set_index('date', drop=False, inplace=True)  # date 列设为index

            if len(Ddata) > 0:
                # 读取季factors
                last_report_type_str = str(Ddata.report_type[len(Ddata) - 1][0:10])   # 找更新交易日对应的report_type

                query_line = ("select [code],[report_type],[ETP5],[AGRO],[MLEV],[S_GPM],[C_GPM],[T_GPM],[S_NPM],[C_NPM], \
                              [T_NPM],[S_ROE],[C_ROE],[T_ROE],[S_ROA],[C_ROA],[T_ROA] from quarter_factors_update \
                              where code='%(cd)s' and report_type>='%(dt)s' order by report_type DESC" %
                              {'cd': stock_code, 'dt': last_report_type_str})
                Qdata = pd.read_sql_query(query_line, sqlite_engine)
                Qdata.set_index('report_type', drop=False, inplace=True)  # report_type 列设为index

                if table_type is '1m':
                    # 读取周数据
                    last_week_date = last_date + timedelta(days=-10)    # 向前多取一周时间, 以防跟月对其的时候, 取不到上1周数据
                    last_week_date_str = str(last_week_date.date())
                    query_line = ("select [code],[dateS],[dateI],[VOLBT],[stock_change],[index_change]  \
                                  from week_factors_update where code='%(cd)s' and [dateS]>'%(dt)s' order by dateS DESC" %
                                  {'cd': stock_code, 'dt': last_week_date_str})
                    Wdata = pd.read_sql_query(query_line, sqlite_engine)
                    Wdata.set_index('dateS', drop=False, inplace=True)  # dateS 列设为index

                    # 读取月factors
                    query_line = ("select [code],[stock_trade_date],[index_trade_dateI],[ALPHA],[BETA],[SIGMA], \
                                  [BTSG],[SERDP],[stock_change],[index_change] from month_factors_update \
                                  where code='%(cd)s' and stock_trade_date>'%(dt)s' order by stock_trade_date DESC" %
                                  {'cd': stock_code, 'dt': last_date_str})
                    Mdata = pd.read_sql_query(query_line, sqlite_engine)
                    Mdata.columns = ['code', 'dateS', 'dateI', 'ALPHA', 'BETA', 'SIGMA',
                                     'BTSG', 'SERDP', 'stock_change', 'index_change']
                    Mdata.set_index('dateS', drop=False, inplace=True)  # dateS 列设为index

                    # 主时间轴 设为 "月"
                    df = pd.DataFrame(Mdata.loc[:, ['code', 'dateS', 'dateI', 'stock_change']], index=Mdata.dateS,
                                        columns=['code', 'dateS', 'dateI', 'stock_change', 'ETOP','ETP5', 'Growth',
                                                 'Leverage', 'STO_1M', 'STO_3M', 'STO_6M', 'STO_12M', 'STO_60M',
                                                 'ALPHA', 'RSTR_1M', 'RSTR_3M', 'RSTR_6M', 'RSTR_12M', 'Size',
                                                 'BTOP', 'STOP', 'HILO', 'BTSG', 'DASTD', 'LPRI', 'CMRA', 'VOLBT',
                                                 'SERDP', 'BETA', 'SIGMA',
                                                 'S_GPM', 'C_GPM', 'T_GPM', 'S_NPM', 'C_NPM', 'T_NPM',
                                                 'S_ROE', 'C_ROE', 'T_ROE', 'S_ROA', 'C_ROA', 'T_ROA',
                                                 'industry', 'industry_code', 'concept', 'IndustryChange'])
                elif table_type is '1w':
                    # 读取周数据
                    query_line = ("select code,dateS,dateI,VOLBT,stock_change,ALPHA,BETA,SIGMA,BTSG,SERDP \
                                  from week_factors_update where code='%(cd)s' and [dateS]>'%(dt)s' order by dateS DESC" %
                                  {'cd': stock_code, 'dt': last_date_str})
                    Wdata = pd.read_sql_query(query_line, sqlite_engine)
                    Wdata.set_index('dateS', drop=False, inplace=True)  # dateS 列设为index
                    # 主时间轴 设为 "周"
                    df = pd.DataFrame(Wdata.loc[:, ['code', 'dateS', 'dateI', 'stock_change']], index=Wdata.dateS,
                                        columns=['code', 'dateS', 'dateI', 'stock_change', 'ETOP','ETP5', 'Growth',
                                                 'Leverage', 'STO_1M', 'STO_3M', 'STO_6M', 'STO_12M', 'STO_60M',
                                                 'ALPHA', 'RSTR_1M', 'RSTR_3M', 'RSTR_6M', 'RSTR_12M', 'Size',
                                                 'BTOP', 'STOP', 'HILO', 'BTSG', 'DASTD', 'LPRI', 'CMRA', 'VOLBT',
                                                 'SERDP', 'BETA', 'SIGMA',
                                                 'S_GPM', 'C_GPM', 'T_GPM', 'S_NPM', 'C_NPM', 'T_NPM',
                                                 'S_ROE', 'C_ROE', 'T_ROE', 'S_ROA', 'C_ROA', 'T_ROA',
                                                 'industry', 'industry_code', 'concept', 'IndustryChange'])

                df['industry'] = ''
                df['industry_code'] = ''
                df['concept'] = ''

                # 取万德的行业数据
                Wind_stock_code = str(stock_code[2:8] + '.' + stock_code[0:2].upper())
                end_date = str(df.dateS[0][0:10])
                stt_date = str(df.dateS[len(df) - 1][0:10])
                if table_type is '1m':
                    industry_data = w.wsd(Wind_stock_code, "concept, industry2, industrycode", stt_date, end_date,
                                               "industryType=1", "industryStandard=1", "Period=M")
                elif table_type is '1w':
                    industry_data = w.wsd(Wind_stock_code, "concept, industry2, industrycode", stt_date, end_date,
                                               "industryType=1", "industryStandard=1", "Period=W")
                if industry_data.ErrorCode != 0:
                    print 'Wind Error: ' + str(industry_data.ErrorCode)
                    exit(1)
                df_industry_data = pd.DataFrame(industry_data.Times, columns=['dateI'])
                df_industry_data['concept'] = pd.DataFrame(industry_data.Data[0])
                df_industry_data['industry'] = pd.DataFrame(industry_data.Data[1])
                df_industry_data['industry_code'] = pd.DataFrame(industry_data.Data[2])
                df_industry_data.set_index('dateI', drop=False, inplace=True)

                print len(df)
                for i in range(0, len(df)):
                    print i
                    if i >= 119:
                        pass
                    dateI = df.dateI[i]
                    dateNw  = df.dateS[i]
                    dateNw_str = str(dateNw[0:10])
                    rp_typeNw = Ddata.report_type[dateNw]
                    # df.loc[dateNw, 'report_type'] = rp_typeNw

                    # 日factors 对齐
                    df.loc[dateNw, ['STO_1M', 'STO_3M', 'STO_6M', 'STO_12M', 'STO_60M',
                                    'ETOP', 'BTOP', 'STOP', 'LPRI',
                                    'RSTR_1M', 'RSTR_3M', 'RSTR_6M', 'RSTR_12M', 'HILO', 'DASTD', 'CMRA']] \
                        = Ddata.loc[dateNw, ['STO_1M', 'STO_3M', 'STO_6M', 'STO_12M', 'STO_60M',
                                             'ETOP', 'BTOP', 'STOP', 'LPRI',
                                             'RSTR_1M', 'RSTR_3M', 'RSTR_6M', 'RSTR_12M', 'HILO', 'DASTD', 'CMRA']]
                    df.loc[dateNw, 'Size'] = Ddata.loc[dateNw, 'LNCAP']

                    # 季factors 对齐
                    tmpVolm = Qdata[Qdata.report_type == rp_typeNw]
                    if len(tmpVolm)>0:
                        # print tmpVolm
                        df.loc[dateNw, ['ETP5', 'S_GPM', 'C_GPM', 'T_GPM', 'S_NPM', 'C_NPM','T_NPM',
                                        'S_ROE', 'C_ROE', 'T_ROE', 'S_ROA', 'C_ROA', 'T_ROA']] \
                            = tmpVolm.loc[tmpVolm.report_type[0],
                                       ['ETP5', 'S_GPM', 'C_GPM', 'T_GPM', 'S_NPM', 'C_NPM','T_NPM',
                                        'S_ROE', 'C_ROE', 'T_ROE', 'S_ROA', 'C_ROA', 'T_ROA']]
                        df.loc[dateNw, 'Growth'] = tmpVolm.loc[tmpVolm.report_type[0], 'AGRO']
                        df.loc[dateNw, 'Leverage'] = tmpVolm.loc[tmpVolm.report_type[0], 'MLEV']

                    if table_type is '1m':
                        # 周factors 对齐
                        tmp_vol = Wdata[Wdata.dateS <= dateNw].head(1)
                        if len(tmp_vol) > 0:
                            df.loc[dateNw, 'VOLBT'] = tmp_vol['VOLBT'][0]
                        # 月factors 对齐
                        df.loc[dateNw, ['ALPHA', 'BTSG', 'SERDP', 'BETA', 'SIGMA']] \
                            = Mdata.loc[dateNw, ['ALPHA', 'BTSG', 'SERDP', 'BETA', 'SIGMA']]
                    elif table_type is '1w':
                        # 周factors 对齐
                        df.loc[dateNw, ['VOLBT', 'ALPHA', 'BTSG', 'SERDP', 'BETA', 'SIGMA']] \
                            = Wdata.loc[dateNw, ['VOLBT', 'ALPHA', 'BTSG', 'SERDP', 'BETA', 'SIGMA']]

                    # 行业数据
                    if i == 0:  # 最新一天的数据需单独读取
                        wind_industry_data = w.wsd(Wind_stock_code, "concept, industry2, industrycode",
                                                   dateNw_str, dateNw_str, "industryType=1", "industryStandard=1")
                        if wind_industry_data.ErrorCode != 0:
                            print 'Wind Error: ' + str(wind_industry_data.ErrorCode)
                            exit(1)
                        df.loc[dateNw, 'concept'] = wind_industry_data.Data[0][0]
                        df.loc[dateNw, 'industry'] = wind_industry_data.Data[1][0]
                        df.loc[dateNw, 'industry_code'] = wind_industry_data.Data[2][0]
                    else:
                        df.loc[dateNw, 'concept'] = df_industry_data.loc[dateI, 'concept'][0]
                        df.loc[dateNw, 'industry'] = df_industry_data.loc[dateI, 'industry'][0]
                        df.loc[dateNw, 'industry_code'] = df_industry_data.loc[dateI, 'industry_code'][0]

                    if isinstance(df.industry[i], basestring):    # 判断是否 字符型
                        if df.industry[i] is not None and df.industry[i] is not '':
                            df.loc[dateNw, 'IndustryChange'] = industry_change_df.loc[dateI, df.industry[i]]

                df.to_sql(table_name, sqlite_engine, if_exists='append', index=False, chunksize=100000)
                tmp_len = str(len(df))
            else:
                tmp_len = '0'
            print 'update_all_factors_rolling_table: ' + stock_code + '  len: ' + tmp_len
            pass

        w.stop()
        return 0

    def update_all_factors_rolling_table_FAST_FOR_SHORTERM(self, table_name, table_type):

        sqlite_engine = self.sqlite_engine

        query_line = ("select dateS from %(table_name)s order by dateS desc" % {'table_name': table_name})
        cursor = pd.read_sql_query(query_line, sqlite_engine)
        if len(cursor) > 1:
            last_date = get_format_date(str(cursor.ix[0].values)[3:13]) + timedelta(days=1)  # 加一天, 因为 sqlite 的[date]> 效果等于 [date]>=
            last_date_str = str(last_date.date())
        else:
            last_date_str = '1900-01-01'

        if table_type is '1m':
            query_line = ("select * from industry_change_1m where dateI>'%(dt)s' order by dateI desc" % {'dt': last_date_str})
        elif table_type is '1w':
            query_line = ("select * from industry_change_1w where dateI>'%(dt)s' order by dateI desc" % {'dt': last_date_str})
        industry_change_df = pd.read_sql_query(query_line, sqlite_engine)
        industry_change_df.set_index('dateI', drop=False, inplace=True)  # date 列设为index

        w.start()
        # 读取日factors
        query_line = ("select [code],[date],[report_type],[ETOP],[STO_1M],[STO_3M],[STO_6M],[STO_12M],[STO_60M]  \
                      ,[RSTR_1M],[RSTR_3M],[RSTR_6M],[RSTR_12M],[LNCAP],[BTOP],[STOP],[HILO],[DASTD],[LPRI],[CMRA]   \
                      from day_factors_update where [date]>'%(dt)s' order by date DESC" %
                      {'dt': last_date_str})
        Ddata_all = pd.read_sql_query(query_line, sqlite_engine)
        D_code_list = pd.Series(Ddata_all['code'].copy()).unique()
        Ddata_all.set_index('code', drop=False, inplace=True)

        if len(Ddata_all) > 0:
            if table_type is '1m':
                # 读取周数据
                last_week_date = last_date + timedelta(days=-10)    # 向前多取一周时间, 以防跟月对其的时候, 取不到上1周数据
                last_week_date_str = str(last_week_date.date())
                query_line = ("select [code],[dateS],[dateI],[VOLBT],[stock_change],[index_change]  \
                              from week_factors_update where [dateS]>'%(dt)s' order by dateS DESC" %
                              {'dt': last_week_date_str})
                Wdata_all = pd.read_sql_query(query_line, sqlite_engine)
                W_code_list = pd.Series(Wdata_all['code'].copy()).unique()
                Wdata_all.set_index('code', drop=False, inplace=True)

                # 读取月factors
                query_line = ("select [code],[stock_trade_date],[index_trade_dateI],[ALPHA],[BETA],[SIGMA], \
                              [BTSG],[SERDP],[stock_change],[index_change] from month_factors_update \
                              where stock_trade_date>'%(dt)s' order by stock_trade_date DESC" %
                              {'dt': last_date_str})
                Mdata_all = pd.read_sql_query(query_line, sqlite_engine)
                Mdata_all.columns = ['code', 'dateS', 'dateI', 'ALPHA', 'BETA', 'SIGMA',
                                    'BTSG', 'SERDP', 'stock_change', 'index_change']
                M_code_list = pd.Series(Mdata_all['code'].copy()).unique()
                Mdata_all.set_index('code', drop=False, inplace=True)
                stock_list = list(set(D_code_list) & set(W_code_list) & set(M_code_list))

            elif table_type is '1w':
                # 读取周数据
                query_line = ("select code,dateS,dateI,VOLBT,stock_change,ALPHA,BETA,SIGMA,BTSG,SERDP \
                              from week_factors_update where [dateS]>'%(dt)s' order by dateS DESC" %
                              {'dt': last_date_str})
                Wdata_all = pd.read_sql_query(query_line, sqlite_engine)
                W_code_list = pd.Series(Wdata_all['code'].copy()).unique()
                Wdata_all.set_index('code', drop=False, inplace=True)
                stock_list = list(set(D_code_list) & set(W_code_list))
            else:
                stock_list = []
                print 'table_type error in update_all_factors_rolling_table()'
                exit(1)

            for stock_code in stock_list:
                print 'update_all_factors_rolling_table: ' + stock_code
                Ddata = Ddata_all.loc[stock_code, :]
                if Ddata._typ != 'dataframe':
                    Ddata = pd.DataFrame(Ddata).T
                Ddata.set_index('date', drop=False, inplace=True)  # date 列设为index

                if len(Ddata) > 0:
                    # 读取季factors
                    last_report_type_str = str(Ddata.report_type[len(Ddata) - 1][0:10])   # 找更新交易日对应的report_type

                    query_line = ("select [code],[report_type],[ETP5],[AGRO],[MLEV],[S_GPM],[C_GPM],[T_GPM],[S_NPM],[C_NPM], \
                                  [T_NPM],[S_ROE],[C_ROE],[T_ROE],[S_ROA],[C_ROA],[T_ROA] from quarter_factors_update \
                                  where code='%(cd)s' and report_type>='%(dt)s' order by report_type DESC" %
                                  {'cd': stock_code, 'dt': last_report_type_str})
                    Qdata = pd.read_sql_query(query_line, sqlite_engine)
                    Qdata.set_index('report_type', drop=False, inplace=True)  # report_type 列设为index

                    if table_type is '1m':
                        # 读取周数据
                        Wdata = Wdata_all.loc[stock_code, :]
                        if Wdata._typ != 'dataframe':
                            Wdata = pd.DataFrame(Wdata).T
                        Wdata.set_index('dateS', drop=False, inplace=True)  # dateS 列设为index

                        # 读取月factors
                        Mdata = Mdata_all.loc[stock_code, :]
                        if Mdata._typ != 'dataframe':
                            Mdata = pd.DataFrame(Mdata).T
                        Mdata.set_index('dateS', drop=False, inplace=True)  # dateS 列设为index

                        # 主时间轴 设为 "月"
                        df = pd.DataFrame(Mdata.loc[:, ['code', 'dateS', 'dateI', 'stock_change']], index=Mdata.dateS,
                                            columns=['code', 'dateS', 'dateI', 'stock_change', 'ETOP','ETP5', 'Growth',
                                                     'Leverage', 'STO_1M', 'STO_3M', 'STO_6M', 'STO_12M', 'STO_60M',
                                                     'ALPHA', 'RSTR_1M', 'RSTR_3M', 'RSTR_6M', 'RSTR_12M', 'Size',
                                                     'BTOP', 'STOP', 'HILO', 'BTSG', 'DASTD', 'LPRI', 'CMRA', 'VOLBT',
                                                     'SERDP', 'BETA', 'SIGMA',
                                                     'S_GPM', 'C_GPM', 'T_GPM', 'S_NPM', 'C_NPM', 'T_NPM',
                                                     'S_ROE', 'C_ROE', 'T_ROE', 'S_ROA', 'C_ROA', 'T_ROA',
                                                     'industry', 'industry_code', 'concept', 'IndustryChange'])
                    elif table_type is '1w':
                        # 读取周数据
                        Wdata = Wdata_all.loc[stock_code, :]
                        Wdata.set_index('dateS', drop=False, inplace=True)  # dateS 列设为index
                        # 主时间轴 设为 "周"
                        df = pd.DataFrame(Wdata.loc[:, ['code', 'dateS', 'dateI', 'stock_change']], index=Wdata.dateS,
                                            columns=['code', 'dateS', 'dateI', 'stock_change', 'ETOP','ETP5', 'Growth',
                                                     'Leverage', 'STO_1M', 'STO_3M', 'STO_6M', 'STO_12M', 'STO_60M',
                                                     'ALPHA', 'RSTR_1M', 'RSTR_3M', 'RSTR_6M', 'RSTR_12M', 'Size',
                                                     'BTOP', 'STOP', 'HILO', 'BTSG', 'DASTD', 'LPRI', 'CMRA', 'VOLBT',
                                                     'SERDP', 'BETA', 'SIGMA',
                                                     'S_GPM', 'C_GPM', 'T_GPM', 'S_NPM', 'C_NPM', 'T_NPM',
                                                     'S_ROE', 'C_ROE', 'T_ROE', 'S_ROA', 'C_ROA', 'T_ROA',
                                                     'industry', 'industry_code', 'concept', 'IndustryChange'])

                    df['industry'] = ''
                    df['industry_code'] = ''
                    df['concept'] = ''

                    # 取万德的行业数据
                    Wind_stock_code = str(stock_code[2:8] + '.' + stock_code[0:2].upper())
                    end_date = str(df.dateS[0][0:10])
                    stt_date = str(df.dateS[len(df) - 1][0:10])
                    if table_type is '1m':
                        industry_data = w.wsd(Wind_stock_code, "concept, industry2, industrycode", stt_date, end_date,
                                                   "industryType=1", "industryStandard=1", "Period=M")
                    elif table_type is '1w':
                        industry_data = w.wsd(Wind_stock_code, "concept, industry2, industrycode", stt_date, end_date,
                                                   "industryType=1", "industryStandard=1", "Period=W")
                    if industry_data.ErrorCode != 0:
                        print 'Wind Error: ' + str(industry_data.ErrorCode) + '    try again ...'
                        if table_type is '1m':
                            industry_data = w.wsd(Wind_stock_code, "concept, industry2, industrycode", stt_date, end_date,
                                                   "industryType=1", "industryStandard=1", "Period=M")
                        elif table_type is '1w':
                            industry_data = w.wsd(Wind_stock_code, "concept, industry2, industrycode", stt_date, end_date,
                                                       "industryType=1", "industryStandard=1", "Period=W")
                        if industry_data.ErrorCode != 0:
                            print 'Wind Error: ' + str(industry_data.ErrorCode)
                            exit(1)
                    df_industry_data = pd.DataFrame(industry_data.Times, columns=['dateI'])
                    df_industry_data['concept'] = pd.DataFrame(industry_data.Data[0])
                    df_industry_data['industry'] = pd.DataFrame(industry_data.Data[1])
                    df_industry_data['industry_code'] = pd.DataFrame(industry_data.Data[2])
                    df_industry_data.set_index('dateI', drop=False, inplace=True)

                    for i in range(0, len(df)):
                        dateI = df.dateI[i]
                        dateNw  = df.dateS[i]
                        dateNw_str = str(dateNw[0:10])
                        rp_typeNw = Ddata.report_type[dateNw]
                        # df.loc[dateNw, 'report_type'] = rp_typeNw

                        # 日factors 对齐
                        df.loc[dateNw, ['STO_1M', 'STO_3M', 'STO_6M', 'STO_12M', 'STO_60M',
                                        'ETOP', 'BTOP', 'STOP', 'LPRI',
                                        'RSTR_1M', 'RSTR_3M', 'RSTR_6M', 'RSTR_12M', 'HILO', 'DASTD', 'CMRA']] \
                            = Ddata.loc[dateNw, ['STO_1M', 'STO_3M', 'STO_6M', 'STO_12M', 'STO_60M',
                                                 'ETOP', 'BTOP', 'STOP', 'LPRI',
                                                 'RSTR_1M', 'RSTR_3M', 'RSTR_6M', 'RSTR_12M', 'HILO', 'DASTD', 'CMRA']]
                        df.loc[dateNw, 'Size'] = Ddata.loc[dateNw, 'LNCAP']

                        # 季factors 对齐
                        tmpVolm = Qdata[Qdata.report_type == rp_typeNw]
                        if len(tmpVolm)>0:
                            # print tmpVolm
                            df.loc[dateNw, ['ETP5', 'S_GPM', 'C_GPM', 'T_GPM', 'S_NPM', 'C_NPM','T_NPM',
                                            'S_ROE', 'C_ROE', 'T_ROE', 'S_ROA', 'C_ROA', 'T_ROA']] \
                                = tmpVolm.loc[tmpVolm.report_type[0],
                                           ['ETP5', 'S_GPM', 'C_GPM', 'T_GPM', 'S_NPM', 'C_NPM','T_NPM',
                                            'S_ROE', 'C_ROE', 'T_ROE', 'S_ROA', 'C_ROA', 'T_ROA']]
                            df.loc[dateNw, 'Growth'] = tmpVolm.loc[tmpVolm.report_type[0], 'AGRO']
                            df.loc[dateNw, 'Leverage'] = tmpVolm.loc[tmpVolm.report_type[0], 'MLEV']

                        if table_type is '1m':
                            # 周factors 对齐
                            tmp_vol = Wdata[Wdata.dateS <= dateNw].head(1)
                            if len(tmp_vol) > 0:
                                df.loc[dateNw, 'VOLBT'] = tmp_vol['VOLBT'][0]
                            # 月factors 对齐
                            df.loc[dateNw, ['ALPHA', 'BTSG', 'SERDP', 'BETA', 'SIGMA']] \
                                = Mdata.loc[dateNw, ['ALPHA', 'BTSG', 'SERDP', 'BETA', 'SIGMA']]
                        elif table_type is '1w':
                            # 周factors 对齐
                            df.loc[dateNw, ['VOLBT', 'ALPHA', 'BTSG', 'SERDP', 'BETA', 'SIGMA']] \
                                = Wdata.loc[dateNw, ['VOLBT', 'ALPHA', 'BTSG', 'SERDP', 'BETA', 'SIGMA']]

                        # 行业数据
                        if i == 0:  # 最新一天的数据需单独读取
                            wind_industry_data = w.wsd(Wind_stock_code, "concept, industry2, industrycode",
                                                       dateNw_str, dateNw_str, "industryType=1", "industryStandard=1")
                            if wind_industry_data.ErrorCode != 0:
                                print 'Wind Error: ' + str(wind_industry_data.ErrorCode) + '   try again...'
                                wind_industry_data = w.wsd(Wind_stock_code, "concept, industry2, industrycode",
                                                       dateNw_str, dateNw_str, "industryType=1", "industryStandard=1")
                                if wind_industry_data.ErrorCode != 0:
                                    print 'Wind Error: ' + str(wind_industry_data.ErrorCode)
                                    exit(1)

                            df.loc[dateNw, 'concept'] = wind_industry_data.Data[0][0]
                            df.loc[dateNw, 'industry'] = wind_industry_data.Data[1][0]
                            df.loc[dateNw, 'industry_code'] = wind_industry_data.Data[2][0]
                        else:
                            df.loc[dateNw, 'concept'] = df_industry_data.loc[dateI, 'concept'][0]
                            df.loc[dateNw, 'industry'] = df_industry_data.loc[dateI, 'industry'][0]
                            df.loc[dateNw, 'industry_code'] = df_industry_data.loc[dateI, 'industry_code'][0]

                        if isinstance(df.industry[i], basestring):    # 判断是否 字符型
                            if df.industry[i] is not None:
                                df.loc[dateNw, 'IndustryChange'] = industry_change_df.loc[dateI, df.industry[i]]

                    df.to_sql(table_name, sqlite_engine, if_exists='append', index=False, chunksize=100000)
                    tmp_len = str(len(df))
                else:
                    tmp_len = '0'
                print 'update_all_factors_rolling_table: ' + stock_code + '  len: ' + tmp_len
                pass

        w.stop()
        return 0

    def update_standard_table(self, table_name_from, table_name_to):
        de_industry_flag = False
        sqlite_engine = self.sqlite_engine

        query_line = ("select distinct dateI from %(table_name)s order by [dateI] desc" % {'table_name': table_name_to})
        cursor = pd.read_sql_query(query_line, sqlite_engine)
        if len(cursor) > 1:
            last_date = get_format_date(str(cursor.ix[0].values)[3:13]) + timedelta(days=1)  # 加一天, 因为 sqlite 的[date]> 效果等于 [date]>=
            last_date_str = str(last_date.date())
        else:
            last_date_str = '2005-01-01'

        query_line = ("select * from %(table_name)s where [dateI]>'%(date_from)s' order by [dateI]"
                      % {'table_name': table_name_from, 'date_from': last_date_str})
        df_factors = pd.read_sql(query_line, sqlite_engine)
        df_factors.set_index('dateI', drop=False, inplace=True)

        df_factors = df_factors[df_factors['dateS'] == df_factors['dateI']]  # 用'dateS'获取以去除期末停牌股
        df_factors = df_factors[df_factors != ''].copy()
        df_factors = df_factors.dropna()  # dropna() 去除nan数据

        date_list = df_factors.dateI
        date_list = list(date_list.drop_duplicates())
        print 'standardize_f_tables: '
        for date in date_list:
            print date
            df_today = df_factors[df_factors['dateI'] == date].copy()

            if de_industry_flag is True:
                factor_list = list(['ETOP', 'ETP5', 'Growth', 'Leverage', 'STO_1M', 'STO_3M', 'STO_6M',
                                    'STO_12M', 'STO_60M', 'ALPHA', 'RSTR_1M', 'RSTR_3M', 'RSTR_6M', 'RSTR_12M',
                                    'Size', 'BTOP', 'STOP', 'HILO', 'BTSG', 'DASTD', 'LPRI', 'CMRA', 'VOLBT',
                                    'SERDP', 'BETA', 'SIGMA', 'S_GPM', 'C_GPM', 'T_GPM', 'S_NPM', 'C_NPM',
                                    'T_NPM', 'S_ROE', 'C_ROE', 'T_ROE', 'S_ROA', 'C_ROA', 'T_ROA'])
                # print industry_de
                industry_list = pd.Series(df_today['industry_code'].copy()).unique()
                industry_list = industry_list.tolist()
                print '个因子按行业加权平均'
                for industry_code in industry_list:  # 某日df按行业循环
                    print industry_code
                    df_today_some_industry = df_today[df_today['industry_code']==industry_code].copy()
                    for factor in factor_list:  # 某日某行业df按factor循环
                        df_tmp = df_today_some_industry.loc[:, [factor, 'Size']].copy()
                        size_sum = 0
                        factor_sum = 0
                        for ii in range(0, len(df_tmp)):
                            df_tmp.iloc[ii, 1] = math.exp(df_tmp.iloc[ii, 1])
                            factor_sum += (df_tmp.iloc[ii, 0] * df_tmp.iloc[ii, 1])
                            size_sum += df_tmp.iloc[ii, 1]
                        df_today_some_industry.loc[:, [factor]] = df_tmp.iloc[:, 0] - (factor_sum / size_sum)
                    df_today.loc[df_today['industry_code'] == industry_code] = df_today_some_industry
                df_factors.loc[df_factors['dateI'] == date] = df_today

            print '因子标准化'
            df_factors.loc[df_factors['dateI'] == date, ['ETOP']] = standardize_f(df_factors.ETOP[date])
            df_factors.loc[df_factors['dateI'] == date, ['ETP5']] = standardize_f(df_factors.ETP5[date])
            df_factors.loc[df_factors['dateI'] == date, ['Growth']] = standardize_f(df_factors.Growth[date])
            df_factors.loc[df_factors['dateI'] == date, ['Leverage']] = standardize_f(df_factors.Leverage[date])
            df_factors.loc[df_factors['dateI'] == date, ['STO_1M']] = standardize_f(df_factors.STO_1M[date])
            df_factors.loc[df_factors['dateI'] == date, ['STO_3M']] = standardize_f(df_factors.STO_3M[date])
            df_factors.loc[df_factors['dateI'] == date, ['STO_6M']] = standardize_f(df_factors.STO_6M[date])
            df_factors.loc[df_factors['dateI'] == date, ['STO_12M']] = standardize_f(df_factors.STO_12M[date])
            df_factors.loc[df_factors['dateI'] == date, ['STO_60M']] = standardize_f(df_factors.STO_60M[date])
            df_factors.loc[df_factors['dateI'] == date, ['ALPHA']] = standardize_f(df_factors.ALPHA[date])
            df_factors.loc[df_factors['dateI'] == date, ['RSTR_1M']] = standardize_f(df_factors.RSTR_1M[date])
            df_factors.loc[df_factors['dateI'] == date, ['RSTR_3M']] = standardize_f(df_factors.RSTR_3M[date])
            df_factors.loc[df_factors['dateI'] == date, ['RSTR_6M']] = standardize_f(df_factors.RSTR_6M[date])
            df_factors.loc[df_factors['dateI'] == date, ['RSTR_12M']] = standardize_f(df_factors.RSTR_12M[date])
            df_factors.loc[df_factors['dateI'] == date, ['Size']] = standardize_f(df_factors.Size[date])
            df_factors.loc[df_factors['dateI'] == date, ['BTOP']] = standardize_f(df_factors.BTOP[date])
            df_factors.loc[df_factors['dateI'] == date, ['STOP']] = standardize_f(df_factors.STOP[date])
            df_factors.loc[df_factors['dateI'] == date, ['HILO']] = standardize_f(df_factors.HILO[date])
            df_factors.loc[df_factors['dateI'] == date, ['BTSG']] = standardize_f(df_factors.BTSG[date])
            df_factors.loc[df_factors['dateI'] == date, ['DASTD']] = standardize_f(df_factors.DASTD[date])
            df_factors.loc[df_factors['dateI'] == date, ['LPRI']] = standardize_f(df_factors.LPRI[date])
            df_factors.loc[df_factors['dateI'] == date, ['CMRA']] = standardize_f(df_factors.CMRA[date])
            df_factors.loc[df_factors['dateI'] == date, ['VOLBT']] = standardize_f(df_factors.VOLBT[date])
            df_factors.loc[df_factors['dateI'] == date, ['SERDP']] = standardize_f(df_factors.SERDP[date])
            df_factors.loc[df_factors['dateI'] == date, ['BETA']] = standardize_f(df_factors.BETA[date])
            df_factors.loc[df_factors['dateI'] == date, ['SIGMA']] = standardize_f(df_factors.SIGMA[date])
            df_factors.loc[df_factors['dateI'] == date, ['S_GPM']] = standardize_f(df_factors.S_GPM[date])
            df_factors.loc[df_factors['dateI'] == date, ['C_GPM']] = standardize_f(df_factors.C_GPM[date])
            df_factors.loc[df_factors['dateI'] == date, ['T_GPM']] = standardize_f(df_factors.T_GPM[date])
            df_factors.loc[df_factors['dateI'] == date, ['S_NPM']] = standardize_f(df_factors.S_NPM[date])
            df_factors.loc[df_factors['dateI'] == date, ['C_NPM']] = standardize_f(df_factors.C_NPM[date])
            df_factors.loc[df_factors['dateI'] == date, ['T_NPM']] = standardize_f(df_factors.T_NPM[date])
            df_factors.loc[df_factors['dateI'] == date, ['S_ROE']] = standardize_f(df_factors.S_ROE[date])
            df_factors.loc[df_factors['dateI'] == date, ['C_ROE']] = standardize_f(df_factors.C_ROE[date])
            df_factors.loc[df_factors['dateI'] == date, ['T_ROE']] = standardize_f(df_factors.T_ROE[date])
            df_factors.loc[df_factors['dateI'] == date, ['S_ROA']] = standardize_f(df_factors.S_ROA[date])
            df_factors.loc[df_factors['dateI'] == date, ['C_ROA']] = standardize_f(df_factors.C_ROA[date])
            df_factors.loc[df_factors['dateI'] == date, ['T_ROA']] = standardize_f(df_factors.T_ROA[date])
            # Industry 因子暴漏设为1的情况下, IndustryChange就是该因子收益, 所以IndustryChange不用标准化

        # 输出标准化后的因子表格到sql
        df_factors.to_sql(table_name_to, sqlite_engine, if_exists='append', index=False, chunksize=100000)

        return 0

    def update_weighting_table(self, table_name_from, table_name_to):

        sqlite_engine = self.sqlite_engine

        query_line = ("select distinct dateI from %(table)s order by dateI desc" % {'table': table_name_to})
        cursor = pd.read_sql_query(query_line, sqlite_engine)
        if len(cursor) > 1:
            # 加一天, 因为 sqlite 和 pandas的unicode格式下 的[date]> 效果等于 [date]>=
            last_date = get_format_date(str(cursor.ix[0].values)[3:13]) + timedelta(days=1)
            last_date_str = str(last_date.date())
        else:
            last_date_str = '1900-01-01'
            last_date = get_format_date(last_date_str)

        for stock_code in self.stock_list.stock_code:

            # 读取 all_factors_1M_for_roll
            stt_date = last_date + timedelta(days=-65)  # 多读取一部分数据以保证可以找到上一期的因子
            stt_date_str = str(stt_date.date())
            query_line = ("select * from %(table)s where code='%(cd)s' and dateI>'%(dt)s' order by dateI DESC" %
                          {'table': table_name_from, 'cd': stock_code, 'dt': stt_date_str})
            df_all_factors_for_roll = pd.read_sql_query(query_line, sqlite_engine)
            df_all_factors_for_roll.set_index('dateI', drop=False, inplace=True)  # dateI 列设为index

            # 根据 _roll 的table设定时间轴
            df = pd.DataFrame(df_all_factors_for_roll.loc[:, ['code', 'dateS', 'dateI', 'stock_change']],
                                        index=df_all_factors_for_roll.dateI,
                                        columns=['code', 'dateS', 'dateI', 'stock_change', 'ETOP', 'ETP5', 'Growth',
                                                 'Leverage', 'STO_1M', 'STO_3M', 'STO_6M', 'STO_12M', 'STO_60M',
                                                 'ALPHA', 'RSTR_1M', 'RSTR_3M', 'RSTR_6M', 'RSTR_12M', 'Size',
                                                 'BTOP', 'STOP', 'HILO', 'BTSG', 'DASTD', 'LPRI', 'CMRA', 'VOLBT',
                                                 'SERDP', 'BETA', 'SIGMA',
                                                 'S_GPM', 'C_GPM', 'T_GPM', 'S_NPM', 'C_NPM', 'T_NPM',
                                                 'S_ROE', 'C_ROE', 'T_ROE', 'S_ROA', 'C_ROA', 'T_ROA',
                                                 'industry', 'industry_code', 'concept', 'IndustryChange'])
            df = df[df.dateI > last_date_str]
            if len(df) > 0:  # 用数据需要更新时
                if len(df) < len(df_all_factors_for_roll):  # weighting table 为更新数据, 全部都有上一期的
                    date_len = len(df)
                elif len(df) == len(df_all_factors_for_roll):  # weighting table 从头全部计算
                    date_len = len(df)-1

                for i in range(0, date_len):
                    dateNw  = df.dateI[i]
                    datePre = df_all_factors_for_roll.dateI[i + 1]
                    df.loc[dateNw, ['ETOP', 'ETP5', 'Growth',
                                    'Leverage', 'STO_1M', 'STO_3M', 'STO_6M', 'STO_12M', 'STO_60M',
                                    'ALPHA', 'Size',
                                    'BTOP', 'STOP', 'BTSG', 'LPRI', 'VOLBT',
                                    'SERDP', 'BETA', 'SIGMA',
                                    'S_GPM', 'C_GPM', 'T_GPM', 'S_NPM', 'C_NPM', 'T_NPM',
                                    'S_ROE', 'C_ROE', 'T_ROE', 'S_ROA', 'C_ROA', 'T_ROA']] \
                        = df_all_factors_for_roll.loc[datePre,
                                   ['ETOP', 'ETP5', 'Growth',
                                    'Leverage', 'STO_1M', 'STO_3M', 'STO_6M', 'STO_12M', 'STO_60M',
                                    'ALPHA', 'Size',
                                    'BTOP', 'STOP', 'BTSG', 'LPRI', 'VOLBT',
                                    'SERDP', 'BETA', 'SIGMA',
                                    'S_GPM', 'C_GPM', 'T_GPM', 'S_NPM', 'C_NPM', 'T_NPM',
                                    'S_ROE', 'C_ROE', 'T_ROE', 'S_ROA', 'C_ROA', 'T_ROA']]
                    # 与收益率相关的因子, 对其上期, 同时受到停牌影响
                    if((get_format_date(str(dateNw)[0:10]) - get_format_date(str(datePre)[0:10])).days<57):
                        df.loc[dateNw, ['RSTR_1M', 'RSTR_3M', 'RSTR_6M', 'RSTR_12M', 'HILO', 'DASTD', 'CMRA']]\
                            = df_all_factors_for_roll.loc[datePre, ['RSTR_1M', 'RSTR_3M', 'RSTR_6M', 'RSTR_12M', 'HILO', 'DASTD', 'CMRA']]
                    df.loc[dateNw, ['industry', 'industry_code', 'concept', 'IndustryChange']] \
                        = df_all_factors_for_roll.loc[datePre,
                                   [ 'industry', 'industry_code', 'concept', 'IndustryChange']]
                df.to_sql(table_name_to, sqlite_engine, if_exists='append', index=False, chunksize=100000)
            print 'update_weighting_table: ' + stock_code + '   len: ' + str(len(df))
        return 0