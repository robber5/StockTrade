# coding=utf-8
import datetime
import pymssql
import pandas as pd
import numpy as np
from statsmodels import regression
import statsmodels.api as sm
import matplotlib.pyplot as plt
import math
import os

class MSSQL:
    def __init__(self, host, user, pwd, db):
        self.host = host
        self.user = user
        self.pwd = pwd
        self.db = db

    def __GetConnect(self):
        """
        得到连接信息
        返回: conn.cursor()
        """
        if not self.db:
            raise (NameError, "没有设置数据库信息")
        self.conn = pymssql.connect(host=self.host, user=self.user, password=self.pwd, database=self.db, charset="utf8")
        cur = self.conn.cursor()
        if not cur:
            raise (NameError, "连接数据库失败")
        else:
            return cur

    def ExecQuery(self, sql):
        """
        执行查询语句
        返回的是一个包含tuple的list，list的元素是记录行，tuple的元素是每行记录的字段

        调用示例：
                ms = MSSQL(host="localhost",user="sa",pwd="123456",db="PythonWeiboStatistics")
                resList = ms.ExecQuery("SELECT id,NickName FROM WeiBoUser")
                for (id,NickName) in resList:
                    print str(id),NickName
        """
        cur = self.__GetConnect()
        cur.execute(sql)
        resList = cur.fetchall()

        # 查询完毕后必须关闭连接
        self.conn.close()
        return resList

    def ExecQueryPara(self, sql, para):
        cur = self.__GetConnect()
        cur.execute(sql, para)
        resList = cur.fetchall()

        # 查询完毕后必须关闭连接
        self.conn.close()
        return resList

    def ExecNonQuery(self, sql):
        """
        执行非查询语句

        调用示例：
            cur = self.__GetConnect()
            cur.execute(sql)
            self.conn.commit()
            self.conn.close()
        """
        cur = self.__GetConnect()
        cur.execute(sql)
        self.conn.commit()
        self.conn.close()

@profile
def ss():
    # 查找已有文件
    dir = './quarter_factors'
    files = os.listdir(dir)
    file_list = []
    for name in files:
        stock_code = name[0:8]
        file_list.append(stock_code)

    print file_list

    # 连接数据库
    ms = MSSQL(host="localhost", user="USER-GH61M1ULCU\Administrator", pwd="12qwasZX", db="stocks")

    # 读取股票列表
    SQL_tuple = ms.ExecQuery("SELECT DISTINCT [股票代码] FROM [stocks].[dbo].[all_financial_data] ")
    stock_list = pd.DataFrame(SQL_tuple, columns=['stock_code'])

    # 循环计算
    for stock_code in stock_list.stock_code:

        if stock_code in file_list:  # 如果已经有结果文件. 继续
            continue

        print stock_code

        SQL_tuple = ms.ExecQueryPara("select code,Year(date),  \
        (select top 1 market_value from stocks.dbo.stock_data b where b.code = a.code and Year(b.date)=year(a.date) order by date desc)  \
        from stocks.dbo.stock_data a where code=%s group by code,Year(date) order by code,Year(date) ", stock_code)
        MkVl_1Y = pd.DataFrame(SQL_tuple, columns=['code', 'year', 'market_value'])
        MkVl_1Y.set_index('year', drop=False, inplace=True)  # year 列设为index
        # print MkVl_1Y

        # 获取年报的营业收入数据
        SQL_tuple = ms.ExecQueryPara("SELECT [股票代码], [报告类型], [报告日期], [总市值], [营业总收入], [营业利润],  \
        [净利润], [资产总计], [负债合计], [长期负债合计], [其中：优先股]  \
        FROM [stocks].[dbo].[all_financial_data] WHERE 股票代码=%s AND 报告日期 IS NOT NULL  \
        AND 营业总收入 IS NOT NULL  \
        AND 净利润 IS NOT NULL  \
        ORDER BY 报告类型 DESC", stock_code)
        stock_data_qt = pd.DataFrame(SQL_tuple, columns=['code', 'report_type', 'report_date', 'market_value', \
                                                        'op_earning', 'op_profit', 'net_profit', 'total_assets',  \
                                                        'total_liabilities', 'L_term_liabilities', 'preference_share'])
        stock_data_qt.set_index('report_type', drop=False, inplace=True)  # date列设为index
        # print stock_data_qt

        for i in range(0, len(stock_data_qt)-1):
            typeNow = stock_data_qt.report_type[i]
            typePre = stock_data_qt.report_type[i+1]
            stock_data_qt.loc[typeNow, 'report_term'] = round((typeNow-typePre).days/30.0)
            # print typeNow.month
            if typePre.month == 12:
                stock_data_qt.loc[typeNow, 'qt_net_profit'] = stock_data_qt.loc[typeNow, 'net_profit']
                stock_data_qt.loc[typeNow, 'qt_op_profit']  = stock_data_qt.loc[typeNow, 'op_profit']
                stock_data_qt.loc[typeNow, 'qt_op_earning'] = stock_data_qt.loc[typeNow, 'op_earning']
            else:
                stock_data_qt.loc[typeNow, 'qt_net_profit'] = stock_data_qt['net_profit'][i] - stock_data_qt['net_profit'][i+1]
                stock_data_qt.loc[typeNow, 'qt_op_profit']  = stock_data_qt['op_profit'][i]  - stock_data_qt['op_profit'][i+1]
                stock_data_qt.loc[typeNow, 'qt_op_earning'] = stock_data_qt['op_earning'][i] - stock_data_qt['op_earning'][i+1]

        rpList = list(stock_data_qt.report_type)
        for i in range(0, len(stock_data_qt)-1):
        # for i in range(0, 5):
            typeNow = stock_data_qt.report_type[i]
            # print typeNow
            # if str(typeNow.date()) == '2007-03-31':
            #     print typeNow
            #     print i
            #     print stock_data_qt.report_term
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
            for j in range(i, len(stock_data_qt)-1):
                L_months += stock_data_qt['report_term'][j]
                if L_months == 60 and flg_60months == 0:
                    # print str(stock_data_qt['report_type'][i].date())+'---'+str(stock_data_qt['report_type'][j].date())
                    qt5Y_nt_prftList.append(stock_data_qt.qt_net_profit[j])
                    tt_asstList.append(stock_data_qt.total_assets[j])
                    rp_termList.append(stock_data_qt.report_term[j])
                    flg_60months = 1
                elif L_months > 60 and flg_60months == 0:
                    wt_upto60m = L_m_to_5Y/stock_data_qt['report_term'][j]
                    # print str(stock_data_qt['report_type'][i].date())+'---'+str(stock_data_qt['report_type'][j].date())+' xxxxx '+str(wt_upto60m)
                    qt5Y_nt_prftList.append(stock_data_qt.qt_net_profit[j]*wt_upto60m)
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
                    wt_upto12m = L_m_to_12M/stock_data_qt['report_term'][j]
                    # print str(stock_data_qt['report_type'][i].date())+'---'+str(stock_data_qt['report_type'][j].date())+' xxxxx '+str(wt_upto12m)
                    qt12M_nt_prftList.append(stock_data_qt.qt_net_profit[j]*wt_upto12m)
                    qt12M_op_earnList.append(stock_data_qt.qt_op_earning[j]*wt_upto12m)
                    qt12M_op_prftList.append(stock_data_qt.qt_op_profit[j]*wt_upto12m)
                    flg_12months = 1
                elif flg_12months == 0:
                    qt12M_nt_prftList.append(stock_data_qt.qt_net_profit[j])
                    qt12M_op_earnList.append(stock_data_qt.qt_op_earning[j])
                    qt12M_op_prftList.append(stock_data_qt.qt_op_profit[j])
                    L_m_to_12M = 12 - L_months
            # print qt_nt_prftList
            # print tt_asstList
            # print rp_termList
            #

            # 计算 ETP5 : 最近五年平均净利润/最近五年每年 12 月 31 日市
            yearNow = stock_data_qt.report_date[i].year
            if len(MkVl_1Y[MkVl_1Y.year == yearNow-5]>0) and flg_60months == 1:
                mean_nt_prft_5Y = sum(qt5Y_nt_prftList)*0.2
                mean_mkt_val_5Y = sum(MkVl_1Y.loc[yearNow-5:yearNow-1]['market_value'])*0.2
                stock_data_qt.loc[typeNow,  'ETP5'] = mean_nt_prft_5Y/mean_mkt_val_5Y

            # print stock_data_qt.loc[stock_data_qt.index[i],  'ETP5']

            # 计算 AGRO : 最近五年总资产的回归系数/最近五年平均总资产
            if flg_60months == 1 and len(tt_asstList) > 1 and len(rp_termList) > 1:   # 超过5年的研报才做回归
                tt_asstList.reverse()
                rp_termList.reverse()

                mean_tt_asst_5Y = sum(tt_asstList)/len(tt_asstList)
                # print mean_tt_asst_5Y
                rp_termList[0] /= 12.0
                tt_asstList[0] /= mean_tt_asst_5Y
                for k in range(1, len(rp_termList)):
                    rp_termList[k] = (rp_termList[k]/12.0 + rp_termList[k-1])
                    tt_asstList[k] /= mean_tt_asst_5Y

                Y = np.transpose(tt_asstList)
                X = np.transpose(rp_termList)

                bench = sm.add_constant(X)
                model = regression.linear_model.OLS(Y, bench).fit()
                alpha = model.params[0]
                beta = model.params[1]

                stock_data_qt.loc[typeNow, 'AGRO'] = beta

            # 计算 最近 12 个月的滚动因子
            if flg_12months == 1:   # 研报覆盖期超过12个月
                tot_asset = stock_data_qt.total_assets[i]
                net_asset = tot_asset - stock_data_qt.total_liabilities[i]
                op_earn_12M = sum(qt12M_op_earnList)
                op_prft_12M = sum(qt12M_op_prftList)
                ne_prft_12M = sum(qt12M_nt_prftList)

                # 计算 T_GPM : 最近 12 个月毛利润/最近 12 个月营业收入
                stock_data_qt.loc[typeNow, 'T_GPM'] = op_prft_12M/op_earn_12M
                # 计算 T_NPM : 最近 12 个月净利润/最近 12 个月营业收入
                stock_data_qt.loc[typeNow, 'T_NPM'] = ne_prft_12M/op_earn_12M
                # 计算 T_ROE : 最近 12 个月净利润/最新报告期净资产
                stock_data_qt.loc[typeNow, 'T_ROE'] = ne_prft_12M/net_asset
                # 计算 T_ROA : 最近 12 个月净利润/最新报告期总资产
                stock_data_qt.loc[typeNow, 'T_ROA'] = ne_prft_12M/tot_asset

            # 计算 单季 因子
            # 计算 MLEV : 市值杠杆 （市值+优先股权+长期负债）/市值
            L_term_lib = stock_data_qt.loc[typeNow, 'L_term_liabilities']
            prfr_share = stock_data_qt.loc[typeNow, 'preference_share']
            mkt_value = stock_data_qt.loc[typeNow, 'market_value']
            if str(L_term_lib) != 'None':
                if np.isnan(L_term_lib) == False and L_term_lib > 0:
                    # print str(prfr_share)
                    if str(prfr_share) == 'None':
                        stock_data_qt.loc[typeNow,  'MLEV'] = 1.0+L_term_lib/mkt_value
                    elif np.isnan(prfr_share):
                        stock_data_qt.loc[typeNow,  'MLEV'] = 1.0+L_term_lib/mkt_value
                    else:
                        stock_data_qt.loc[typeNow,  'MLEV'] = 1.0+(L_term_lib+prfr_share)/mkt_value

            # 计算 S_GPM : 单季毛利率 当年单季毛利润/当年单季营业收入
            stock_data_qt.loc[typeNow, 'S_GPM'] = stock_data_qt.qt_op_profit[i]/stock_data_qt.qt_op_earning[i]
            # 计算 S_NPM : 单季净利率 当年单季净利润/当年单季营业收入
            stock_data_qt.loc[typeNow, 'S_NPM'] = stock_data_qt.qt_net_profit[i]/stock_data_qt.qt_op_earning[i]
            # 计算 S_ROE :单季 ROE  当年单季净利润/最新报告期净资产
            stock_data_qt.loc[typeNow, 'S_ROE'] = stock_data_qt.qt_net_profit[i]/(stock_data_qt.total_assets[i]-stock_data_qt.total_liabilities[i])
            # 计算 S_ROA :单季 ROA 	当年单季净利润/最新报告期总资产
            stock_data_qt.loc[typeNow, 'S_ROA'] = stock_data_qt.qt_net_profit[i]/stock_data_qt.total_assets[i]

            # 计算 C_GPM : 累计毛利率  当年累计毛利润/当年累计营业收入
            stock_data_qt.loc[typeNow, 'C_GPM'] = stock_data_qt.op_profit[i]/stock_data_qt.op_earning[i]
            # 计算 C_NPM : 累计净利率  当年累计净利润/当年累计营业收入
            stock_data_qt.loc[typeNow, 'C_NPM'] = stock_data_qt.net_profit[i]/stock_data_qt.op_earning[i]
            # 计算 C_ROE : 累计 ROE    当年累计净利润/最新报告期净资产
            stock_data_qt.loc[typeNow, 'C_ROE'] = stock_data_qt.net_profit[i]/(stock_data_qt.total_assets[i]-stock_data_qt.total_liabilities[i])
            # 计算 C_ROA : 累计 ROA    当年累计净利润/最新报告期总资产
            stock_data_qt.loc[typeNow, 'C_ROA'] = stock_data_qt.net_profit[i]/stock_data_qt.total_assets[i]

        stock_data_qt.to_csv('./quarter_factors/' + stock_code + '_F_rpQuarter.csv', index=False)

ss()
