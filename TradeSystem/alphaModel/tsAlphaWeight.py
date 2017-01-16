# coding=utf-8


import pandas as pd
import matplotlib.pyplot as plt
import datetime
import pymssql
from sqlalchemy import create_engine
from statsmodels import regression
import statsmodels.api as sm
import numpy as np
import time


class MSSQL:
    def __init__(self, host, user, pwd, db):
        self.host = host
        self.user = user
        self.pwd = pwd
        self.db = db

    def __getconnect(self):
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

    def execquery(self, sql):
        """
        执行查询语句
        返回的是一个包含tuple的list，list的元素是记录行，tuple的元素是每行记录的字段

        调用示例：
                ms = MSSQL(host="localhost",user="sa",pwd="123456",db="PythonWeiboStatistics")
                resList = ms.ExecQuery("SELECT id,NickName FROM WeiBoUser")
                for (id,NickName) in resList:
                    print str(id),NickName
        """
        cur = self.__getconnect()
        cur.execute(sql)
        reslist = cur.fetchall()

        # 查询完毕后必须关闭连接
        self.conn.close()
        return reslist


    def execqueryparam(self, sql, param):
        cur = self.__getconnect()
        cur.execute(sql, param)
        reslist = cur.fetchall()

        # 查询完毕后必须关闭连接
        self.conn.close()
        return reslist

    def execnonquery(self, sql):
        """
        执行非查询语句

        调用示例：
            cur = self.__GetConnect()
            cur.execute(sql)
            self.conn.commit()
            self.conn.close()
        """
        cur = self.__getconnect()
        cur.execute(sql)
        self.conn.commit()
        self.conn.close()


def standardize_f(fctor):
        # 因子1准备
        X0 = fctor.dropna()
        X1 = fctor.copy()
        # #因子去极值处理
        m_X = np.median(X0)
        mad_X = np.median(abs(X0-m_X))
        thresh_max = m_X+5*mad_X
        thresh_min = m_X-5*mad_X
        X1[X1 > thresh_max] = thresh_max
        X1[X1 < thresh_min] = thresh_min
        # 因子值标准化
        u_X = np.mean(X1.dropna())
        std_X = np.std(X1.dropna())
        X1 = (X1-u_X)/std_X
        return X1


class StockScreener(object):
    """筛选器"""

    def __init__(self, stockpool, _period, _start_date):
        self.basestockpool = stockpool
        self.period = _period
        self.start_date = datetime.datetime.strptime(_start_date, '%Y-%m-%d')
        self.datedict = {}
        self.engine = create_engine('sqlite:///D:\stock.db')
        self.ms = MSSQL(host="USER-GH61M1ULCU", user="sa", pwd="windows-999", db="stocks")

        self.date_today = datetime.datetime(1900, 1, 1)
        self.date_yesterday = datetime.datetime(1900, 1, 1)

    def handle_date(self, current_date):
        act_flag = False

        # 日期buffer滚动
        self.date_yesterday = self.date_today
        self.date_today = current_date

        # 处理-生成 self.datedict{}
        if self.date_yesterday >= self.start_date:  # 日期大于alpha模型起始日期
            if self.date_today.month > self.date_yesterday.month:   # 日期换月
                self.datedict['est_date_now'] = self.date_yesterday  # 取上个月最后一个交易日期
                act_flag = True



        return act_flag

    def get_linear_beta(self, stockR, stockF, F_name):
        # dfTMP = pd.merge(stockF.loc[:, ['code', F_name]].dropna(), stockR.dropna(), on='code', how='inner')

        if F_name == 'All':
            dfTMP = stockF.copy()
            dfTMP = dfTMP.dropna()
            # print dfTMP
            X = dfTMP.loc[:,['ETOP', 'ETP5', 'Growth', 'Leverage', 'STO_1M', 'STO_3M', 'STO_6M', 'STO_12M', 'STO_60M', 'ALPHA',
                             'RSTR_1M', 'RSTR_3M', 'RSTR_6M', 'RSTR_12M', 'Size', 'BTOP', 'STOP', 'HILO', 'BTSG', 'DASTD', 'LPRI', 'CMRA', 'VOLBT',
                             'SERDP', 'BETA', 'SIGMA', 'S_GPM', 'C_GPM', 'T_GPM', 'S_NPM', 'C_NPM', 'T_NPM', 'S_ROE', 'C_ROE', 'T_ROE',
                             'S_ROA', 'C_ROA', 'T_ROA']]
            # 所有因子列做标准化处理
            for i in range(1, len(X.T)):
                # print X.iloc[:, i]
                X.iloc[:, i] = standardize_f(X.iloc[:, i])
        elif F_name == 'Test':
            dfTMP = stockF.loc[:, ['code', 'Size', 'Value', 'stock_change']].copy()
            dfTMP = dfTMP.dropna()
            X = dfTMP.loc[:,['Size', 'Value']]
            # 所有因子列做标准化处理
            for i in range(0, len(X.T)):
                # print X.iloc[:, i].head(1)
                X.iloc[:, i] = standardize_f(X.iloc[:, i])
        else:
            dfTMP = stockF.loc[:, ['code', F_name, 'stock_change']].copy()
            dfTMP = dfTMP.dropna()
            X = dfTMP[F_name]
            X.iloc[:, 1] = standardize_f(X.iloc[:, 1])

        # print X


        Y = dfTMP['stock_change']*100.
        bench = sm.add_constant(X)
        model = regression.linear_model.OLS(Y, bench).fit()

        alpha = model.params[0]
        beta = model.params[1:len(model.params)]
        # beta = beta[0]

        # # 计算残差
        # # print X
        # print F_name + '    ' + str(beta)
        # Y_hat = np.dot(X, beta) + alpha
        # residual = Y - Y_hat
        # # print residual
        #
        # # 画图 (只对 1纬X 有效)
        # plt.scatter(X, Y, alpha=0.3)  # Plot the raw data
        # plt.plot(X, Y_hat, 'r', alpha=0.9)  # Add the regression line, colored in red
        # plt.xlabel('X Value')
        # plt.ylabel('Y Value')
        # plt.show()

        return beta

    # 返回股票池
    def get_weight_list(self, est_len):
        select_date = datetime.datetime.strftime(self.datedict['est_date_now'], '%Y-%m-%d')
        select_date_now = datetime.datetime.strftime(self.datedict['est_date_now'], '%Y-%m-%d %H:%M:%S')

        if self.basestockpool == 'ZZ800':
            sql_tuple = self.ms.execquery(
                "SELECT LOWER(SUBSTRING(代码,8,2)) + SUBSTRING(代码,0,7) AS [code] FROM dbo.ZZ500 "
                "where [日期↓]<='" + select_date +
                "' GROUP BY 代码 HAVING SUM(statecode) = 1 UNION "
                "select LOWER(SUBSTRING(代码,8,2)) + SUBSTRING(代码,0,7) AS [code] FROM dbo.HS300 "
                "where [日期↓]<='" + select_date +
                "' GROUP BY 代码 HAVING SUM(statecode) = 1")

        if self.basestockpool != 'All':
            dfstockpool = pd.DataFrame(sql_tuple, columns=['code'])

        dateBegin = self.datedict['est_date_now'] - datetime.timedelta(days = est_len)  # 取当日之前alpha_est_len个自然的数据做线性回归
        dateBegin =  datetime.datetime.strftime(dateBegin, '%Y-%m-%d %H:%M:%S')
        # print dateBegin

        sqlstr = ("SELECT * FROM stock_all_F_1M_weighting \
                where dateI<='%(select_date_now)s' \
                and dateI>'%(dateBegin)s' \
                  And [stock_change] is not null \
                  And [stock_change]<=1.4 And [stock_change]>=-0.5 \
               order by dateI desc"
              % {'select_date_now': str(select_date_now), 'dateBegin': str(dateBegin)})
        # print sqlstr


        dfFactor = pd.read_sql(sqlstr, self.engine)
        # print dfFactor
        if self.basestockpool != 'All':
            dfFactor = pd.merge(dfstockpool, dfFactor, on='code', how='left')     # merge会去掉前一期的股票, 计算beta值时会减少样本数量
        # print dfFactor
        # 公式计算修正处
        # industry_list = pd.read_csv('.\Industry_IndexCode.csv')
        dfFactor.set_index('dateI', drop=False, inplace=True)
        # print dfFactor
        T0_stockR = dfFactor.loc[:, ['code', 'stock_change']].copy()
        T1_stockF = dfFactor.loc[
            :, ['code', 'ETOP', 'ETP5', 'Growth', 'Leverage', 'STO_1M', 'STO_3M', 'STO_6M', 'STO_12M', 'STO_60M', 'ALPHA',
                'RSTR_1M', 'RSTR_3M', 'RSTR_6M', 'RSTR_12M', 'Size', 'BTOP', 'STOP', 'HILO', 'BTSG', 'DASTD', 'LPRI', 'CMRA', 'VOLBT',
                'SERDP', 'BETA', 'SIGMA', 'S_GPM', 'C_GPM', 'T_GPM', 'S_NPM', 'C_NPM', 'T_NPM', 'S_ROE', 'C_ROE', 'T_ROE',
                'S_ROA', 'C_ROA', 'T_ROA' , 'stock_change']].copy()
        # print T1_stockF
        T1_F_Beta = pd.DataFrame(np.zeros([2, 39]),
                                 columns=[  'ETOP', 'ETP5', 'Growth', 'Leverage', 'STO_1M', 'STO_3M', 'STO_6M', 'STO_12M', 'STO_60M', 'ALPHA',
                                            'RSTR_1M', 'RSTR_3M', 'RSTR_6M', 'RSTR_12M', 'Size', 'BTOP', 'STOP', 'HILO', 'BTSG', 'DASTD', 'LPRI', 'CMRA', 'VOLBT',
                                            'SERDP', 'BETA', 'SIGMA', 'S_GPM', 'C_GPM', 'T_GPM', 'S_NPM', 'C_NPM', 'T_NPM', 'S_ROE', 'C_ROE', 'T_ROE',
                                            'S_ROA', 'C_ROA', 'T_ROA', 'Industry'])
        T1_F_Beta = T1_F_Beta.head(1)
        # print T1_F_Beta

        F_Beta_tmp = self.get_linear_beta(T0_stockR, T1_stockF, 'All')
        # print F_Beta_tmp
        # print len(F_Beta_tmp)

        T1_F_Beta.ETOP[0]      = F_Beta_tmp.ETOP
        T1_F_Beta.ETP5[0]      = F_Beta_tmp.ETP5
        # T1_F_Beta.Growth[0]    = F_Beta_tmp.Growth          # 2?  # 3?
        # T1_F_Beta.Leverage[0]  = F_Beta_tmp.Leverage        # 2?  # 3?
        # T1_F_Beta.STO_1M[0]    = F_Beta_tmp.STO_1M          # 2?  # 3?
        # T1_F_Beta.STO_3M[0]    = F_Beta_tmp.STO_3M          # 2?
        T1_F_Beta.STO_6M[0]    = F_Beta_tmp.STO_6M
        T1_F_Beta.STO_12M[0]   = F_Beta_tmp.STO_12M
        T1_F_Beta.STO_60M[0]   = F_Beta_tmp.STO_60M
        T1_F_Beta.ALPHA[0]     = F_Beta_tmp.ALPHA
        T1_F_Beta.RSTR_1M[0]   = F_Beta_tmp.RSTR_1M
        T1_F_Beta.RSTR_3M[0]   = F_Beta_tmp.RSTR_3M         # 3?
        # T1_F_Beta.RSTR_6M[0]   = F_Beta_tmp.RSTR_6M       # 2?  # 3?
        # T1_F_Beta.RSTR_12M[0]  = F_Beta_tmp.RSTR_12M      # 2?  # 3?
        T1_F_Beta.Size[0]      = F_Beta_tmp.Size
        # T1_F_Beta.BTOP[0]      = F_Beta_tmp.BTOP          # 2?   # 3?
        T1_F_Beta.STOP[0]      = F_Beta_tmp.STOP
        T1_F_Beta.HILO[0]      = F_Beta_tmp.HILO
        T1_F_Beta.BTSG[0]      = F_Beta_tmp.BTSG
        # T1_F_Beta.DASTD[0]     = F_Beta_tmp.DASTD         # 2?  # 3?
        T1_F_Beta.LPRI[0]      = F_Beta_tmp.LPRI
        T1_F_Beta.CMRA[0]      = F_Beta_tmp.CMRA
        T1_F_Beta.VOLBT[0]     = F_Beta_tmp.VOLBT           # 3?
        T1_F_Beta.SERDP[0]     = F_Beta_tmp.SERDP
        # T1_F_Beta.BETA[0]      = F_Beta_tmp.BETA          # 2?  # 3?
        T1_F_Beta.SIGMA[0]     = F_Beta_tmp.SIGMA           # 3?
        T1_F_Beta.S_GPM[0]     = F_Beta_tmp.S_GPM
        T1_F_Beta.C_GPM[0]     = F_Beta_tmp.C_GPM           #  3?
        T1_F_Beta.T_GPM[0]     = F_Beta_tmp.T_GPM
        T1_F_Beta.S_NPM[0]     = F_Beta_tmp.S_NPM           #  3?
        T1_F_Beta.C_NPM[0]     = F_Beta_tmp.C_NPM
        T1_F_Beta.T_NPM[0]     = F_Beta_tmp.T_NPM
        T1_F_Beta.S_ROE[0]     = F_Beta_tmp.S_ROE           #  3?
        T1_F_Beta.C_ROE[0]     = F_Beta_tmp.C_ROE
        T1_F_Beta.T_ROE[0]     = F_Beta_tmp.T_ROE
        T1_F_Beta.S_ROA[0]     = F_Beta_tmp.S_ROA
        T1_F_Beta.C_ROA[0]     = F_Beta_tmp.C_ROA
        T1_F_Beta.T_ROA[0]     = F_Beta_tmp.T_ROA           #  3?
        T1_F_Beta.Industry[0]  = 1.

        # print T1_F_Beta.T
        # print len(T1_F_Beta.T)

        # 获得当期的因子载荷, 进而与 上一期的因子Beta "T1_F_Beta" 相乘得到 当期的得分
        sqlstr = ("SELECT * FROM stock_all_F_1M_rolling \
                where dateI='%(select_date_now)s' "
              % {'select_date_now': str(select_date_now)})
        # print sqlstr
        dfFactor = pd.read_sql(sqlstr, self.engine)
        if self.basestockpool != 'All':
            dfFactor = pd.merge(dfstockpool, dfFactor, on='code', how='left')     # merge会去掉前一期的股票, 计算beta值时会减少样本数量
        # dfFactor.set_index('dateI', drop=False, inplace=True)
        # print dfFactor
        T0_stockF = dfFactor.loc[
            :, ['code', 'ETOP', 'ETP5', 'Growth', 'Leverage', 'STO_1M', 'STO_3M', 'STO_6M', 'STO_12M', 'STO_60M', 'ALPHA',
                         'RSTR_1M', 'RSTR_3M', 'RSTR_6M', 'RSTR_12M', 'Size', 'BTOP', 'STOP', 'HILO', 'BTSG', 'DASTD', 'LPRI', 'CMRA', 'VOLBT',
                         'SERDP', 'BETA', 'SIGMA', 'S_GPM', 'C_GPM', 'T_GPM', 'S_NPM', 'C_NPM', 'T_NPM', 'S_ROE', 'C_ROE', 'T_ROE',
                         'S_ROA', 'C_ROA', 'T_ROA', 'IndustryChange']].copy()
        T0_stockF = T0_stockF.dropna()
        # print T0_stockF.iloc[:,1:11]
        # f_mtrx = np.matrix(T0_stockF.iloc[:,1:11])
        estValue = np.dot(T0_stockF.iloc[:,1:40], T1_F_Beta.T)
        T0_stockF['estValue'] = estValue
        dfTMP = T0_stockF.sort_values(by='estValue', ascending=False)
        dfStockWeight = dfTMP.loc[:, ['code','estValue']]
        # print dfStockWeight

        return dfStockWeight
