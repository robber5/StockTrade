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
        thresh_max = m_X+15.0*mad_X
        thresh_min = m_X-15.0*mad_X
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
        dfTMP = stockF.loc[:, ['code', F_name]].copy()
        X = dfTMP[F_name]

        # print X
        Y = stockR['stock_change'] - stockF['IndustryChange']
        bench = sm.add_constant(X)
        model = regression.linear_model.OLS(Y, bench).fit()

        alpha = model.params[0]
        beta = model.params[1]

        t = model.tvalues[1]
        rsq = model.rsquared_adj
        f = model.fvalue
        f_p = model.f_pvalue

        test = [t, 10000.0*rsq, f, f_p]

        output = beta * f

        return output

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

        sqlstr = ("SELECT * FROM all_F_1M_weighting_update_std \
                where dateI<='%(select_date_now)s' \
                and dateI>'%(dateBegin)s' \
                  And [stock_change] is not null \
                  And [stock_change]<=1.486 And [stock_change]>=-0.667 \
               order by dateI desc"
              % {'select_date_now': str(select_date_now), 'dateBegin': str(dateBegin)})
        # print sqlstr

        dfFactor = pd.read_sql(sqlstr, self.engine)
        # print dfFactor
        if self.basestockpool != 'All':
            dfFactor = pd.merge(dfstockpool, dfFactor, on='code', how='left')     # merge会去掉前一期的股票, 计算beta值时会减少样本数量

        dfFactor.set_index('dateI', drop=False, inplace=True)
        # print dfFactor
        T0_stockR = dfFactor.loc[:, ['code', 'stock_change']].copy()
        T1_stockF = dfFactor.loc[
            :, ['code', 'ETOP', 'ETP5', 'Growth', 'Leverage', 'STO_1M', 'STO_3M', 'STO_6M', 'STO_12M', 'STO_60M', 'ALPHA',
                'RSTR_1M', 'RSTR_3M', 'RSTR_6M', 'RSTR_12M', 'Size', 'BTOP', 'STOP', 'HILO', 'BTSG', 'DASTD', 'LPRI', 'CMRA', 'VOLBT',
                'SERDP', 'BETA', 'SIGMA', 'S_GPM', 'C_GPM', 'T_GPM', 'S_NPM', 'C_NPM', 'T_NPM', 'S_ROE', 'C_ROE', 'T_ROE',
                'S_ROA', 'C_ROA', 'T_ROA', 'IndustryChange']].copy()
        # print T1_stockF
        hist_f_beta_r = pd.DataFrame(np.zeros([2, 39]), columns=[ 'ETOP', 'ETP5', 'Growth', 'Leverage', 'STO_1M', 'STO_3M', 'STO_6M', 'STO_12M', 'STO_60M', 'ALPHA',
                                            'RSTR_1M', 'RSTR_3M', 'RSTR_6M', 'RSTR_12M', 'Size', 'BTOP', 'STOP', 'HILO', 'BTSG', 'DASTD', 'LPRI', 'CMRA', 'VOLBT',
                                            'SERDP', 'BETA', 'SIGMA', 'S_GPM', 'C_GPM', 'T_GPM', 'S_NPM', 'C_NPM', 'T_NPM', 'S_ROE', 'C_ROE', 'T_ROE',
                                            'S_ROA', 'C_ROA', 'T_ROA', 'Industry'])
        hist_f_beta_r   = hist_f_beta_r.head(1)
        # print hist_f_beta_r

        hist_f_beta_r.Industry[0]  = 1.0
        hist_f_beta_r.ETOP[0]      = self.get_linear_beta(T0_stockR, T1_stockF, 'ETOP')
        hist_f_beta_r.ETP5[0]      = self.get_linear_beta(T0_stockR, T1_stockF, 'ETP5')
        hist_f_beta_r.Growth[0]    = self.get_linear_beta(T0_stockR, T1_stockF, 'Growth')
        hist_f_beta_r.Leverage[0]  = self.get_linear_beta(T0_stockR, T1_stockF, 'Leverage')
        hist_f_beta_r.STO_1M[0]    = self.get_linear_beta(T0_stockR, T1_stockF, 'STO_1M')
        hist_f_beta_r.STO_3M[0]    = self.get_linear_beta(T0_stockR, T1_stockF, 'STO_3M')
        hist_f_beta_r.STO_6M[0]    = self.get_linear_beta(T0_stockR, T1_stockF, 'STO_6M')
        hist_f_beta_r.STO_12M[0]   = self.get_linear_beta(T0_stockR, T1_stockF, 'STO_12M')
        hist_f_beta_r.STO_60M[0]   = self.get_linear_beta(T0_stockR, T1_stockF, 'STO_60M')
        hist_f_beta_r.ALPHA[0]     = self.get_linear_beta(T0_stockR, T1_stockF, 'ALPHA')
        hist_f_beta_r.RSTR_1M[0]   = self.get_linear_beta(T0_stockR, T1_stockF, 'RSTR_1M')
        hist_f_beta_r.RSTR_3M[0]   = self.get_linear_beta(T0_stockR, T1_stockF, 'RSTR_3M')
        hist_f_beta_r.RSTR_6M[0]   = self.get_linear_beta(T0_stockR, T1_stockF, 'RSTR_6M')
        hist_f_beta_r.RSTR_12M[0]  = self.get_linear_beta(T0_stockR, T1_stockF, 'RSTR_12M')
        hist_f_beta_r.Size[0]      = self.get_linear_beta(T0_stockR, T1_stockF, 'Size')
        hist_f_beta_r.BTOP[0]      = self.get_linear_beta(T0_stockR, T1_stockF, 'BTOP')
        hist_f_beta_r.STOP[0]      = self.get_linear_beta(T0_stockR, T1_stockF, 'STOP')
        hist_f_beta_r.HILO[0]      = self.get_linear_beta(T0_stockR, T1_stockF, 'HILO')
        hist_f_beta_r.BTSG[0]      = self.get_linear_beta(T0_stockR, T1_stockF, 'BTSG')
        hist_f_beta_r.DASTD[0]     = self.get_linear_beta(T0_stockR, T1_stockF, 'DASTD')
        hist_f_beta_r.LPRI[0]      = self.get_linear_beta(T0_stockR, T1_stockF, 'LPRI')
        hist_f_beta_r.CMRA[0]      = self.get_linear_beta(T0_stockR, T1_stockF, 'CMRA')
        hist_f_beta_r.VOLBT[0]     = self.get_linear_beta(T0_stockR, T1_stockF, 'VOLBT')
        hist_f_beta_r.SERDP[0]     = self.get_linear_beta(T0_stockR, T1_stockF, 'SERDP')
        hist_f_beta_r.BETA[0]      = self.get_linear_beta(T0_stockR, T1_stockF, 'BETA')
        hist_f_beta_r.SIGMA[0]     = self.get_linear_beta(T0_stockR, T1_stockF, 'SIGMA')
        hist_f_beta_r.S_GPM[0]     = self.get_linear_beta(T0_stockR, T1_stockF, 'S_GPM')
        hist_f_beta_r.C_GPM[0]     = self.get_linear_beta(T0_stockR, T1_stockF, 'C_GPM')
        hist_f_beta_r.T_GPM[0]     = self.get_linear_beta(T0_stockR, T1_stockF, 'T_GPM')
        hist_f_beta_r.S_NPM[0]     = self.get_linear_beta(T0_stockR, T1_stockF, 'S_NPM')
        hist_f_beta_r.C_NPM[0]     = self.get_linear_beta(T0_stockR, T1_stockF, 'C_NPM')
        hist_f_beta_r.T_NPM[0]     = self.get_linear_beta(T0_stockR, T1_stockF, 'T_NPM')
        hist_f_beta_r.S_ROE[0]     = self.get_linear_beta(T0_stockR, T1_stockF, 'S_ROE')
        hist_f_beta_r.C_ROE[0]     = self.get_linear_beta(T0_stockR, T1_stockF, 'C_ROE')
        hist_f_beta_r.T_ROE[0]     = self.get_linear_beta(T0_stockR, T1_stockF, 'T_ROE')
        hist_f_beta_r.S_ROA[0]     = self.get_linear_beta(T0_stockR, T1_stockF, 'S_ROA')
        hist_f_beta_r.C_ROA[0]     = self.get_linear_beta(T0_stockR, T1_stockF, 'C_ROA')
        hist_f_beta_r.T_ROA[0]     = self.get_linear_beta(T0_stockR, T1_stockF, 'T_ROA')

        # 获得当期的因子载荷, 进而与 上一期的因子Beta "hist_f_beta_r" 相乘得到 当期的得分
        sqlstr = ("SELECT * FROM all_F_1M_rolling_update_std \
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
        # print T0_stockF.iloc[:, 1:40]

        estValue = np.dot(T0_stockF.iloc[:, 1:40], hist_f_beta_r.T)
        T0_stockF['estValue'] = estValue
        dfTMP = T0_stockF.sort_values(by='estValue', ascending=False)
        dfStockWeight = dfTMP.loc[:, ['code', 'estValue']]
        # print dfStockWeight
        return dfStockWeight