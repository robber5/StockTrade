# coding=utf-8

"""
filename:FactorRolling.py
Author：zyf
LastEditTime:2016/9/6
"""

import pandas as pd
import matplotlib.pyplot as plt
import datetime
import pymssql
from sqlalchemy import create_engine
from statsmodels import regression
import statsmodels.api as sm
import numpy as np


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


class StockScreener(object):
    """筛选器"""

    def __init__(self, stockpool, _num, _datedict):
        self.num = _num
        self.basestockpool = stockpool
        self.datedict = _datedict
        self.engine = create_engine('sqlite:///E:\Wanda_Work\sqlite\stock.db')
        self.ms = MSSQL(host="USER-GH61M1ULCU", user="sa", pwd="windows-999", db="stocks")

    def get_linear_beta(self, stockR, stockF, F_name):
        dfTMP = pd.merge(stockF.loc[:, ['code', F_name]].dropna(), stockR.dropna(), on='code', how='inner')
        # print dfTMP

        X = dfTMP[F_name]
        Y = dfTMP['stock_change']
        bench = sm.add_constant(X)
        model = regression.linear_model.OLS(Y, bench).fit()

        alpha = model.params[0]
        beta = model.params[1:len(model.params)]
        return beta

    # 返回股票池
    def getbuylist(self):
        buylist = []
        select_date = datetime.datetime.strftime(self.datedict['est_date_now'], '%Y-%m-%d')
        select_date_now = datetime.datetime.strftime(self.datedict['est_date_now'], '%Y-%m-%d %H:%M:%S')
        select_date_pre = datetime.datetime.strftime(self.datedict['est_date_pre'], '%Y-%m-%d %H:%M:%S')

        if self.basestockpool == 'ZZ800':
            sql_tuple = self.ms.execquery(
                "SELECT LOWER(SUBSTRING(代码,8,2)) + SUBSTRING(代码,0,7) AS [code] FROM dbo.ZZ500 "
                "where [日期↓]<'" + select_date +
                "' GROUP BY 代码 HAVING sum(statecode) = 1 UNION "
                "select LOWER(SUBSTRING(代码,8,2)) + SUBSTRING(代码,0,7) AS [code] FROM dbo.HS300 "
                "where [日期↓]<'" + select_date +
                "' GROUP BY 代码 HAVING sum(statecode) = 1")

        dfstockpool = pd.DataFrame(sql_tuple, columns=['code'])

        sqlstr = "SELECT * FROM stock_800_1M_rolling " \
                 "where dateI IN ('" + select_date_now + "','" + select_date_pre + "')"

        dfFactor = pd.read_sql(sqlstr, self.engine)
        # dfFactor = pd.merge(dfstockpool, dfFactor, on='code', how='left')     # merge会去掉前一期的股票, 计算beta值时会减少样本数量

        # 公式计算修正处
        # industry_list = pd.read_csv('.\Industry_IndexCode.csv')
        dfFactor.set_index('dateI', drop=False, inplace=True)
        T0_stockR = dfFactor.loc[select_date_now, ['code', 'stock_change']].copy()
        T1_stockF = dfFactor.loc[
            select_date_pre, ['code', 'Earnings_Yield', 'Growth', 'Leverage', 'Liquidity', 'Momentum', 'Size', 'Value',
                              'Volatility', 'Financial_Quality']].copy()
        T1_F_Beta = pd.DataFrame(np.zeros([2, 10]),
                                 columns=['Earnings_Yield', 'Growth', 'Leverage', 'Liquidity', 'Momentum', 'Size',
                                          'Value', 'Volatility', 'Financial_Quality', 'Industry'])
        T1_F_Beta = T1_F_Beta.head(1)

        T1_F_Beta.Earnings_Yield[0] = self.get_linear_beta(T0_stockR, T1_stockF, 'Earnings_Yield')
        T1_F_Beta.Growth[0] = self.get_linear_beta(T0_stockR, T1_stockF, 'Growth')
        T1_F_Beta.Leverage[0] = self.get_linear_beta(T0_stockR, T1_stockF, 'Leverage')
        T1_F_Beta.Liquidity[0] = self.get_linear_beta(T0_stockR, T1_stockF, 'Liquidity')
        T1_F_Beta.Momentum[0] = self.get_linear_beta(T0_stockR, T1_stockF, 'Momentum')
        T1_F_Beta.Size[0] = self.get_linear_beta(T0_stockR, T1_stockF, 'Size')
        T1_F_Beta.Value[0] = self.get_linear_beta(T0_stockR, T1_stockF, 'Value')
        T1_F_Beta.Volatility[0] = self.get_linear_beta(T0_stockR, T1_stockF, 'Volatility')
        T1_F_Beta.Financial_Quality[0] = self.get_linear_beta(T0_stockR, T1_stockF, 'Financial_Quality')
        T1_F_Beta.Industry[0] = 1.0

        # (1) 单因子, 权重1
        # (2) 单因子, get_linear_beta 权重
        T1_F_Beta.Earnings_Yield[0] = 0.        #(1) 1.46   =
        T1_F_Beta.Growth[0] = 0.                #(1) 0.87   <
        T1_F_Beta.Leverage[0] = 0.              #(1) 1.41   =
        T1_F_Beta.Liquidity[0] = 0.             #(1) 0.21   <<.     (2) 1.22 <
        T1_F_Beta.Momentum[0] = 0.              #(1) 1.19   <.
        T1_F_Beta.Size[0] = 0.                  #(1) 1.388  =.
        T1_F_Beta.Value[0] = 0.                 #(1) 1.79.  >
        T1_F_Beta.Volatility[0] = 0.            #(1) 3.8    >
        T1_F_Beta.Financial_Quality[0] = 0.     #(1) 0.91   <
        T1_F_Beta.Industry[0] = 1.              #(1) 1.48   >

        # print T1_F_Beta.T

        # 获得当期的因子载荷, 进而与 上一期的因子Beta "T1_F_Beta" 相乘得到 当期的得分
        T0_stockF = dfFactor.loc[
            select_date_now, ['code', 'Earnings_Yield', 'Growth', 'Leverage', 'Liquidity', 'Momentum', 'Size', 'Value',
                              'Volatility', 'Financial_Quality', 'IndustryChange']].copy()
        T0_stockF = T0_stockF.dropna()
        # print T0_stockF.iloc[:,1:11]

        # f_mtrx = np.matrix(T0_stockF.iloc[:,1:11])
        estValue = np.dot(T0_stockF.iloc[:, 1:11], T1_F_Beta.T)
        T0_stockF['estValue'] = estValue
        # print T0_stockF.sort(columns='estValue', ascending=False).head(self.num)
        dfTMP = T0_stockF.sort_values(by='estValue', ascending=False).head(self.num)
        buylist = dfTMP['code'].values
        # print buylist

        return buylist


class Account(object):
    """基金账户"""

    def __init__(self, _capital_base, _current_date, _num):
        # 净值
        self.fundvalue = 1
        # 上一K线结束后的现金头寸
        self.cash = _capital_base
        # 当前交易日
        self.current_date = _current_date
        self.num = _num  # 最大持仓股票数量
        # 净值记录dataframe,包含日期,基金净值,指数净值
        self.dfFundvalue = pd.DataFrame(columns=('date', 'fundvalue', 'benchmarkvalue'))
        # 调仓记录dataframe，包含日期,操作方向,股票代码,股票名称,买入卖出价格(不复权),操作手数
        self.dfOperate = pd.DataFrame(columns=('date', 'stockcode', 'operatetype', 'referencenum',
                                               'referenceprice'))
        # 持仓dataframe，包含股票代码，前复权的股数价格
        self.dfPosition = pd.DataFrame(columns=('stockcode', 'referencenum'))
        self.buylist = []
        self.benchmarkvalue = 0


class ChangebyTime(object):
    """个股择时模块"""

    def __init__(self, _account):
        self.account = _account

    def changebystock(self):
        """择时"""
        print('个股择时操作！')
        return self.account


class Trade(object):
    """交易控制"""

    # stockpool:总股票池
    # benchmark:基准指数
    # start:回测起始时间
    # end:回测结束时间
    # base_est_date_now:本期因子测算日期
    # base_est_date_pre：上期因子测算日期
    # refresh_rate:调仓频率
    # freq:调仓周期 d-日，m-月，y-年
    # capital_base:初始资金
    # chosen_num:持仓股票数量
    def __init__(self, _stockpool, _benchmark, _start, _end, _base_est_date_now, _base_est_date_pre, _refresh_rate,
                 _freq, _capital_base, _chosen_num):
        self.stockpool = _stockpool
        self.benchmark = _benchmark
        self.start = datetime.datetime.strptime(_start, '%Y-%m-%d')
        self.end = datetime.datetime.strptime(_end, '%Y-%m-%d')
        self.refresh_rate = _refresh_rate
        self.freq = _freq
        self.capital_base = _capital_base
        self.chosen_num = _chosen_num
        # 滑点（固定）
        self.slippage = 0
        # 买入手续费（固定）
        self.buycommission = 0
        # 卖出手续费（固定）
        self.sellcommission = 0
        # 数据库连接
        self.ms = MSSQL(host="USER-GH61M1ULCU", user="sa", pwd="windows-999", db="stocks")
        # 交易日历
        self.tradecalendars = []
        # 调仓日
        self.changecalendars = []
        # 时间字典
        self.datedict = {"est_date_now": datetime.datetime.strptime(_base_est_date_now, '%Y-%m-%d'),
                         "est_date_pre": datetime.datetime.strptime(_base_est_date_pre, '%Y-%m-%d')}

    def infoprinter(self):
        """信息显示"""
        print('股票池:' + self.stockpool)
        print('基准指数:' + self.benchmark)
        print('回测起始时间:' + datetime.datetime.strftime(self.start, '%Y-%m-%d'))
        print('回测结束时间:' + datetime.datetime.strftime(self.end, '%Y-%m-%d'))
        print('调仓频率:' + str(self.refresh_rate))
        print('调仓周期:' + self.freq)
        print('初始资金:' + str(self.capital_base))
        print('持仓股票数量:' + str(self.chosen_num))
        print('滑点:' + str(self.slippage))
        print('买入手续费:' + str(self.buycommission))
        print('卖出手续费:' + str(self.sellcommission))

    def backtest(self):
        """回测"""
        account = Account(self.capital_base, self.start, self.chosen_num)
        account.current_date = self.start

        startstr = datetime.datetime.strftime(self.start, '%Y-%m-%d')
        endstr = datetime.datetime.strftime(self.end, '%Y-%m-%d')
        eststr = datetime.datetime.strftime(self.datedict["est_date_now"], '%Y-%m-%d')

        # 获取指数起始值
        returnlist = self.ms.execquery(
            "SELECT top 1 [close] AS indexclose FROM index_data WHERE index_code = '" + self.benchmark +
            "' AND date < '" + startstr + "' ORDER BY date DESC")
        account.benchmarkvalue = returnlist[0][0]

        # 记录下初始值
        account.benchmarkvaluebase = account.benchmarkvalue

        row = pd.DataFrame(
            [dict(date=account.current_date, fundvalue=1, benchmarkvalue=1), ])
        account.dfFundvalue = account.dfFundvalue.append(row)

        # 创建交易日历加入Trade类中
        sqlstr = "SELECT [date] FROM index_data WHERE [date]>='" + eststr + "' AND [date]<='" + endstr + "'"
        calendars = self.ms.execquery(sqlstr)
        for d in calendars:
            self.tradecalendars.append(d[0])
        del calendars

        sqlstr = "SELECT (SELECT top 1 [date] FROM index_data b WHERE year(a.[date]) = year(b.[date]) AND " \
                 "month(a.[date]) = month(b.[date]) ORDER BY date ASC)  FROM index_data a WHERE [date]>= '" \
                 + startstr + "' AND [date] < '" + endstr + "' GROUP BY year([date]),month([date])"
        changecalendars = self.ms.execquery(sqlstr)
        for d in changecalendars:
            self.changecalendars.append(d[0])
        del changecalendars

        while account.current_date <= self.end:
            self.handle_date(account)
            account.current_date = account.current_date + datetime.timedelta(days=1)





        # 输出到csv
        account.dfFundvalue.to_csv('fundvalue.csv', index=False, encoding='gbk')
        account.dfPosition.to_csv('position.csv', encoding='gbk')

        # 绘制净值曲线
        plt.plotfile('fundvalue.csv', ('date', 'fundvalue', 'benchmarkvalue'), subplots=False)
        plt.show()
        self.infoprinter()

    def handle_date(self, account):
        """每日调仓"""
        if account.current_date in self.tradecalendars:

            # 调仓
            if account.current_date in self.changecalendars:
                if (self.changecalendars.index(account.current_date)) != 0:
                    self.datedict['est_date_now'] = self.tradecalendars[
                        self.tradecalendars.index(account.current_date) + 1]

                # 选股
                account.buylist = StockScreener(self.stockpool, self.chosen_num + 5,
                                                self.datedict).getbuylist()

                # 合并成操作列表之后获取开盘价格
                positionlist = account.dfPosition.index.values
                operatelist = []
                for stk in positionlist:
                    operatelist.append(stk)
                for stk in account.buylist:
                    operatelist.append(stk)

                # 获取所有需要交易的股票的前复权开盘价格
                dfOperatelist = self.get_open_history(account.current_date, operatelist)

                # 获取停牌的股票
                selectstr = ""
                for stk in account.dfPosition.index.values:
                    if stk not in dfOperatelist['stockcode'].values:
                        selectstr = selectstr + '\'' + stk + '\','
                selectstr = selectstr[:-1]

                if selectstr == "":
                    selectstr = "'1'"

                selectdate = datetime.datetime.strftime(account.current_date, '%Y-%m-%d')

                sqlstr = "SELECT code, (SELECT top 1 adjust_price_f FROM stock_data b WHERE b.[date] < '" + selectdate + \
                         "' AND b.code = a.code ORDER BY[date] DESC) FROM stock_data a WHERE code IN (" + selectstr + \
                         ") GROUP BY code"

                sql_tuple = self.ms.execquery(sqlstr)

                dfSuspendedstock = pd.DataFrame(sql_tuple, columns=('stockcode', 'adjust_price_f'))
                dfSuspendedstock = dfSuspendedstock.groupby('stockcode').sum()
                dfSuspendedstock = pd.merge(dfSuspendedstock, account.dfPosition, left_index='stockcode',
                                            right_index='stockcode')
                dfSuspendedstock['value'] = dfSuspendedstock['adjust_price_f'] * dfSuspendedstock['referencenum']
                suspendedvalue = dfSuspendedstock['value'].sum()

                # 调仓
                for stk in positionlist:
                    if stk not in account.buylist and stk not in dfSuspendedstock.index.values:
                        dftemp = dfOperatelist[dfOperatelist['stockcode'] == stk]
                        self.order_to(account, stk, 0, dftemp['adjust_open_f'].values[0])

                # 帐号总资金
                allvalue = account.fundvalue * self.capital_base - suspendedvalue

                eachvalue = allvalue / self.chosen_num

                i = 0

                for stk in account.buylist:
                    if stk in dfOperatelist['stockcode'].values:
                        if dfOperatelist[dfOperatelist['stockcode'] == stk].notnull().values[0][1]:
                            i += 1
                            if i > 10:
                                break
                            dftemp = dfOperatelist[dfOperatelist['stockcode'] == stk]
                            amount = int(eachvalue / dftemp['adjust_open_f'].values[0] / 100) * 100
                            self.order_to(account, stk, amount, dftemp['adjust_open_f'].values[0])
                        else:
                            f = open('out.txt',"a")
                            f.write(str(stk)+'有退市风险:'+str(account.current_date)+'\n')
                            f.close( )

                # 持仓变化
                account.dfPosition = account.dfOperate.loc[:, ['stockcode', 'referencenum']]
                account.dfPosition = (account.dfPosition.groupby('stockcode').sum())
                account.dfPosition = account.dfPosition[account.dfPosition.referencenum > 0]

                account.dfPosition.to_csv(
                    'E:/Wanda_Work/Changelog/' + datetime.datetime.strftime(account.current_date, '%Y-%m-%d') + '-position.csv',
                    encoding='gbk')

                # 本期变更为上期
                self.datedict['est_date_pre'] = self.datedict['est_date_now']

            # 个股择时
            account = ChangebyTime(account).changebystock()

            # 计算净值变化
            historystock = self.get_stock_close(account.dfPosition, account.current_date)
            historyindex = self.get_index_close(account.current_date)

            historystock = historystock.groupby('code').sum()

            dftemp = pd.merge(account.dfPosition, historystock, left_index='stockcode', right_index='code')

            dftemp['value'] = dftemp['referencenum'] * dftemp['adjust_price_f']
            dftemp = dftemp['value']
            account.fundvalue = (dftemp.sum() + account.cash) / self.capital_base

            # 除以初始值
            account.benchmarkvalue = historyindex / account.benchmarkvaluebase

            row = pd.DataFrame(
                [dict(date=account.current_date, fundvalue=account.fundvalue, benchmarkvalue=account.benchmarkvalue), ])
            account.dfFundvalue = account.dfFundvalue.append(row)

            print(datetime.datetime.strftime(account.current_date, '%Y-%m-%d') + '净值:' + str(account.fundvalue))

    @staticmethod
    def order_to(account, _stockcode, _referencenum, _price):
        """买卖操作"""
        if _stockcode in account.dfPosition.index:
            havenum = account.dfPosition.loc[_stockcode].values[0]
        else:
            havenum = 0

        if _referencenum >= havenum:
            _operatetype = '买入'
        else:
            _operatetype = '卖出'

        account.cash -= (_referencenum - havenum) * _price
        row = pd.DataFrame([dict(date=account.current_date, stockcode=_stockcode, operatetype=_operatetype,
                                 referencenum=(_referencenum - havenum), referenceprice=_price), ])

        account.dfOperate = account.dfOperate.append(row)

    def get_open_history(self, current_date, operatelist):
        """获取当日开盘价(前复权)"""
        selectdate = datetime.datetime.strftime(current_date, '%Y-%m-%d')
        selectstr = ""
        for stk in operatelist:
            selectstr = selectstr + '\'' + stk + '\','
        selectstr = selectstr[:-1]
        sql_tuple = self.ms.execquery(
            "SELECT code,[open]*adjust_price_f/[close] AS adjust_open_f FROM stock_data "
            "WHERE DATE = '" + selectdate + "' AND code IN (" + selectstr + ")")
        dfOperatelist = pd.DataFrame(sql_tuple, columns=('stockcode', 'adjust_open_f'))
        return dfOperatelist

    def get_stock_close(self, df, date):
        """获取股票池收盘价"""
        selectdate = datetime.datetime.strftime(date, '%Y-%m-%d')
        selectlist = ''
        for stk in df.index.values:
            selectlist = selectlist + '\'' + stk + '\','
        selectlist = selectlist[:-1]

        queryline = "SELECT code,adjust_price_f FROM stock_data WHERE date = '" + selectdate + \
            "' AND code IN ( " + selectlist + " )"
        # print queryline
        sql_tuple = self.ms.execquery( queryline )

        history = pd.DataFrame(sql_tuple, columns=['code', 'adjust_price_f'])
        return history

    def get_index_close(self, date):
        """获取指数收盘价"""
        selectdate = datetime.datetime.strftime(date, '%Y-%m-%d')
        indexvalue = self.ms.execquery(
            "SELECT [close] AS indexclose FROM index_data WHERE index_code = '" + self.benchmark +
            "' AND date = '" + selectdate + "'")
        return indexvalue[0][0]


# stockpool:总股票池
# benchmark:基准指数
# start:回测起始时间
# end:回测结束时间
# refresh_rate:调仓频率
# freq:调仓周期 d-日，m-月，y-年
# capital_base:初始资金
# chosen_num:持仓股票数量
Trade('ZZ800', 'sh000906', '2007-04-01', '2016-06-01', '2007-03-30', '2007-02-28', 1, 'm', 1000000, 10).backtest()


# 滑点和手续费还没有写入
# 增加可选计算周期
