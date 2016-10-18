# coding=utf-8

"""
filename:FactorRolling.py
Author：zyf
LastEditTime:2016/9/26
"""

import pandas as pd
import matplotlib.pyplot as plt
import datetime
import pymssql


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
        self.slippage = 0.003
        # 买入手续费（固定）
        self.buycommission = 0.0003
        # 卖出手续费（固定）
        self.sellcommission = 0.0013
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

        # 时间预处理
        startstr = datetime.datetime.strftime(self.start, '%Y-%m-%d')
        endstr = datetime.datetime.strftime(self.end, '%Y-%m-%d')
        eststr = datetime.datetime.strftime(self.datedict["est_date_now"], '%Y-%m-%d')

        # # 获取指数起始值
        # returnlist = self.ms.execquery(
        #     "SELECT TOP 1 [close] AS indexclose FROM index_data WHERE index_code = '" + self.benchmark +
        #     "' AND date < '" + startstr + "' ORDER BY date DESC")
        # account.benchmarkvaluebase = returnlist[0][0]
        #
        # # 使用指数起始值做初始化
        # account.benchmarkvalue = account.benchmarkvaluebase
        #
        # # 净值起始值设为1
        # row = pd.DataFrame(
        #     [dict(date=account.current_date, fundvalue=1, benchmarkvalue=1), ])
        # account.dfFundvalue = account.dfFundvalue.append(row)

        # 创建日历
        # sqlstr = "SELECT [date] FROM index_data WHERE [date]>='" + eststr + "' AND [date]<='" + endstr + "'"
        # calendars = self.ms.execquery(sqlstr)
        # for d in calendars:
        #     self.tradecalendars.append(d[0])

        sqlstr = "SELECT [date] FROM stock_data WHERE code = 'sz002252' and [date]>='" + startstr + "' AND [date]<='" + endstr + "'"
        calendars = self.ms.execquery(sqlstr)
        for d in calendars:
            self.tradecalendars.append(d[0])

        sqlstr = "SELECT (SELECT TOP 1 [date] FROM index_data b WHERE year(a.[date]) = year(b.[date]) AND " \
                 "month(a.[date]) = month(b.[date]) ORDER BY date ASC)  FROM index_data a WHERE [date]>= '" \
                 + startstr + "' AND [date] < '" + endstr + "' GROUP BY year([date]),month([date])"
        changecalendars = self.ms.execquery(sqlstr)
        for d in changecalendars:
            self.changecalendars.append(d[0])

        # 按日执行策略
        while account.current_date <= self.end:
            self.handle_date(account)
            account.current_date = account.current_date + datetime.timedelta(days=1)

        # 输出到csv
        # account.dfFundvalue.to_csv('fundvalue.csv', index=False, encoding='gbk')
        # account.dfPosition.to_csv('position.csv', encoding='gbk') #好像没什么意义

        # 绘制净值曲线
        # plt.plotfile('fundvalue.csv', ('date', 'fundvalue', 'benchmarkvalue'), subplots=False)
        # plt.show()

        # 打印各项参数
        # self.infoprinter()

    def handle_date(self, account):
        """每日调仓"""

        dfZig = pd.read_csv('sz000885dfZig.csv')
        dfStock = pd.read_csv('sz002252dfATR.csv').loc[:, ['code', 'date', 'close', 'preclose']]

        if account.current_date in self.tradecalendars:

            # 调仓
            # if account.current_date in self.changecalendars:
            if dfStock[dfStock['date'] == datetime.datetime.strftime(account.current_date, '%Y-%m-%d')].empty is False:
                if dfStock[dfStock['date'] == datetime.datetime.strftime(account.current_date, '%Y-%m-%d')]['preclose'].values[0] > max((dfZig[dfZig['date'] <= datetime.datetime.strftime(account.current_date, '%Y-%m-%d')]).sort_values(by='date', ascending=False).head(4)['price'].values):
                    # 获取价格买入全部
                    print(str(account.current_date) + ":buy price:" + str(dfStock[dfStock['date'] == datetime.datetime.strftime(account.current_date, '%Y-%m-%d')]['preclose'].values[0]))
                if dfStock[dfStock['date'] == datetime.datetime.strftime(account.current_date, '%Y-%m-%d')]['preclose'].values[0] < min((dfZig[dfZig['date'] <= datetime.datetime.strftime(account.current_date, '%Y-%m-%d')]).sort_values(by='date', ascending=False).head(2)['price'].values):
                    print(str(account.current_date) + ":sell price:" + str(dfStock[dfStock['date'] == datetime.datetime.strftime(account.current_date, '%Y-%m-%d')]['preclose'].values[0]))


            # if account.current_date in self.changecalendars:
            #     if (self.changecalendars.index(account.current_date)) != 0:
            #         self.datedict['est_date_now'] = self.tradecalendars[
            #             self.tradecalendars.index(account.current_date) + 1]
            #
            #     # 选股
            #     account.buylist = StockScreener(self.stockpool, self.chosen_num + 5,
            #                                     self.datedict).getbuylist()
            #
            #     # 合并成操作列表之后获取开盘价格
            #     positionlist = account.dfPosition.index.values
            #     operatelist = []
            #     for stk in positionlist:
            #         operatelist.append(stk)
            #     for stk in account.buylist:
            #         operatelist.append(stk)
            #
            #     # 获取所有需要交易的股票的前复权开盘价格
            #     dfOperatelist = self.get_open_history(account.current_date, operatelist)
            #
            #     print(dfOperatelist)
            #
            #     # 获取停牌的股票
            #     selectstr = ""
            #     for stk in account.dfPosition.index.values:
            #         if stk not in dfOperatelist['stockcode'].values:
            #             selectstr = selectstr + '\'' + stk + '\','
            #     selectstr = selectstr[:-1]
            #
            #     if selectstr == "":
            #         selectstr = "'1'"
            #
            #     selectdate = datetime.datetime.strftime(account.current_date, '%Y-%m-%d')
            #
            #     sqlstr = "SELECT code, (SELECT TOP 1 adjust_price_f FROM stock_data b WHERE b.[date] < '" + selectdate + \
            #              "' AND b.code = a.code ORDER BY[date] DESC) FROM stock_data a WHERE code IN (" + selectstr + \
            #              ") GROUP BY code"
            #
            #     sql_tuple = self.ms.execquery(sqlstr)
            #
            #     dfSuspendedstock = pd.DataFrame(sql_tuple, columns=('stockcode', 'adjust_price_f'))
            #     dfSuspendedstock = dfSuspendedstock.groupby('stockcode').sum()
            #     dfSuspendedstock = pd.merge(dfSuspendedstock, account.dfPosition, left_index='stockcode',
            #                                 right_index='stockcode')
            #     dfSuspendedstock['value'] = dfSuspendedstock['adjust_price_f'] * dfSuspendedstock['referencenum']
            #     suspendedvalue = dfSuspendedstock['value'].sum()
            #
            #     # 调仓
            #     for stk in positionlist:
            #         if stk not in account.buylist and stk not in dfSuspendedstock.index.values:
            #             dftemp = dfOperatelist[dfOperatelist['stockcode'] == stk]
            #             self.order_to(account, stk, 0, dftemp['adjust_open_f'].values[0])
            #
            #     # 帐号总资金
            #     allvalue = account.fundvalue * self.capital_base - suspendedvalue
            #
            #     eachvalue = allvalue / self.chosen_num
            #
            #     i = 0
            #
            #     for stk in account.buylist:
            #         if stk in dfOperatelist['stockcode'].values:
            #             if dfOperatelist[dfOperatelist['stockcode'] == stk].notnull().values[0][1]:
            #                 i += 1
            #                 if i > 10:
            #                     break
            #                 dftemp = dfOperatelist[dfOperatelist['stockcode'] == stk]
            #                 amount = int(eachvalue / dftemp['adjust_open_f'].values[0] / 100) * 100
            #                 self.order_to(account, stk, amount, dftemp['adjust_open_f'].values[0])
            #             else:
            #                 f = open('out.txt', "a")
            #                 f.write(str(stk) + '有退市风险:' + str(account.current_date) + '\n')
            #                 f.close()
            #
            #     # 持仓变化
            #     account.dfPosition = account.dfOperate.loc[:, ['stockcode', 'referencenum']]
            #     account.dfPosition = (account.dfPosition.groupby('stockcode').sum())
            #     account.dfPosition = account.dfPosition[account.dfPosition.referencenum > 0]
            #
            #     account.dfPosition.to_csv(
            #         'D:/Changelog/' + datetime.datetime.strftime(account.current_date, '%Y-%m-%d') + '-position.csv',
            #         encoding='gbk')
            #
            #     # 本期变更为上期
            #     self.datedict['est_date_pre'] = self.datedict['est_date_now']

            # 计算净值变化
            # historystock = self.get_stock_close(account.dfPosition, account.current_date)
            # historyindex = self.get_index_close(account.current_date)
            #
            # historystock = historystock.groupby('code').sum()
            # dftemp = pd.merge(account.dfPosition, historystock, left_index='stockcode', right_index='code')
            # dftemp['value'] = dftemp['referencenum'] * dftemp['adjust_price_f']
            # dftemp = dftemp['value']
            # account.fundvalue = (dftemp.sum() + account.cash) / self.capital_base
            #
            # account.benchmarkvalue = historyindex / account.benchmarkvaluebase
            #
            # row = pd.DataFrame(
            #     [dict(date=account.current_date, fundvalue=account.fundvalue, benchmarkvalue=account.benchmarkvalue), ])
            # account.dfFundvalue = account.dfFundvalue.append(row)
            #
            # print(datetime.datetime.strftime(account.current_date, '%Y-%m-%d') + '净值:' + str(account.fundvalue))

    def order_to(self, account, _stockcode, _referencenum, _price):
        """买卖操作"""
        if _stockcode in account.dfPosition.index:
            havenum = account.dfPosition.loc[_stockcode].values[0]
        else:
            havenum = 0

        if _referencenum >= havenum:
            _operatetype = '买入'
            account.cash = account.cash - (_referencenum - havenum) * _price - abs(
                (_referencenum - havenum) * _price) * (self.slippage + self.buycommission)
        else:
            _operatetype = '卖出'
            account.cash = account.cash - (_referencenum - havenum) * _price - abs(
                (_referencenum - havenum) * _price) * (self.slippage + self.sellcommission)

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
            "SELECT code,[open]*adjust_price_f/[close] AS adjust_open_f FROM stock_data WHERE date = '" + selectdate +
            "' AND code IN (" + selectstr + ")")
        dfOperatelist = pd.DataFrame(sql_tuple, columns=('stockcode', 'adjust_open_f'))

        return dfOperatelist

    def get_stock_close(self, df, date):
        """获取股票池收盘价(前复权)"""
        selectdate = datetime.datetime.strftime(date, '%Y-%m-%d')
        selectlist = ''
        for stk in df.index.values:
            selectlist = selectlist + '\'' + stk + '\','
        selectlist = selectlist[:-1]

        queryline = "SELECT code,adjust_price_f FROM stock_data WHERE date = '" + selectdate + \
                    "' AND code IN ( " + selectlist + " )"
        # print queryline
        sql_tuple = self.ms.execquery(queryline)

        history = pd.DataFrame(sql_tuple, columns=['code', 'adjust_price_f'])

        # 对停牌的价格做下处理
        suspension = list(set(df.index.values).difference(set(history['code'].values)))

        if len(suspension) != 0:
            for stk in suspension:
                queryline = "SELECT top 1 code,adjust_price_f FROM stock_data WHERE date < '" + selectdate + \
                    "' AND code = '" + stk + "' order by date DESC"
                sql_tuple = self.ms.execquery(queryline)
                dfOperatelistAdd = pd.DataFrame(sql_tuple, columns=('code', 'adjust_price_f'))
                history = pd.concat([history, dfOperatelistAdd])

        return history

    def get_index_close(self, date):
        """获取指数收盘价(前复权)"""
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
Trade('ZZ800', 'sh000906', '1999-10-20', '2016-06-01', '2007-03-30', '2007-02-28', 1, 'm', 1000000, 10).backtest()


# 滑点和手续费还没有写入
# 增加可选计算周期
