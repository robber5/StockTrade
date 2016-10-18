# coding=utf-8

"""
filename:FactorRolling.py
Author：zyf
LastEditTime:2016/9/26
"""

import pandas as pd
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
        self.initMACD = -10000.0
        self.histMACD = pd.DataFrame(self.initMACD, index=['000001.ZICN'], columns=['preShortEMA', 'preLongEMA', 'preDIF', 'preDEA'])
        self.shortWin = 12  # 短期EMA平滑天数
        self.longWin = 26  # 长期EMA平滑天数
        self.macdWin = 9  # DEA线平滑天数
        self.days = 0

        sql_tuple = self.ms.execquery(
            "SELECT index_code,[date],[close] FROM index_data WHERE index_code = 'sh000001' order by date")
        self.dfIndex = pd.DataFrame(sql_tuple,
                             columns=('index_code', 'date', 'close'))
        sql_tuple = self.ms.execquery(
            "SELECT code,[date],[open]*[adjust_price_f]/[close] as [open],[adjust_price_f] as [close] FROM stock_data WHERE code = 'sz000885' order by date")
        self.dfStock = pd.DataFrame(sql_tuple,
                                    columns=('stock_code', 'date', 'open', 'close'))

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

        sqlstr = "SELECT [date] FROM index_data WHERE index_code = 'sh000001' and [date]>='" + startstr + "' AND [date]<='" + endstr + "'"
        calendars = self.ms.execquery(sqlstr)
        for d in calendars:
            self.tradecalendars.append(d[0])

        # 按日执行策略
        while account.current_date <= self.end:
            self.handle_date(account)
            account.current_date = account.current_date + datetime.timedelta(days=1)

    def handle_date(self, account):
        """每日调仓"""
        if account.current_date in self.tradecalendars:
            self.days += 1
            prices = self.dfIndex[self.dfIndex['date'] == datetime.datetime.strftime(account.current_date, '%Y-%m-%d')]['close'].values[0]

            preShortEMA = self.histMACD.at['000001.ZICN', 'preShortEMA']
            preLongEMA = self.histMACD.at['000001.ZICN', 'preLongEMA']
            preDIF = self.histMACD.at['000001.ZICN', 'preDIF']
            preDEA = self.histMACD.at['000001.ZICN', 'preDEA']
            if preShortEMA == self.initMACD or preLongEMA == self.initMACD:
                self.histMACD.at['000001.ZICN', 'preShortEMA'] = prices
                self.histMACD.at['000001.ZICN', 'preLongEMA'] = prices
                self.histMACD.at['000001.ZICN', 'preDIF'] = 0
                self.histMACD.at['000001.ZICN', 'preDEA'] = 0
                return

            shortEMA = preShortEMA * 1.0 * (self.shortWin - 1) / (self.shortWin + 1) + prices * 2.0 / (self.shortWin + 1)
            longEMA = preLongEMA * 1.0 * (self.longWin - 1) / (self.longWin + 1) + prices * 2.0 / (self.longWin + 1)
            DIF = shortEMA - longEMA
            DEA = preDEA * 1.0 * (self.macdWin - 1) / (self.macdWin + 1) + DIF * 2.0 / (self.macdWin + 1)

            self.histMACD.at['000001.ZICN', 'preShortEMA'] = shortEMA
            self.histMACD.at['000001.ZICN', 'preLongEMA'] = longEMA
            self.histMACD.at['000001.ZICN', 'preDIF'] = DIF
            self.histMACD.at['000001.ZICN', 'preDEA'] = DEA

            if self.days + 1 > self.longWin and self.days % 1 == 0:
                if preDIF > preDEA and DIF < DEA and self.dfStock[self.dfStock['date'] == datetime.datetime.strftime(account.current_date, '%Y-%m-%d')].empty is False:
                    sellprice = self.dfStock[self.dfStock['date'] == datetime.datetime.strftime(account.current_date, '%Y-%m-%d')]['open'].values[0]
                    print('卖出:at' + str(account.current_date)+ '价格' + str(sellprice))
                if preDIF < preDEA and DIF > DEA and self.dfStock[self.dfStock['date'] == datetime.datetime.strftime(account.current_date, '%Y-%m-%d')].empty is False:
                    buyprice = self.dfStock[self.dfStock['date'] == datetime.datetime.strftime(account.current_date, '%Y-%m-%d')]['open'].values[0]
                    # per_cash = account.cash
                    # per_amount = per_cash / PrePrices[-1]
                    # amount = int(per_amount / 100) * 100
                    print('买入:at' + str(account.current_date)+ '价格' + str(buyprice))

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
Trade('ZZ800', 'sh000906', '1992-10-20', '2016-06-01', '2007-03-30', '2007-02-28', 1, 'm', 1000000, 10).backtest()


# 滑点和手续费还没有写入
# 增加可选计算周期
