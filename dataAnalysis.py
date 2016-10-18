# coding=utf-8

"""
filename:dataAnalysis
Author：zyf
LastEditTime:2016/9/29

window=25  是参数
code = 'sz000885'  是参数
核心是算出ATR
"""

import pandas as pd
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

sql_tuple = MSSQL(host="USER-GH61M1ULCU", user="sa", pwd="windows-999", db="stocks").execquery(
    "SELECT code,[date],[high],[low],[close],[adjust_price_f] FROM stock_data WHERE code = 'sz002252' order by date")
dfATR = pd.DataFrame(sql_tuple, columns=('code', 'date', 'real_high', 'real_low', 'real_close', 'adjust_open_f'))

dfATR['high'] = dfATR['adjust_open_f'] / dfATR['real_close'] * dfATR['real_high']

dfATR['low'] = dfATR['adjust_open_f'] / dfATR['real_close'] * dfATR['real_low']

dfATR['close'] = dfATR['adjust_open_f']

dfATR['High_Low'] = abs(dfATR['high'] - dfATR['low'])

dfATR['preclose'] = dfATR['close'].rolling(window=2, center=False).sum() - dfATR['close']

dfATR['High_preClose'] = abs(dfATR['high'] - dfATR['preclose'])

dfATR['PreClose_Low'] = abs(dfATR['preclose'] - dfATR['low'])

dfATR = dfATR.fillna(value=0)

dfTemp = dfATR.loc[:, ['code', 'date', 'High_Low', 'High_preClose', 'PreClose_Low']]

dfATR['TR'] = dfTemp.max(1)

dfATR['ATR_' + str(25)] = dfATR['TR'].rolling(window=25, center=False).mean()

dfATR = dfATR.loc[:, ['code', 'date', 'high', 'low', 'close', 'preclose', 'ATR_25']]

dfATR.to_csv('sz002252dfATR.csv', index=False, encoding='gbk')

dfZig = pd.DataFrame(columns=('stock', 'date', 'price', 'type'))

code = dfATR.head(1)['code'].values[0]

zigHighPrice = dfATR.head(1)['high'].values[0]

zigHighPosition = dfATR.head(1)['date'].values[0]

zigLowPrice = dfATR.head(1)['low'].values[0]

zigLowPosition = dfATR.head(1)['date'].values[0]

bLastHigh = False

for date in dfATR['date'].values:
    dfTemp = dfATR[dfATR['date'] == date]
    nowPrice = dfTemp['close'].values[0]
    nowATR = dfTemp['ATR_25'].values[0]
    nowPosition = dfTemp['date'].values[0]

    if nowPrice >= zigHighPrice:
        print(str(nowPosition) + ':更新最高点')
        zigHighPrice = nowPrice
        zigHighPosition = nowPosition
        continue
    if zigHighPrice - 2 * nowATR > nowPrice and bLastHigh is not True:
        print(str(nowPosition) + ':记录一个新高点,同时刷新一个低点')
        row = pd.DataFrame([dict(stock=code, date=zigHighPosition, price=zigHighPrice, type="high"), ])
        dfZig = dfZig.append(row)
        zigLowPrice = nowPrice
        zigLowPosition = nowPosition
        bLastHigh = True
        continue
    if nowPrice <= zigLowPrice:
        print(str(nowPosition) + ':更新最低点')
        zigLowPrice = nowPrice
        zigLowPosition = nowPosition
        continue
    if nowPrice > zigLowPrice + 2 * nowATR and bLastHigh is not False:
        print(str(nowPosition) + ':记录一个新低点,同时刷新一个高点')
        row = pd.DataFrame([dict(stock=code, date=zigLowPosition, price=zigLowPrice, type="low"), ])
        dfZig = dfZig.append(row)
        zigHighPrice = nowPrice
        zigHighPosition = nowPosition
        bLastHigh = False
        continue

    dfZig.to_csv(code + 'dfZig.csv', index=False, encoding='gbk')
