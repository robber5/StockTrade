# coding=utf-8

"""
filename:FactorRolling.py
Author：zyf
LastEditTime:2016/9/26
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

df = pd.read_csv('trade.csv')

ms = MSSQL(host="USER-GH61M1ULCU", user="sa", pwd="windows-999", db="stocks")

sql_tuple = ms.execquery(
    "SELECT index_code,[date],[close] FROM index_data WHERE index_code = 'sh000001' order by date")

dfIndex = pd.DataFrame(sql_tuple, columns=('index_code', 'date', 'close'))

sql_tuple = ms.execquery(
    "SELECT code,[date],[adjust_price_f] as [close] FROM stock_data WHERE code = 'sz000885' order by date")

dfStock = pd.DataFrame(sql_tuple, columns=('stock_code', 'date', 'close'))

cash = 1000000
amount = 0

f = open("test.txt", "a")
f.write("时间,cash")

for day in dfIndex['date'].values:
    if df[df['date'] == str(day)[0:10]].empty is False:
        dfday = df[df['date'] == str(day)[0:10]]
        if dfday['Type'].values[0] == 'Buy':
            amount = int(cash / dfday['price'].values[0] / 100) * 100
            cash -= amount * dfday['price'].values[0]
        if dfday['Type'].values[0] == 'Sell':
            cash += amount * dfday['price'].values[0]
            amount = 0
    if dfStock[dfStock['date'] == day].empty is False:
        print(str(day)[0:10] + "," + str(cash + amount * dfStock[dfStock['date'] == day]['close'].values[0]))
        f.write(str(day)[0:10] + "," + str(cash + amount * dfStock[dfStock['date'] == day]['close'].values[0]))

f.close()




